# 阅读说明：业务数据库访问与备份：反射现有器械表、只读检查结构、先备份后事务写入。
# 普通任务调用import_csv；daily账本类LedgerDatabase继承此类复用去重、派生字段和备份。
# 这里不自动创建器械表；完整registration_no_original唯一索引是去重的最后保障。
# 本文件只增加解释注释，业务代码保持原样。

import csv
import datetime
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import tempfile
from pathlib import Path

from sqlalchemy import Date, DateTime, JSON, MetaData, Table, create_engine, event, inspect, select
from sqlalchemy.exc import IntegrityError
from export_medical_device import FIELDS, atomic, clean, date, writecsv
from api.collection_rules import derive_fields, eligibility


class DeviceDatabase:
    """数据库服务：检查原表、生成可校验备份，并在事务中插入新增完整证号。"""
    # settings含目标URL和器械表名；反射真实表并验证结构。autoload读取已有表，不会CREATE TABLE。
    def __init__(self, settings):
        """初始化本对象的依赖与资源；实际职责见类说明及参数。"""
        if not settings.database_url:
            raise RuntimeError('未配置 NMPA_DATABASE_URL')
        if not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*', settings.table):
            raise ValueError('非法表名')
        self.settings = settings
        self.engine = create_engine(settings.database_url, pool_pre_ping=True, hide_parameters=True)
        if self.engine.dialect.name == 'sqlite':
            # Ensure SAVEPOINT release cannot commit outside the outer transaction.
            # 仅为SQLite离线测试设置手工事务；正式MySQL连接不进入此函数。
            @event.listens_for(self.engine, 'connect')
            def sqlite_connect(connection, record):
                """设置 SQLite 测试连接的事务行为；正式 MySQL 不使用本分支。"""
                connection.isolation_level = None
            # SQLite测试显式BEGIN，防止嵌套SAVEPOINT释放时绕开外层事务的回滚验证。
            @event.listens_for(self.engine, 'begin')
            def sqlite_begin(connection):
                """明确启动 SQLite 测试事务，保证回滚验证与正式流程一致。"""
                connection.exec_driver_sql('BEGIN')
        if self.engine.dialect.name not in ('mysql', 'sqlite'):
            raise ValueError('仅支持 MySQL；SQLite 用于本地验证')
        # 这里只从已有数据库读取表定义，不创建器械表；字段或索引不符合约束会在下一步报错。
        self.table = Table(settings.table, MetaData(), autoload_with=self.engine)
        self.check_schema()

    # dispose释放连接池；调用方应在finally执行，避免异常后遗留数据库资源。
    def close(self):
        """关闭持有的连接或资源，避免运行结束后仍占用文件与数据库。"""
        self.engine.dispose()

    # 核对22字段、可空body_part、字段长度、完整证号唯一键和InnoDB；只检查，不自动修表。
    def check_schema(self):
        """只读检查表字段、InnoDB 引擎及完整证号唯一索引。"""
        missing = set(FIELDS) - set(self.table.c.keys())
        if missing:
            raise ValueError('原表缺少字段：' + ', '.join(sorted(missing)))
        if not self.table.c.body_part.nullable:
            raise ValueError('body_part必须允许NULL；请先执行313增量迁移')
        if self.engine.dialect.name == 'mysql':
            size = getattr(self.table.c.production_address.type, 'length', None)
            if size is not None and size < 2048:
                raise ValueError('production_address需至少2048字符')
        inspector = inspect(self.engine)
        unique = inspector.get_unique_constraints(self.settings.table)
        unique += [i for i in inspector.get_indexes(self.settings.table) if i.get('unique')]
        unique += [inspector.get_pk_constraint(self.settings.table)]
        # 必须是完整证号单列唯一索引；复合唯一键或前缀唯一索引不足以保证当前去重语义。
        if not any(i.get('column_names') == ['registration_no_original'] and
                   not i.get('dialect_options', {}).get('mysql_length') for i in unique):
            raise ValueError('原表必须有完整 registration_no_original 的单列唯一索引；程序不会自动改表')
        if self.engine.dialect.name == 'mysql':
            options = inspector.get_table_options(self.settings.table)
            if options.get('mysql_engine', '').lower() != 'innodb':
                raise ValueError('原表必须使用 InnoDB 才能保证备份和事务回滚')

    # 按job_id生成备份及SHA-256清单，先写.tmp后替换；只有非空且校验可用才返回备份信息。
    def backup(self, job_id):
        """先备份原表并生成校验信息；备份失败时不进入入库阶段。"""
        directory = self.settings.data_dir / 'backups'
        directory.mkdir(parents=True, exist_ok=True)
        extension = '.sql' if self.engine.dialect.name == 'mysql' else '.sqlite3'
        target = directory / (job_id + extension)
        temp = directory / (job_id + extension + '.tmp')
        try:
            if self.engine.dialect.name == 'sqlite':
                database = self.engine.url.database
                if not database or database == ':memory:':
                    raise ValueError('备份要求文件型 SQLite，禁止使用内存库')
                with sqlite3.connect(database) as source, sqlite3.connect(temp) as destination:
                    source.backup(destination)
                    if destination.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                        raise RuntimeError('备份完整性检查失败')
            else:
                self._mysql_dump(temp)
            if temp.stat().st_size == 0:
                raise RuntimeError('备份文件为空')
            temp.chmod(0o600)
            # Windows 的 fsync/_commit 要求可写句柄；rb 可能报 OSError。
            # r+b 不截断文件，保持备份内容不变，同时满足 Windows/Linux 的同步要求。
            with temp.open('r+b') as stream:
                os.fsync(stream.fileno())
            # 备份完整写出并同步后才把临时文件替换成正式文件，再生成长度/摘要清单。
            temp.replace(target)
            manifest = {'id': job_id, 'file': target.name, 'table': self.settings.table,
                        'bytes': target.stat().st_size, 'sha256': file_hash(target),
                        'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat()}
            atomic(directory / (job_id + '.json'), json.dumps(manifest, ensure_ascii=False, indent=2))
            return manifest
        finally:
            temp.unlink(missing_ok=True)

    # 用权限受限的临时client.cnf传凭据，执行mysqldump；检查退出码和目标表结构，失败阻止入库。
    def _mysql_dump(self, target):
        """用临时凭据文件调用 mysqldump，避免把密码放在命令行参数中。"""
        url = self.engine.url
        # 仅负责MySQL客户端配置值的引号/换行转义；不是SQL字符串拼接函数。
        def quote(value):
            """转义 MySQL 客户端配置文件中的特殊字符。"""
            return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r') + '"'
        if not url.database:
            raise ValueError('连接 URL 必须指定数据库')
        values = {'user': url.username or '', 'password': url.password or '',
                  'host': url.host or 'localhost', 'port': str(url.port or 3306),
                  'default-character-set': url.query.get('charset', 'utf8mb4')}
        for option in ('ssl_ca', 'ssl_cert', 'ssl_key'):
            if option in url.query:
                values[option.replace('_', '-')] = url.query[option]
        # Password is kept out of argv, API responses, and logs.
        with tempfile.TemporaryDirectory(prefix='nmpa-backup-') as secret_dir:
            config = Path(secret_dir) / 'client.cnf'
            config.write_text('[client]\n' + ''.join(f'{k}={quote(v)}\n' for k, v in values.items()), encoding='utf-8')
            config.chmod(0o600)
            with target.open('wb') as stream:
                try:
                    result = subprocess.run([self.settings.mysqldump, '--defaults-extra-file=' + str(config),
                        '--single-transaction', '--quick', '--skip-lock-tables', '--no-tablespaces',
                        '--skip-add-drop-table', '--set-gtid-purged=OFF', '--', url.database,
                        *getattr(self, 'backup_tables', [self.settings.table])],
                        stdout=stream, stderr=subprocess.PIPE, timeout=180)
                except OSError as exc:
                    # 只展示系统错误码，不泄露进程参数、配置内容或密码。
                    code = getattr(exc, 'winerror', None)
                    label = f'WinError={code}' if code is not None else f'errno={exc.errno}'
                    raise RuntimeError(f'无法启动 mysqldump（{label}）；请检查是否指向真实的 mysqldump.exe 文件及其运行权限，未执行入库') from None
            if result.returncode:
                raise RuntimeError('mysqldump 备份失败；检查备份工具、数据库权限和连接配置，未执行入库')
        found_schema = False
        with target.open('rb') as stream:
            for line in stream:
                if ('CREATE TABLE `' + self.settings.table + '`').encode() in line:
                    found_schema = True
                    break
        if not found_schema:
            raise RuntimeError('备份缺少目标表结构，未执行入库')

    # 按清单核对文件长度与摘要；备份缺失、变动或损坏都会抛错，不能继续写库。
    def verify_backup(self, backup):
        """核对备份文件及哈希，拒绝使用丢失或被篡改的备份。"""
        target = self.settings.data_dir / 'backups' / backup['file']
        if target.stat().st_size != backup['bytes'] or file_hash(target) != backup['sha256']:
            raise RuntimeError('备份文件校验失败，拒绝写入')

    # 将标准字符串转换为数据库Date/DateTime/JSON；未匹配膝髋的空body_part写SQL NULL。
    def sql_row(self, row):
        """把标准 CSV 字段转为 SQL 可写入的字段和值。"""
        converted = dict(row)
        for name in FIELDS:
            kind = self.table.c[name].type
            if isinstance(kind, DateTime):
                converted[name] = datetime.datetime.fromisoformat(row[name]) if row[name] else None
            elif isinstance(kind, Date):
                converted[name] = datetime.date.fromisoformat(row[name]) if row[name] else None
            elif isinstance(kind, JSON):
                converted[name] = json.loads(row[name]) if row[name] else None
        if not converted.get('body_part'):
            converted['body_part'] = None
        return converted

    # 用完整证号精确查询，MySQL加FOR UPDATE行锁；不会用纯数字registration_no合并不同完整证号。
    def existing_row(self, connection, number):
        """按完整注册证号查询并锁定已有记录；不依据纯数字证号合并。"""
        statement = select(self.table).where(self.table.c.registration_no_original == number)
        if self.engine.dialect.name == 'mysql':
            statement = statement.with_for_update()
        result = connection.execute(statement).mappings().first()
        return dict(result) if result is not None else None

    # 同证号已有记录保留原业务文本，只按保留文本重算body_part和collection_tag，并返回实际值及更新计数。
    def refresh_existing_derived_fields(self, connection, existing):
        """Recalculate from the business text we retain; never union old keyword tags."""
        if eligibility(existing)[0] != 'eligible':
            raise ValueError('已有记录的数字证号与313规则不一致，请人工核对')
        derived = derive_fields(existing)
        expected = {'body_part': derived['body_part'] or None, 'collection_tag': derived['collection_tag']}
        updates = {key: value for key, value in expected.items() if existing.get(key) != value}
        if updates:
            result = connection.execute(self.table.update().where(
                self.table.c.id == existing['id'],
                self.table.c.registration_no_original == existing['registration_no_original']
            ).values(**updates))
            if result.rowcount != 1:
                raise RuntimeError('派生字段更新未命中唯一业务记录，事务已中止')
        return dict(existing, **expected), {key: int(key in updates) for key in expected}

    # 读取已保存SQL行的22字段并转换成JSON可写值；用于账本记录实际保留内容，而非未被采用的新候选。
    @staticmethod
    def business_snapshot(row):
        """Capture the actual retained SQL row, not a different incoming candidate."""
        result = {key: row.get(key) for key in FIELDS}
        for key, value in result.items():
            if isinstance(value, (datetime.date, datetime.datetime)):
                result[key] = value.isoformat()
            elif isinstance(value, (dict, list)):
                result[key] = json.dumps(value, ensure_ascii=False)
        return result

    # 比较库中实际业务值与新候选，输出before/after差异；报告变化不代表覆盖了这些业务字段。
    @staticmethod
    def changed_fields(old, row):
        """比较已有记录与本次采集字段，生成变化报告而不覆盖旧记录。"""
        changes = {}
        for name in FIELDS:
            value = old[name]
            if isinstance(value, (list, dict)):
                value = json.dumps(value, ensure_ascii=False)
            elif isinstance(value, (datetime.date, datetime.datetime)):
                value = value.isoformat()
            before, after = clean(value), clean(row[name])
            if name in ('approval_date', 'effective_date', 'expiry_date'):
                before = date(before)
            if name == 'source_file':
                try:
                    if set(json.loads(before)) == set(json.loads(after)):
                        continue
                except (ValueError, TypeError):
                    pass
            if before != after:
                changes[name] = {'before': before, 'after': after}
        return changes

    # 普通任务入库：全量校验CSV和备份后开启一个事务，新增或跳过同证号，最后写入库/变化报告。
    def import_csv(self, path, backup, output):
        """先校验备份和CSV；同号按库中业务文本重算派生字段，新号插入，其他业务字段不覆盖。"""
        self.verify_backup(backup)
        rows = []
        from export_medical_device import validate
        with Path(path).open(encoding='utf-8-sig', newline='') as stream:
            reader = csv.DictReader(stream)
            if reader.fieldnames != FIELDS:
                raise ValueError('CSV 字段或顺序不匹配')
            seen = set()
            for row in reader:
                if None in row or any(v is None for v in row.values()):
                    raise ValueError('CSV 列数错误')
                errors = validate(row)
                if errors or row['registration_no_original'] in seen:
                    raise ValueError('入库候选未通过校验或包含重复完整证号')
                seen.add(row['registration_no_original'])
                rows.append(row)
        inserted, duplicate, changed = [], [], []
        body_part_updated = collection_tag_updated = derived_updated = 0
        details = []
        # 事务边界：正常离开with提交，任意未被处理的异常回滚本批；文件报告生成在提交之后。
        with self.engine.begin() as connection:
            for row in rows:
                existing = self.existing_row(connection, row['registration_no_original'])
                if existing is None:
                    try:
                        # 嵌套SAVEPOINT只隔离可能的唯一键冲突；其他约束错误不能作为重复证号静默跳过。
                        with connection.begin_nested():
                            connection.execute(self.table.insert().values(**self.sql_row(row)))
                    except IntegrityError as exc:
                        # Only a concurrent duplicate certificate may be skipped.
                        code = getattr(exc.orig, 'sqlite_errorcode', None) if self.engine.dialect.name == 'sqlite' else exc.orig.args[0]
                        if code not in (1062, 1555, 2067):
                            raise
                        existing = self.existing_row(connection, row['registration_no_original'])
                        if existing is None:
                            raise  # An unrelated constraint must roll back the entire batch.
                    else:
                        inserted.append(row)
                        continue
                duplicate.append(row)
                # 同号保留已有产品业务文本，仅补算派生字段；changed报告记录差异，不等于执行全字段更新。
                actual, updated = self.refresh_existing_derived_fields(connection, existing)
                body_part_updated += updated['body_part']
                collection_tag_updated += updated['collection_tag']
                derived_updated += int(any(updated.values()))
                changes = self.changed_fields(actual, row)
                if changes:
                    changed.append(row)
                    details.append({'registration_no_original': row['registration_no_original'], 'fields': changes})
        output = Path(output)
        writecsv(output / 'inserted.csv', FIELDS, inserted)
        writecsv(output / 'existing_changed.csv', FIELDS, changed)
        atomic(output / 'database_changes.json', json.dumps(details, ensure_ascii=False, indent=2))
        return {'inserted': len(inserted), 'skipped_existing': len(duplicate),
                'changed_existing': len(changed), 'body_part_updated': body_part_updated, 'collection_tag_updated': collection_tag_updated, 'derived_updated': derived_updated,
                'backup_id': backup['id']}


# 流式读取文件计算SHA-256，避免一次把大备份载入内存；用于验证备份和候选完整性。
def file_hash(path):
    """计算文件 SHA-256，确认备份或排队后的 CSV 未被修改。"""
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()
