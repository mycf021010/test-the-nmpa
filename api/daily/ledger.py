"""Business imports and their ledger commit in the same target-database transaction."""
from pathlib import Path

from sqlalchemy import BigInteger, Column, Integer, JSON, MetaData, String, Table, UniqueConstraint, select
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

from api.database import DeviceDatabase
from export_medical_device import FIELDS, atomic, validate, writecsv
from .contracts import DailyError as ControlError
from .store import digest, dumps, now


def ledger_schema():
    """定义与业务写入同事务提交的批次、明细和来源关联表，不自动执行DDL。"""
    metadata = MetaData()
    batch = Table('t_nmpa_daily_import_batch', metadata,
                  Column('id', String(32), primary_key=True),
                  Column('source_instance_id', String(32), nullable=False),
                  Column('operation_id', String(32), nullable=False),
                  Column('snapshot_hash', String(64), nullable=False),
                  Column('target_dataset_id', String(64), nullable=False),
                  Column('backup_json', JSON, nullable=False), Column('result_json', JSON, nullable=False),
                  Column('committed_at', String(40), nullable=False),
                  UniqueConstraint('source_instance_id', 'operation_id', name='uk_nmpa_import_operation'),
                  mysql_engine='InnoDB', mysql_charset='utf8mb4')
    item = Table('t_nmpa_daily_import_item', metadata,
                 Column('import_id', String(32), primary_key=True), Column('ordinal', Integer, primary_key=True),
                 Column('registration_no_original', String(128), nullable=False),
                 Column('business_id', BigInteger), Column('result', String(24), nullable=False),
                 Column('changed', Integer, nullable=False),
                 Column('changed_fields', JSON, nullable=False), Column('row_json', JSON, nullable=False),
                 mysql_engine='InnoDB', mysql_charset='utf8mb4')
    matches = Table('t_nmpa_daily_record_match', metadata,
                    Column('id', String(64), primary_key=True), Column('business_id', BigInteger, nullable=False),
                    Column('registration_no_original', String(128), nullable=False),
                    Column('source_instance_id', String(32), nullable=False), Column('category', String(32), nullable=False),
                    Column('query_keyword', String(40), nullable=False), Column('source_record_id', String(255), nullable=False),
                    Column('first_seen_at', String(40), nullable=False),
                    mysql_engine='InnoDB', mysql_charset='utf8mb4')
    return metadata, batch, item, matches


def target_identity(settings):
    """计算不含密码的目标数据集身份，防止旧候选被重试到新库。"""
    if not settings.database_url:
        return digest(['unconfigured', settings.table])
    url = make_url(settings.database_url)
    name = url.get_backend_name()
    database = str(Path(url.database).resolve()) if name == 'sqlite' and url.database else url.database
    return digest([name, url.host, url.port or (3306 if name == 'mysql' else None), database, settings.table])


class LedgerDatabase(DeviceDatabase):
    def __init__(self, settings):
        """检查目标表及账本字段、InnoDB和幂等唯一约束，并指定四表备份范围。"""
        super().__init__(settings)
        self.ledger_metadata, self.batches, self.items, self.matches = ledger_schema()
        from sqlalchemy import inspect
        try:
            inspector = inspect(self.engine)
            for table in (self.batches, self.items, self.matches):
                if not inspector.has_table(table.name):
                    raise ControlError('LEDGER_MIGRATION_REQUIRED', '请先在目标测试数据库执行A方案入库账本迁移', 503)
                columns = {column['name'] for column in inspector.get_columns(table.name)}
                if not set(table.c.keys()).issubset(columns):
                    raise ControlError('LEDGER_SCHEMA_MISMATCH', '目标库的A方案账本字段版本不符，请核对迁移脚本', 503)
                if self.engine.dialect.name == 'mysql' and inspector.get_table_options(table.name).get('mysql_engine', '').lower() != 'innodb':
                    raise ControlError('LEDGER_ENGINE_INVALID', '入库账本和关联表必须使用InnoDB', 503)
            unique = inspector.get_unique_constraints(self.batches.name) + inspector.get_indexes(self.batches.name)
            if not any(set(index.get('column_names', [])) == {'source_instance_id', 'operation_id'}
                       and index.get('unique', True) for index in unique):
                raise ControlError('LEDGER_UNIQUE_REQUIRED', '入库账本缺少操作唯一约束，禁止写入', 503)
        except BaseException:
            self.close()
            raise
        self.backup_tables = [settings.table, self.batches.name, self.items.name, self.matches.name]

    def migrate(self):
        """拒绝运行账号自动DDL，要求管理员使用显式迁移入口。"""
        raise RuntimeError('运行账号不自动迁移业务库，请由管理员执行 migrate_daily_mysql.py')

    def lookup(self, source_instance_id, operation_id):
        """按源实例和原操作编号查询提交证据，用于COMMIT响应丢失核对。"""
        with self.engine.connect() as connection:
            row = connection.execute(select(self.batches).where(
                self.batches.c.source_instance_id == source_instance_id,
                self.batches.c.operation_id == operation_id)).mappings().first()
            return dict(row) if row else None

    def import_snapshot(self, job, snapshot, backup):
        """校验固定候选并在同一事务新增业务、补全部位、记录明细与提交证据。"""
        self.verify_backup(backup)
        payload = snapshot['payload']
        if digest(payload) != snapshot['hash'] or payload['target_dataset_id'] != target_identity(self.settings):
            raise ControlError('SNAPSHOT_TARGET_MISMATCH', '快照或目标数据库已变化，禁止写入')
        members = payload['members']
        for member in members:
            row = member['row']
            if set(row) != set(FIELDS) or row['body_part'] not in ('膝', '髋', '膝，髋', '未分类'):
                raise ControlError('SNAPSHOT_INVALID', '候选字段不符合A方案契约')
            errors = validate(dict(row, body_part='膝') if row['body_part'] == '未分类' else row)
            if errors:
                raise ControlError('SNAPSHOT_INVALID', '快照中的候选未通过字段校验')
        if len({member['row']['registration_no_original'] for member in members}) != len(members):
            raise ControlError('SNAPSHOT_INVALID', '快照包含重复完整证号')
        result = {'inserted': 0, 'skipped_existing': 0, 'changed_existing': 0, 'body_part_updated': 0, 'backup_id': backup['id'], 'import_id': job['id']}
        with self.engine.begin() as connection:
            # The unique reservation and final ledger are invisible until this transaction commits.
            connection.execute(self.batches.insert().values(id=job['id'], source_instance_id=payload['source_instance_id'],
                operation_id=job['operation_id'], snapshot_hash=snapshot['hash'], target_dataset_id=payload['target_dataset_id'],
                backup_json=backup, result_json={}, committed_at=now()))
            for ordinal, member in enumerate(members):
                row = member['row']
                existing = self.existing_row(connection, row['registration_no_original'])
                changed = {}
                if existing is None:
                    try:
                        with connection.begin_nested():
                            inserted = connection.execute(self.table.insert().values(**self.sql_row(row)))
                            business_id = inserted.inserted_primary_key[0]
                    except IntegrityError as exc:
                        code = getattr(exc.orig, 'sqlite_errorcode', None) if self.engine.dialect.name == 'sqlite' else exc.orig.args[0]
                        if code not in (1062, 1555, 2067):
                            raise
                        existing = self.existing_row(connection, row['registration_no_original'])
                        if existing is None:
                            raise
                if existing is not None:
                    status, business_id = 'skipped_existing', existing['id']
                    merged_part, updated = self.merge_existing_body_part(connection, existing, row['body_part'])
                    result['body_part_updated'] += int(updated)
                    changed = self.changed_fields(existing, dict(row, body_part=merged_part))
                    result['skipped_existing'] += 1
                    result['changed_existing'] += int(bool(changed))
                else:
                    status = 'inserted'
                    result['inserted'] += 1
                connection.execute(self.items.insert().values(import_id=job['id'], ordinal=ordinal,
                    registration_no_original=row['registration_no_original'], business_id=business_id,
                    result=status, changed=int(bool(changed)), changed_fields=changed, row_json=row))
                for match in member['matches']:
                    identity = digest([business_id, payload['source_instance_id'], match])
                    if connection.execute(select(self.matches.c.id).where(self.matches.c.id == identity)).first() is None:
                        connection.execute(self.matches.insert().values(id=identity, business_id=business_id,
                            registration_no_original=row['registration_no_original'], source_instance_id=payload['source_instance_id'],
                            category=match['category'], query_keyword=match['keyword'], source_record_id=match['source_id'], first_seen_at=now()))
            connection.execute(self.batches.update().where(self.batches.c.id == job['id']).values(result_json=result, committed_at=now()))
        return result

    def record_page(self, import_id, result=None, changed=None, offset=0, limit=20):
        """分页读取目标库真实提交明细，不用候选数代替新增数。"""
        from sqlalchemy import func
        statement = select(self.items).where(self.items.c.import_id == import_id)
        if result:
            statement = statement.where(self.items.c.result == result)
        if changed is not None:
            statement = statement.where(self.items.c.changed == int(changed))
        with self.engine.connect() as connection:
            total = connection.execute(select(func.count()).select_from(statement.subquery())).scalar_one()
            rows = [dict(row) for row in connection.execute(statement.order_by(self.items.c.ordinal).limit(limit).offset(offset)).mappings()]
        return {'items': rows, 'total': total}

    def rebuild_reports(self, import_id, directory):
        """从事务账本重建可丢弃报告文件，不把文件当作提交证明。"""
        records = self.record_page(import_id, limit=10**9)['items']
        directory.mkdir(parents=True, exist_ok=True)
        writecsv(directory / 'inserted.csv', FIELDS, [r['row_json'] for r in records if r['result'] == 'inserted'])
        atomic(directory / 'items.json', dumps(records))

