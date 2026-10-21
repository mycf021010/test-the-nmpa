# 阅读说明：daily的MySQL事务账本实现。器械新增、批次、逐条结果和来源关联在同一个事务内提交。
# 本模块定义三张辅助表，但正常构造只检查已有表；建表由migrate_daily_mysql.py显式执行。
# 如果以后只保留器械表，需要重写账本核对/恢复策略，不能直接删除三张表或注释插入语句。
# 本文件只增加解释注释，业务代码保持原样。

"""Business imports and their ledger commit in the same target-database transaction."""
from pathlib import Path

from sqlalchemy import BigInteger, Column, Integer, JSON, MetaData, String, Table, UniqueConstraint, select
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

from api.database import DeviceDatabase
from export_medical_device import FIELDS, atomic, validate, writecsv
from .contracts import DailyError as ControlError
from .store import digest, dumps, now
from api.collection_rules import RULE_VERSION, FIELD_SCHEMA_VERSION


# 只返回SQLAlchemy表定义：batch批次、item逐条结果、matches来源关联；调用此函数本身不执行建表。
def ledger_schema():
    """定义与业务写入同事务提交的批次、明细和来源关联表，不自动执行DDL。"""
    metadata = MetaData()
    # 批次表：操作唯一键提供提交凭据；先占位写入，最终结果在同一事务里更新。
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
    # 明细表：记录每个候选的真实business_id、inserted/skip和实际保留行，不用候选数代替新增数。
    item = Table('t_nmpa_daily_import_item', metadata,
                 Column('import_id', String(32), primary_key=True), Column('ordinal', Integer, primary_key=True),
                 Column('registration_no_original', String(128), nullable=False),
                 Column('business_id', BigInteger), Column('result', String(24), nullable=False),
                 Column('changed', Integer, nullable=False),
                 Column('changed_fields', JSON, nullable=False), Column('row_json', JSON, nullable=False),
                 mysql_engine='InnoDB', mysql_charset='utf8mb4')
    # 来源表：同一器械可能来自现行/历史不同类别，分别保留来源ID和关键词。
    matches = Table('t_nmpa_daily_record_match', metadata,
                    Column('id', String(64), primary_key=True), Column('business_id', BigInteger, nullable=False),
                    Column('registration_no_original', String(128), nullable=False),
                    Column('source_instance_id', String(32), nullable=False), Column('category', String(32), nullable=False),
                    Column('query_keyword', String(40), nullable=False), Column('source_record_id', String(255), nullable=False),
                    Column('first_seen_at', String(40), nullable=False),
                    mysql_engine='InnoDB', mysql_charset='utf8mb4')
    return metadata, batch, item, matches


# 由不含密码的数据库类型/主机/端口/库名/表名计算稳定身份；防止恢复任务被写入另一个目标库。
def target_identity(settings):
    """计算不含密码的目标数据集身份，防止旧候选被重试到新库。"""
    if not settings.database_url:
        return digest(['unconfigured', settings.table])
    url = make_url(settings.database_url)
    name = url.get_backend_name()
    database = str(Path(url.database).resolve()) if name == 'sqlite' and url.database else url.database
    return digest([name, url.host, url.port or (3306 if name == 'mysql' else None), database, settings.table])


class LedgerDatabase(DeviceDatabase):
    # 先检查器械表，再只读核对三张账本的字段、InnoDB及操作唯一约束；缺表直接报迁移错误。
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

    # 运行账号禁止在这里自动DDL，明确抛错；真正显式迁移由管理员入口完成。
    def migrate(self):
        """拒绝运行账号自动DDL，要求管理员使用显式迁移入口。"""
        raise RuntimeError('运行账号不自动迁移业务库，请由管理员执行 migrate_daily_mysql.py')

    # 用source_instance_id与operation_id查询已提交批次；用来核对COMMIT成功但网络响应丢失的情况。
    def lookup(self, source_instance_id, operation_id):
        """按源实例和原操作编号查询提交证据，用于COMMIT响应丢失核对。"""
        with self.engine.connect() as connection:
            row = connection.execute(select(self.batches).where(
                self.batches.c.source_instance_id == source_instance_id,
                self.batches.c.operation_id == operation_id)).mappings().first()
            return dict(row) if row else None

    # 验证备份、固定候选及目标后，在同一MySQL事务写器械/批次/明细/来源；全部成功才返回真实计数。
    def import_snapshot(self, job, snapshot, backup):
        """校验固定候选并在同一事务新增业务、补全部位、记录明细与提交证据。"""
        self.verify_backup(backup)
        payload = snapshot['payload']
        if digest(payload) != snapshot['hash'] or payload['target_dataset_id'] != target_identity(self.settings):
            raise ControlError('SNAPSHOT_TARGET_MISMATCH', '快照或目标数据库已变化，禁止写入')
        if payload.get('rule_version') != RULE_VERSION or payload.get('field_schema_version') != FIELD_SCHEMA_VERSION:
            raise ControlError('RULE_VERSION_MISMATCH', '旧规则快照禁止按313入库')
        members = payload['members']
        for member in members:
            row = member['row']
            if set(row) != set(FIELDS):
                raise ControlError('SNAPSHOT_INVALID', '候选字段不符合A方案契约')
            errors = validate(row)
            if errors:
                raise ControlError('SNAPSHOT_INVALID', '快照中的候选未通过字段校验')
        if len({member['row']['registration_no_original'] for member in members}) != len(members):
            raise ControlError('SNAPSHOT_INVALID', '快照包含重复完整证号')
        result = {'inserted': 0, 'skipped_existing': 0, 'changed_existing': 0, 'body_part_updated': 0, 'collection_tag_updated': 0, 'derived_updated': 0, 'backup_id': backup['id'], 'import_id': job['id']}
        # 四表同事务：器械表+三张辅助表一起提交/回滚；把账本单独搬到本地文件后不能照搬这个一致性假设。
        with self.engine.begin() as connection:
            # The unique reservation and final ledger are invisible until this transaction commits.
            connection.execute(self.batches.insert().values(id=job['id'], source_instance_id=payload['source_instance_id'],
                operation_id=job['operation_id'], snapshot_hash=snapshot['hash'], target_dataset_id=payload['target_dataset_id'],
                backup_json=backup, result_json={}, committed_at=now()))
            # 先按完整证号查已有行；只有确实不存在时插入器械，其他来源出现同号就跳过新增。
            for ordinal, member in enumerate(members):
                row = member['row']
                existing = self.existing_row(connection, row['registration_no_original'])
                changed = {}
                if existing is None:
                    try:
                        # 可能另一个写入方抢先插入同证号；只接受明确重复键并重新找到该完整证号，否则回滚整批。
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
                    # 已有证号只按库中保留文本重算部位/骨科标签；不拿新候选覆盖已有名称、结构和用途。
                    actual, updated = self.refresh_existing_derived_fields(connection, existing)
                    result['body_part_updated'] += updated['body_part']
                    result['collection_tag_updated'] += updated['collection_tag']
                    result['derived_updated'] += int(any(updated.values()))
                    changed = self.changed_fields(actual, row)
                    result['skipped_existing'] += 1
                    result['changed_existing'] += int(bool(changed))
                else:
                    status = 'inserted'
                    result['inserted'] += 1
                actual = self.existing_row(connection, row['registration_no_original'])
                # row_json记录数据库实际值：跳过同号时不能错误保存为这次候选内容。
                actual_row = self.business_snapshot(actual)
                connection.execute(self.items.insert().values(import_id=job['id'], ordinal=ordinal,
                    registration_no_original=row['registration_no_original'], business_id=business_id,
                    result=status, changed=int(bool(changed)), changed_fields=changed, row_json=actual_row))
                for match in member['matches']:
                    identity = digest([business_id, payload['source_instance_id'], match])
                    if connection.execute(select(self.matches.c.id).where(self.matches.c.id == identity)).first() is None:
                        connection.execute(self.matches.insert().values(id=identity, business_id=business_id,
                            registration_no_original=row['registration_no_original'], source_instance_id=payload['source_instance_id'],
                            category=match['category'], query_keyword=match['keyword'], source_record_id=match['source_id'], first_seen_at=now()))
            # 离开事务前保存最终计数；只有整个事务COMMIT后lookup才能读到这份真实提交证据。
            connection.execute(self.batches.update().where(self.batches.c.id == job['id']).values(result_json=result, committed_at=now()))
        return result

    # 按批次查询真实提交明细，可筛inserted/skipped_existing或变化标记；不是重新查询采集候选。
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

    # 从已提交账本重建inserted.csv和items.json；这些文件是可再生报告，不单独充当事务提交证据。
    def rebuild_reports(self, import_id, directory):
        """从事务账本重建可丢弃报告文件，不把文件当作提交证明。"""
        records = self.record_page(import_id, limit=10**9)['items']
        directory.mkdir(parents=True, exist_ok=True)
        writecsv(directory / 'inserted.csv', FIELDS, [r['row_json'] for r in records if r['result'] == 'inserted'])
        atomic(directory / 'items.json', dumps(records))

