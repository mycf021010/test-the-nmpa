"""仅显式授权的一次性13329端口MySQL探针；不读取真实项目配置。"""
import json
import os
import tempfile
import uuid
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from unittest.mock import patch
from sqlalchemy import BigInteger,Column,Date,JSON,MetaData,String,Table,Text,create_engine,select,text
from api.config import Settings
from api.daily.contracts import JobRequest,RoundRequest
from api.daily.engine import process_daily
from api.daily.ledger import LedgerDatabase,ledger_schema
from api.daily.store import DailyStore
from export_medical_device import FIELDS,LIMITS
from test_daily import DailyClient


def main():
    """创建随机测试库、执行真实备份及同号补全/回滚，输出结果后删除仅本探针创建的库。"""
    if os.getenv('NMPA_A_DISPOSABLE_MYSQL')!='yes':
        raise SystemExit('需要显式NMPA_A_DISPOSABLE_MYSQL=yes；仅适用于本机13329专用测试实例')
    name='nmpa_a_'+uuid.uuid4().hex
    admin=create_engine('mysql+pymysql://root@127.0.0.1:13329/',hide_parameters=True)
    engine=None
    try:
        with admin.begin() as db:
            db.execute(text('CREATE DATABASE `'+name+'` CHARACTER SET utf8mb4'))
        with tempfile.TemporaryDirectory() as folder:
            settings=Settings(Path(folder),'synthetic','mysql+pymysql://root@127.0.0.1:13329/'+name,mysqldump='/opt/homebrew/bin/mysqldump',daily_enabled=True)
            engine=create_engine(settings.database_url,hide_parameters=True)
            metadata=MetaData();columns=[Column('id',BigInteger,primary_key=True,autoincrement=True)]
            for field in FIELDS:
                kind=JSON() if field=='source_file' else Date() if field in ('approval_date','effective_date','expiry_date') else String(LIMITS[field]) if LIMITS[field] else Text()
                columns.append(Column(field,kind,unique=field=='registration_no_original'))
            Table(settings.table,metadata,*columns,mysql_engine='InnoDB');metadata.create_all(engine);ledger_schema()[0].create_all(engine)
            store=DailyStore(settings,migrate=True);round_id=store.open_round(RoundRequest(request_id='a'*32))['round_id']
            DailyClient.total=3;DailyClient.details=[];DailyClient.fail_after=None;DailyClient.reverse=False
            def run(keyword,serial):
                """在随机测试库中执行一个合成来源子任务。"""
                job=store.create_job(JobRequest(request_id=f'{serial:032x}',daily_task_id=f'{100+serial:032x}',subtask_id=f'{200+serial:032x}',round_id=round_id,category='domestic',keyword=keyword,target_records=80))
                process_daily(store,DailyClient)
                return store.job(job['job_id'])
            knee=run('膝',1);assert knee['state']=='succeeded',knee
            # Force a failure after business UPDATE but before item INSERT in the same transaction.
            from sqlalchemy import event
            def failing_factory(config):
                """仅给本探针引擎注入明细写失败，验证真实InnoDB整体回滚。"""
                database=LedgerDatabase(config)
                def fail_item(connection,cursor,statement,parameters,context,executemany):
                    """在第一次部位UPDATE之后故意让明细INSERT失败。"""
                    if statement.startswith('INSERT INTO t_nmpa_daily_import_item'):
                        raise RuntimeError('synthetic item failure')
                event.listen(database.engine,'before_cursor_execute',fail_item)
                return database
            hip_req=JobRequest(request_id='2'*32,daily_task_id='3'*32,subtask_id='4'*32,round_id=round_id,category='domestic',keyword='髋',target_records=80)
            hip_id=store.create_job(hip_req)['job_id'];process_daily(store,DailyClient,failing_factory)
            database=LedgerDatabase(settings)
            try:
                with engine.connect() as db:
                    assert db.execute(select(database.table.c.body_part)).scalars().all()==['膝']*3
                    assert db.execute(select(database.batches.c.id)).scalars().all()==[knee['id']]
            finally:database.close()
            store.resume(hip_id)
            original=LedgerDatabase.import_snapshot
            def lost_ack(database,*args):
                """实际COMMIT成功后丢失ACK，下一次不得重复备份或更新。"""
                original(database,*args)
                from sqlalchemy.exc import OperationalError
                raise OperationalError('synthetic',None,Exception('lost ack'))
            with patch.object(LedgerDatabase,'import_snapshot',lost_ack):process_daily(store,DailyClient)
            assert store.job(hip_id)['state']=='retry_wait'
            store.resume(hip_id)
            with patch.object(LedgerDatabase,'backup',side_effect=AssertionError('must reconcile')):process_daily(store,DailyClient)
            hip=store.job(hip_id);assert hip['state']=='succeeded',hip
            with engine.connect() as db:
                assert db.execute(text('SELECT body_part FROM t_medical_device ORDER BY id')).scalars().all()==['膝，髋']*3
                version=db.execute(text('SELECT VERSION()')).scalar_one()
            dumps=list((settings.data_dir/'backups').glob('*.sql'))
            assert dumps and all(table in dumps[0].read_text() for table in ['t_medical_device','t_nmpa_daily_import_batch','t_nmpa_daily_import_item','t_nmpa_daily_record_match'])
            result={'mysql_version':version,'inserted':knee['result']['inserted'],'hip_inserted':hip['result']['inserted'],'hip_body_part_updated':hip['result']['body_part_updated'],'real_innodb_rollback':True,'commit_ack_loss_reconciled':True,'four_tables_in_backup':True,'source_details_not_repeated':len(DailyClient.details)==6}
            print(json.dumps(result,ensure_ascii=False))
    finally:
        if engine:engine.dispose()
        with admin.begin() as db:db.execute(text('DROP DATABASE IF EXISTS `'+name+'`'))
        admin.dispose()


if __name__=='__main__':main()
