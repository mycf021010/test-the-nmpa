"""Explicit synthetic probe on dedicated loopback MySQL port 13339; no private settings."""
import json, os, re, sys, tempfile, uuid
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from sqlalchemy import create_engine, select, text, event, inspect
from api.config import Settings
from api.daily.store import DailyStore
from api.daily.contracts import JobRequest, RoundRequest
from api.daily.engine import process_daily
from api.daily.ledger import LedgerDatabase
from test_daily import DailyClient


def execute_sql(engine, source):
    source=re.sub(r'^\s*--.*$', '', source, flags=re.M)
    source=re.sub(r'USE\s+`?ak_medical`?\s*;', '', source, flags=re.I)
    with engine.begin() as db:
        for statement in source.split(';'):
            if statement.strip():db.execute(text(statement))


def main():
    if os.getenv('NMPA_313_DISPOSABLE_MYSQL')!='yes':
        raise SystemExit('Only with NMPA_313_DISPOSABLE_MYSQL=yes on dedicated localhost:13339')
    root=Path(__file__).resolve().parents[3]
    admin=create_engine('mysql+pymysql://root@127.0.0.1:13339/', hide_parameters=True)
    name='nmpa_313_probe_'+uuid.uuid4().hex
    target=create_engine('mysql+pymysql://root@127.0.0.1:13339/'+name,hide_parameters=True)
    output={'mysql_port':13339,'synthetic_only':True}
    try:
        with admin.begin() as db:db.execute(text('CREATE DATABASE `'+name+'` CHARACTER SET utf8mb4'))
        execute_sql(target,(root/'database/CREATE_fresh_313_medical_device.sql').read_text())
        execute_sql(target,(root/'database/A002_python_daily_ledger.sql').read_text())
        output['fresh_user_sql']=True
        java_sql=(root/'database/A001_java_daily.sql').read_text()
        execute_sql(target,java_sql.replace('t_nmpa_313_daily_', 't_nmpa_daily_').replace('fk_nmpa_313_daily_task','fk_nmpa_daily_task'))
        execute_sql(target,java_sql);execute_sql(target,java_sql)
        output['old_new_java_tables_coexist']=True
        with target.begin() as db:
            db.execute(text("ALTER TABLE t_medical_device DROP COLUMN collection_tag, MODIFY production_address VARCHAR(1024), MODIFY body_part VARCHAR(16) NOT NULL, ADD CONSTRAINT chk_md_reg8_body_part CHECK(body_part IN ('膝','髋','膝，髋'))"))
        migration=(root/'database/UPGRADE_existing_medical_device_313.sql').read_text()
        execute_sql(target,migration);execute_sql(target,migration)
        cols={c['name']:c for c in inspect(target).get_columns('t_medical_device')}
        assert cols['body_part']['nullable'] and cols['production_address']['type'].length==2048
        output['incremental_sql_and_repeat']=True
        with tempfile.TemporaryDirectory() as folder:
            settings=Settings(Path(folder),'synthetic',str(target.url),mysqldump='/opt/homebrew/opt/mysql/bin/mysqldump',daily_enabled=True)
            store=DailyStore(settings,migrate=True)
            round_id=store.open_round(RoundRequest(request_id='a'*32))['round_id']
            DailyClient.total=3;DailyClient.details=[];DailyClient.fail_after=None;DailyClient.reverse=False;DailyClient.invalid=False
            def request(serial,category):
                return JobRequest(request_id=f'{serial:032x}',daily_task_id=f'{100+serial:032x}',subtask_id=f'{200+serial:032x}',round_id=round_id,category=category,keyword='313',target_records=80)
            first=store.create_job(request(1,'domestic'))['job_id'];process_daily(store,DailyClient)
            assert store.job(first)['state']=='succeeded',store.job(first)
            assert store.job(first)['result']['inserted']==3
            output['first_insert_and_mysqldump']=True
            database=LedgerDatabase(settings)
            with database.engine.begin() as db:db.execute(database.table.update().values(body_part='髋',collection_tag=None))
            database.close()
            second=store.create_job(request(2,'imported'))['job_id']
            def failing_factory(config):
                db=LedgerDatabase(config)
                def fail_item(conn,cursor,statement,*args):
                    if statement.startswith('INSERT INTO t_nmpa_daily_import_item'):raise RuntimeError('synthetic ledger error')
                event.listen(db.engine,'before_cursor_execute',fail_item)
                return db
            process_daily(store,DailyClient,failing_factory)
            assert store.job(second)['state']=='blocked'
            database=LedgerDatabase(settings)
            with database.engine.connect() as db:
                assert db.execute(select(database.table.c.body_part)).scalars().all()==['髋']*3
                assert db.execute(select(database.table.c.collection_tag)).scalars().all()==[None]*3
            database.close();output['derived_updates_and_ledger_rollback']=True
            store.resume(second);process_daily(store,DailyClient)
            result=store.job(second)['result'];assert result['body_part_updated']==result['collection_tag_updated']==3
            output['resume_without_recollection']=len(DailyClient.details)==6
            with target.begin() as db:db.execute(text("UPDATE t_medical_device SET product_name='其他骨科器械'"))
            third=store.create_job(request(3,'domestic_history'))['job_id'];process_daily(store,DailyClient)
            assert store.job(third)['result']['inserted']==0
            database=LedgerDatabase(settings)
            with database.engine.connect() as db:
                assert db.execute(select(database.table.c.body_part)).scalars().all()==[None]*3
                entries=db.execute(select(database.items.c.row_json).where(database.items.c.import_id==third)).scalars().all()
                assert all(row['body_part'] is None and row['product_name']=='其他骨科器械' for row in entries)
            database.close();output['nullable_anatomy_and_actual_ledger']=True
            before=len(DailyClient.details)
            fourth=store.create_job(request(4,'imported_history'))['job_id']
            original=LedgerDatabase.import_snapshot
            def lost_ack(db,*args):
                original(db,*args)
                from sqlalchemy.exc import OperationalError
                raise OperationalError('synthetic',None,Exception('lost ack'))
            with patch.object(LedgerDatabase,'import_snapshot',lost_ack):process_daily(store,DailyClient)
            assert store.job(fourth)['state']=='retry_wait'
            store.resume(fourth)
            with patch.object(LedgerDatabase,'backup',side_effect=AssertionError('must reconcile')):process_daily(store,DailyClient)
            assert store.job(fourth)['state']=='succeeded'
            assert len(DailyClient.details)==before+3
            output['commit_ack_loss_reconciled']=True
            output['round_completed']=store.round(round_id)['state']=='completed'
        print(json.dumps(output,ensure_ascii=False,indent=2))
    finally:
        target.dispose()
        with admin.begin() as db:db.execute(text('DROP DATABASE IF EXISTS `'+name+'`'))
        admin.dispose()


if __name__=='__main__':main()
