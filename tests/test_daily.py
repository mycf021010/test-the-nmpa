"""A方案离线集成：合成来源、临时SQLite，不请求真实网站或连接业务数据库。"""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from fastapi.testclient import TestClient
from sqlalchemy import create_engine,select
from api.app import create_app
from api.config import Settings
from api.daily.contracts import DailyError,JobRequest,RoundRequest
from api.daily.engine import process_daily
from api.daily.ledger import LedgerDatabase,ledger_schema
from api.daily.store import DailyStore
from nmpa.storage import CSV_COLUMNS
from test_api import create_test_table
from test_pipeline import FakeClient


class DailyClient(FakeClient):
    """提供可控分页、同号膝髋及中断点的合成来源。"""
    total=200
    details=[]
    fail_after=None
    reverse=False
    invalid=False

    def search(self,item,keyword,page,size):
        """生成稳定顺序列表，可切换顺序用于边界变化检查。"""
        ids=list(range((page-1)*size,min(page*size,self.total)))
        if self.reverse:
            ids.reverse()
        return {'data':{'pageNum':page,'total':self.total,'list':[{'id':str(i)} for i in ids]}}

    def detail(self,item,record_id):
        """同一来源ID对膝髋返回相同产品，支持在固定数量后模拟断网。"""
        if self.fail_after is not None and len(self.details)>=self.fail_after:
            from requests.exceptions import Timeout
            raise Timeout('synthetic only')
        self.details.append((item,record_id))
        row={column:'' for column in CSV_COLUMNS if column!='注'}
        row.update({'id':record_id,'注册证编号':'国械注准2026'+str(record_id).zfill(8),
                    '注册人名称':'合成公司','产品名称':'合成器械','管理类别':'III',
                    '批准日期':'2026-01-01','有效期至':'invalid' if self.invalid else '2030-01-01'})
        return {'data':{'detail':row}}


class DailyTests(unittest.TestCase):
    """验证精确配额、轮次、事务、同号部位及接口兼容的主要不变量。"""
    def setUp(self):
        """为每个用例创建独立文件型控制库和业务库。"""
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        target=self.root/'target.sqlite3'
        create_test_table(target)
        self.settings=Settings(self.root/'control','synthetic-test-key','sqlite:///'+str(target),daily_enabled=True)
        self.store=DailyStore(self.settings,migrate=True)
        engine=create_engine(self.settings.database_url)
        ledger_schema()[0].create_all(engine)
        engine.dispose()
        DailyClient.total=200;DailyClient.details=[];DailyClient.fail_after=None;DailyClient.reverse=False;DailyClient.invalid=False
        self.round_id=self.store.open_round(RoundRequest(request_id='a'*32))['round_id']
        self.serial=0

    def tearDown(self):
        """移除本用例临时数据，不操作外部数据库和原项目文件。"""
        self.temp.cleanup()

    def request(self,category='domestic',keyword='膝',target=83):
        """创建稳定唯一的单小类请求，目标仍遵守80至100限制。"""
        self.serial+=1
        return JobRequest(request_id=f'{self.serial:032x}',daily_task_id=f'{1000+self.serial:032x}',subtask_id=f'{100+self.serial:032x}',
                          round_id=self.round_id,category=category,keyword=keyword,target_records=target)

    def run_job(self,request):
        """执行一次离线worker单元并返回持久化任务结果。"""
        job_id=self.store.create_job(request)['job_id']
        self.assertTrue(process_daily(self.store,DailyClient))
        return self.store.job(job_id)

    def test_83_records_resume_at_page_nine_fourth_record(self):
        """第一天83条后第二天接续剩余详情，既不跳页也不重复计数。"""
        first=self.run_job(self.request())
        self.assertEqual((first['state'],first['processed']),('succeeded',83))
        stream=self.store.round(self.round_id)['streams']
        self.assertEqual(next(s['page'] for s in stream if s['category']=='domestic' and s['keyword']=='膝'),9)
        second=self.run_job(self.request(target=80))
        self.assertEqual(second['processed'],80)
        self.assertEqual([identity for _,identity in DailyClient.details],list(map(str,range(163))))

    def test_request_replay_and_conflict(self):
        """创建响应丢失后同编号返回原任务，不同参数拒绝。"""
        request=self.request()
        created=self.store.create_job(request)
        self.assertEqual(created,self.store.create_job(request))
        self.assertEqual(created,self.store.request(request.request_id))
        with self.assertRaises(DailyError):
            self.store.create_job(request.model_copy(update={'target_records':90}))
        with self.assertRaises(DailyError):
            self.store.create_job(self.request(keyword='髋'))

    def test_knee_then_hip_merges_existing_body_part(self):
        """跨子任务同证号只保留原业务ID，并补全为膝髋。"""
        knee=self.run_job(self.request(target=80))
        hip=self.run_job(self.request(keyword='髋',target=80))
        self.assertEqual(knee['result']['inserted'],80)
        self.assertEqual((hip['result']['inserted'],hip['result']['skipped_existing'],hip['result']['body_part_updated']),(0,80,80))
        database=LedgerDatabase(self.settings)
        try:
            with database.engine.connect() as db:
                values=db.execute(select(database.table.c.body_part)).scalars().all()
                self.assertEqual(values,['膝，髋']*80)
        finally:
            database.close()

    def test_interruption_preserves_quota_and_recovery(self):
        """网络中断保存3条详情，恢复原任务只补剩余80条。"""
        request=self.request()
        DailyClient.fail_after=3
        result=self.run_job(request)
        self.assertEqual((result['state'],result['processed']),('retry_wait',3))
        DailyClient.fail_after=None
        self.store.resume(result['id'])
        process_daily(self.store,DailyClient)
        self.assertEqual(self.store.job(result['id'])['processed'],83)
        self.assertEqual(len(DailyClient.details),83)

    def test_all_eight_exhaust_then_open_next_round(self):
        """不足配额时自然耗尽，全部8类完成后新请求自动开新轮。"""
        DailyClient.total=3
        for category in ('domestic','domestic_history','imported','imported_history'):
            for keyword in ('膝','髋'):
                result=self.run_job(self.request(category,keyword))
                self.assertEqual((result['processed'],result['source_exhausted']),(3,1))
        self.assertEqual(self.store.round(self.round_id)['state'],'completed')
        new=self.store.open_round(RoundRequest(request_id='f'*32))['round_id']
        self.assertNotEqual(new,self.round_id)
        self.assertTrue(all(s['page']==1 and not s['exhausted'] for s in self.store.round(new)['streams']))
        self.assertEqual(self.store.open_round(RoundRequest(request_id='a'*32))['round_id'],self.round_id)

    def test_boundary_change_blocks_instead_of_silently_skipping(self):
        """跨日来源第9页排序变化时保留断点并阻塞，不误报当天成功。"""
        self.run_job(self.request())
        DailyClient.reverse=True
        result=self.run_job(self.request())
        self.assertEqual((result['state'],result['error_code']),('blocked','SOURCE_BOUNDARY_CHANGED'))
        self.assertEqual(len(DailyClient.details),83)

    def test_commit_ack_loss_reconciles_without_second_write(self):
        """业务提交成功后响应丢失，重试只核账本，不再次备份和插入。"""
        original=LedgerDatabase.import_snapshot
        def lost_ack(database,*args):
            """先完成真实临时库事务，再模拟ACK丢失。"""
            original(database,*args)
            from sqlalchemy.exc import OperationalError
            raise OperationalError('synthetic',None,Exception('lost ack'))
        with patch.object(LedgerDatabase,'import_snapshot',lost_ack):
            first=self.run_job(self.request(target=80))
        self.assertEqual(first['state'],'retry_wait')
        self.store.resume(first['id'])
        with patch.object(LedgerDatabase,'backup',side_effect=AssertionError('must reconcile')):
            process_daily(self.store,DailyClient)
        result=self.store.job(first['id'])
        self.assertEqual((result['state'],result['result']['inserted']),('succeeded',80))
        self.assertEqual(len(DailyClient.details),80)

    def test_duplicate_quota_same_daily_stream_rejected(self):
        """即使第一次任务已完成，同一日同一小类也不能换编号再分配80条。"""
        first_request=self.request(target=80)
        self.run_job(first_request)
        other=self.request(target=80).model_copy(update={'daily_task_id':first_request.daily_task_id})
        with self.assertRaises(DailyError) as context:
            self.store.create_job(other)
        self.assertEqual(context.exception.code,'DAILY_QUOTA_EXISTS')

    def test_body_part_update_and_ledger_failure_roll_back_together(self):
        """部位已UPDATE后明细写入失败，整个事务回滚且后续只恢复入库。"""
        self.run_job(self.request(target=80))
        from sqlalchemy import event,func
        def failing_database(settings):
            """仅在临时测试引擎上注入明细写入故障。"""
            database=LedgerDatabase(settings)
            def fail_item(connection,cursor,statement,parameters,context,executemany):
                """在业务部位UPDATE之后、明细INSERT之前触发异常。"""
                if statement.startswith('INSERT INTO t_nmpa_daily_import_item'):
                    raise RuntimeError('synthetic transaction failure')
            event.listen(database.engine,'before_cursor_execute',fail_item)
            return database
        job_id=self.store.create_job(self.request(keyword='髋',target=80))['job_id']
        process_daily(self.store,DailyClient,failing_database)
        self.assertEqual(self.store.job(job_id)['state'],'blocked')
        database=LedgerDatabase(self.settings)
        try:
            with database.engine.connect() as db:
                self.assertEqual(db.execute(select(database.table.c.body_part)).scalars().all(),['膝']*80)
                self.assertEqual(db.execute(select(func.count()).select_from(database.batches)).scalar_one(),1)
        finally:
            database.close()
        self.store.resume(job_id);process_daily(self.store,DailyClient)
        self.assertEqual(self.store.job(job_id)['result']['body_part_updated'],80)
        self.assertEqual(len(DailyClient.details),160)

    def test_target_change_before_collection_blocks_without_source_requests(self):
        """接收任务后改目标库也不能把原任务的采集/入库自动改投新库。"""
        from dataclasses import replace
        job_id=self.store.create_job(self.request())['job_id']
        self.store.settings=replace(self.settings,database_url='sqlite:///'+str(self.root/'other.sqlite3'))
        process_daily(self.store,DailyClient)
        self.assertEqual(self.store.job(job_id)['error_code'],'TARGET_CHANGED')
        self.assertEqual(DailyClient.details,[])
        self.assertFalse((self.root/'other.sqlite3').exists())

    def test_invalid_source_rows_exhaust_round_with_explicit_issues(self):
        """已取得但校验失败的详情计处理数、不入库，轮次保留带说明终态。"""
        DailyClient.total=1;DailyClient.invalid=True
        for category in ('domestic','domestic_history','imported','imported_history'):
            for keyword in ('膝','髋'):
                result=self.run_job(self.request(category,keyword))
                self.assertEqual((result['processed'],result['invalid'],result['state']),(1,1,'succeeded_with_issues'))
                self.assertEqual(result['result']['inserted'],0)
        self.assertEqual(self.store.round(self.round_id)['state'],'completed_with_issues')

    def test_graceful_worker_stop_recovers_automatically(self):
        """SIGTERM转中断后的原任务在持有worker锁重启时自动接续。"""
        request=self.request();job_id=self.store.create_job(request)['job_id']
        original=DailyClient.detail
        def stop_after_three(client,item,record_id):
            """仅在临时合成来源完成3条后模拟进程安全停止。"""
            if len(DailyClient.details)==3:
                raise KeyboardInterrupt()
            return original(client,item,record_id)
        with patch.object(DailyClient,'detail',stop_after_three):
            with self.assertRaises(KeyboardInterrupt):
                process_daily(self.store,DailyClient)
        self.assertEqual((self.store.job(job_id)['state'],self.store.job(job_id)['processed']),('interrupted',3))
        self.store.recover();process_daily(self.store,DailyClient)
        self.assertEqual(self.store.job(job_id)['state'],'succeeded')
        self.assertEqual(len(DailyClient.details),83)

    def test_api_auth_validation_and_legacy_unchanged(self):
        """新接口要求密钥，80至100以外参数拒绝，旧v1类别接口保留。"""
        with TestClient(create_app(self.settings)) as client:
            self.assertEqual(client.get('/api/v1/daily/capabilities').status_code,401)
            headers={'X-API-Key':self.settings.api_key}
            self.assertEqual(client.get('/api/v1/categories',headers=headers).status_code,200)
            body=self.request().model_dump(mode='json');body['target_records']=79
            self.assertEqual(client.post('/api/v1/daily/jobs',headers=headers,json=body).status_code,422)
