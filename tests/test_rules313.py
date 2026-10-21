"""Business acceptance tests using synthetic sources and disposable SQL databases."""
import json
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import select, text
from fastapi.testclient import TestClient
from export_medical_device import convert, validate, FIELDS
from api.app import create_app
from api.collection_rules import eligibility, derive_fields, infer_body_part, RULE_VERSION
from api.daily.normalization import merge_candidates
from api.daily.ledger import LedgerDatabase
from api.daily.engine import process_daily
import test_daily


def candidate(number='国械注进20223130618', **fields):
    row = convert({'注册证编号': number, '注册人名称':'合成公司', '产品名称':'合成器械',
                   '管理类别':'III', '批准日期':'2026-01-01', '有效期至':'2030-01-01'}, '313', '进口医疗器械（注册）')
    return derive_fields(dict(row, **fields))


class RuleTests(unittest.TestCase):
    def test_exact_digit_positions_not_substring(self):
        self.assertEqual(eligibility(candidate())[0], 'eligible')
        self.assertEqual(eligibility(candidate('国械注进20223203130'))[0], 'filtered')
        self.assertEqual(eligibility(candidate('无编号'))[0], 'invalid')
        self.assertEqual(eligibility(candidate('编号123456'))[0], 'invalid')

    def test_all_three_fields_and_only_those_fields(self):
        for field in ('product_name','structure_description','intended_use'):
            self.assertEqual(candidate(**{field:'包含膝'})['body_part'], '膝')
            self.assertEqual(candidate(**{field:'包含髋'})['body_part'], '髋')
        row = candidate(structure_description='膝', intended_use='髋')
        self.assertEqual(row['body_part'], '膝，髋')
        row = candidate(remark='膝关节系统注销公告', registrant_name='膝公司', model_specification='髋')
        self.assertEqual(row['body_part'], '')
        self.assertEqual(row['collection_tag'], '骨科')
        self.assertEqual(validate(row), [])

    def test_literal_rule_and_nullable_inputs(self):
        self.assertEqual(infer_body_part({'intended_use':'不用于膝'}), '膝')
        self.assertEqual(infer_body_part({'intended_use':None}), '')

    def test_22_fields_capacity_and_derived_invariants(self):
        self.assertEqual(len(FIELDS),22)
        row = candidate(production_address='地'*2048)
        self.assertEqual(validate(row), [])
        self.assertTrue(any('production_address' in e for e in validate(dict(row,production_address='地'*2049))))
        self.assertTrue(any('collection_tag' in e for e in validate(dict(row,collection_tag=''))))
        self.assertTrue(any('body_part' in e for e in validate(dict(row,body_part='膝'))))

    def test_merge_uses_final_business_fields_and_reports_conflicts(self):
        row=candidate(structure_description='含髋')
        source=lambda r,c:dict(row=r,category=c,keyword='313',source_id=c)
        valid,conflicts=merge_candidates([source(row,'domestic'),source(row,'imported')])
        self.assertEqual((len(valid),len(conflicts)),(1,0))
        self.assertEqual(valid[0]['row']['body_part'],'髋')
        self.assertEqual(len(valid[0]['matches']),2)
        valid,conflicts=merge_candidates([source(row,'domestic'),source(candidate(product_name='另一个产品'),'imported')])
        self.assertEqual((len(valid),len(conflicts)),(0,1))


class FlowTests(unittest.TestCase):
    def setUp(self):
        self.h = test_daily.DailyTests()
        self.h.setUp()

    def tearDown(self):
        self.h.tearDown()

    def test_filtered_invalid_and_eligible_are_disjoint_and_null_is_persisted(self):
        h=self.h
        test_daily.DailyClient.total=6
        original=test_daily.DailyClient.detail
        def mixed(client,item,identity):
            result=original(client,item,identity)
            row=result['data']['detail']
            if identity=='1':row['注册证编号']='国械注准20223203130'
            if identity=='2':row['注册证编号']='缺编号'
            if identity=='3':row.update({'产品名称':'其他骨科器械','备注':'膝关节公告'})
            if identity=='4':row.update({'产品名称':'合成膝器械','适用范围':'含髋'})
            if identity=='5':row['有效期至']='bad-date'
            return result
        with patch.object(test_daily.DailyClient,'detail',mixed):
            job=h.run_job(h.request(target=80))
        self.assertEqual((job['processed'],job['filtered'],job['invalid'],job['eligible']),(6,1,2,3))
        self.assertEqual(job['result']['inserted'],3)
        db=LedgerDatabase(h.settings)
        with db.engine.connect() as connection:
            values=connection.execute(select(db.table.c.body_part)).scalars().all()
            self.assertCountEqual(values,['膝',None,'膝，髋'])
            self.assertEqual(connection.execute(select(db.table.c.collection_tag)).scalars().all(),['骨科']*3)
        db.close()

    def test_all_filtered_is_success_without_infinite_topup_or_backup(self):
        h=self.h;test_daily.DailyClient.total=4
        original=test_daily.DailyClient.detail
        def filtered(client,item,identity):
            result=original(client,item,identity)
            result['data']['detail']['注册证编号']='国械注进20223203130'
            return result
        with patch.object(test_daily.DailyClient,'detail',filtered), patch.object(LedgerDatabase,'backup',side_effect=AssertionError('zero write needs no backup')):
            job=h.run_job(h.request(target=80))
        self.assertEqual((job['state'],job['processed'],job['filtered']),('succeeded',4,4))
        self.assertEqual(job['result']['inserted'],0)
        self.assertEqual(len(test_daily.DailyClient.details),4)

    def test_existing_row_uses_retained_text_and_ledger_reports_actual_values(self):
        h=self.h;test_daily.DailyClient.total=1
        first=h.run_job(h.request())
        db=LedgerDatabase(h.settings)
        with db.engine.begin() as connection:
            connection.execute(db.table.update().values(product_name='库中保留文本',body_part='膝',collection_tag=None))
        db.close()
        second=h.run_job(h.request(category='imported'))
        self.assertEqual((second['result']['inserted'],second['result']['derived_updated']),(0,1))
        db=LedgerDatabase(h.settings)
        with db.engine.connect() as connection:
            actual=connection.execute(select(db.table)).mappings().one()
            item=connection.execute(select(db.items).where(db.items.c.import_id==second['id'])).mappings().one()
            self.assertEqual(actual['product_name'],'库中保留文本')
            self.assertIsNone(actual['body_part'])
            self.assertEqual(actual['collection_tag'],'骨科')
            self.assertEqual(item['row_json']['product_name'],actual['product_name'])
            self.assertIsNone(item['row_json']['body_part'])
        db.close()

    def test_four_new_streams_and_old_keywords_are_rejected(self):
        h=self.h
        streams=h.store.round(h.round_id)['streams']
        self.assertEqual(len(streams),4)
        self.assertEqual({s['keyword'] for s in streams},{'313'})
        with TestClient(create_app(h.settings)) as client:
            headers={'X-API-Key':h.settings.api_key}
            caps=client.get('/api/v1/daily/capabilities',headers=headers).json()
            self.assertEqual(caps['rule_version'],RULE_VERSION)
            self.assertEqual(caps['field_schema_version'],22)
            request=h.request().model_dump(mode='json');request['keyword']='膝'
            self.assertEqual(client.post('/api/v1/daily/jobs',json=request,headers=headers).status_code,422)
            request=h.request().model_dump(mode='json');request['rule_version']='legacy'
            self.assertEqual(client.post('/api/v1/daily/jobs',json=request,headers=headers).status_code,422)
            accepted=client.post('/api/v1/daily/jobs',json=h.request().model_dump(mode='json'),headers=headers)
            self.assertEqual(accepted.status_code,202,accepted.text)
            status=client.get('/api/v1/daily/jobs/'+accepted.json()['job_id'],headers=headers).json()
            self.assertEqual(status['rule_version'],RULE_VERSION)
            self.assertEqual((status['processed'],status['filtered'],status['eligible']),(0,0,0))

    def test_old_control_file_is_not_opened_or_rewritten(self):
        old=self.h.settings.data_dir/'daily-a.sqlite3'
        old.write_bytes(b'legacy-sentinel-do-not-touch')
        from api.daily.store import DailyStore
        again=DailyStore(self.h.settings,migrate=True)
        self.assertEqual(again.instance_id,self.h.store.instance_id)
        self.assertEqual(old.read_bytes(),b'legacy-sentinel-do-not-touch')


if __name__ == '__main__':
    unittest.main()
