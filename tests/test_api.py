import csv
import json
import logging
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine
from sqlalchemy.exc import IntegrityError

import export_medical_device as converter
from api.app import create_app
from api.collector import collect
from api.config import Settings
from api.database import DeviceDatabase
from api.models import CollectRequest
from api.tasks import TaskStore
from api.worker import process_one
from test_pipeline import FakeClient


def create_test_table(path, unique_numeric=False, unique_product=False):
    engine = create_engine('sqlite:///' + str(path))
    metadata = MetaData()
    columns = [Column('id', Integer, primary_key=True)]
    for field in converter.FIELDS:
        columns.append(Column(field, String, nullable=field in ('body_part','collection_tag'),
                              unique=field == 'registration_no_original' or
                              (unique_numeric and field == 'registration_no') or
                              (unique_product and field == 'product_name')))
    Table('t_medical_device', metadata, *columns)
    metadata.create_all(engine)
    engine.dispose()


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.database_path = self.root / 'original.sqlite3'
        create_test_table(self.database_path)
        self.settings = Settings(self.root / 'service', 'test-key', 'sqlite:///' + str(self.database_path))
        self.app = create_app(self.settings)
        self.client = TestClient(self.app)
        self.headers = {'X-API-Key': 'test-key'}
        FakeClient.fail_detail = False
        FakeClient.omit_category = False
        FakeClient.calls = 0

    def tearDown(self):
        self.client.close()
        self.tmp.cleanup()

    def run_task(self, body=None):
        response = self.client.post('/api/v1/jobs', json=body or {'categories': ['domestic'], 'keywords': ['313']},
                                    headers=self.headers)
        self.assertEqual(response.status_code, 202, response.text)
        job_id = response.json()['job_id']
        handled = process_one(self.settings, self.app.state.tasks,
            collector=lambda request, batch, progress: collect(request, batch, progress, FakeClient))
        self.assertTrue(handled)
        return job_id, self.client.get('/api/v1/jobs/' + job_id, headers=self.headers).json()

    def test_import_limited_preview_without_collecting_again(self):
        job_id, task = self.run_task({'categories': ['domestic'], 'keywords': ['313'], 'dry_run': True, 'max_pages': 1})
        url = '/api/v1/jobs/' + job_id + '/import'
        self.assertEqual(self.client.post(url).status_code, 401)
        response = self.client.post(url, headers=self.headers)
        self.assertEqual(response.status_code, 202, response.text)
        self.assertEqual(self.client.post(url, headers=self.headers).status_code, 409)
        def forbidden(*args):
            raise AssertionError('Import must not collect again')
        process_one(self.settings, self.app.state.tasks, collector=forbidden)
        task = self.app.state.tasks.get(job_id)
        self.assertEqual(task['state'], 'succeeded', task)
        self.assertEqual(task['result']['database']['inserted'], 1)
        self.assertTrue((self.settings.data_dir / 'backups' / (job_id + '-preview.sqlite3')).exists())
        self.assertEqual(self.client.post(url, headers=self.headers).status_code, 409)
        other, _ = self.run_task({'categories': ['domestic'], 'keywords': ['313'], 'dry_run': True, 'max_pages': 1})
        self.assertEqual(self.client.post('/api/v1/jobs/' + other + '/import', headers=self.headers).status_code, 202)
        process_one(self.settings, self.app.state.tasks, collector=forbidden)
        self.assertEqual(self.app.state.tasks.get(other)['result']['database']['inserted'], 0)

    def test_retry_failed_full_import_reuses_collected_data(self):
        from retry_failed_import import retry
        with patch.object(DeviceDatabase, 'import_csv', side_effect=RuntimeError('simulated failure')):
            job_id, task = self.run_task()
        self.assertEqual(task['state'], 'failed')
        self.assertTrue(task['result']['collection']['complete'])
        result = retry(self.settings, job_id)
        self.assertEqual(result['inserted'], 1)
        self.assertEqual(self.app.state.tasks.get(job_id)['state'], 'succeeded')
        self.assertTrue(result['backup_id'].startswith(job_id + '-retry-'))
        with self.assertRaises(ValueError):
            retry(self.settings, job_id)

    def test_retry_failed_confirmed_preview_reuses_original_result(self):
        from retry_failed_import import retry
        job_id, _ = self.run_task({'categories':['domestic'],'keywords':['313'],'dry_run':True,'max_pages':1})
        self.assertEqual(self.client.post('/api/v1/jobs/'+job_id+'/import',headers=self.headers).status_code,202)
        with patch.object(DeviceDatabase,'backup',side_effect=RuntimeError('backup failed')):
            process_one(self.settings,self.app.state.tasks)
        self.assertEqual(self.app.state.tasks.get(job_id)['state'],'failed')
        result = retry(self.settings,job_id)
        self.assertEqual(result['inserted'],1)
        self.assertEqual(self.app.state.tasks.get(job_id)['state'],'succeeded')
        with self.assertRaises(ValueError):
            retry(self.settings,job_id)

    def test_retry_failed_preview_rejects_modified_csv(self):
        from retry_failed_import import retry
        job_id, _ = self.run_task({'categories':['domestic'],'keywords':['313'],'dry_run':True,'max_pages':1})
        self.client.post('/api/v1/jobs/'+job_id+'/import',headers=self.headers)
        with patch.object(DeviceDatabase,'backup',side_effect=RuntimeError('backup failed')):
            process_one(self.settings,self.app.state.tasks)
        candidate=self.settings.data_dir/'jobs'/job_id/'database/t_medical_device.csv'
        with candidate.open('a',encoding='utf-8') as stream:
            stream.write('changed')
        with patch.object(DeviceDatabase,'backup') as backup:
            with self.assertRaises(ValueError):
                retry(self.settings,job_id)
            backup.assert_not_called()

    def test_realistic_limited_audit_can_import_but_page_errors_cannot(self):
        class LimitedClient(FakeClient):
            def search(self, item, keyword, page, size):
                return {'data': {'pageNum': page, 'total': 20, 'list': [{'id': item + '-' + str(i)} for i in range(size)]}}
        response = self.client.post('/api/v1/jobs', json={'categories': ['domestic'], 'keywords': ['313'], 'dry_run': True, 'max_pages': 1}, headers=self.headers)
        job_id = response.json()['job_id']
        process_one(self.settings, self.app.state.tasks, collector=lambda request, batch, progress: collect(request, batch, progress, LimitedClient))
        url = '/api/v1/jobs/' + job_id
        task = self.client.get(url, headers=self.headers).json()
        self.assertTrue(task['can_import_preview'], task)
        from api.preview_import import validate_preview
        validate_preview(self.settings, task)
        path = next((self.settings.data_dir / 'jobs' / job_id / 'raw').rglob('*.report.json'))
        report = json.loads(path.read_text())
        report['page_errors'] = [1]
        path.write_text(json.dumps(report))
        self.assertFalse(self.client.get(url, headers=self.headers).json()['can_import_preview'])
        self.assertEqual(self.client.post(url + '/import', headers=self.headers).status_code, 409)

    def test_preview_backup_failure_never_writes(self):
        job_id, _ = self.run_task({'categories': ['domestic'], 'keywords': ['313'], 'dry_run': True, 'max_pages': 1})
        self.assertEqual(self.client.post('/api/v1/jobs/' + job_id + '/import', headers=self.headers).status_code, 202)
        with patch.object(DeviceDatabase, 'backup', side_effect=RuntimeError('backup failed')):
            process_one(self.settings, self.app.state.tasks)
        self.assertEqual(self.app.state.tasks.get(job_id)['state'], 'failed')
        with sqlite3.connect(self.database_path) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM t_medical_device').fetchone()[0], 0)

    def test_preview_import_rejects_errors_and_changed_file(self):
        job_id, task = self.run_task({'categories': ['domestic'], 'keywords': ['313'], 'dry_run': True, 'max_pages': 1})
        original = task['result']
        task['result']['collection']['issues'].append('详情失败')
        self.app.state.tasks.update(job_id, 'partial', result=task['result'])
        self.assertEqual(self.client.post('/api/v1/jobs/' + job_id + '/import', headers=self.headers).status_code, 409)
        task['result']['collection']['issues'].remove('详情失败')
        self.app.state.tasks.update(job_id, 'partial', result=task['result'])
        self.assertEqual(self.client.post('/api/v1/jobs/' + job_id + '/import', headers=self.headers).status_code, 202)
        path = self.settings.data_dir / 'jobs' / job_id / 'database' / 't_medical_device.csv'
        path.write_text('changed')
        process_one(self.settings, self.app.state.tasks)
        self.assertEqual(self.app.state.tasks.get(job_id)['state'], 'failed')
        self.assertFalse((self.settings.data_dir / 'backups').exists())

    def test_rest_categories_backup_insert_repeat_and_preview(self):
        response = self.client.get('/api/v1/categories', headers=self.headers)
        self.assertEqual(len(response.json()), 4)
        job_id, task = self.run_task()
        self.assertEqual(task['state'], 'succeeded', task)
        self.assertEqual(task['result']['database']['inserted'], 1)
        backup = self.settings.data_dir / 'backups' / (job_id + '.sqlite3')
        with sqlite3.connect(backup) as connection:
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM t_medical_device').fetchone()[0], 0)
        with sqlite3.connect(self.database_path) as connection:
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM t_medical_device').fetchone()[0], 1)
        records = self.client.get('/api/v1/jobs/' + job_id + '/records', headers=self.headers).json()['records']
        self.assertEqual(list(records[0]), converter.FIELDS)
        _, repeated = self.run_task()
        self.assertEqual(repeated['result']['database']['inserted'], 0)
        self.assertEqual(repeated['result']['database']['skipped_existing'], 1)

    def test_auth_invalid_categories_and_partial_import_rejection(self):
        self.assertEqual(self.client.post('/api/v1/jobs', json={}).status_code, 401)
        for body in ({'categories': ['unknown']}, {'categories': []}, {'keywords': ['踝']},
                     {'categories': ['domestic', 'domestic']}, {'max_pages': 1}, {'extra': 'bad'}):
            self.assertEqual(self.client.post('/api/v1/jobs', json=body, headers=self.headers).status_code, 422)
        self.assertEqual(self.client.get('/api/v1/jobs/missing', headers=self.headers).status_code, 404)

    def test_selected_categories_do_not_require_unselected_libraries(self):
        job_id, task = self.run_task({'categories': ['domestic_history'], 'keywords': ['313']})
        self.assertEqual(task['state'], 'succeeded', task)
        self.assertEqual(task['result']['collection']['valid_count'], 1)

    def test_dry_run_and_limited_run_never_back_up_or_write(self):
        _, task = self.run_task({'categories': ['domestic'], 'keywords': ['313'], 'dry_run': True, 'max_pages': 1})
        self.assertEqual(task['state'], 'partial')
        self.assertIsNone(task['result']['database'])
        self.assertFalse((self.settings.data_dir / 'backups').exists())
        with sqlite3.connect(self.database_path) as connection:
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM t_medical_device').fetchone()[0], 0)

    def test_backup_failure_prevents_insert(self):
        with patch.object(DeviceDatabase, 'backup', side_effect=RuntimeError('备份失败')):
            _, task = self.run_task()
        self.assertEqual(task['state'], 'failed')
        with sqlite3.connect(self.database_path) as connection:
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM t_medical_device').fetchone()[0], 0)

    def test_collection_validation_failure_blocks_backup(self):
        def invalid(request, batch, progress):
            output, summary = collect(request, batch, progress, FakeClient)
            summary['complete'] = False
            return output, summary
        task = self.app.state.tasks.create(CollectRequest(categories=['domestic']).model_dump(mode='json'))
        with patch.object(DeviceDatabase, 'backup') as backup:
            process_one(self.settings, self.app.state.tasks, collector=invalid)
            backup.assert_not_called()
        self.assertEqual(self.app.state.tasks.get(task['id'])['state'], 'partial')

    def test_backup_tamper_prevents_insert(self):
        database = DeviceDatabase(self.settings)
        try:
            backup = database.backup('test-backup')
            with (self.settings.data_dir / 'backups' / backup['file']).open('ab') as stream:
                stream.write(b'tamper')
            with self.assertRaises(RuntimeError):
                database.import_csv(self.root / 'unused.csv', backup, self.root)
        finally:
            database.close()

    def test_transaction_rollback_on_unrelated_unique_constraint(self):
        self.database_path.unlink()
        create_test_table(self.database_path, unique_product=True)
        rows = [converter.convert({'注册证编号': no, '注册人名称': '公司', '产品名称': '相同产品',
            '管理类别': 'III', '批准日期': '2026-01-02', '有效期至': '2030-01-02'}, '膝', converter.CATS[0])
                for no in ('国械注准20263130001', '国械注准20263130002')]
        path = self.root / 'rows.csv'
        converter.writecsv(path, converter.FIELDS, rows)
        database = DeviceDatabase(self.settings)
        try:
            backup = database.backup('rollback')
            with self.assertRaises(IntegrityError):
                database.import_csv(path, backup, self.root / 'output')
        finally:
            database.close()
        with sqlite3.connect(self.database_path) as connection:
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM t_medical_device').fetchone()[0], 0)

    def test_same_digits_different_full_certificate_both_insert(self):
        rows = [converter.convert({'注册证编号': no, '注册人名称': '公司', '产品名称': '产品',
            '管理类别': 'III', '批准日期': '2026-01-02', '有效期至': '2030-01-02'}, '膝', converter.CATS[0])
                for no in ('国械注准20263130001', '国械注进20263130001')]
        path = self.root / 'rows.csv'
        converter.writecsv(path, converter.FIELDS, rows)
        database = DeviceDatabase(self.settings)
        try:
            result = database.import_csv(path, database.backup('collision'), self.root / 'output')
            self.assertEqual(result['inserted'], 2)
        finally:
            database.close()

    def test_existing_changes_are_reported_without_overwrite(self):
        self.run_task()
        with sqlite3.connect(self.database_path) as connection:
            connection.execute("UPDATE t_medical_device SET registrant_name='数据库旧公司'")
        _, task = self.run_task()
        self.assertEqual(task['result']['database']['changed_existing'], 1)
        self.assertEqual(task['result']['database']['inserted'], 0)
        with sqlite3.connect(self.database_path) as connection:
            self.assertEqual(connection.execute('SELECT registrant_name FROM t_medical_device').fetchone()[0], '数据库旧公司')

    def test_queue_persists_and_only_claims_once(self):
        task = self.app.state.tasks.create({'dry_run': True})
        restarted = TaskStore(self.settings.data_dir)
        self.assertEqual(restarted.claim()['id'], task['id'])
        self.assertIsNone(self.app.state.tasks.claim())
        restarted.recover_interrupted()
        self.assertEqual(restarted.get(task['id'])['state'], 'interrupted')

    def test_missing_database_allows_only_dry_run(self):
        self.settings.database_url = ''
        self.assertEqual(self.client.post('/api/v1/jobs', json={}, headers=self.headers).status_code, 503)
        self.assertEqual(self.client.post('/api/v1/jobs', json={'dry_run': True}, headers=self.headers).status_code, 202)


if __name__ == '__main__':
    unittest.main()
