"""离线断点恢复测试；不发送真实网站请求。"""
import tempfile
import unittest
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from requests.exceptions import ConnectionError
from fastapi.testclient import TestClient
from api.app import create_app
from api.collector import collect
from api.config import Settings
from api.resume import validate_resume
from api.worker import process_one
from test_api import create_test_table
from test_pipeline import FakeClient

class InterruptedClient(FakeClient):
    pages = []
    details = []
    broken = True
    def search(self, item, keyword, page, size):
        type(self).pages.append(page)
        if page == 2 and self.broken:
            raise ConnectionError('private-url-and-secret')
        return {'data': {'pageNum': page, 'total': 3, 'list': [{'id': item + '-' + str(page)}]}}
    def detail(self, item, record_id):
        type(self).details.append(record_id)
        result = super().detail(item, record_id)
        result['data']['detail']['注册证编号'] = '国械注准2026300000' + record_id[-1]
        return result

class ResumeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db = self.root / 'devices.sqlite3'
        create_test_table(self.db)
        self.settings = Settings(self.root / 'service', 'test-key', 'sqlite:///' + str(self.db))
        self.app = create_app(self.settings)
        self.tasks = self.app.state.tasks
        self.client = TestClient(self.app)
        self.headers = {'X-API-Key': 'test-key'}
        InterruptedClient.pages, InterruptedClient.details, InterruptedClient.broken = [], [], True
        FakeClient.fail_detail, FakeClient.omit_category = False, False
        self.id = self.tasks.create({'categories': ['domestic'], 'keywords': ['膝'],
                                    'dry_run': False, 'max_pages': 0, 'page_size': 1})['id']
    def tearDown(self):
        self.client.close()
        self.tmp.cleanup()
    def run_worker(self):
        return process_one(self.settings, self.tasks,
            collector=lambda r, b, p: collect(r, b, p, InterruptedClient))
    def test_resume_skips_saved_pages_and_imports_after_completion(self):
        self.run_worker()
        task = self.tasks.get(self.id)
        self.assertEqual(task['state'], 'failed')
        self.assertIn('采集来源连接失败', task['error'])
        self.assertNotIn('private-url', task['error'])
        self.assertNotIn('check_backup', task['error'])
        self.assertFalse((self.settings.data_dir / 'backups').exists())
        url = '/api/v1/jobs/' + self.id
        self.assertTrue(self.client.get(url, headers=self.headers).json()['can_resume_collection'])
        self.assertEqual(self.client.post(url + '/resume').status_code, 401)
        self.assertEqual(self.client.post(url + '/resume', headers=self.headers).status_code, 202)
        self.assertEqual(self.client.post(url + '/resume', headers=self.headers).status_code, 409)
        self.assertEqual(self.tasks.get(self.id)['request'], task['request'])
        InterruptedClient.broken = False
        self.run_worker()
        finished = self.tasks.get(self.id)
        self.assertEqual(finished['state'], 'succeeded', finished)
        self.assertEqual(InterruptedClient.pages, [1, 2, 2, 3])
        self.assertEqual(len(InterruptedClient.details), 3)
        self.assertEqual(finished['result']['database']['inserted'], 3)
        self.assertEqual(self.client.post(url + '/resume', headers=self.headers).status_code, 409)
    def test_missing_or_completed_result_cannot_resume(self):
        self.tasks.update(self.id, 'failed')
        url = '/api/v1/jobs/' + self.id + '/resume'
        self.assertEqual(self.client.post(url, headers=self.headers).status_code, 409)
        self.tasks.update(self.id, 'queued')
        self.run_worker()
        self.tasks.update(self.id, 'failed', result={'collection': {'complete': True}, 'database': None})
        self.assertEqual(self.client.post(url, headers=self.headers).status_code, 409)
    def test_concurrent_resume_queues_once(self):
        self.run_worker()
        def submit(_):
            try:
                self.tasks.enqueue_resume(self.id, lambda t: validate_resume(self.settings, t))
                return True
            except ValueError:
                return False
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sorted(pool.map(submit, range(2))), [False, True])
    def test_changed_source_schema_preserves_checkpoint(self):
        self.run_worker()
        from nmpa.storage import Store
        raw = next((self.settings.data_dir / 'jobs' / self.id / 'raw').glob('*/*/data.sqlite3'))
        store = Store(raw.parent)
        try:
            with self.assertRaisesRegex(ValueError, '字段结构已变化'):
                store.job(store.db.execute('SELECT item_id FROM jobs').fetchone()[0], '膝', 1, {})
            self.assertEqual(store.db.execute('SELECT COUNT(*) FROM pages').fetchone()[0], 1)
        finally:
            store.close()
