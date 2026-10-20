"""验证 IDE 本地配置优先级、特殊字符密码以及停止后重新启动的锁行为。"""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from sqlalchemy.engine import make_url
from api.config import Settings
from api.worker import worker_lock

class IdeConfigTests(unittest.TestCase):
    def test_local_configuration_overrides_stale_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'settings.json'
            path.write_text(json.dumps({'api_key':'local-test-key','data_dir':directory,
                'mysql':{'username':'test_user','password':'test@#:/secret','database':'ak_medical'}}),encoding='utf-8')
            with patch.dict(os.environ, {'NMPA_SETTINGS_FILE':str(path),'NMPA_API_KEY':'old-test-key'}):
                settings = Settings.from_env()
                self.assertEqual(settings.api_key,'local-test-key')
                self.assertEqual(make_url(settings.database_url).password,'test@#:/secret')
                self.assertEqual(settings.data_dir,Path(directory).resolve())

    def test_null_mysql_clears_stale_connection(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'settings.json'
            path.write_text('{"mysql":null}',encoding='utf-8')
            with patch.dict(os.environ, {'NMPA_SETTINGS_FILE':str(path),'NMPA_DATABASE_URL':'sqlite:///old.db'}):
                self.assertEqual(Settings.from_env().database_url,'')

    def test_worker_lock_blocks_duplicate_and_releases_on_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = Settings(Path(directory),'test-key')
            with worker_lock(settings):
                with self.assertRaises(FileExistsError):
                    with worker_lock(settings):
                        pass
            # 文件仍在，但系统锁已经释放；无需手动删除旧锁文件。
            self.assertTrue((settings.data_dir/'worker.lock').exists())
            with worker_lock(settings):
                pass

if __name__ == '__main__':
    unittest.main()
