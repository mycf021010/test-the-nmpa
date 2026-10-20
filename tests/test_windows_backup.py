"""回归 Windows 备份同步句柄及外部程序错误提示，不调用真实 mysqldump。"""
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from sqlalchemy.engine import URL
from api.config import Settings
from api.database import DeviceDatabase
from test_api import create_test_table

class WindowsBackupTests(unittest.TestCase):
    def test_backup_sync_handle_is_writable_without_truncating_data(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            original = root / 'original.sqlite3'
            create_test_table(original)
            database = DeviceDatabase(Settings(root/'service','test-key','sqlite:///'+str(original)))
            fsync = os.fsync
            def require_writable_handle(fd):
                # 0 字节写入不改变内容；只读句柄会失败，模拟 Windows 同步的访问要求。
                os.write(fd,b'')
                fsync(fd)
            try:
                with patch('api.database.os.fsync', side_effect=require_writable_handle):
                    backup = database.backup('writable-handle-test')
                database.verify_backup(backup)
                self.assertGreater(backup['bytes'],0)
            finally:
                database.close()

    def test_mysqldump_launch_error_exposes_code_but_not_raw_exception(self):
        with tempfile.TemporaryDirectory() as folder:
            database = object.__new__(DeviceDatabase)
            database.settings = Settings(Path(folder),'test-key',mysqldump='missing-dump.exe')
            database.engine = SimpleNamespace(url=URL.create('mysql+pymysql',username='test-user',
                password='do-not-expose-password',database='ak_medical'))
            with patch('api.database.subprocess.run',side_effect=OSError(2,'do-not-expose-password')):
                with self.assertRaises(RuntimeError) as caught:
                    database._mysql_dump(Path(folder)/'backup.tmp')
            self.assertIn('errno=2',str(caught.exception))
            self.assertNotIn('do-not-expose-password',str(caught.exception))

if __name__ == '__main__':
    unittest.main()
