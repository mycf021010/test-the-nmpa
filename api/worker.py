# 阅读说明：后台主循环：取得操作系统排他锁后，交替检查普通队列和daily队列。
# 普通任务走process_one；每日小类配额走daily.engine.process_daily。
# 主循环负责接收停止信号、状态落盘、资源释放；不负责Java的每天/每10分钟调度。
# 本文件只增加解释注释，业务代码保持原样。

import argparse
import logging
import os
import signal
import time
from requests.exceptions import RequestException, ConnectionError, Timeout, HTTPError
from contextlib import contextmanager

from .collector import collect
from .config import Settings
from .database import DeviceDatabase
from .models import CollectRequest
from .tasks import TaskStore
from .preview_import import validate_preview, preview_path

LOG = logging.getLogger(__name__)


# 返回False表示普通队列无任务；有任务则执行一份并返回True。采集失败、备份失败和SQL错误各自落盘。
def process_one(settings, tasks, collector=collect, database_factory=DeviceDatabase):
    """领取一个任务，执行采集或预览入库；失败时保存错误状态并关闭数据库连接。"""
    task = tasks.claim()
    if task is None:
        return False
    job_id = task['id']
    database = None
    result = None
    try:
        from .collection_rules import RULE_VERSION
        if task['request'].get('rule_version') != RULE_VERSION:
            raise ValueError('旧规则任务已封存，请用313创建新轮次，保留旧任务供核对')
        # 分支一：已确认试采入库，只读原CSV，不再执行来源采集。
        if task['request'].get('import_existing'):
            validate_preview(settings, task, queued=True)
            database = database_factory(settings)
            result = task['result']
            tasks.update(job_id, 'backing_up', result=result)
            backup = database.backup(job_id + '-preview')
            tasks.update(job_id, 'inserting', progress={'backup_id': backup['id']})
            result['database'] = database.import_csv(preview_path(settings, task), backup,
                                                      preview_path(settings, task).parent)
            tasks.update(job_id, 'succeeded', result=result)
            return True
        # 分支二：正常采集任务；自动入库先检查表结构，以免爬完才发现数据库不可用。
        request = CollectRequest.model_validate(task['request'])
        if not request.dry_run:
            database = database_factory(settings)  # Read-only schema validation before collection.

        # 采集回调，把当前类别、关键词、页码或converting阶段保存到原任务，接口轮询才能看到进度。
        def progress(value):
            """把采集进度写入任务队列，供网页轮询查询。"""
            tasks.update(job_id, 'converting' if value.get('stage') == 'converting' else 'collecting', progress=value)

        output, summary = collector(request, settings.data_dir / 'jobs' / job_id, progress)
        result = {'collection': summary, 'database': None}
        # 试采只保存转换结果；即使字段合格，此分支也不自动写库，等待人工确认接口。
        if request.dry_run:
            tasks.update(job_id, 'succeeded' if summary['complete'] else 'partial', result=result)
            return True
        # 普通全量自动入库要求完整采集；不能把网络中断后的残缺结果直接当全量成功。
        if not summary['complete']:
            tasks.update(job_id, 'partial', result=result, error='采集或字段校验不完整，未执行备份和入库')
            return True
        tasks.update(job_id, 'backing_up', result=result)
        backup = database.backup(job_id)
        tasks.update(job_id, 'inserting', progress={'backup_id': backup['id']})
        result['database'] = database.import_csv(output / 't_medical_device.csv', backup, output)
        tasks.update(job_id, 'succeeded', result=result)
    except KeyboardInterrupt:
        tasks.update(job_id, 'interrupted', result=result, error='worker 已停止；重新提交时按唯一证号避免重复入库')
        raise
    # 把网络、系统、数据库类失败转换为持久任务错误；finally统一关闭数据库，不能只在成功时关闭。
    except Exception as exc:
        # Do not expose connection URLs, driver SQL, credentials or business rows.
        if isinstance(exc, Timeout):
            message = '采集来源请求超时，保留断点；确认网络恢复后可继续采集'
        elif isinstance(exc, ConnectionError):
            message = '采集来源连接失败，保留断点；检查网络、代理和来源网站访问情况'
        elif isinstance(exc, HTTPError):
            code = exc.response.status_code if exc.response is not None else '未知'
            message = f'采集来源返回 HTTP {code}，请求未成功；请检查来源网站提示后再决定是否继续'
        elif isinstance(exc, RequestException):
            message = '采集来源请求失败，保留断点；请检查网络和来源网站访问情况'
        elif isinstance(exc, OSError):
            # 文件/进程系统错误和 MySQL 权限错误分开报告；不展示原始异常参数。
            winerror = getattr(exc, 'winerror', None)
            code = f'WinError={winerror}' if winerror is not None else f'errno={exc.errno}'
            message = f'文件或外部程序操作失败（{code}）；请运行 check_backup.py 检查备份工具和数据目录'
        else:
            message = str(exc) if isinstance(exc, (ValueError, RuntimeError)) else '请检查运行环境、数据库权限或表约束'
        tasks.update(job_id, 'failed', result=result, error=type(exc).__name__ + ': ' + message)
        LOG.error('任务 %s 失败，异常类型 %s', job_id, type(exc).__name__)
    finally:
        if database is not None:
            database.close()
    return True


# 持有data_dir/worker.lock的操作系统锁，保证同一数据目录只有一个worker；宕机由系统释放。
@contextmanager
def worker_lock(settings):
    """操作系统文件锁：IDE 强制停止后自动释放，避免残留 PID 文件阻止重启。"""
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    path = settings.data_dir / 'worker.lock'
    stream = os.fdopen(os.open(path, os.O_CREAT | os.O_RDWR, 0o600), 'r+b')
    locked = False
    try:
        if os.name == 'nt':
            import msvcrt
            if path.stat().st_size == 0:
                stream.write(b'0')
                stream.flush()
            stream.seek(0)
            try:
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise FileExistsError('已有 worker 正在运行，请只保留一个后台任务进程') from exc
        else:
            import fcntl
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise FileExistsError('已有 worker 正在运行') from exc
        locked = True
        stream.seek(0)
        stream.write(str(os.getpid()).encode('ascii'))
        stream.truncate()
        stream.flush()
        yield
    finally:
        if locked:
            if os.name == 'nt':
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        stream.close()
        # 保留锁文件，不删除。删除会导致另一个进程锁住不同 inode，破坏互斥。


# 启动时恢复遗留状态，然后交替检查daily与普通队列；无任务睡1秒，--once只跑一轮领取。
def main():
    """程序入口：解析启动参数并运行当前模块的流程。"""
    parser = argparse.ArgumentParser(description='独立后台 worker；与 API 共享持久化任务队列')
    parser.add_argument('--once', action='store_true', help='只处理一个排队任务，用于调试')
    args = parser.parse_args()
    settings = Settings.from_env()
    tasks = TaskStore(settings.data_dir)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    # 收到SIGTERM抛KeyboardInterrupt，交由外层保存中断状态和清理资源，不直接强杀业务事务。
    def stop(signum, frame):
        """将停止信号转换为中断，让运行中的任务记录停止状态。"""
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, stop)
    try:
        with worker_lock(settings):
            tasks.recover_interrupted()
            daily = None
            if settings.daily_enabled:
                from .daily.store import DailyStore
                from .daily.engine import process_daily
                daily = DailyStore(settings)
                daily.recover()
            # 轮流优先检查daily/普通队列，避免某一队列持续有任务时另一队列长期得不到处理。
            prefer_daily = True
            while True:
                handled = process_daily(daily) if daily and prefer_daily else False
                if not handled:
                    handled = process_one(settings, tasks)
                    if not handled and daily and not prefer_daily:
                        handled = process_daily(daily)
                prefer_daily = not prefer_daily
                if args.once:
                    break
                if not handled:
                    time.sleep(1)
    except KeyboardInterrupt:
        LOG.info('worker 已停止')


if __name__ == '__main__':
    main()
