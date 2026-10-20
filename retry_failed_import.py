"""重试失败的全量入库或已确认的试采入库；复用原 CSV，不重新采集。"""
import argparse
import csv
import json
import re
import uuid
from api.config import Settings
from api.database import DeviceDatabase
from api.tasks import TaskStore
from api.worker import worker_lock
from api.preview_import import validate_preview
from export_medical_device import FIELDS


def retry(settings, job_id, database_factory=DeviceDatabase):
    if not re.fullmatch('[a-f0-9]{32}', job_id):
        raise ValueError('任务编号格式不正确')
    tasks = TaskStore(settings.data_dir)
    with worker_lock(settings):
        task = tasks.get(job_id)
        if task is None:
            raise ValueError('找不到原任务，请检查数据目录')
        result = task.get('result') or {}
        summary = result.get('collection') or {}
        if task['state'] not in ('failed', 'interrupted') or result.get('database') is not None:
            raise ValueError('只允许重试尚无入库结果的失败/中断任务')
        if task['request'].get('dry_run'):
            # 必须是用户已点击过确认入库的试采；再次验证原始审计和确认时的文件哈希。
            if not task['request'].get('import_existing'):
                raise ValueError('此试采尚未确认入库，请通过页面确认')
            validate_preview(settings, task, queued=True)
        elif not summary.get('complete') or summary.get('issues') or summary.get('validation_errors'):
            raise ValueError('仅允许完整采集且校验通过的全量任务')
        output = settings.data_dir / 'jobs' / job_id / 'database'
        candidate = output / 't_medical_device.csv'
        with candidate.open(encoding='utf-8-sig', newline='') as stream:
            reader = csv.DictReader(stream)
            if reader.fieldnames != FIELDS:
                raise ValueError('原结果字段不匹配')
            if sum(1 for _ in reader) != summary.get('valid_count'):
                raise ValueError('原结果数量与任务报告不一致')
        db = None
        try:
            db = database_factory(settings)
            tasks.update(job_id, 'backing_up', result=result)
            backup = db.backup(job_id + '-retry-' + uuid.uuid4().hex[:8])
            tasks.update(job_id, 'inserting', progress={'backup_id': backup['id']})
            result['database'] = db.import_csv(candidate, backup, output)
            tasks.update(job_id, 'succeeded', result=result)
            return result['database']
        except BaseException as exc:
            code = getattr(getattr(exc, 'orig', None), 'args', [None])[0]
            message = type(exc).__name__ + (f' MySQL错误码：{code}' if isinstance(code, int) else '')
            tasks.update(job_id, 'failed', result=result, error=message)
            raise
        finally:
            if db is not None:
                db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('job_id')
    args = parser.parse_args()
    try:
        result = retry(Settings.from_env(), args.job_id)
        print('入库完成：')
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except FileExistsError:
        print('Worker 仍在运行，请先在 PyCharm 停止 Worker，再运行重试入口。')
        raise SystemExit(1)
    except Exception as exc:
        code = getattr(getattr(exc, 'orig', None), 'args', [None])[0]
        print('失败类型：', type(exc).__name__)
        if isinstance(code, int):
            print('MySQL错误码：', code)
        elif isinstance(exc, (ValueError, FileNotFoundError)):
            print(str(exc))
        raise SystemExit(1)


if __name__ == '__main__':
    main()
