# 阅读说明：普通任务的采集恢复检查，只接受保留断点、尚未进入最终结果/入库阶段的失败或中断任务。
# 恢复保持原类别、关键词和任务编号；入库阶段失败另走retry_failed_import.py。
# 本文件只增加解释注释，业务代码保持原样。

"""采集中断恢复资格；不用于备份/入库失败重试，不改变采集范围。"""
import sqlite3
from .models import CollectRequest


# 验证普通任务状态、原规则和只读SQLite断点；通过时不修改数据，后续enqueue_resume才会重新排队。
def validate_resume(settings, task):
    """只恢复保留断点且未生成最终结果的失败任务，避免重复入库。"""
    from .collection_rules import RULE_VERSION
    if task.get('request', {}).get('rule_version') != RULE_VERSION:
        raise ValueError('旧膝髋任务不能按313继续；请创建新的313任务')
    if task['state'] not in ('failed', 'interrupted'):
        raise ValueError('仅采集失败或中断的任务可以继续')
    if task['request'].get('import_existing') or task.get('result') is not None or task.get('progress', {}).get('backup_id'):
        raise ValueError('此任务已进入结果或入库阶段，请使用入库重试流程')
    request = CollectRequest.model_validate(task['request'])
    if not request.dry_run and not settings.database_url:
        raise ValueError('尚未配置数据库，不能继续自动入库任务')
    raw = settings.data_dir / 'jobs' / task['id'] / 'raw'
    checkpoints = list(raw.glob('*/*/data.sqlite3'))
    if not checkpoints:
        raise ValueError('未找到采集断点，请检查原数据目录或创建新任务')
    # 只读检查，不修改原始断点；损坏文件不能当作可恢复数据。
    try:
        for path in checkpoints:
            with sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True) as db:
                if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                    raise ValueError('采集断点损坏，请保留文件后检查')
                db.execute('SELECT job FROM jobs LIMIT 1').fetchone()
    except sqlite3.Error as exc:
        raise ValueError('采集断点无法读取，请检查数据目录') from exc
