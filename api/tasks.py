import datetime
import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path


def now():
    """生成统一的 UTC 时间字符串，避免多台机器时区不同。"""
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


class TaskStore:
    """Durable queue shared by API and a separately started single worker."""
    def __init__(self, directory):
        """初始化本对象的依赖与资源；实际职责见类说明及参数。"""
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / 'tasks.sqlite3'
        with self.connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY, state TEXT NOT NULL, request TEXT NOT NULL,
                progress TEXT NOT NULL, result TEXT, error TEXT,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL)''')

    @contextmanager
    def connect(self):
        """连接持久化 SQLite 队列，API 和 worker 通过它共享任务。"""
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def create(self, request):
        """保存一个新任务并返回任务编号，不在 HTTP 请求中执行耗时采集。"""
        job_id = uuid.uuid4().hex
        with self.connect() as db:
            db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?)',
                       (job_id, 'queued', json.dumps(request, ensure_ascii=False), '{}', None, None, now(), now()))
        return self.get(job_id)

    def get(self, job_id):
        """按任务编号读取持久化状态；不存在时返回 None。"""
        with self.connect() as db:
            row = db.execute('SELECT * FROM tasks WHERE id=?', (job_id,)).fetchone()
        if row is None:
            return None
        result = dict(row)
        for key in ('request', 'progress', 'result'):
            result[key] = json.loads(result[key]) if result[key] is not None else None
        return result

    def update(self, job_id, state, progress=None, result=None, error=None):
        """持久化任务状态、进度或结果；仅更新调用方提供的字段。"""
        with self.connect() as db:
            db.execute('''UPDATE tasks SET state=?, progress=COALESCE(?,progress),
                          result=COALESCE(?,result),error=?,updated_at=? WHERE id=?''',
                       (state, json.dumps(progress, ensure_ascii=False) if progress is not None else None,
                        json.dumps(result, ensure_ascii=False) if result is not None else None,
                        error, now(), job_id))

    def enqueue_import(self, job_id, validate):
        """Atomically admit one import of an existing preview."""
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM tasks WHERE id=?', (job_id,)).fetchone()
            if row is None:
                raise LookupError('任务不存在')
            task = dict(row)
            for key in ('request', 'progress', 'result'):
                task[key] = json.loads(task[key]) if task[key] is not None else None
            digest = validate(task)
            request = dict(task['request'], import_existing=True, preview_sha256=digest)
            db.execute('UPDATE tasks SET state=?,request=?,progress=?,error=NULL,updated_at=? WHERE id=?',
                       ('queued', json.dumps(request, ensure_ascii=False),
                        json.dumps({'stage': 'queued', 'operation': 'import_existing'}), now(), job_id))
        return self.get(job_id)

    def enqueue_resume(self, job_id, validate):
        """在一个事务中校验并重新排队，双击或并发提交只会成功一次。"""
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM tasks WHERE id=?', (job_id,)).fetchone()
            if row is None:
                raise LookupError('任务不存在')
            task = dict(row)
            for key in ('request', 'progress', 'result'):
                task[key] = json.loads(task[key]) if task[key] is not None else None
            validate(task)
            db.execute("UPDATE tasks SET state='queued',progress=?,error=NULL,updated_at=? WHERE id=?",
                       (json.dumps({'stage': 'queued', 'operation': 'resume_collection',
                                    'previous_progress': task['progress']}, ensure_ascii=False), now(), job_id))
        return self.get(job_id)

    def claim(self):
        """通过事务领取一个待处理任务，防止同一任务重复执行。"""
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT id FROM tasks WHERE state='queued' ORDER BY created_at LIMIT 1").fetchone()
            if row is None:
                return None
            db.execute("UPDATE tasks SET state='collecting',updated_at=? WHERE id=?", (now(), row['id']))
        return self.get(row['id'])

    def recover_interrupted(self):
        """启动时把上次停止时未完成的任务标记为中断，避免假装仍在运行。"""
        with self.connect() as db:
            db.execute("""UPDATE tasks SET state='interrupted',error=?,updated_at=?
                          WHERE state IN ('collecting','converting','backing_up','inserting')""",
                       ('上次 worker 意外结束，请重新提交任务；已入库数据由唯一键防止重复', now()))
