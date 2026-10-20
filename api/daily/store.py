"""每日限量任务控制库：短事务、幂等请求、独立轮次与流断点。"""
import json
import sqlite3
import uuid
from contextlib import contextmanager
from .contracts import DailyError, digest, dumps, now

SCHEMA_VERSION = 1
CATEGORIES = ('domestic', 'domestic_history', 'imported', 'imported_history')
KEYWORDS = ('膝', '髋')
SCHEMA = '''
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS rounds(id TEXT PRIMARY KEY,page_size INTEGER NOT NULL,state TEXT NOT NULL,created_at TEXT NOT NULL,completed_at TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_round ON rounds(state) WHERE state='active';
CREATE TABLE IF NOT EXISTS streams(round_id TEXT NOT NULL,category TEXT NOT NULL,keyword TEXT NOT NULL,page INTEGER NOT NULL DEFAULT 1,
 exhausted INTEGER NOT NULL DEFAULT 0,item_id TEXT,schema_json TEXT,PRIMARY KEY(round_id,category,keyword));
CREATE TABLE IF NOT EXISTS requests(id TEXT PRIMARY KEY,action TEXT NOT NULL,hash TEXT NOT NULL,result TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,daily_task_id TEXT NOT NULL,subtask_id TEXT NOT NULL UNIQUE,
 round_id TEXT NOT NULL,category TEXT NOT NULL,keyword TEXT NOT NULL,target INTEGER NOT NULL,target_dataset_id TEXT NOT NULL,state TEXT NOT NULL,
 collection_complete INTEGER NOT NULL DEFAULT 0,source_exhausted INTEGER NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,
 retry_at TEXT,error_code TEXT,error_message TEXT,result TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS one_daily_quota ON jobs(daily_task_id,category,keyword);
CREATE TABLE IF NOT EXISTS pages(round_id TEXT NOT NULL,category TEXT NOT NULL,keyword TEXT NOT NULL,page INTEGER NOT NULL,
 response TEXT NOT NULL,PRIMARY KEY(round_id,category,keyword,page));
CREATE TABLE IF NOT EXISTS records(round_id TEXT NOT NULL,category TEXT NOT NULL,keyword TEXT NOT NULL,source_id TEXT NOT NULL,
 page INTEGER NOT NULL,ordinal INTEGER NOT NULL,raw TEXT NOT NULL,detail TEXT,normalized TEXT,errors TEXT,job_id TEXT,
 PRIMARY KEY(round_id,category,keyword,source_id));
CREATE INDEX IF NOT EXISTS member_records ON records(job_id,page,ordinal);
CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT NOT NULL,created_at TEXT NOT NULL,kind TEXT NOT NULL,message TEXT NOT NULL);
'''


class DailyStore:
    """API与单worker共用的本地SQLite控制库；禁止跨主机共享SQLite文件。"""
    def __init__(self, settings, migrate=False):
        """显式迁移时建库；正常启动只读检查版本，不自动修改旧任务库。"""
        self.settings = settings
        self.path = settings.data_dir / 'daily-a.sqlite3'
        if migrate:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.connect() as db:
                db.executescript(SCHEMA)
                db.execute('INSERT OR IGNORE INTO meta VALUES(?,?)', ('version', str(SCHEMA_VERSION)))
                db.execute('INSERT OR IGNORE INTO meta VALUES(?,?)', ('instance_id', uuid.uuid4().hex))
        if not self.path.exists():
            raise DailyError('MIGRATION_REQUIRED', '请先显式执行A方案控制库迁移', 503)
        with self.connect() as db:
            try:
                version = db.execute("SELECT value FROM meta WHERE key='version'").fetchone()
                self.instance_id = db.execute("SELECT value FROM meta WHERE key='instance_id'").fetchone()[0]
                if not version or int(version[0]) != SCHEMA_VERSION:
                    raise ValueError()
            except (sqlite3.Error, TypeError, ValueError):
                raise DailyError('SCHEMA_VERSION', 'A方案控制库版本不符，请核对数据目录和迁移版本', 503) from None

    @contextmanager
    def connect(self):
        """提供有超时、外键校验及提交/回滚的短写事务，不在网络等待时持锁。"""
        db = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('PRAGMA busy_timeout=15000')
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def require(row):
        """将不存在的控制记录转换为稳定404领域错误。"""
        if row is None:
            raise DailyError('NOT_FOUND', '任务或轮次不存在', 404)
        return row

    def replay(self, db, request_id, action, payload):
        """校验同编号动作与参数，返回原确认结果，避免超时后重复创建。"""
        prior = db.execute('SELECT * FROM requests WHERE id=?', (request_id,)).fetchone()
        if prior and (prior['action'] != action or prior['hash'] != digest(payload)):
            raise DailyError('REQUEST_CONFLICT', '同一请求编号不能使用不同动作或参数')
        return json.loads(prior['result']) if prior else None

    def remember(self, db, request_id, action, payload, result):
        """与状态创建同事务保存请求确认，保证幂等查询可恢复响应。"""
        db.execute('INSERT INTO requests VALUES(?,?,?,?)', (request_id, action, digest(payload), dumps(result)))
        return result

    def open_round(self, request):
        """幂等返回未完成轮，或在上一轮结束后创建新的8个独立断点。"""
        payload = request.model_dump(mode='json')
        with self.connect() as db:
            previous = self.replay(db, request.request_id, 'round', payload)
            if previous:
                return previous
            current = db.execute("SELECT * FROM rounds WHERE state='active'").fetchone()
            if current and current['page_size'] != request.page_size:
                raise DailyError('CONFIG_CHANGED', '当前轮每页条数不可修改，完成本轮后再调整')
            if not current:
                round_id = uuid.uuid4().hex
                db.execute('INSERT INTO rounds VALUES(?,?,?,?,NULL)', (round_id, request.page_size, 'active', now()))
                db.executemany('INSERT INTO streams(round_id,category,keyword) VALUES(?,?,?)',
                               [(round_id, category, keyword) for category in CATEGORIES for keyword in KEYWORDS])
            else:
                round_id = current['id']
            return self.remember(db, request.request_id, 'round', payload, {'round_id': round_id, 'source_instance_id': self.instance_id})

    def round(self, round_id):
        """返回轮次及8个流的可核对摘要，不暴露完整来源详情。"""
        with self.connect() as db:
            result = dict(self.require(db.execute('SELECT * FROM rounds WHERE id=?', (round_id,)).fetchone()))
            result['streams'] = [dict(row) for row in db.execute('SELECT * FROM streams WHERE round_id=? ORDER BY category,keyword', (round_id,))]
            for stream in result['streams']:
                stream.pop('schema_json', None)
            return result

    def create_job(self, request):
        """为单个小类创建固定配额；全局串行资格与幂等确认同时提交。"""
        from .ledger import target_identity
        payload = request.model_dump(mode='json')
        with self.connect() as db:
            previous = self.replay(db, request.request_id, 'job', payload)
            if previous:
                return previous
            round_row = self.require(db.execute('SELECT * FROM rounds WHERE id=?', (request.round_id,)).fetchone())
            if round_row['state'] != 'active':
                raise DailyError('ROUND_COMPLETED', '该轮已经完成，不能再追加每日配额')
            if db.execute("SELECT 1 FROM jobs WHERE state NOT IN ('succeeded','succeeded_with_issues') LIMIT 1").fetchone():
                raise DailyError('JOB_ACTIVE', '上一子任务尚未完成或待处理，禁止并发启动下一项')
            if db.execute('SELECT 1 FROM jobs WHERE daily_task_id=? AND category=? AND keyword=?', (request.daily_task_id,request.category.value,request.keyword.value)).fetchone():
                raise DailyError('DAILY_QUOTA_EXISTS', '同一每日任务的小类不能重新分配第二份配额')
            if db.execute('SELECT 1 FROM jobs WHERE subtask_id=?', (request.subtask_id,)).fetchone():
                raise DailyError('SUBTASK_EXISTS', '该子任务已绑定其他请求编号')
            self.require(db.execute('SELECT * FROM streams WHERE round_id=? AND category=? AND keyword=?',
                                   (request.round_id, request.category.value, request.keyword.value)).fetchone())
            job_id = uuid.uuid4().hex
            timestamp = now()
            db.execute('''INSERT INTO jobs(id,request_id,daily_task_id,subtask_id,round_id,category,keyword,target,target_dataset_id,state,created_at,updated_at)
                          VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''', (job_id,request.request_id,request.daily_task_id,request.subtask_id,
                          request.round_id,request.category.value,request.keyword.value,request.target_records,target_identity(self.settings),'queued',timestamp,timestamp))
            return self.remember(db, request.request_id, 'job', payload, {'job_id': job_id, 'state': 'queued'})

    def job(self, job_id):
        """读取真实处理计数、最终入库结果和阶段，供Java轮询。"""
        with self.connect() as db:
            result = dict(self.require(db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()))
            counts = db.execute('SELECT COUNT(*),COALESCE(SUM(errors != ?),0) FROM records WHERE job_id=?', ('[]',job_id)).fetchone()
            result.update(processed=counts[0], invalid=counts[1], result=json.loads(result['result']) if result['result'] else None)
            return result

    def request(self, request_id):
        """在Java响应丢失后按原编号核对Python是否已经受理。"""
        with self.connect() as db:
            row = self.require(db.execute('SELECT result FROM requests WHERE id=?', (request_id,)).fetchone())
            return json.loads(row['result'])

    def event(self, job_id, kind, message):
        """记录不含凭据的结构化进度事件；详情落盘后才记录成功。"""
        with self.connect() as db:
            db.execute('INSERT INTO events(job_id,created_at,kind,message) VALUES(?,?,?,?)', (job_id,now(),kind,message))

    def recover(self):
        """仅在持有原worker操作系统排他锁后，恢复上次意外退出的在途任务。"""
        with self.connect() as db:
            db.execute("UPDATE jobs SET state='queued',updated_at=? WHERE state IN ('collecting','backing_up','inserting','interrupted')", (now(),))

    def resume(self, job_id):
        """显式恢复被阻塞的原任务，保留配额、已采数据及入库编号。"""
        with self.connect() as db:
            row = self.require(db.execute('SELECT state FROM jobs WHERE id=?', (job_id,)).fetchone())
            if row['state'] not in ('blocked','interrupted','retry_wait','queued'):
                raise DailyError('INVALID_STATE', '当前任务不能恢复')
            db.execute("UPDATE jobs SET state='queued',attempts=0,retry_at=NULL,updated_at=? WHERE id=?", (now(),job_id))
        return {'job_id': job_id, 'state': 'queued'}
