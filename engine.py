# 阅读说明：daily后台执行核心：领取一个配额→检查目标库→继续断点采集→固定快照→备份→事务入库。
# 采集完成与入库完成独立记录；数据库阶段失败不必重新采集全部详情。
# 仅确认MySQL账本提交后才记succeeded，网络不确定时保留原编号核对。
# 本文件只增加解释注释，业务代码保持原样。

"""单worker执行A方案小类；采集与独立入库阶段可分别恢复。"""
import json
import logging
from datetime import datetime, timedelta, timezone
from requests.exceptions import ConnectionError, Timeout, HTTPError
from sqlalchemy.exc import OperationalError
from export_medical_device import atomic
from api.collection_rules import RULE_VERSION, FIELD_SCHEMA_VERSION
from .checkpoints import Checkpoints
from .contracts import DailyError, digest, dumps, now
from .ledger import LedgerDatabase, target_identity
from .normalization import merge_candidates

LOG = logging.getLogger(__name__)


# 处理到job.target个来源详情或本流耗尽；filtered/invalid也占来源额度，但不会成为合格入库候选。
def collect_quota(store, job, client_factory):
    """精确处理剩余目标条数，保存第9页第4条等页内位置，不伪造全源完成。"""
    checkpoints = Checkpoints(store,job)
    stream = checkpoints.stream()
    if not stream['exhausted']:
        with store.connect() as db:
            size = db.execute('SELECT page_size FROM rounds WHERE id=?',(job['round_id'],)).fetchone()[0]
        with client_factory(interval=1.0) as client:
            item_id,schema = checkpoints.identify(client)
            # 配额单位是已保存来源详情条数，不是最终新增条数；筛掉非313和已有证号会使新增少于目标。
            while store.job(job['id'])['processed'] < job['target']:
                stream = checkpoints.stream()
                if stream['exhausted']:
                    break
                data = checkpoints.page(client,item_id,schema,stream['page'],size)
                for record in checkpoints.pending(stream['page']):
                    if store.job(job['id'])['processed'] >= job['target']:
                        break
                    errors=checkpoints.save_detail(record,client.detail(item_id,record['source_id']),schema)
                    count = store.job(job['id'])['processed']
                    message = f"{job['category']}/{job['keyword']} 第{stream['page']}页详情已落盘，处理{count}/{job['target']}；"+('校验失败：'+'；'.join(errors[:5]) if errors else f"已判定；正常过滤{store.job(job['id'])['filtered']}条")
                    store.event(job['id'],'detail_saved',message)
                    LOG.info('daily=%s subtask=%s round=%s job=%s %s',job['daily_task_id'],job['subtask_id'],job['round_id'],job['id'],message)
                # 可能在页中达到target；advance见本页尚有空详情便不翻页，下次从本页剩余详情开始。
                checkpoints.advance(stream['page'],data['total'],size)
    exhausted = checkpoints.stream()['exhausted']
    with store.connect() as db:
        db.execute("UPDATE jobs SET collection_complete=1,source_exhausted=?,state='backing_up',updated_at=? WHERE id=?",(exhausted,now(),job['id']))
    store.event(job['id'],'quota_complete','本批配额处理结束；来源耗尽状态独立保存')


# 若已有snapshot.json则验证摘要和规则后沿用；否则合并本job有效候选并固定目标身份/来源关联。
def frozen_snapshot(store,job):
    """固定本日有效候选及目标身份；已保存快照恢复时校验摘要而不重建。"""
    path = store.settings.data_dir/'daily-imports'/job['id']/'snapshot.json'
    # 断点恢复读取冻结快照，禁止用后来的业务数据悄悄重建同一入库操作的候选。
    if path.exists():
        snapshot = json.loads(path.read_text(encoding='utf-8'))
        if snapshot['payload'].get('rule_version') != RULE_VERSION or snapshot['payload'].get('field_schema_version') != FIELD_SCHEMA_VERSION:
            raise DailyError('RULE_VERSION_CHANGED','旧规则快照不能由新版重新解释')
        if digest(snapshot['payload']) != snapshot['hash']:
            raise DailyError('SNAPSHOT_CHANGED','固定入库文件摘要不符，禁止重建后继续')
        return snapshot
    with store.connect() as db:
        candidates = [dict(row) for row in db.execute('SELECT * FROM records WHERE job_id=? AND errors=? AND filter_state="eligible" ORDER BY page,ordinal',(job['id'],'[]'))]
    members,conflicts = merge_candidates([{'row':json.loads(row['normalized']),'category':row['category'],'keyword':row['keyword'],'source_id':row['source_id']} for row in candidates])
    payload = {'rule_version':RULE_VERSION,'field_schema_version':FIELD_SCHEMA_VERSION,'source_instance_id':store.instance_id,'target_dataset_id':job['target_dataset_id'],'members':members,'merge_conflicts':conflicts}
    snapshot = {'payload':payload,'hash':digest(payload)}
    atomic(path,dumps(snapshot))
    return snapshot


# 先用原operation_id核对MySQL账本；已提交复用原结果，未提交才备份和写库，最后回写SQLite成功状态。
def import_collected(store,job,database_factory):
    """先核账本再备份入库；COMMIT不确定后复用原编号恢复，不重新采集。"""
    snapshot = frozen_snapshot(store,job)
    if snapshot['payload']['target_dataset_id'] != target_identity(store.settings):
        raise DailyError('TARGET_CHANGED','原任务目标库发生变化，禁止将旧候选写入新库')
    result = {'inserted':0,'skipped_existing':0,'changed_existing':0,'body_part_updated':0,'collection_tag_updated':0,'derived_updated':0,'backup_id':None}
    # 有效候选为空时不写MySQL账本；仍汇总本配额的processed/filtered/invalid并结束采集任务。
    if snapshot['payload']['members']:
        database = database_factory(store.settings)
        try:
            # 先查与器械写入同事务的MySQL批次：解决SQL已COMMIT但客户端没收到确认的情况。
            prior = database.lookup(store.instance_id,job['request_id'])
            if prior:
                if prior['snapshot_hash'] != snapshot['hash']:
                    raise DailyError('LEDGER_CONFLICT','原入库编号绑定的快照不一致')
                result = prior['result_json']
            else:
                backup = database.backup('daily-'+job['id'])
                with store.connect() as db:
                    db.execute("UPDATE jobs SET state='inserting',updated_at=? WHERE id=?",(now(),job['id']))
                result = database.import_snapshot({'id':job['id'],'operation_id':job['request_id']},snapshot,backup)
        finally:
            database.close()
    result = {**result,'processed':store.job(job['id'])['processed'],'invalid':store.job(job['id'])['invalid'],
              'merge_conflicts':len(snapshot['payload']['merge_conflicts']),
              'filtered':store.job(job['id'])['filtered'],'valid':store.job(job['id'])['eligible'],'eligible':store.job(job['id'])['eligible'],
              'rule_version':RULE_VERSION,'field_schema_version':FIELD_SCHEMA_VERSION,
              'eligible_unique':len(snapshot['payload']['members'])}
    with store.connect() as db:
        state = 'succeeded_with_issues' if result['invalid'] or result['merge_conflicts'] else 'succeeded'
        db.execute('UPDATE jobs SET state=?,result=?,error_code=NULL,error_message=NULL,updated_at=? WHERE id=?',(state,dumps(result),now(),job['id']))
        if not db.execute('SELECT 1 FROM streams WHERE round_id=? AND exhausted=0',(job['round_id'],)).fetchone():
            if not db.execute("SELECT 1 FROM jobs WHERE round_id=? AND state NOT IN ('succeeded','succeeded_with_issues')",(job['round_id'],)).fetchone():
                issues = db.execute("SELECT 1 FROM jobs WHERE round_id=? AND state='succeeded_with_issues'",(job['round_id'],)).fetchone()
                db.execute('UPDATE rounds SET state=?,completed_at=? WHERE id=?',('completed_with_issues' if issues else 'completed',now(),job['round_id']))
        db.execute('INSERT INTO events(job_id,created_at,kind,message) VALUES(?,?,?,?)',
                   (job['id'],now(),'import_confirmed',f"入库确认：新增{result['inserted']}，已有{result['skipped_existing']}，部位补全{result.get('body_part_updated',0)}"))


    if result['invalid'] or result['merge_conflicts']:
        LOG.warning('daily=%s subtask=%s job=%s 带说明完成：有效=%s 无效=%s 合并冲突=%s',job['daily_task_id'],job['subtask_id'],job['id'],result['valid'],result['invalid'],result['merge_conflicts'])
    LOG.info('daily=%s subtask=%s job=%s 入库确认：新增=%s 已有=%s 部位补全=%s',job['daily_task_id'],job['subtask_id'],job['id'],result['inserted'],result['skipped_existing'],result.get('body_part_updated',0))


# 从到期queued/retry_wait领取一份配额，完成采集及入库；返回False表示无到期任务，有处理返回True。
def process_daily(store,client_factory=None,database_factory=LedgerDatabase):
    """原worker持排他锁调用；每次领取一个到期任务，失败退避有上限且不泄漏凭据。"""
    with store.connect() as db:
        row = db.execute("SELECT * FROM jobs WHERE state='queued' OR (state='retry_wait' AND retry_at<=?) ORDER BY created_at LIMIT 1",(now(),)).fetchone()
        if not row:
            return False
        job = dict(row)
        db.execute("UPDATE jobs SET state=?,attempts=attempts+1,updated_at=? WHERE id=?",('backing_up' if job['collection_complete'] else 'collecting',now(),job['id']))
    try:
        if client_factory is None:
            from nmpa.pure import PureNMPAClient
            client_factory = PureNMPAClient
        if job['target_dataset_id'] != target_identity(store.settings):
            raise DailyError('TARGET_CHANGED','原任务目标库配置已变化，禁止自动改投新库')
        if not job['collection_complete']:
            preflight=database_factory(store.settings)
            preflight.close()
            collect_quota(store,job,client_factory)
        import_collected(store,job,database_factory)
    # 停止进程记interrupted并保留已采详情，向上继续抛出让worker主循环安全退出。
    except KeyboardInterrupt:
        with store.connect() as db:
            db.execute("UPDATE jobs SET state='interrupted',error_code='WORKER_STOPPED',error_message='执行进程停止，保留断点',updated_at=? WHERE id=?",(now(),job['id']))
        raise
    # 异常分为有上限的暂时重试和blocked；最终处置查看下面error_code及attempts，不把失败写成成功。
    except Exception as error:
        retryable = isinstance(error,(ConnectionError,Timeout,OperationalError)) or (isinstance(error,HTTPError) and error.response is not None and error.response.status_code in (500,502,503,504))
        attempts = job['attempts']+1
        retry = retryable and attempts < store.settings.daily_max_attempts
        state = 'retry_wait' if retry else 'blocked'
        code = error.code if isinstance(error,DailyError) else ('TEMPORARY_FAILURE' if retryable else 'EXECUTION_FAILED')
        message = error.message if isinstance(error,DailyError) else '采集或入库失败，断点和原提交编号已保留，请检查网络、表约束、备份及服务日志'
        retry_at = (datetime.now(timezone.utc)+timedelta(seconds=min(store.settings.daily_retry_max_seconds,store.settings.daily_retry_seconds*2**min(attempts-1,16)))).isoformat() if retry else None
        with store.connect() as db:
            db.execute('UPDATE jobs SET state=?,error_code=?,error_message=?,retry_at=?,updated_at=? WHERE id=?',(state,code,message,retry_at,now(),job['id']))
        LOG.error('daily=%s subtask=%s job=%s code=%s type=%s state=%s',job['daily_task_id'],job['subtask_id'],job['id'],code,type(error).__name__,state)
        store.event(job['id'],'error',message)
    return True
