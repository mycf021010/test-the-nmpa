# 阅读说明：普通任务的采集流水线：按类别逐一采集列表和详情，落盘断点，再做标准字段导出。
# 本文件不直接写MySQL；API普通任务的worker在采集完成后调用DeviceDatabase入库。
# daily每日配额走api/daily/engine.py，不能把两条任务链的断点目录混用。
# 本文件只增加解释注释，业务代码保持原样。

"""HTTP collector -> four-category snapshots -> validated database CSV.

Export is offline; collection imports the original transport only when needed.
"""
import argparse
import datetime
import json
import logging
import math
import os
import signal
import uuid
from contextlib import contextmanager
from pathlib import Path

from export_medical_device import CATS, atomic, export
from nmpa.storage import Store

LOG = logging.getLogger(__name__)


# 用排他创建.pipeline.lock防止两个命令写同一批次目录；它不同于worker的操作系统文件锁。
@contextmanager
def batch_lock(batch):
    """锁定本批输出目录，避免两个采集流程同时改同一批文件。"""
    path = batch / '.pipeline.lock'
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(fd, 'w') as stream:
            stream.write(str(os.getpid()))
        yield
    # 即使被停止也尝试导出已采数据；保留部分结果不表示完整性已经通过。
    finally:
        path.unlink(missing_ok=True)


# 把原始SQLite详情适配成normalized类别JSON，并附总数、限页、详情失败及原始审计地址。
def snapshot(store, job, category, keyword, destination, limited=False):
    """Adapt audited SQLite details to the existing conversion contract."""
    report = store.audit(job)
    rows = []
    missing = 0
    for fields, detail in store.db.execute(
        'SELECT fields,detail_fields FROM records WHERE job=? ORDER BY page,id', (job,)
    ):
        row = json.loads(fields)
        if detail is None:
            row['_detail_error'] = '详情尚未完成'
            missing += 1
        else:
            row.update(json.loads(detail))
        rows.append(row)
    complete = report['complete']
    document = {
        'metadata': {
            'category': category, 'searchValue': keyword,
            'siteTotal': report['expected_total'], 'total': len(rows),
            'listComplete': complete, 'detailsFail': missing,
            'limitPerCategory': len(rows) if limited and rows else None,
            'collectionErrors': [] if complete else ['原始采集完整性检查未通过'],
            'rawReport': str(store.output / (job + '.report.json')),
        },
        'data': rows,
    }
    atomic(destination / (category + '.json'), json.dumps(document, ensure_ascii=False, indent=2))
    return report


# 单类别+关键词循环分页：已有列表复用，未完成详情继续拉取，每页导出；finally尽量保留已完成数据。
def collect_job(client, item, keyword, output, max_pages, page_size, on_progress=None):
    """按类别和关键词分页采集，保存列表与详情断点。"""
    with_store = Store(output)
    job = None
    try:
        schema = client.schema(item['itemId'])
        # 本目录属于原任务的一个类别和关键词，来源 ID 改变时不能混入旧数据。
        identities = with_store.db.execute('SELECT item_id,keyword,page_size FROM jobs').fetchall()
        if any(tuple(row) != (item['itemId'], keyword, page_size) for row in identities):
            raise ValueError('来源类别已变化，不能复用旧断点，请创建新任务')
        job = with_store.job(item['itemId'], keyword, page_size, schema)
        page = 1
        while max_pages == 0 or page <= max_pages:
            response = with_store.page(job, page)
            if response is None:
                response = client.search(item['itemId'], keyword, page, page_size)
                with_store.save_page(job, page, response, schema)
            data = response['data']
            LOG.info('%s / %s，第 %s 页，网站总数 %s', keyword, item['itemName'], page, data['total'])
            for record_id in with_store.pending_details(job, page):
                with_store.save_detail(job, record_id, client.detail(item['itemId'], record_id), schema)
            with_store.export(job)
            if on_progress:
                on_progress({'category': item['itemName'], 'keyword': keyword,
                             'page': page, 'total': data['total'],
                             'expected_pages': max(1, math.ceil(data['total'] / page_size))})
            if page >= max(1, math.ceil(data['total'] / page_size)):
                break
            if not data['list']:
                raise RuntimeError('尚未到末页却返回空列表')
            page += 1
    # 即使被停止也尝试导出已采数据；保留部分结果不表示完整性已经通过。
    finally:
        try:
            if job is not None:
                with_store.export(job)
                snapshot(with_store, job, item['itemName'], keyword, output.parent.parent.parent / 'normalized' / keyword,
                         limited=max_pages > 0)
        finally:
            with_store.close()


# 读取batch.json确认采集状态，将所有关键词目录转成标准CSV；输出导出路径和完整性摘要。
def export_batch(batch, baseline=None, output=None):
    """将已采集详情转换为标准字段，并写出 CSV 与校验报告。"""
    manifest = json.loads((batch / 'batch.json').read_text(encoding='utf-8'))
    issues = list(manifest.get('issues', []))
    if manifest.get('state') != 'collected':
        issues.append('采集任务未成功完成：' + manifest.get('state', 'unknown'))
    if manifest['max_pages'] > 0:
        issues.append('本次为限页试采，不能作为完整入库批次')
    destination = Path(output) if output else batch / ('database_' + uuid.uuid4().hex[:8])
    summary = export([batch / 'normalized' / kw for kw in manifest['keywords']],
                     destination, baseline, issues)
    atomic(batch / 'latest_export.json', json.dumps({'output': str(destination.resolve()), **summary},
                                                  ensure_ascii=False, indent=2))
    return destination, summary


# 命令行四类别串行入口，固定批次身份并拒绝更换来源ID；异常记录failed/interrupted，保留断点后导出。
def collect_batch(batch, keywords, max_pages=0, page_size=10, proxy=None, interval=1,
                  baseline=None, client_factory=None):
    """组织各类别关键词的采集、断点恢复和最终导出。"""
    from api.collection_rules import RULE_VERSION, FIELD_SCHEMA_VERSION
    if keywords != ['313']:
        raise ValueError('新规则仅允许313搜索')
    batch = Path(batch).resolve()
    batch.mkdir(parents=True, exist_ok=True)
    with batch_lock(batch):
        identity = {'rule_version':RULE_VERSION,'field_schema_version':FIELD_SCHEMA_VERSION,'keywords': keywords, 'max_pages': max_pages, 'page_size': page_size}
        manifest_path = batch / 'batch.json'
        if manifest_path.exists():
            previous = json.loads(manifest_path.read_text(encoding='utf-8'))
            if any(previous.get(k) != v for k, v in identity.items()):
                raise ValueError('同一批次续抓必须使用相同关键词、page-size 和 max-pages')
        manifest = {**identity, 'state': 'running', 'issues': []}
        if manifest_path.exists() and previous.get('item_ids'):
            manifest['item_ids'] = previous['item_ids']
        atomic(manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2))
        try:
            if client_factory is None:
                from nmpa.pure import PureNMPAClient
                client_factory = PureNMPAClient
            with client_factory(proxy=proxy, interval=interval) as client:
                categories = client.categories()
                items = {}
                for category in CATS:
                    matches = [c for c in categories if c.get('itemName') == category and c.get('itemType') == 'table']
                    if len(matches) != 1:
                        raise RuntimeError('无法唯一识别必需类目：' + category)
                    items[category] = matches[0]
                # Never resume a checkpoint against a different category table.
                ids = {cat: item['itemId'] for cat, item in items.items()}
                if manifest_path.exists() and 'previous' in locals() and previous.get('item_ids', ids) != ids:
                    raise ValueError('网站类目 ID 已变化，请使用新的批次目录')
                manifest['item_ids'] = ids
                atomic(manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2))
                for keyword in keywords:
                    for index, category in enumerate(CATS):
                        output = batch / 'raw' / keyword / str(index)
                        collect_job(client, items[category], keyword, output, max_pages, page_size)
            manifest['state'] = 'collected'
        except KeyboardInterrupt:
            manifest['state'] = 'interrupted'
            manifest['issues'].append('用户停止采集，可在同一批次续抓')
        except Exception as exc:
            manifest['state'] = 'failed'
            manifest['issues'].append(f'{type(exc).__name__}: {exc}')
            LOG.exception('采集失败，保留断点')
        finally:
            atomic(manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2))
        destination, summary = export_batch(batch, baseline)
        return destination, summary


# 解析collect/export和页数等参数，配置日志/停止信号；按完整性用退出码区分成功、未完整和参数/文件失败。
def main(argv=None):
    """程序入口：解析启动参数并运行当前模块的流程。"""
    parser = argparse.ArgumentParser(description='纯 HTTP 采集 + 四类合并去重 + 22 字段转换 + 新增对比')
    sub = parser.add_subparsers(dest='command', required=True)
    crawl = sub.add_parser('collect', help='采集四类注册库，并自动导出数据库 CSV')
    crawl.add_argument('--keywords', nargs='+', choices=['313'], default=['313'])
    crawl.add_argument('--batch', help='新目录采最新数据；原目录续抓旧批次')
    crawl.add_argument('--max-pages', type=int, default=0)
    crawl.add_argument('--page-size', type=int, default=10)
    crawl.add_argument('--proxy')
    crawl.add_argument('--interval', type=float, default=1)
    offline = sub.add_parser('export', help='离线重新转换已有批次，不访问网站')
    offline.add_argument('--batch', required=True)
    offline.add_argument('--output', help='必须为不存在的新目录')
    for command in (crawl, offline):
        command.add_argument('--baseline', help='已入库数据导出的 22 字段 CSV；不自动更新')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if args.command == 'collect' and (args.max_pages < 0 or not 1 <= args.page_size <= 100 or args.interval < 0):
        parser.error('max-pages >= 0，page-size 1..100，interval >= 0')
    if args.baseline:
        from export_medical_device import baseline
        try:
            baseline(args.baseline)
        except (ValueError, OSError) as exc:
            parser.error('基准异常：' + str(exc))
    previous_term = signal.signal(signal.SIGTERM, lambda signum, frame: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        if args.command == 'collect':
            batch = args.batch or str(Path(__file__).resolve().parent / 'batches' /
                ('batch_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '_' + uuid.uuid4().hex[:8]))
            output, summary = collect_batch(batch, list(dict.fromkeys(args.keywords)), args.max_pages,
                args.page_size, args.proxy, args.interval, args.baseline)
        else:
            output, summary = export_batch(Path(args.batch).resolve(), args.baseline, args.output)
        print(json.dumps({'output': str(output.resolve()), **summary}, ensure_ascii=False, indent=2))
        return 0 if summary['complete'] else 2
    except (ValueError, OSError) as exc:
        LOG.error('%s', exc)
        return 1
    # 即使被停止也尝试导出已采数据；保留部分结果不表示完整性已经通过。
    finally:
        signal.signal(signal.SIGTERM, previous_term)


if __name__ == '__main__':
    raise SystemExit(main())
