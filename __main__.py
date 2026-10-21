# 阅读说明：较底层的命令行入口，可查类别或采单个类别；与HTTP API、Java定时调用是不同入口。
# 直接运行会按参数访问来源网站；末尾finally保留导出和审计结果后关闭原始断点库。
# 本文件只增加解释注释，业务代码保持原样。

import argparse
import json
import logging
import math
import sys
import signal
from pathlib import Path

# Dependencies are installed into the Python environment selected by the launcher.
from .pure import PureNMPAClient
from .storage import Store


# 解析分类/单类采集命令，准备会话和原始Store，按页数预算采集并输出审计；不会写MySQL业务数据。
def main():
    """程序入口：解析启动参数并运行当前模块的流程。"""
    parser = argparse.ArgumentParser(description='NMPA 医疗器械查询：默认纯 Node 补环境 + 签名 HTTP 分页/详情')
    parser.add_argument('--proxy', help='显式代理，例如 http://127.0.0.1:7897；默认直连，不继承环境代理')
    parser.add_argument('--interval', type=float, default=1.0)
    parser.add_argument('--log-file', help='同时写入 UTF-8 日志文件，适合服务器后台运行')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('categories', help='获取当前医疗器械库及 itemId')
    crawl = sub.add_parser('crawl')
    crawl.add_argument('--keyword', choices=['313'], default='313')
    crawl.add_argument('--item-id', default='ff80808183cad7500183cb66fe690285')
    crawl.add_argument('--page-size', type=int, default=10)
    crawl.add_argument('--start-page', type=int, default=1)
    crawl.add_argument('--max-pages', type=int, default=1, help='本次最多处理页数，默认 1；0 表示直到末页')
    crawl.add_argument('--details', action='store_true')
    crawl.add_argument('--output', default='outputs')
    crawl.add_argument('--reference-csv', help='用于核验详情字段与样本记录的参考 CSV')
    args = parser.parse_args()
    if args.command == 'crawl':
        if not args.keyword.strip(): parser.error('--keyword 不能为空')
        if args.start_page < 1 or not 1 <= args.page_size <= 100 or args.max_pages < 0:
            parser.error('start-page >= 1，page-size 1..100，max-pages >= 0')
    handlers = [logging.StreamHandler()]
    if args.log_file:
        path = Path(args.log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(path, encoding='utf-8'))
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', handlers=handlers)
    received_signal = 0
    # 记录收到的信号并抛KeyboardInterrupt，外层finally导出和关闭断点文件。
    def stop(signum, frame):
        """将停止信号转换为中断，让运行中的任务记录停止状态。"""
        nonlocal received_signal
        received_signal = signum
        raise KeyboardInterrupt
    previous_term = signal.signal(signal.SIGTERM, stop)
    store = None
    job = None
    try:
        with PureNMPAClient(proxy=args.proxy, interval=args.interval) as client:
            categories = client.categories()
            if args.command == 'categories':
                print(json.dumps(categories, ensure_ascii=False, indent=2))
                return 0
            item = next((c for c in categories if c.get('itemId') == args.item_id and c.get('itemType') == 'table'), None)
            if item is None: raise RuntimeError('item-id 不是当前医疗器械可查询数据表')
            schema = client.schema(args.item_id)
            store = Store(args.output)
            job = store.job(args.item_id, args.keyword, args.page_size, schema)
            page = args.start_page
            processed = 0
            while args.max_pages == 0 or processed < args.max_pages:
                response = store.page(job, page)
                if response is None:
                    response = client.search(args.item_id, args.keyword, page, args.page_size)
                    store.save_page(job, page, response, schema)
                else:
                    logging.info('复用已提交的第 %s 页', page)
                data = response['data']
                logging.info('%s，第 %s 页：%s 条，总计 %s', item['itemName'], page, len(data['list']), data['total'])
                if args.details:
                    for record_id in store.pending_details(job, page):
                        result = client.detail(args.item_id, record_id)
                        store.save_detail(job, record_id, result, schema)
                        logging.debug('详情已保存：%s', record_id)
                    logging.info('第 %s 页详情已补齐', page)
                store.export(job)
                processed += 1
                if page >= max(1, math.ceil(data['total']/args.page_size)): break
                if not data['list']: raise RuntimeError('尚未到末页却返回空列表，停止并保留断点')
                page += 1
            print(str(store.export(job).resolve()))
            report = store.audit(job, args.reference_csv)
            print(json.dumps(report, ensure_ascii=False, indent=2))
            if args.max_pages == 0 and args.start_page == 1 and args.details and not report['complete']:
                raise RuntimeError('采集结束，但完整性核验未通过，详见报告')
        return 0
    except KeyboardInterrupt:
        logging.warning('已中断，数据库保留已完成页面与详情')
        return 128 + received_signal if received_signal else 130
    except Exception as exc:
        logging.error('%s: %s', type(exc).__name__, exc)
        return 1
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        if store:
            if job:
                store.export(job)
                store.audit(job, getattr(args, 'reference_csv', None))
            store.close()


if __name__ == '__main__':
    raise SystemExit(main())
