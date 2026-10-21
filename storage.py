# 阅读说明：普通采集链的原始断点库，每个类别任务输出目录下有data.sqlite3。
# 保存来源列表、详情原文和字段映射，导出JSONL/CSV并审计完整性；不创建MySQL辅助表。
# 本文件只增加解释注释，业务代码保持原样。

import json
import csv
import math
from collections import Counter
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DOMESTIC = 'ff80808183cad7500183cb66fe690285'
NOTE_URL = 'https://datasearch.nmpa.gov.cn/datasearch/search-help.html#ylqx_1_6'
CSV_COLUMNS = ['注册证编号','注册人名称','注册人住所','生产地址','产品名称','管理类别','型号规格',
               '结构及组成/主要组成成分','适用范围/预期用途','产品储存条件及有效期','附件',
               '其他内容','备注','审批部门','批准日期','生效日期','有效期至','变更情况','注']


# 紧凑序列化原始响应并保留中文，存到本地SQLite/JSONL便于复核。
def dumps(value):
    """统一序列化 JSON，保留中文并保持输出可比较。"""
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


# 把来源字段alias映射为中文desc；listFeild/detailFeild拼写沿用来源协议。
def field_map(schema, kind):
    """建立来源字段与导出字段的映射。"""
    return {f['alias']: f['desc'] for f in schema[kind]}


class Store:
    """保存采集断点、列表和详情，支持中断后继续。"""
    # 在output/data.sqlite3打开或创建原始断点库及三张本地表，跟业务MySQL无关。
    def __init__(self, output):
        """初始化本对象的依赖与资源；实际职责见类说明及参数。"""
        self.output = Path(output)
        self.output.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.output / 'data.sqlite3')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS jobs(job TEXT PRIMARY KEY, item_id TEXT, keyword TEXT, page_size INTEGER, schema_json TEXT);
        CREATE TABLE IF NOT EXISTS pages(job TEXT, page INTEGER, response TEXT, PRIMARY KEY(job,page));
        CREATE TABLE IF NOT EXISTS records(job TEXT, id TEXT, page INTEGER, raw TEXT, fields TEXT, detail_raw TEXT, detail_fields TEXT, crawled_at TEXT, PRIMARY KEY(job,id));
        ''')

    # 关闭这个类别任务的SQLite连接；调用方通常在finally执行。
    def close(self):
        """关闭持有的连接或资源，避免运行结束后仍占用文件与数据库。"""
        self.db.close()

    # 用来源ID/关键词/每页条数生成稳定键，并验证旧schema一致；身份变了必须新建批次。
    def job(self, item_id, keyword, page_size, schema):
        """建立或读取同一采集范围的断点任务。"""
        import hashlib
        key = hashlib.sha256(dumps([item_id, keyword, page_size]).encode()).hexdigest()[:24]
        previous = self.db.execute('SELECT item_id,keyword,page_size,schema_json FROM jobs WHERE job=?', (key,)).fetchall()
        identity = (item_id, keyword, page_size, dumps(schema))
        if any(tuple(row) != identity for row in previous):
            raise ValueError('来源类别或字段结构已变化，不能复用旧断点，请创建新任务')
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO jobs VALUES(?,?,?,?,?)',
                            (key, item_id, keyword, page_size, dumps(schema)))
        return key

    # 先查已保存页，存在就反序列化复用；None表示调用方需要向来源请求这一页。
    def page(self, job, page):
        """读取某一页的已保存列表。"""
        row = self.db.execute('SELECT response FROM pages WHERE job=? AND page=?', (job, page)).fetchone()
        return json.loads(row[0]) if row else None

    # 校验主键并把列表记录与页响应在同一SQLite事务保存，避免页码已记但记录未写。
    def save_page(self, job, number, response, schema):
        """持久化一页列表及其审计信息。"""
        columns = field_map(schema, 'listFeild')
        id_alias = next((f['alias'] for f in schema['listFeild'] if f['desc'] == 'NMPA_DK'), None)
        if not id_alias:
            raise RuntimeError('列表配置没有 NMPA_DK 主键，停止以避免错误去重')
        now = datetime.now(timezone.utc).isoformat()
        with self.db:
            for record in response['data']['list']:
                if not record.get(id_alias):
                    raise RuntimeError('记录缺少 NMPA_DK')
                fields = {columns.get(k, k): v for k, v in record.items()}
                self.db.execute('''INSERT INTO records(job,id,page,raw,fields,crawled_at) VALUES(?,?,?,?,?,?)
                    ON CONFLICT(job,id) DO UPDATE SET page=excluded.page,raw=excluded.raw,fields=excluded.fields,crawled_at=excluded.crawled_at''',
                                (job, record[id_alias], number, dumps(record), dumps(fields), now))
            # Commit the checkpoint together with its records, never before them.
            self.db.execute('INSERT OR REPLACE INTO pages VALUES(?,?,?)', (job, number, dumps(response)))

    # 返回本页detail_raw为空的记录ID，只给未完成详情发请求。
    def pending_details(self, job, page):
        """找出列表中尚未完成详情采集的记录。"""
        return [r[0] for r in self.db.execute('SELECT id FROM records WHERE job=? AND page=? AND detail_raw IS NULL ORDER BY id', (job, page))]

    # 校验详情字段和来源主键，保存详情原文及中文字段；异常不会把错误响应标成已完成。
    def save_detail(self, job, record_id, response, schema):
        """保存详情和字段结构，供标准化导出使用。"""
        columns = field_map(schema, 'detailFeild')
        raw = response['data']['detail']
        missing = sorted(set(columns) - set(raw))
        if missing:
            raise RuntimeError(f'详情缺少字段：{missing}；保留断点以便重试')
        id_alias = next((a for a, d in columns.items() if d == 'NMPA_DK'), None)
        if id_alias and raw.get(id_alias) != record_id:
            raise RuntimeError('详情主键与请求主键不一致')
        fields = {columns.get(k, k): v for k, v in response['data']['detail'].items()}
        with self.db:
            self.db.execute('UPDATE records SET detail_raw=?,detail_fields=? WHERE job=? AND id=?',
                            (dumps(response), dumps(fields), job, record_id))

    # 导出本地原始JSONL并触发CSV导出；包含尚无详情的列表状态，供审计识别未完成部分。
    def export(self, job):
        """输出标准记录及审计结果，供下一阶段判断能否入库。"""
        target = self.output / (job + '.jsonl')
        temp = target.with_suffix('.jsonl.tmp')
        meta = self.db.execute('SELECT item_id,keyword FROM jobs WHERE job=?', (job,)).fetchone()
        with temp.open('w', encoding='utf-8') as f:
            for row in self.db.execute('SELECT id,page,raw,fields,detail_raw,detail_fields,crawled_at FROM records WHERE job=? ORDER BY page,id', (job,)):
                record_id, page, raw, fields, detail_raw, detail_fields, crawled_at = row
                f.write(dumps(dict(id=record_id, item_id=meta[0], keyword=meta[1], page=page,
                                   fields=json.loads(fields), raw=json.loads(raw),
                                   detail=json.loads(detail_fields) if detail_fields else None,
                                   detail_raw=json.loads(detail_raw) if detail_raw else None,
                                   crawled_at=crawled_at)) + '\n')
        temp.replace(target)
        self.export_csv(job)
        return target

    # 只导出有detail_raw的详情行，字段来自来源schema；这不是最终22字段业务CSV。
    def export_csv(self, job):
        """导出已保存数据为 CSV。"""
        item_id, schema_json = self.db.execute('SELECT item_id,schema_json FROM jobs WHERE job=?', (job,)).fetchone()
        columns = CSV_COLUMNS if item_id == DOMESTIC else [f['desc'] for f in json.loads(schema_json)['detailFeild'] if f['desc'] != 'NMPA_DK']
        target = self.output / (job + '.csv')
        temp = target.with_suffix('.csv.tmp')
        with temp.open('w', encoding='utf-8-sig', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=columns, extrasaction='ignore')
            writer.writeheader()
            # Export only completed details; incomplete list rows never masquerade as detail rows.
            for fields, in self.db.execute('SELECT detail_fields FROM records WHERE job=? AND detail_raw IS NOT NULL ORDER BY page,id', (job,)):
                record = json.loads(fields)
                if item_id == DOMESTIC:
                    record['注'] = NOTE_URL
                writer.writerow(record)
        temp.replace(target)
        return target

    # 核对全部应有页数、每页条数、重复ID、详情字段及CSV行数，只有全部满足才complete=True。
    def audit(self, job, reference_csv=None):
        """核对分页、详情数量、重复项和错误，判断来源采集完整性。"""
        item_id, keyword, size, schema_json = self.db.execute('SELECT item_id,keyword,page_size,schema_json FROM jobs WHERE job=?', (job,)).fetchone()
        schema = json.loads(schema_json)
        pages = {number: json.loads(response)['data'] for number, response in self.db.execute('SELECT page,response FROM pages WHERE job=? ORDER BY page', (job,))}
        total = pages.get(1, {}).get('total')
        expected = max(1, math.ceil(total / size)) if total is not None else None
        missing_pages = sorted(set(range(1, expected + 1)) - set(pages)) if expected is not None else [1]
        alias = next(f['alias'] for f in schema['listFeild'] if f['desc'] == 'NMPA_DK')
        ids = Counter(r.get(alias) for page in pages.values() for r in page['list'])
        duplicate_ids = {k: v for k, v in ids.items() if v > 1}
        row_count, detail_count = self.db.execute('SELECT COUNT(*),COUNT(detail_raw) FROM records WHERE job=?', (job,)).fetchone()
        required = set(field_map(schema, 'detailFeild'))
        missing_fields = {}
        certificates = Counter()
        for record_id, detail, fields in self.db.execute('SELECT id,detail_raw,detail_fields FROM records WHERE job=? AND detail_raw IS NOT NULL', (job,)):
            missing = sorted(required - set(json.loads(detail)['data']['detail']))
            if missing: missing_fields[record_id] = missing
            if item_id == DOMESTIC:
                certificates[json.loads(fields).get('注册证编号')] += 1
        page_errors = [p for p, data in pages.items() if total is not None and
                       (data['total'] != total or data.get('pageNum') != p or
                        len(data['list']) != max(0, min(size, total - (p - 1)*size)))]
        expected_columns = CSV_COLUMNS if item_id == DOMESTIC else [f['desc'] for f in schema['detailFeild'] if f['desc'] != 'NMPA_DK']
        missing_csv_source_columns = sorted(set(expected_columns) - {'注'} - set(field_map(schema, 'detailFeild').values()))
        with self.export_csv(job).open(encoding='utf-8-sig', newline='') as stream:
            csv_reader = csv.DictReader(stream)
            actual_columns = csv_reader.fieldnames
            csv_rows = sum(1 for _ in csv_reader)
        report = dict(item_id=item_id, keyword=keyword, expected_total=total, page_size=size,
                      expected_pages=expected, saved_pages=len(pages), missing_pages=missing_pages,
                      page_errors=page_errors, list_rows=sum(ids.values()), unique_records=row_count,
                      duplicate_ids=duplicate_ids, completed_details=detail_count, missing_detail_fields=missing_fields,
                      repeated_registration_numbers={k: v for k, v in certificates.items() if v > 1},
                      csv_columns=actual_columns, csv_rows=csv_rows,
                      missing_csv_source_columns=missing_csv_source_columns,
                      note_source='参考 CSV 及其原爬虫中的页面帮助链接；不是详情 API 字段',
                      audited_at=datetime.now(timezone.utc).isoformat())
        report['complete'] = (total is not None and not missing_pages and not page_errors and not duplicate_ids
                              and not missing_fields and not missing_csv_source_columns
                              and actual_columns == expected_columns
                              and row_count == detail_count == csv_rows == sum(ids.values()) == total)
        if reference_csv:
            with Path(reference_csv).open(encoding='utf-8-sig', newline='') as stream:
                reader = csv.DictReader(stream)
                reference_columns = reader.fieldnames
                reference = list(reader)
            with self.export_csv(job).open(encoding='utf-8-sig', newline='') as stream:
                reader = csv.DictReader(stream)
                actual_columns = reader.fieldnames
                actual = {r['注册证编号']: r for r in reader}
            differences, absent = [], []
            for ref in reference:
                number = ref['注册证编号']
                if number not in actual:
                    absent.append(number)
                    continue
                for column in reference_columns:
                    if (ref.get(column) or '') != (actual[number].get(column) or ''):
                        differences.append(dict(registration=number, column=column,
                                                reference=ref.get(column), current=actual[number].get(column),
                                                equal_after_whitespace_normalization=
                                                ' '.join((ref.get(column) or '').split()) ==
                                                ' '.join((actual[number].get(column) or '').split())))
            report['reference_comparison'] = dict(path=str(Path(reference_csv).resolve()), rows=len(reference),
                                                  columns_identical=reference_columns == actual_columns,
                                                  missing_records=absent, field_differences=differences,
                                                  exact_match=not absent and not differences and reference_columns == actual_columns,
                                                  match_after_whitespace_normalization=not absent and reference_columns == actual_columns
                                                  and all(d['equal_after_whitespace_normalization'] for d in differences))
        target = self.output / (job + '.report.json')
        temp = target.with_suffix('.json.tmp')
        temp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        temp.replace(target)
        return report
