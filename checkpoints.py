# 阅读说明：逐页、逐条详情的断点管理。流由轮次+类别+关键词唯一定位。
# 配额可能在一页中间结束，例如处理83条后停止；下一任务会接着本页未完成详情继续。
# 只有本页所有详情落盘，才推进到下一页；来源排序或身份变化时阻塞核对。
# 本文件只增加解释注释，业务代码保持原样。

"""对来源页、详情和每日归属做原子落盘，避免83条任务跳过页内剩余7条。"""
import json
from api.models import CATEGORY_NAMES, Category
from nmpa.storage import field_map
from .contracts import DailyError, dumps, now
from .normalization import normalize_detail
from api.collection_rules import eligibility


class Checkpoints:
    """封装单个流的数据一致性；外部请求在短事务之外执行。"""
    # 绑定store、job与三元组(round_id, category, keyword)，所有后续SQL都限定在同一来源流。
    def __init__(self, store, job):
        """绑定持久化任务与类别关键词，不自行生成或重置断点。"""
        self.store, self.job = store, job
        self.key = (job['round_id'],job['category'],job['keyword'])

    # 读取本流已保存的页码、来源item_id、字段结构和耗尽状态；不能从日志推测下一页。
    def stream(self):
        """读取当前流的权威页码、来源身份、结构和耗尽状态。"""
        with self.store.connect() as db:
            return dict(self.store.require(db.execute('SELECT * FROM streams WHERE round_id=? AND category=? AND keyword=?',self.key).fetchone()))

    # 按来源中文类别找到唯一item_id并验证字段主键；与原断点身份不一致时阻塞，避免混入另一张来源表。
    def identify(self, client):
        """正常发现来源类别并核对已保存的身份/字段结构，不跳过来源限制。"""
        name = CATEGORY_NAMES[Category(self.job['category'])]
        items = [item for item in client.categories() if item.get('itemName') == name and item.get('itemType') == 'table']
        if len(items) != 1:
            raise DailyError('SOURCE_CATEGORY_CHANGED','来源类别无法唯一识别，需要人工核对')
        item_id = items[0]['itemId']
        schema = client.schema(item_id)
        if not isinstance(schema,dict) or not all(isinstance(schema.get(key),list) for key in ('listFeild','detailFeild')):
            raise DailyError('SOURCE_SCHEMA_CHANGED','来源字段结构不完整')
        for kind in ('listFeild','detailFeild'):
            if len([field for field in schema[kind] if field.get('desc') == 'NMPA_DK']) != 1:
                raise DailyError('SOURCE_SCHEMA_CHANGED','来源缺少唯一主键字段')
        saved = self.stream()
        if saved['item_id'] and (saved['item_id'] != item_id or json.loads(saved['schema_json']) != schema):
            raise DailyError('SOURCE_SCHEMA_CHANGED','来源身份或字段结构变化，保留旧断点等待核对')
        with self.store.connect() as db:
            db.execute('UPDATE streams SET item_id=?,schema_json=? WHERE round_id=? AND category=? AND keyword=?', (item_id,dumps(schema),*self.key))
        return item_id,schema

    # 验证本页页码、总数、条数和主键；恢复时核对已存顺序，列表与页内ordinal在同一事务保存。
    def page(self, client, item_id, schema, page, page_size):
        """取得并验证来源页；恢复时核对保存列表，遇排序变化不静默错过数据。"""
        response = client.search(item_id,self.job['keyword'],page,page_size)
        data = response.get('data',{}) if isinstance(response,dict) else {}
        records = data.get('list')
        total = data.get('total')
        id_alias = next(field['alias'] for field in schema['listFeild'] if field['desc']=='NMPA_DK')
        if not isinstance(records,list) or not isinstance(total,int) or isinstance(total,bool) or total < 0 or data.get('pageNum') != page:
            raise DailyError('SOURCE_PAGE_INVALID','来源分页结构不符合约定')
        ids = []
        for record in records:
            identity = record.get(id_alias) if isinstance(record,dict) else None
            if isinstance(identity,bool) or not isinstance(identity,(str,int)) or not str(identity).strip() or len(str(identity))>255:
                raise DailyError('SOURCE_PAGE_INVALID','来源主键缺失、类型或长度异常')
            ids.append(str(identity))
        if any(not identity for identity in ids) or len(set(ids)) != len(ids) or len(records)>page_size:
            raise DailyError('SOURCE_PAGE_INVALID','来源列表主键缺失、重复或条数异常')
        # 总数、页码和page_size共同决定本页应有条数；提前空页不能当作来源已经耗尽。
        expected = max(0,min(page_size,total-(page-1)*page_size))
        if len(records) != expected:
            raise DailyError('SOURCE_BOUNDARY_CHANGED','来源页条数与总数不符，停止以避免跳页')
        with self.store.connect() as db:
            previous = db.execute('SELECT response FROM pages WHERE round_id=? AND category=? AND keyword=? AND page=?',(*self.key,page)).fetchone()
            if previous:
                old = json.loads(previous[0])['data']
                if [str(row.get(id_alias,'')) for row in old['list']] != ids:
                    raise DailyError('SOURCE_BOUNDARY_CHANGED','当前来源页顺序变化，旧详情已保留，需要核对')
            else:
                for ordinal,(identity,record) in enumerate(zip(ids,records)):
                    if db.execute('SELECT 1 FROM records WHERE round_id=? AND category=? AND keyword=? AND source_id=?',(*self.key,identity)).fetchone():
                        raise DailyError('SOURCE_BOUNDARY_CHANGED','来源记录跨页重复，暂停避免漏采')
                    db.execute('INSERT INTO records(round_id,category,keyword,source_id,page,ordinal,raw) VALUES(?,?,?,?,?,?,?)',(*self.key,identity,page,ordinal,dumps(record)))
            db.execute('INSERT INTO pages VALUES(?,?,?,?,?) ON CONFLICT(round_id,category,keyword,page) DO UPDATE SET response=excluded.response',(*self.key,page,dumps(response)))
        return data

    # 只取本页detail IS NULL的记录，按ordinal恢复来源顺序；已有详情不重复分配给新配额。
    def pending(self, page):
        """按来源页内顺序读取未取得详情的记录，已落盘的详情不重复获取。"""
        with self.store.connect() as db:
            return [dict(row) for row in db.execute('SELECT * FROM records WHERE round_id=? AND category=? AND keyword=? AND page=? AND detail IS NULL ORDER BY ordinal',(*self.key,page))]

    # 核对详情主键和字段，把原响应、标准字段、校验错误、过滤状态及当前job归属一起落盘。
    def save_detail(self, record, response, schema):
        """校验详情身份/结构后，原子保存详情、转换结果和本日任务归属。"""
        raw = response.get('data',{}).get('detail') if isinstance(response,dict) else None
        fields = field_map(schema,'detailFeild')
        id_alias = next(field['alias'] for field in schema['detailFeild'] if field['desc']=='NMPA_DK')
        if not isinstance(raw,dict) or not set(fields).issubset(raw) or str(raw.get(id_alias,'')) != record['source_id']:
            raise DailyError('SOURCE_DETAIL_INVALID','来源详情缺字段或主键不符，保留断点')
        source = {fields.get(key,key):value for key,value in raw.items()}
        normalized,errors = normalize_detail(source,self.job['keyword'],CATEGORY_NAMES[Category(self.job['category'])])
        # 非313为正常filtered，不等于校验失败；字段错误会改成invalid，两个计数分开。
        status, reason = eligibility(normalized)
        if errors:
            status = 'invalid'
            reason = reason or 'FIELD_VALIDATION_FAILED'
        with self.store.connect() as db:
            # 保存详情和绑定当前job同事务完成；下次配额只处理剩下detail为空的记录。
            db.execute('UPDATE records SET detail=?,normalized=?,errors=?,job_id=?,filter_state=?,filter_reason=? WHERE round_id=? AND category=? AND keyword=? AND source_id=? AND detail IS NULL',
                       (dumps(response),dumps(normalized),dumps(errors),self.job['id'],status,reason,*self.key,record['source_id']))
            db.execute('UPDATE jobs SET updated_at=? WHERE id=?',(now(),self.job['id']))
        return errors

    # 本页还有未保存详情则返回False；全部完成才推进页码，真实末页保留本页并标exhausted=1。
    def advance(self, page, total, size):
        """只有整页详情都已保存才推进页码，并在真实末页标记来源耗尽。"""
        with self.store.connect() as db:
            if db.execute('SELECT 1 FROM records WHERE round_id=? AND category=? AND keyword=? AND page=? AND detail IS NULL',(*self.key,page)).fetchone():
                return False
            exhausted = int(page*size>=total)
            db.execute('UPDATE streams SET page=?,exhausted=? WHERE round_id=? AND category=? AND keyword=?',(page if exhausted else page+1,exhausted,*self.key))
            return True
