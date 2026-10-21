# 阅读说明：来源HTTP访问的公共层：分类配置、字段结构、分页列表、逐条详情。
# 负责间隔、超时、响应校验和有限重试；会话初始化由子类提供。
# HTTP400/403/412首次出现时现有实现会重新初始化一次；这条日志本身不能证明账号或IP已被永久封禁。
# 本文件只增加解释注释，业务代码保持原样。

import json
import logging
import time
from email.utils import parsedate_to_datetime

import requests

from .signing import make_sign

LOG = logging.getLogger(__name__)
ORIGIN = 'https://datasearch.nmpa.gov.cn'
HOME = ORIGIN + '/datasearch/home-index.html#category=ylqx'
BASE = ORIGIN + '/datasearch/'


class NMPAError(RuntimeError):
    """来源接口异常类型，区分 HTTP 和业务响应错误。"""
    pass


class NMPAClient:
    """Signed HTTP transport; subclasses initialize the dynamic Node session."""

    # 建立requests.Session并设置显式代理、超时和最小0.5秒间隔；trust_env=False不继承终端代理。
    def __init__(self, *, proxy=None, interval=1.0, timeout=30):
        """初始化本对象的依赖与资源；实际职责见类说明及参数。"""
        self.proxy = proxy
        self.interval = max(interval, 0.5)
        self.timeout = timeout
        self.session = requests.Session()
        # Keep all session requests on the same explicit network route.
        self.session.trust_env = False
        if proxy:
            self.session.proxies.update({'http': proxy, 'https': proxy})
        self.session.headers.update({'Accept': 'application/json, text/plain, */*',
                                     'Accept-Language': 'zh-CN,zh;q=0.9',
                                     'Referer': HOME.split('#')[0]})
        self.secret = 'nmpasecret2020'
        self._last_request = 0.0
        self._clock = None

    # 进入with时调用子类bootstrap；初始化失败也关闭session，再把异常交给上层。
    def __enter__(self):
        """进入上下文管理，方便调用方自动释放资源。"""
        try:
            self.bootstrap()
        except BaseException:
            self.close()
            raise
        return self

    # 无论with内成功或抛错都关闭HTTP会话；异常不在这里吞掉。
    def __exit__(self, *args):
        """离开上下文管理时关闭连接，即使发生异常也执行。"""
        self.close()

    # 关闭requests会话和连接池，避免长时间采集结束后占用网络资源。
    def close(self):
        """关闭持有的连接或资源，避免运行结束后仍占用文件与数据库。"""
        self.session.close()

    # 基类不实现来源会话准备；实际使用的PureNMPAClient覆盖此方法，不能直接实例化基类开始采集。
    def bootstrap(self):
        """初始化来源会话及业务脚本，为后续查询准备参数。"""
        raise NotImplementedError('使用 PureNMPAClient 初始化 Node 会话')

    # 根据单调时钟计算等待间隔，不使用易受系统时间调整影响的本地日期时间。
    def _pace(self):
        """控制请求间隔，避免连续发送过快。"""
        delay = self.interval - (time.monotonic() - self._last_request)
        if delay > 0:
            time.sleep(delay)
        self._last_request = time.monotonic()

    # 统一请求与响应校验；最多两次尝试，首次400/403/412重新初始化一次，最终失败向上抛错。
    def _http_json(self, path, params=None, signed=False):
        """发送来源 HTTP 请求并检查响应，再解析 JSON。"""
        # 这是现有有限重试：最多两次，不是无限重置会话；第二次仍失败就向worker报告异常。
        for attempt in range(2):
            values = dict(params or {})
            if not signed:
                values.setdefault('date', int(time.time()*1000))
            headers = {}
            if signed:
                if self._clock is None or time.monotonic() - self._clock[1] >= 30:
                    date = self.session.get(BASE + 'config/DATE.json',
                                        params={'date': int(time.time()*1000)}, timeout=self.timeout)
                    if date.status_code in (400, 403, 412):
                        if attempt == 0:
                            self._clock = None
                            self.bootstrap()
                            continue
                        raise NMPAError(f'服务器时钟接口被拦截：HTTP {date.status_code}')
                    date.raise_for_status()
                    if not date.headers.get('Date'):
                        raise NMPAError('服务器响应缺少 Date，无法构造可信时间戳')
                    self._clock = (parsedate_to_datetime(date.headers['Date']).timestamp(), time.monotonic())
                values['timestamp'] = int((self._clock[0] + time.monotonic() - self._clock[1])*1000)
                headers = {'timestamp': str(values['timestamp']),
                           'sign': make_sign(values, self.secret), 'token': 'false'}
            self._pace()
            response = self.session.get(BASE + path, params=values, headers=headers,
                                        timeout=self.timeout)
            # 这里就是“HTTP412，重新初始化会话后重试一次”日志的位置；刷新会话不清空采集断点。
            if response.status_code in (400, 403, 412) and attempt == 0:
                LOG.warning('HTTP %s，重新初始化会话后重试一次', response.status_code)
                self._clock = None
                self.bootstrap()
                continue
            response.raise_for_status()
            try:
                data = response.json()
            except ValueError as exc:
                raise NMPAError(f'{path} 返回非 JSON；不能视为成功') from exc
            if signed and data.get('code') != 200:
                raise NMPAError(f'业务接口失败：{json.dumps(data, ensure_ascii=False)[:500]}')
            return data
        raise NMPAError(f'{path} 请求失败')

    # 读取来源分类配置，找到医疗器械item_3的itemList；来源结构改变即报错，不猜类别ID。
    def categories(self):
        """返回前端可选择的采集类别。"""
        data = self._http_json('config/NMPA_DATA.json')
        groups = data if isinstance(data, list) else data.get('data', data)
        if not isinstance(groups, list):
            raise NMPAError('分类配置结构已变化')
        group = next((g for g in groups if g.get('paraCode') == 'item_3'), None)
        if group is None:
            raise NMPAError('分类配置没有医疗器械 item_3')
        return group['itemList']

    # 取指定item_id的字段配置，供列表/详情别名映射和主键校验使用。
    def schema(self, item_id):
        """读取来源类目的字段结构。"""
        return self._http_json(f'config/{item_id}.json')

    # 请求一页列表并要求data.list和返回页码正确；不会在这里自动遍历所有页。
    def search(self, item_id, keyword, page_num=1, page_size=10):
        """查询指定类别和关键词的一页列表。"""
        result = self._http_json('data/nmpadata/search',
                                dict(isSenior='N', itemId=item_id, pageNum=page_num,
                                     pageSize=page_size, searchValue=keyword), signed=True)
        data = result.get('data')
        if not isinstance(data, dict) or not isinstance(data.get('list'), list):
            raise NMPAError('查询响应缺少 data.list')
        if data.get('pageNum') != page_num:
            raise NMPAError('服务端返回的页码与请求不一致')
        return result

    # 按来源item_id和记录主键取一条详情，data.detail必须是字典，否则不能保存成成功数据。
    def detail(self, item_id, record_id):
        """查询一条列表记录的详细内容。"""
        result = self._http_json('data/nmpadata/queryDetail',
                                dict(itemId=item_id, id=record_id), signed=True)
        if not isinstance(result.get('data', {}).get('detail'), dict):
            raise NMPAError('详情响应缺少 data.detail')
        return result
