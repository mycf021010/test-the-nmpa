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

    def __enter__(self):
        """进入上下文管理，方便调用方自动释放资源。"""
        try:
            self.bootstrap()
        except BaseException:
            self.close()
            raise
        return self

    def __exit__(self, *args):
        """离开上下文管理时关闭连接，即使发生异常也执行。"""
        self.close()

    def close(self):
        """关闭持有的连接或资源，避免运行结束后仍占用文件与数据库。"""
        self.session.close()

    def bootstrap(self):
        """初始化来源会话及业务脚本，为后续查询准备参数。"""
        raise NotImplementedError('使用 PureNMPAClient 初始化 Node 会话')

    def _pace(self):
        """控制请求间隔，避免连续发送过快。"""
        delay = self.interval - (time.monotonic() - self._last_request)
        if delay > 0:
            time.sleep(delay)
        self._last_request = time.monotonic()

    def _http_json(self, path, params=None, signed=False):
        """发送来源 HTTP 请求并检查响应，再解析 JSON。"""
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

    def schema(self, item_id):
        """读取来源类目的字段结构。"""
        return self._http_json(f'config/{item_id}.json')

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

    def detail(self, item_id, record_id):
        """查询一条列表记录的详细内容。"""
        result = self._http_json('data/nmpadata/queryDetail',
                                dict(itemId=item_id, id=record_id), signed=True)
        if not isinstance(result.get('data', {}).get('detail'), dict):
            raise NMPAError('详情响应缺少 data.detail')
        return result
