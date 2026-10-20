"""使用 Node 运行来源页面环境，保持原有客户端接口。"""
import os
import json
import logging
import subprocess
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse

from .client import NMPAClient, NMPAError, HOME, BASE
from .signing import make_sign

ROOT = Path(__file__).resolve().parents[1]
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
LOG = logging.getLogger(__name__)


class ChallengeParser(HTMLParser):
    """提取页面脚本和挑战所需内容，供运行时执行。"""
    def __init__(self, html):
        """初始化本对象的依赖与资源；实际职责见类说明及参数。"""
        super().__init__()
        self.scripts = []
        self.meta = None
        self.current = None
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        """解析 HTML 开始标签及属性，收集脚本来源。"""
        attrs = dict(attrs)
        if tag == 'meta' and attrs.get('id') and attrs.get('content') and attrs.get('r') == 'm':
            self.meta = {k: attrs[k] for k in ('id', 'content')}
        if tag == 'script':
            self.current = [attrs, '']

    def handle_data(self, data):
        """收集当前 HTML 标签中的脚本文本。"""
        if self.current is not None:
            self.current[1] += data

    def handle_endtag(self, tag):
        """在标签结束时完成对应脚本内容收集。"""
        if tag == 'script' and self.current is not None:
            self.scripts.append(self.current)
            self.current = None


def node(script, data):
    """调用本地 Node 子进程执行 JavaScript，返回解析后的运行结果。"""
    result = subprocess.run([os.getenv('NMPA_NODE', 'node'), str(ROOT / 'js' / script)], input=json.dumps(data),
                            capture_output=True, text=True, encoding='utf-8', cwd=ROOT, timeout=30)
    if result.returncode:
        raise NMPAError(f'Node 补环境失败：{result.stderr[-1500:]}')
    return json.loads(result.stdout)


class PureNMPAClient(NMPAClient):
    """来源客户端的 Node 运行时实现，保留原有采集接口。"""
    def _solve_page(self, url, referrer, marker=None):
        """初始化来源页面运行时并得到当前会话的 Cookie。"""
        host = urlparse(url).hostname
        for attempt in range(4):
            response = self.session.get(url, headers={'Referer': referrer}, timeout=self.timeout)
            response.encoding = 'utf-8'
            if response.status_code == 200 and (marker is None or marker in response.text):
                return
            parser = ChallengeParser(response.text)
            scripts = [(a, b) for a, b in parser.scripts if a.get('r') == 'm']
            external = next((a['src'] for a, b in scripts if a.get('src')), None)
            if not parser.meta or not external:
                raise NMPAError(f'未识别动态挑战：HTTP {response.status_code}')
            script_url = urljoin(url, external)
            if urlparse(script_url).netloc != urlparse(url).netloc:
                raise NMPAError('挑战脚本跨域，停止执行')
            js = self.session.get(script_url, timeout=self.timeout)
            js.raise_for_status()
            visible = '; '.join(f'{c.name}={c.value}' for c in self.session.cookies
                                if c.domain.lstrip('.') == urlparse(url).hostname and 'HttpOnly' not in c._rest)
            result = node('dom-environment.cjs', dict(html=response.text, url=url, userAgent=UA,
                          meta=parser.meta, scriptUrl=external, external=js.text,
                          inline='\n'.join(b for a, b in scripts if '$_ts' in b),
                          after='\n'.join(b for a, b in scripts if not a.get('src') and '$_ts' not in b),
                          cookie=visible, referrer=referrer, normalizeChrome=True, trace=False))
            if not result.get('cookie'):
                raise NMPAError('补环境没有产生 Cookie')
            for item in result['cookie'].split('; '):
                name, value = item.split('=', 1)
                self.session.cookies.set(name, value, domain=urlparse(url).hostname, path='/')
            url = result.get('redirect') or url
            if urlparse(url).hostname != host:
                raise NMPAError('挑战导航地址跨域')
            LOG.info('%s：纯 Node 已生成动态 Cookie，第 %s 次挑战', host, attempt + 1)
        else:
            raise NMPAError('补环境未通过首页校验，不启用浏览器回退')

    def bootstrap(self):
        """初始化来源会话及业务脚本，为后续查询准备参数。"""
        self._clock = None
        self.session.cookies.clear()
        self.session.headers['User-Agent'] = UA
        self._solve_page('https://www.nmpa.gov.cn/', 'https://www.nmpa.gov.cn/')
        self._solve_page(HOME.split('#')[0], 'https://www.nmpa.gov.cn/', marker='js/ajax.js')
        js = self.session.get(BASE + 'js/ajax.js', timeout=self.timeout)
        js.raise_for_status()
        vectors = [dict(isSenior='N', itemId='test', pageNum=2, pageSize=10,
                        searchValue="髋关节 !()~*' &+", timestamp=1234567890000),
                   dict(itemId='test', id='abc', timestamp=1234567890000)]
        decoded = node('business-signature.cjs', dict(source=js.text, vectors=vectors))
        self.secret = decoded['secret']
        if any(v['sign'] != make_sign(v['params'], self.secret) for v in decoded['vectors']):
            raise NMPAError('当前业务 JS 与 Python 签名不一致')
        LOG.info('纯 Node 会话初始化成功，当前业务 JS 签名校验通过；未启动浏览器')
