"""Offline checks of the APIs used by the challenge, without a browser."""
import os
from api.local_config import load_local_config
load_local_config()
import json
import platform
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
import requests

print(f'OS: {platform.system()}; Python: {platform.python_version()}; requests: {requests.__version__}')
assert sys.version_info >= (3, 11), 'Python 3.11+ required'
version = subprocess.check_output([os.getenv('NMPA_NODE', 'node'), '-p', 'process.versions.node'], text=True).strip()
assert version.split('.')[0] == '24', f'Use Node.js 24; found {version}'
sample = dict(url='https://datasearch.nmpa.gov.cn/datasearch/home-index.html',
              referrer='https://www.nmpa.gov.cn/', userAgent='Mozilla/5.0 Chrome/131.0.0.0',
              html='<html><body></body></html>', cookie='', trace=False, normalizeChrome=True,
              inline='', external="if(typeof document.all !== 'undefined' || document.all !== null && Boolean(document.all))throw Error('document.all');document.cookie='runtime_check=ok;path=/';", after='')
result = subprocess.run([os.getenv('NMPA_NODE', 'node'), str(ROOT / 'js/dom-environment.cjs')], input=json.dumps(sample),
                        capture_output=True, text=True, encoding='utf-8', cwd=ROOT, timeout=30)
if result.returncode:
    raise RuntimeError(result.stderr)
assert 'runtime_check=ok' in json.loads(result.stdout)['cookie']
print(f'Node {version}: jsdom, V8 document.all, environment shims and Cookie execution OK')
print('离线环境检查通过；接着运行 main.py 和 worker_main.py。')
