"""Reconstructed from the live datasearch/js/ajax.js, not guessed keys."""
import hashlib
from urllib.parse import quote


def canonical_query(params: dict) -> str:
    """按接口约定规范化查询参数顺序和编码。"""
    def js_string(v):
        """保持 Python 与 JavaScript 字符串转换规则一致。"""
        if isinstance(v, bool):
            return 'true' if v else 'false'
        return str(v)
    # The frontend checks value != '' using loose equality (0 and false omitted).
    raw = '&'.join(f'{k}={js_string(v)}' for k, v in params.items()
                   if v is not None and v != '' and v is not False and v != 0)
    # paramsStrSort splits the raw string on '&', including '&' inside a value.
    return '&'.join(sorted(raw.split('&'), key=lambda s: s.encode('utf-16-be', errors='surrogatepass')))


def make_sign(params: dict, secret: str = 'nmpasecret2020') -> str:
    """生成当前请求使用的签名参数。"""
    payload = canonical_query(params) + '&' + secret
    # encodeURIComponent keeps * and apostrophe; frontend additionally escapes !()~.
    encoded = quote(payload, safe="-_.!~*'()")
    for char, replacement in [('!', '%21'), ('(', '%28'), (')', '%29'), ('~', '%7E')]:
        encoded = encoded.replace(char, replacement)
    return hashlib.md5(encoded.encode('utf-8')).hexdigest()
