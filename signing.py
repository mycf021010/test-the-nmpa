# 阅读说明：保持现有业务请求的参数编码与签名一致。
# 这是来源请求协议的一部分；与Java发送的X-API-Key无关，不应把二者配置混淆。
# 本文件只增加解释注释，业务代码保持原样。

"""Reconstructed from the live datasearch/js/ajax.js, not guessed keys."""
import hashlib
from urllib.parse import quote


# 按现有来源JS的字符串和排序语义构造参数文本；协议兼容层，不是业务筛选规则。
def canonical_query(params: dict) -> str:
    """按接口约定规范化查询参数顺序和编码。"""
    # 布尔值转JS小写true/false，其余按字符串表示，避免Python True造成参数不一致。
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


# 对既有协议所需参数编码并生成摘要；保持原实现，用于请求一致性校验，不是接口API密钥。
def make_sign(params: dict, secret: str = 'nmpasecret2020') -> str:
    """生成当前请求使用的签名参数。"""
    payload = canonical_query(params) + '&' + secret
    # encodeURIComponent keeps * and apostrophe; frontend additionally escapes !()~.
    encoded = quote(payload, safe="-_.!~*'()")
    for char, replacement in [('!', '%21'), ('(', '%28'), (')', '%29'), ('~', '%7E')]:
        encoded = encoded.replace(char, replacement)
    return hashlib.md5(encoded.encode('utf-8')).hexdigest()
