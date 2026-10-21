# 阅读说明：313业务规则的统一入口，普通任务和daily任务共同使用。
# 从完整注册证号提取数字，第5至7位必须等于313；不是任意位置出现313就保留。
# 膝/髋仅从产品名称、结构组成、适用范围三个业务字段推导，不从搜索词推导。
# 本文件只增加解释注释，业务代码保持原样。

"""Versioned business rules. Search terms are never anatomy labels."""
import re

# 协议与字段版本需要Java、API、worker一致；业务规则变更不能只改字符串而复用旧快照。
RULE_VERSION = 'orthopedics-313-v1'
FIELD_SCHEMA_VERSION = 22
PROTOCOL = 'daily-313-1'
SEARCH_KEYWORD = '313'
BODY_FIELDS = ('product_name', 'structure_description', 'intended_use')


# 只提取ASCII数字0至9，去掉国械注进等前缀；例如国械注进20223130618得到20223130618。
def numeric_certificate(value):
    return ''.join(re.findall('[0-9]', str(value or '')))


# 检查完整证号和纯数字字段一致，再判断digits[4:7]；返回eligible/filtered/invalid和稳定原因码。
def eligibility(row):
    """Return eligible/filtered/invalid and stable reason, independently of anatomy."""
    original = str(row.get('registration_no_original') or '').strip()
    digits = numeric_certificate(original)
    if not original or len(digits) < 7:
        return 'invalid', 'CERTIFICATE_INVALID'
    if str(row.get('registration_no') or '') != digits:
        return 'invalid', 'CERTIFICATE_DIGITS_MISMATCH'
    # Python切片从0开始：4对应第5位，7之前截止，即第5、6、7位。
    # 国械注进20223130618提取数字后是2022 + 313 + 0618，因此符合条件。
    if digits[4:7] != SEARCH_KEYWORD:
        return 'filtered', 'CERTIFICATE_NOT_313'
    return 'eligible', None


# 遍历product_name、structure_description、intended_use；任一字段出现膝/髋即加入，固定输出膝，髋顺序。
def infer_body_part(row):
    values = [str(row.get(key) or '') for key in BODY_FIELDS]
    # 同时命中输出中文逗号“膝，髋”；只命中一个输出单个字，都未命中输出空串，写库时转NULL。
    return '，'.join(part for part in ('膝', '髋') if any(part in value for value in values))


# 复制输入而不原地修改；填推导出的body_part，只有符合313才填collection_tag=骨科。
def derive_fields(row):
    result = dict(row)
    result['body_part'] = infer_body_part(result)
    result['collection_tag'] = '骨科' if eligibility(result)[0] == 'eligible' else ''
    return result
