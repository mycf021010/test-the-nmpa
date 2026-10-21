# 阅读说明：daily详情的标准化和同完整证号候选合并。
# 先区分正常过滤和字段错误，再合并业务文本，最后重新推导部位和骨科标签。
# 同证号有冲突业务字段时排除该候选并报告冲突，不任意覆盖其中一份。
# 本文件只增加解释注释，业务代码保持原样。

"""Daily normalization reuses the versioned 22 fields without treating search terms as anatomy."""
import json
from export_medical_device import convert, validate


# 把单条来源详情转换为22字段；正常非313只计filtered，证号异常或字段校验失败计invalid。
def normalize_detail(fields, keyword, category):
    """313 business eligibility is distinct from field failures and anatomy."""
    from api.collection_rules import eligibility
    if keyword != '313':
        raise ValueError('新任务只允许313搜索')
    row = convert(fields, keyword, category)
    status, reason = eligibility(row)
    if status == 'filtered':
        return row, []
    if status == 'invalid':
        return row, [reason]
    return row, validate(row)


# 按完整证号分组，补充空文本但不接受互相冲突的非空业务值；返回可入库members和冲突清单。
def merge_candidates(candidates):
    """Merge final business text before deriving anatomy; conflicting rows are excluded."""
    from api.collection_rules import derive_fields, eligibility
    groups = {}
    for candidate in candidates:
        if eligibility(candidate['row'])[0] != 'eligible':
            raise ValueError('非313或异常记录不能进入候选快照')
        groups.setdefault(candidate['row']['registration_no_original'], []).append(candidate)
    valid, conflicts = [], []
    for certificate, values in sorted(groups.items()):
        merged = dict(values[0]['row'])
        conflict_fields = []
        for key in merged:
            if key not in ('body_part', 'source_file', 'collection_tag'):
                nonempty = sorted({v['row'][key] for v in values if v['row'][key]})
                if len(nonempty) > 1:
                    conflict_fields.append(key)
                merged[key] = nonempty[0] if nonempty else ''
        if conflict_fields:
            conflicts.append({'registration_no_original': certificate, 'fields': conflict_fields})
            continue
        merged = derive_fields(merged)
        merged['source_file'] = json.dumps(sorted({cat for v in values for cat in json.loads(v['row']['source_file'])}), ensure_ascii=False)
        errors = validate(merged)
        if errors:
            conflicts.append({'registration_no_original': certificate, 'fields': errors})
            continue
        matches = sorted({(v['category'], v['keyword'], v['source_id']) for v in values})
        valid.append({'row': merged, 'matches': [dict(category=c, keyword=k, source_id=s) for c, k, s in matches]})
    return valid, conflicts
