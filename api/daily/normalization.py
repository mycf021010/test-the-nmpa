"""Daily normalization reuses the established 21 fields without treating search terms as anatomy."""
import json
from export_medical_device import convert, validate


def normalize_detail(fields, keyword, category):
    """复用21字段转换和校验，只把膝髋受控词映射为部位。"""
    body_part = keyword if keyword in ('膝', '髋') else '未分类'
    row = convert(fields, body_part, category)
    # Keep legacy validation strict; validate the new controlled value separately.
    legacy_row = dict(row, body_part='膝') if body_part == '未分类' else row
    return row, validate(legacy_row)


def merge_candidates(candidates):
    """Deterministic certificate merge; conflicting business values are excluded for review."""
    groups = {}
    for candidate in candidates:
        groups.setdefault(candidate['row']['registration_no_original'], []).append(candidate)
    valid, conflicts = [], []
    for certificate, values in sorted(groups.items()):
        merged = dict(values[0]['row'])
        conflict_fields = []
        for key in merged:
            if key not in ('body_part', 'source_file'):
                nonempty = sorted({v['row'][key] for v in values if v['row'][key]})
                if len(nonempty) > 1:
                    conflict_fields.append(key)
                merged[key] = nonempty[0] if nonempty else ''
        if conflict_fields:
            conflicts.append({'registration_no_original': certificate, 'fields': conflict_fields})
            continue
        parts = [part for part in ('膝', '髋') if any(part in v['row']['body_part'].split('，') for v in values)]
        merged['body_part'] = '，'.join(parts) or '未分类'
        merged['source_file'] = json.dumps(sorted({cat for v in values for cat in json.loads(v['row']['source_file'])}), ensure_ascii=False)
        matches = sorted({(v['category'], v['keyword'], v['source_id']) for v in values})
        valid.append({'row': merged, 'matches': [dict(category=c, keyword=k, source_id=s) for c, k, s in matches]})
    return valid, conflicts
