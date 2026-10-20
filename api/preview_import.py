"""Import reviewed limited previews, while rejecting actual collection failures."""
import hashlib
import json
from .models import CATEGORY_NAMES, Category
from pathlib import Path

LIMIT_NOTICE = '限页试采，禁止入库'


def preview_path(settings, task):
    """定位当前任务的标准 CSV，并限制路径在任务数据目录中。"""
    return settings.data_dir / 'jobs' / task['id'] / 'database' / 't_medical_device.csv'


def validate_preview(settings, task, queued=False):
    """检查试采状态、字段、采集审计和文件哈希，拒绝不可靠的入库结果。"""
    result = task.get('result') or {}
    summary = result.get('collection') or {}
    request = task.get('request') or {}
    if not request.get('dry_run') or result.get('database') is not None:
        raise ValueError('此任务不是尚未入库的试采结果')
    if not queued and task['state'] not in ('succeeded', 'partial'):
        raise ValueError('任务尚未完成或执行失败，不能入库')
    if summary.get('validation_errors') != 0 or summary.get('valid_count', 0) <= 0:
        raise ValueError('试采结果为空或字段校验失败，不能入库')
    issues = summary.get('issues', [])
    legacy = [i for i in issues if i != LIMIT_NOTICE]
    legacy_ok = False
    if legacy and request.get('max_pages', 0) > 0 and summary.get('limited'):
        legacy_ok = verify_limited_sources(settings, task, legacy)
    if legacy and not legacy_ok:
        raise ValueError('试采存在采集错误，不能入库；仅允许主动限页的结果')
    if not summary.get('complete') and not summary.get('limited'):
        raise ValueError('采集完整性未确认，不能入库')
    path = preview_path(settings, task)
    if not path.is_file():
        raise ValueError('试采结果文件不存在')
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if queued and digest != request.get('preview_sha256'):
        raise ValueError('试采文件在确认后发生变化，已停止入库')
    return digest


def verify_limited_sources(settings, task, issues):
    """Legacy audits require ALL pages; verify completeness within requested limit."""
    request = task['request']
    allowed = set()
    root = settings.data_dir / 'jobs' / task['id']
    try:
        for keyword in request['keywords']:
            for code in request['categories']:
                category = CATEGORY_NAMES[Category(code)]
                path = root / 'normalized' / keyword / (category + '.json')
                data = json.loads(path.read_text(encoding='utf-8'))
                m = data['metadata']
                if m['category'] != category or m['searchValue'] != keyword:
                    return False
                if m.get('detailsFail', 0) or any(e != '原始采集完整性检查未通过' for e in m.get('collectionErrors', [])):
                    return False
                report_path = Path(m['rawReport']).resolve()
                if not report_path.is_relative_to(root.resolve()):
                    return False
                r = json.loads(report_path.read_text(encoding='utf-8'))
                pages = min(request['max_pages'], r['expected_pages'])
                expected = min(r['expected_total'], pages * r['page_size'])
                if r['page_size'] != request['page_size'] or r['keyword'] != keyword:
                    return False
                if r['saved_pages'] != pages or r['missing_pages'] != list(range(pages + 1, r['expected_pages'] + 1)):
                    return False
                if any(r.get(k) for k in ('page_errors', 'duplicate_ids', 'missing_detail_fields', 'missing_csv_source_columns')):
                    return False
                if any(r[k] != expected for k in ('list_rows', 'unique_records', 'completed_details', 'csv_rows')):
                    return False
                if m['total'] != expected or len(data['data']) != expected or m['siteTotal'] != r['expected_total']:
                    return False
                if expected and m.get('limitPerCategory') != expected:
                    return False
                allowed.add(str(path) + ': 原始采集完整性检查未通过')
                allowed.add(str(path) + ': 分页完整性未确认')
        return set(issues).issubset(allowed)
    except (OSError, ValueError, KeyError, TypeError):
        return False
