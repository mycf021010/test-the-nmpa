from pathlib import Path
from export_medical_device import export
from pipeline import collect_job
from .models import CATEGORY_NAMES


def collect(request, batch, progress, client_factory=None):
    """Library entry point: no argparse, interactive input, or main invocation."""
    if client_factory is None:
        from nmpa.pure import PureNMPAClient
        client_factory = PureNMPAClient
    batch = Path(batch)
    categories = [CATEGORY_NAMES[category] for category in request.categories]
    inputs = [batch / 'normalized' / kw.value for kw in request.keywords]
    with client_factory(interval=1.0) as client:
        available = client.categories()
        for index, category in enumerate(categories):
            matches = [item for item in available if item.get('itemName') == category and item.get('itemType') == 'table']
            if len(matches) != 1:
                raise RuntimeError('无法唯一识别类目：' + category)
            for keyword in request.keywords:
                collect_job(client, matches[0], keyword.value, batch / 'raw' / keyword.value / str(index),
                            request.max_pages, request.page_size, progress)
    progress({'stage': 'converting'})
    issues = ['限页试采，禁止入库'] if request.max_pages else []
    destination = batch / 'database'
    summary = export(inputs, destination, run_issues=issues, categories=categories)
    return destination, summary
