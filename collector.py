# 阅读说明：普通任务采集的库函数入口，把请求中的类别代码转为来源中文名称。
# 选多个类别时按for循环串行执行；每个类别先采列表和详情，全部结束后统一导出。
# 本文件只增加解释注释，业务代码保持原样。

from pathlib import Path
from export_medical_device import export
from pipeline import collect_job
from .models import CATEGORY_NAMES


# request是校验后的CollectRequest，batch是本任务目录，progress是状态回调；返回导出目录和采集摘要。
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
