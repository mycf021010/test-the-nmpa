# 阅读说明：普通jobs接口的请求校验模型。限制四种类别、313关键词、页数和规则版本。
# 校验失败由FastAPI/Pydantic返回参数错误；这里不执行采集或SQL。
# 本文件只增加解释注释，业务代码保持原样。

from enum import Enum
from pydantic import BaseModel, ConfigDict, Field, model_validator
from export_medical_device import CATS


class Category(str, Enum):
    """四种采集类别的枚举，避免把页面中文名称当作接口参数。"""
    domestic = 'domestic'
    domestic_history = 'domestic_history'
    imported = 'imported'
    imported_history = 'imported_history'


CATEGORY_NAMES = dict(zip(Category, CATS))


class Keyword(str, Enum):
    """固定313业务搜索词。"""
    orthopedics = '313'


class CollectRequest(BaseModel):
    """Python 接口的请求模型，负责类别、关键词和限页模式校验。"""
    model_config = ConfigDict(extra='forbid')
    # 四类可多选但不能重复；默认四类。这里是普通任务范围，不是每10分钟四类配额的计划。
    categories: list[Category] = Field(default_factory=lambda: list(Category), min_length=1, max_length=4)
    # 本版固定搜索313；膝/髋是结果部位标签，已不作为采集关键词。
    keywords: list[Keyword] = Field(default_factory=lambda: list(Keyword), min_length=1, max_length=1)
    rule_version: str = Field(default='orthopedics-313-v1', pattern='^orthopedics-313-v1$')
    field_schema_version: int = Field(default=22, ge=22, le=22)
    # True只生成试采结果；False要求完整采集后自动入库。试采人工确认入口不受此分支直接控制。
    dry_run: bool = False
    # 0表示抓到来源末页；正数为每个选中类别/关键词的页数上限，不是所有类别合计页数。
    max_pages: int = Field(default=0, ge=0, le=10000)
    # 每次来源列表请求的条数，1至100；与“本次抓多少页”和“最终新增几条”不同。
    page_size: int = Field(default=10, ge=1, le=100)

    # 检查重复类别/关键词和限页模式；普通任务max_pages>0必须dry_run，人工确认入库另走import接口。
    @model_validator(mode='after')
    def check_mode(self):
        """限页只能用于试采；全量自动入库必须不限制页数。"""
        if len(set(self.categories)) != len(self.categories) or len(set(self.keywords)) != len(self.keywords):
            raise ValueError('类别和关键词不能重复')
        if self.max_pages and not self.dry_run:
            raise ValueError('限页试采必须设置 dry_run=true，不能入库')
        return self
