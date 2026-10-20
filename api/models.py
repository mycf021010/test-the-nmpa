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
    """支持的关节关键词枚举。"""
    knee = '膝'
    hip = '髋'


class CollectRequest(BaseModel):
    """Python 接口的请求模型，负责类别、关键词和限页模式校验。"""
    model_config = ConfigDict(extra='forbid')
    categories: list[Category] = Field(default_factory=lambda: list(Category), min_length=1, max_length=4)
    keywords: list[Keyword] = Field(default_factory=lambda: list(Keyword), min_length=1, max_length=2)
    dry_run: bool = False
    max_pages: int = Field(default=0, ge=0, le=10000)
    page_size: int = Field(default=10, ge=1, le=100)

    @model_validator(mode='after')
    def check_mode(self):
        """限页只能用于试采；全量自动入库必须不限制页数。"""
        if len(set(self.categories)) != len(self.categories) or len(set(self.keywords)) != len(self.keywords):
            raise ValueError('类别和关键词不能重复')
        if self.max_pages and not self.dry_run:
            raise ValueError('限页试采必须设置 dry_run=true，不能入库')
        return self
