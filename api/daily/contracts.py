"""定义每日轮转协议、稳定摘要及安全领域错误。"""
import hashlib
import json
from datetime import datetime, timezone
from pydantic import BaseModel, ConfigDict, Field
from api.models import Category, Keyword


class DailyError(Exception):
    """携带可安全返回调用端的领域错误，不包含数据库连接凭据。"""
    def __init__(self, code, message, status=409):
        """保存稳定错误码、中文说明及HTTP状态。"""
        super().__init__(message)
        self.code, self.message, self.status = code, message, status


class StrictRequest(BaseModel):
    """拒绝未知字段，避免新旧接口参数被静默忽略。"""
    model_config = ConfigDict(extra='forbid')


class RoundRequest(StrictRequest):
    """以幂等请求编号打开当前轮；每轮固定来源每页条数。"""
    request_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    page_size: int = Field(default=10, ge=1, le=100)


class JobRequest(StrictRequest):
    """提交当天单类别单关键词的限量任务，数量单位为来源记录。"""
    request_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    daily_task_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    subtask_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    round_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    category: Category
    keyword: Keyword
    target_records: int = Field(ge=80, le=100)


def dumps(value):
    """稳定序列化控制数据，便于摘要比较并保留中文。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def digest(value):
    """计算固定任务参数或候选的SHA-256，防止同编号参数漂移。"""
    return hashlib.sha256(dumps(value).encode()).hexdigest()


def now():
    """生成带UTC时区的持久化时间；展示端转换为北京时间。"""
    return datetime.now(timezone.utc).isoformat()
