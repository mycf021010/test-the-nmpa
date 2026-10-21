# 阅读说明：Java与daily接口之间的参数契约、规则版本、领域错误和稳定JSON摘要。
# request_id是幂等操作编号，不是API密钥；相同编号不能换一组参数。
# 本文件只增加解释注释，业务代码保持原样。

"""定义每日轮转协议、稳定摘要及安全领域错误。"""
import hashlib
import json
from datetime import datetime, timezone
from pydantic import BaseModel, ConfigDict, Field
from api.models import Category, Keyword
from api.collection_rules import RULE_VERSION, FIELD_SCHEMA_VERSION


class DailyError(Exception):
    """携带可安全返回调用端的领域错误，不包含数据库连接凭据。"""
    # DailyError保存业务错误码code、安全中文message和HTTP状态；异常处理器只返回这些受控字段。
    def __init__(self, code, message, status=409):
        """保存稳定错误码、中文说明及HTTP状态。"""
        super().__init__(message)
        self.code, self.message, self.status = code, message, status


class StrictRequest(BaseModel):
    """拒绝未知字段，避免新旧接口参数被静默忽略。"""
    model_config = ConfigDict(extra='forbid')
    rule_version: str = Field(default=RULE_VERSION, pattern='^orthopedics-313-v1$')
    field_schema_version: int = Field(default=FIELD_SCHEMA_VERSION, ge=22, le=22)


class RoundRequest(StrictRequest):
    """以幂等请求编号打开当前轮；每轮固定来源每页条数。"""
    # 幂等请求编号：网络超时后复用原编号核对；不是密码，也不是每次重试都重新随机。
    request_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    page_size: int = Field(default=10, ge=1, le=100)


class JobRequest(StrictRequest):
    """提交当天单类别单关键词的限量任务，数量单位为来源记录。"""
    # 幂等请求编号：网络超时后复用原编号核对；不是密码，也不是每次重试都重新随机。
    request_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    # Java一次每日/模拟日计划的主任务ID，用来关联Java的task表。
    daily_task_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    # Java计划中某一个类别项的ID；一个计划有四项，Python保留此ID供端到端追踪。
    subtask_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    # Python来源轮次ID：可跨多个每日计划保持同一轮四类页码。
    round_id: str = Field(pattern=r'^[a-f0-9]{32}$')
    category: Category
    keyword: Keyword
    # 本次只处理一个类别的80至100条来源详情；非313过滤、无效详情和已存在证号会减少实际新增。
    target_records: int = Field(ge=80, le=100)


# 中文保留、键排序和紧凑分隔生成稳定JSON；同一语义参数才能得到同一摘要。
def dumps(value):
    """稳定序列化控制数据，便于摘要比较并保留中文。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


# 对稳定JSON计算SHA-256，用于幂等参数、固定候选和目标库身份检查，不是用于保存密码。
def digest(value):
    """计算固定任务参数或候选的SHA-256，防止同编号参数漂移。"""
    return hashlib.sha256(dumps(value).encode()).hexdigest()


# 统一生成UTC ISO时间戳，避免Java、Linux和Windows的本地时区混入断点时间。
def now():
    """生成带UTC时区的持久化时间；展示端转换为北京时间。"""
    return datetime.now(timezone.utc).isoformat()
