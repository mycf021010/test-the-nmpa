# 阅读说明：daily HTTP路由，统一使用X-API-Key鉴权。
# 创建轮次/小类任务只落盘，不在HTTP线程中等待网站采集；Java通过状态接口轮询。
# 本文件只增加解释注释，业务代码保持原样。

"""A方案内部接口：复用服务密钥，返回异步任务状态，不新增管理页面。"""
from typing import Annotated
from fastapi import APIRouter, Depends, Path
from .contracts import DailyError, JobRequest, RoundRequest
from api.collection_rules import RULE_VERSION, FIELD_SCHEMA_VERSION, PROTOCOL

ID = Annotated[str,Path(pattern=r'^[a-f0-9]{32}$')]


# 接收已初始化DailyStore和鉴权函数，返回所有daily路由；路由共用/api/v1/daily前缀。
def build_daily_router(store,authenticate):
    """组装小型每日接续协议；所有端点均要求已有X-API-Key鉴权。"""
    router = APIRouter(prefix='/api/v1/daily',dependencies=[Depends(authenticate)])

    # 返回协议/规则/22字段版本、4条来源流及稳定实例ID；这是版本核对，不是worker心跳。
    @router.get('/capabilities')
    def capabilities():
        """返回协议与稳定实例编号，帮助Java拒绝误连另一份断点目录。"""
        return {'protocol':PROTOCOL,'rule_version':RULE_VERSION,'field_schema_version':FIELD_SCHEMA_VERSION,'keywords':['313'],'stream_count':4,'quota_unit':'source_records','source_instance_id':store.instance_id,'target_min':80,'target_max':100}

    # 把RoundRequest交给SQLite控制库幂等打开轮次；未完成轮次会沿用原四类断点。
    @router.post('/rounds/open')
    def open_round(body:RoundRequest):
        """幂等打开当前轮；旧轮完成后新请求才允许创建新轮。"""
        return store.open_round(body)

    # 返回轮次和各来源流exhausted，Java据此跳过已经爬完的类别。
    @router.get('/rounds/{round_id}')
    def round_status(round_id:ID):
        """读取轮次和已耗尽小类，供Java跳过本轮已完成项。"""
        return store.round(round_id)

    # 要求配置目标数据库，再保存单个类别80至100条任务，返回202；实际执行在worker。
    @router.post('/jobs',status_code=202)
    def create_job(body:JobRequest):
        """持久化单小类任务并立即响应，采集由worker异步执行。"""
        if not store.settings.database_url:
            raise DailyError('DATABASE_REQUIRED','A方案自动入库需要配置目标数据库',503)
        return store.create_job(body)

    # 返回处理/过滤/无效计数和真实入库result；Java需等succeeded等终态再推进下一类别。
    @router.get('/jobs/{job_id}')
    def job_status(job_id:ID):
        """返回原任务真实状态和计数，不以已受理冒充采集完成。"""
        return store.job(job_id)

    # 用原request_id找回服务已经受理的job_id；POST响应丢失后查询它，避免换编号重复派发。
    @router.get('/requests/{request_id}')
    def request_status(request_id:ID):
        """根据原请求编号恢复创建确认，不重新分配每日额度。"""
        return store.request(request_id)

    # 把原blocked/interrupted等可恢复任务排回queued，保留轮次、配额、快照和原入库操作ID。
    @router.post('/jobs/{job_id}/resume',status_code=202)
    def resume(job_id:ID):
        """由运维显式恢复原中断任务，保持采集进度和提交编号。"""
        return store.resume(job_id)

    return router


# 加载已迁移的本地控制库、挂载路由并注册DailyError处理；不会自动迁移MySQL表。
def install_daily(app,settings,authenticate):
    """只在显式启用后安装A方案路由及局部安全异常处理。"""
    from .store import DailyStore
    store = DailyStore(settings)
    app.state.daily = store
    app.include_router(build_daily_router(store,authenticate))

    # 将DailyError转换为包含error_code/message的JSON，状态码由领域错误决定；不回传SQL或密码。
    @app.exception_handler(DailyError)
    async def daily_error_handler(request,error):
        """将领域失败转换为安全响应，不返回SQL、真实凭据或完整堆栈。"""
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=error.status,content={'error_code':error.code,'message':error.message})
