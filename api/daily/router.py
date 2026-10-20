"""A方案内部接口：复用服务密钥，返回异步任务状态，不新增管理页面。"""
from typing import Annotated
from fastapi import APIRouter, Depends, Path
from .contracts import DailyError, JobRequest, RoundRequest

ID = Annotated[str,Path(pattern=r'^[a-f0-9]{32}$')]


def build_daily_router(store,authenticate):
    """组装小型每日接续协议；所有端点均要求已有X-API-Key鉴权。"""
    router = APIRouter(prefix='/api/v1/daily',dependencies=[Depends(authenticate)])

    @router.get('/capabilities')
    def capabilities():
        """返回协议与稳定实例编号，帮助Java拒绝误连另一份断点目录。"""
        return {'protocol':'daily-a-1','source_instance_id':store.instance_id,'target_min':80,'target_max':100}

    @router.post('/rounds/open')
    def open_round(body:RoundRequest):
        """幂等打开当前轮；旧轮完成后新请求才允许创建新轮。"""
        return store.open_round(body)

    @router.get('/rounds/{round_id}')
    def round_status(round_id:ID):
        """读取轮次和已耗尽小类，供Java跳过本轮已完成项。"""
        return store.round(round_id)

    @router.post('/jobs',status_code=202)
    def create_job(body:JobRequest):
        """持久化单小类任务并立即响应，采集由worker异步执行。"""
        if not store.settings.database_url:
            raise DailyError('DATABASE_REQUIRED','A方案自动入库需要配置目标数据库',503)
        return store.create_job(body)

    @router.get('/jobs/{job_id}')
    def job_status(job_id:ID):
        """返回原任务真实状态和计数，不以已受理冒充采集完成。"""
        return store.job(job_id)

    @router.get('/requests/{request_id}')
    def request_status(request_id:ID):
        """根据原请求编号恢复创建确认，不重新分配每日额度。"""
        return store.request(request_id)

    @router.post('/jobs/{job_id}/resume',status_code=202)
    def resume(job_id:ID):
        """由运维显式恢复原中断任务，保持采集进度和提交编号。"""
        return store.resume(job_id)

    return router


def install_daily(app,settings,authenticate):
    """只在显式启用后安装A方案路由及局部安全异常处理。"""
    from .store import DailyStore
    store = DailyStore(settings)
    app.state.daily = store
    app.include_router(build_daily_router(store,authenticate))

    @app.exception_handler(DailyError)
    async def daily_error_handler(request,error):
        """将领域失败转换为安全响应，不返回SQL、真实凭据或完整堆栈。"""
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=error.status,content={'error_code':error.code,'message':error.message})
