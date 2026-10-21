# 阅读说明：HTTP接口组装层：鉴权、创建普通任务、查询状态、恢复采集和确认试采入库。
# 接口只把任务写入本地SQLite并返回202；采集和入库由worker异步完成。
# daily_enabled开启时还会挂载/api/v1/daily接口，职责与普通jobs接口不同。
# 本文件只增加解释注释，业务代码保持原样。

import csv
import secrets
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.security import APIKeyHeader
from .config import Settings
from .models import CATEGORY_NAMES, CollectRequest
from .tasks import TaskStore
from .preview_import import validate_preview
from .resume import validate_resume


# 入口：接收可选Settings（测试可注入），返回组装好的FastAPI实例；不会在这里跑采集。
def create_app(settings=None):
    """组装 HTTP 接口、鉴权和任务存储；实际采集由独立 worker 完成。"""
    settings = settings or Settings.from_env()
    if not settings.api_key:
        raise RuntimeError('请配置 NMPA_API_KEY 后启动接口')
    tasks = TaskStore(settings.data_dir)
    # Java FastApiClient 在 HTTP 请求头中发送 X-API-Key；不要将密钥放 URL。
    # 密钥变更后需要重启 API，让这里读取的新 Settings 生效。
    key_header = APIKeyHeader(name='X-API-Key', auto_error=False)

    # 读取X-API-Key并与服务端配置做恒定时间比较；缺失/错误返回401，后续业务函数不再执行。
    def authenticate(key=Depends(key_header)):
        """校验服务之间的接口密钥，使用恒定时间比较避免普通字符串比较。"""
        if not key or not secrets.compare_digest(key, settings.api_key):
            raise HTTPException(status_code=401, detail='无效的 API Key')

    app = FastAPI(title='NMPA 医疗器械采集接口', version='2.0.0-313')
    app.state.tasks = tasks
    # daily与普通任务共用鉴权，但使用各自的SQLite队列；新旧接口编号不能混查。
    if settings.daily_enabled:
        from .daily.router import install_daily
        install_daily(app, settings, authenticate)

    # 仅表示API可响应和是否填写了数据库URL；不会连接数据库，也不能据此确认worker存活。
    @app.get('/health')
    def health():
        """返回服务和数据库配置状态，不返回密码。"""
        return {'status': 'ok', 'database_configured': bool(settings.database_url)}

    # 返回接口类别代码和中文名称，供Java/页面建立选择列表。
    @app.get('/api/v1/categories', dependencies=[Depends(authenticate)])
    def categories():
        """返回前端可选择的采集类别。"""
        return [{'code': code.value, 'name': name} for code, name in CATEGORY_NAMES.items()]

    # 校验自动入库所需数据库配置，把请求保存为queued；返回job_id和状态地址，不等采集完成。
    @app.post('/api/v1/jobs', status_code=status.HTTP_202_ACCEPTED, dependencies=[Depends(authenticate)])
    def create_job(request: CollectRequest):
        """校验请求后创建异步采集任务。"""
        if not request.dry_run and not settings.database_url:
            raise HTTPException(status_code=503, detail='尚未配置数据库；只允许 dry_run=true 的试采')
        # 入队成功只代表受理：HTTP202不代表已采集、已备份或已写入MySQL。
        task = tasks.create(request.model_dump(mode='json'))
        return {'job_id': task['id'], 'state': task['state'], 'status_url': '/api/v1/jobs/' + task['id']}

    # 读取任务后计算恢复/试采入库资格；资格检查失败写入阻止原因，便于页面禁用按钮并解释。
    def get_task(job_id):
        """查询任务并计算当前预览是否允许入库及阻止原因。"""
        task = tasks.get(job_id)
        if task is None:
            raise HTTPException(status_code=404, detail='任务不存在')
        task['can_resume_collection'] = False
        task['resume_block_reason'] = None
        try:
            validate_resume(settings, task)
            task['can_resume_collection'] = True
        except ValueError as exc:
            task['resume_block_reason'] = str(exc)
        task['can_import_preview'] = False
        task['import_block_reason'] = '尚未配置数据库'
        if settings.database_url:
            try:
                validate_preview(settings, task)
                task['can_import_preview'] = True
                task['import_block_reason'] = None
            except ValueError as exc:
                task['import_block_reason'] = str(exc)
        return task

    # 把本地持久化任务及计算后的操作资格返回Java；这里不推进worker状态。
    @app.get('/api/v1/jobs/{job_id}', dependencies=[Depends(authenticate)])
    def task_status(job_id: str):
        """返回任务状态给 Java 后端。"""
        return get_task(job_id)

    # 在队列事务中验证原断点并重新排队；不存在转404，资格不符转409。
    @app.post('/api/v1/jobs/{job_id}/resume', status_code=202, dependencies=[Depends(authenticate)])
    def resume_collection(job_id: str):
        """继续原任务：范围和编号保持不变，worker 读取原目录中的断点。"""
        try:
            task = tasks.enqueue_resume(job_id, lambda task: validate_resume(settings, task))
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return {'job_id': task['id'], 'state': task['state']}

    # 人工确认已有试采：验证候选并把原任务排为import_existing；不会创建新采集范围。
    @app.post('/api/v1/jobs/{job_id}/import' , status_code=202, dependencies=[Depends(authenticate)])
    def import_preview(job_id: str):
        """校验试采文件后原子排队入库，重复确认会被拒绝。"""
        if not settings.database_url:
            raise HTTPException(status_code=503, detail='尚未配置数据库')
        try:
            task = tasks.enqueue_import(job_id, lambda task: validate_preview(settings, task))
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return {'job_id': task['id'], 'state': task['state']}

    # 从该任务导出的标准CSV按offset/limit读取候选，不是查询MySQL新增行的接口。
    @app.get('/api/v1/jobs/{job_id}/records', dependencies=[Depends(authenticate)])
    def records(job_id: str, offset: int = Query(default=0, ge=0), limit: int = Query(default=20, ge=1, le=100)):
        """分页读取标准 CSV，避免将全部数据一次发送给页面。"""
        task = get_task(job_id)
        path = settings.data_dir / 'jobs' / task['id'] / 'database' / 't_medical_device.csv'
        if not path.exists():
            raise HTTPException(status_code=409, detail='字段转换结果尚未生成')
        rows = []
        with path.open(encoding='utf-8-sig', newline='') as stream:
            for index, row in enumerate(csv.DictReader(stream)):
                if index < offset:
                    continue
                if len(rows) == limit:
                    break
                rows.append(row)
        return {'job_id': job_id, 'offset': offset, 'limit': limit, 'records': rows}

    return app
