# 阅读说明：API启动入口。运行它只启动HTTP服务；耗时采集需要另一个worker_main.py进程。
# 配置先由local_config加载，再由create_app组装接口；监听端口默认8000。
# 本文件只增加解释注释，业务代码保持原样。

"""PyCharm 接口启动入口：右键本文件，选择 Run main。"""
import os
import uvicorn
from api.local_config import load_local_config


# 加载与worker共用的配置，再启动Uvicorn应用工厂；返回前服务持续运行。
def main():
    """加载本地配置并启动 API。采集任务由另一个 worker 进程处理。"""
    load_local_config()
    uvicorn.run('api.app:create_app', factory=True,
                host=os.getenv('NMPA_HOST', '127.0.0.1'),
                port=int(os.getenv('NMPA_PORT', '8000')))

if __name__ == '__main__':
    main()
