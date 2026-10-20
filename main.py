"""PyCharm 接口启动入口：右键本文件，选择 Run main。"""
import os
import uvicorn
from api.local_config import load_local_config


def main():
    """加载本地配置并启动 API。采集任务由另一个 worker 进程处理。"""
    load_local_config()
    uvicorn.run('api.app:create_app', factory=True,
                host=os.getenv('NMPA_HOST', '127.0.0.1'),
                port=int(os.getenv('NMPA_PORT', '8000')))

if __name__ == '__main__':
    main()
