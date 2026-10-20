"""PyCharm 后台任务入口：API 运行后，再单独运行本文件；两个都需保持运行。"""
from api.worker import main

if __name__ == '__main__':
    main()
