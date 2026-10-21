# 阅读说明：独立后台执行入口。它调用api.worker.main领取已入队任务。
# API和worker必须使用同一个配置文件、同一个data_dir；单独启动API不会执行采集。
# 本文件只增加解释注释，业务代码保持原样。

"""PyCharm 后台任务入口：API 运行后，再单独运行本文件；两个都需保持运行。"""
from api.worker import main

if __name__ == '__main__':
    main()
