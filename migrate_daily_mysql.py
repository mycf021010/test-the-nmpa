# 阅读说明：MySQL三张daily账本表的显式建表入口。
# 不带--apply只说明步骤；带--apply才执行ledger_schema().create_all。
# 它与本地SQLite迁移migrate_daily.py不同；单表改造前不能省略后仍直接运行现有daily入库。
# 本文件只增加解释注释，业务代码保持原样。

"""A方案账本显式迁移入口，默认只说明步骤，不自动执行DDL。"""
import argparse
from sqlalchemy import create_engine
from api.config import Settings
from api.daily.ledger import ledger_schema


# 没有--apply立即退出；明确指定时才连接配置库create_all三张账本表，不修改器械表。
def main():
    """仅明确传入--apply时，在已核对的测试目标库创建新增账本表。"""
    parser=argparse.ArgumentParser(description='请使用迁移账号核对目标测试库及备份后执行')
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    # 默认分支没有数据库写操作；只有命令显式传--apply才继续下面DDL。
    if not args.apply:
        print('未执行DDL。核对目标测试库、权限和备份后，可明确使用--apply。')
        return
    settings=Settings.from_env()
    if not settings.database_url:
        raise SystemExit('目标数据库未配置')
    engine=create_engine(settings.database_url,hide_parameters=True)
    try:
        # 真正创建MySQL三张辅助表的位置；checkfirst=True只在不存在时创建，不是每次产生新名字的表。
        ledger_schema()[0].create_all(engine,checkfirst=True)
        print('A方案新增账本表已准备，未修改原器械表、未启动采集。')
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()
