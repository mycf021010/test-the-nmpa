"""A方案账本显式迁移入口，默认只说明步骤，不自动执行DDL。"""
import argparse
from sqlalchemy import create_engine
from api.config import Settings
from api.daily.ledger import ledger_schema


def main():
    """仅明确传入--apply时，在已核对的测试目标库创建新增账本表。"""
    parser=argparse.ArgumentParser(description='请使用迁移账号核对目标测试库及备份后执行')
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    if not args.apply:
        print('未执行DDL。核对目标测试库、权限和备份后，可明确使用--apply。')
        return
    settings=Settings.from_env()
    if not settings.database_url:
        raise SystemExit('目标数据库未配置')
    engine=create_engine(settings.database_url,hide_parameters=True)
    try:
        ledger_schema()[0].create_all(engine,checkfirst=True)
        print('A方案新增账本表已准备，未修改原器械表、未启动采集。')
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()
