# 阅读说明：真实数据库只读检查入口：器械表结构、记录数、SELECT FOR UPDATE所需权限。
# 检查后rollback释放锁；不插入/更新器械数据，也不代表daily三表已迁移。
# 本文件只增加解释注释，业务代码保持原样。

"""PyCharm 数据库检查入口：只读取表结构、记录数量和行锁权限，不插入记录。"""
import shutil
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from api.config import Settings
from api.database import DeviceDatabase

if __name__ == '__main__':
    settings = Settings.from_env()
    if not settings.api_key:
        raise RuntimeError('请填写 config/settings.local.json 中的 api_key')
    if not settings.database_url:
        raise RuntimeError('请填写 config/settings.local.json 中的 mysql 连接信息')
    if not (Path(settings.mysqldump).is_file() or shutil.which(settings.mysqldump)):
        raise RuntimeError('找不到 mysqldump.exe，请核对配置中的完整路径')
    database = None
    try:
        database = DeviceDatabase(settings)
        with database.engine.connect() as connection:
            # 表名已通过 DeviceDatabase 标识符校验；参数化 SQL 不支持表名占位符。
            count = connection.execute(text(f'SELECT COUNT(*) FROM `{settings.table}`')).scalar_one()
            # 此处真的申请一次行锁来检测权限，然后rollback释放；不会修改该行。
            connection.execute(text(f'SELECT id FROM `{settings.table}` LIMIT 1 FOR UPDATE')).first()
            connection.rollback()  # 释放检查用行锁，不修改数据。
        print(f'数据库连接成功，表结构和行锁权限检查通过，记录数：{count}')
        print('备份工具路径有效；实际备份是否成功将在入库前验证。')
    except SQLAlchemyError as exc:
        code = getattr(exc.orig, 'args', ('unknown',))[0]
        raise RuntimeError(f'数据库检查失败，错误码 {code}。请检查本机账号、密码、权限；1142 通常需补 UPDATE 权限。') from None
    finally:
        if database is not None:
            database.close()
