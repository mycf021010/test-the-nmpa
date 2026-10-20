"""PyCharm 备份检查入口：读取原表，生成本地备份并校验，不插入或更新业务表。"""
import uuid
from api.config import Settings
from api.database import DeviceDatabase
from sqlalchemy.exc import SQLAlchemyError


def main():
    """用真实 mysqldump 验证完整备份流程，定位数据库连接测试不能发现的问题。"""
    settings = Settings.from_env()
    database = None
    try:
        database = DeviceDatabase(settings)
        backup = database.backup('backup-check-' + uuid.uuid4().hex)
        database.verify_backup(backup)
        print('备份生成及文件校验成功；本次没有插入、更新或删除业务记录。')
        print('备份编号：', backup['id'])
        print('备份字节数：', backup['bytes'])
        print('备份保存在当前配置 data_dir 下的 backups 文件夹。')
    except OSError as exc:
        winerror = getattr(exc, 'winerror', None)
        print('备份文件/外部程序操作失败；系统错误码：',
              f'WinError={winerror}' if winerror is not None else f'errno={exc.errno}')
        print('请检查 data_dir 是否可写、磁盘空间及 mysqldump 文件运行权限。')
        raise SystemExit(1) from None
    except SQLAlchemyError as exc:
        code = getattr(exc.orig, 'args', (None,))[0]
        print('数据库访问失败；错误码：', code if isinstance(code, int) else '未知')
        print('请检查账号密码和原表权限，不要发送配置中的真实密码。')
        raise SystemExit(1) from None
    except (RuntimeError, ValueError) as exc:
        print('备份检查失败：', str(exc))
        raise SystemExit(1) from None
    finally:
        if database is not None:
            database.close()

if __name__ == '__main__':
    main()
