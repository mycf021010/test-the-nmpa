# 阅读说明：显式创建daily的本地SQLite控制库并输出稳定实例编号。
# 不访问来源网站、不创建MySQL表、不启动采集；API、worker、Java要核对同一个实例。
# 本文件只增加解释注释，业务代码保持原样。

"""显式初始化A方案控制库；不连接目标业务数据库。"""
from api.config import Settings
from api.daily.store import DailyStore


# 用当前配置显式初始化本地DailyStore；打印稳定实例ID供Java配置，未启动采集。
def main():
    """读取与API/worker相同配置，初始化轮次库并打印稳定源实例编号。"""
    # migrate=True仅初始化data_dir/daily-313.sqlite3；这个操作与MySQL账本建表分开。
    store = DailyStore(Settings.from_env(),migrate=True)
    print('A方案控制库已初始化；source_instance_id='+store.instance_id)
    print('未启动采集，未连接业务数据库；请配置Java实例核对值后再启用。')


if __name__ == '__main__':
    main()
