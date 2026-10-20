"""显式初始化A方案控制库；不连接目标业务数据库。"""
from api.config import Settings
from api.daily.store import DailyStore


def main():
    """读取与API/worker相同配置，初始化轮次库并打印稳定源实例编号。"""
    store = DailyStore(Settings.from_env(),migrate=True)
    print('A方案控制库已初始化；source_instance_id='+store.instance_id)
    print('未启动采集，未连接业务数据库；请配置Java实例核对值后再启用。')


if __name__ == '__main__':
    main()
