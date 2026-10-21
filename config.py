# 阅读说明：API和worker共用的最终配置对象。from_env会先加载本地JSON，再读取环境变量。
# api_key用于Java与FastAPI鉴权；database_url用于worker连接目标库；二者不是同一种密码。
# 本文件只增加解释注释，业务代码保持原样。

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Settings:
    """API 和 worker 的共享配置：数据目录、密钥、数据库和备份工具。"""
    # 持久数据目录要长期保留：普通队列、daily断点、原始数据、快照与备份都在这里。
    data_dir: Path
    # 与 Java nmpa.collector.api-key 一致；只在服务端配置，不发送到 Vue。
    api_key: str
    # 这是真实业务库连接；不要将含密码的URL写进日志或接口返回。
    database_url: str = ''
    table: str = 't_medical_device'
    # Windows 使用 mysqldump.exe 完整路径；Linux 使用已安装命令或绝对路径。
    mysqldump: str = 'mysqldump'
    # 开关只控制daily路由和worker队列支持；它不会设置Java每晚/每10分钟的调度。
    daily_enabled: bool = False
    daily_max_attempts: int = 3
    daily_retry_seconds: int = 60
    daily_retry_max_seconds: int = 3600

    # dataclass创建后校验daily重试次数与退避上限，非法配置立即失败，避免循环高频重试。
    def __post_init__(self):
        """启用A方案时校验退避配置，避免误配置形成高频无限重试。"""
        if self.daily_enabled and (not 1 <= self.daily_max_attempts <= 20 or
                not 1 <= self.daily_retry_seconds <= self.daily_retry_max_seconds <= 86400):
            raise ValueError('A方案重试次数应为1至20，退避秒数须为1至86400且基础不超过上限')

    # 先调用load_local_config，再把环境值转成Path/bool/int等类型；API和worker须读同一来源。
    @classmethod
    def from_env(cls):
        """API 与 worker 读取同一配置，确保队列、密钥、数据库一致。"""
        from .local_config import load_local_config
        load_local_config()
        root = Path(__file__).resolve().parents[1]
        return cls(Path(os.getenv('NMPA_DATA_DIR', str(root / 'service_data'))).resolve(),
                   os.getenv('NMPA_API_KEY', ''), os.getenv('NMPA_DATABASE_URL', ''),
                   os.getenv('NMPA_TABLE', 't_medical_device'),
                   os.getenv('NMPA_MYSQLDUMP', 'mysqldump'),
                   os.getenv('NMPA_DAILY_ENABLED', 'false').lower() == 'true',
                   int(os.getenv('NMPA_DAILY_MAX_ATTEMPTS', '3')),
                   int(os.getenv('NMPA_DAILY_RETRY_SECONDS', '60')),
                   int(os.getenv('NMPA_DAILY_RETRY_MAX_SECONDS', '3600')))
