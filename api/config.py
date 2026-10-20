import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Settings:
    """API 和 worker 的共享配置：数据目录、密钥、数据库和备份工具。"""
    data_dir: Path
    # 与 Java nmpa.collector.api-key 一致；只在服务端配置，不发送到 Vue。
    api_key: str
    database_url: str = ''
    table: str = 't_medical_device'
    # Windows 使用 mysqldump.exe 完整路径；Linux 使用已安装命令或绝对路径。
    mysqldump: str = 'mysqldump'
    daily_enabled: bool = False
    daily_max_attempts: int = 3
    daily_retry_seconds: int = 60
    daily_retry_max_seconds: int = 3600

    def __post_init__(self):
        """启用A方案时校验退避配置，避免误配置形成高频无限重试。"""
        if self.daily_enabled and (not 1 <= self.daily_max_attempts <= 20 or
                not 1 <= self.daily_retry_seconds <= self.daily_retry_max_seconds <= 86400):
            raise ValueError('A方案重试次数应为1至20，退避秒数须为1至86400且基础不超过上限')

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
