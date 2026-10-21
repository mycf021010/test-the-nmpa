# 阅读说明：把JSON配置转换成进程环境变量，供Settings及Node子进程读取。
# 文件中明确填写的值覆盖原终端环境；没写的项保留环境配置。
# 本地默认config/settings.local.json，云端可通过NMPA_SETTINGS_FILE指定另一文件。
# 本文件只增加解释注释，业务代码保持原样。

"""IDE 本地配置入口。真实密码只写在被 Git 忽略的 JSON 中。"""
import json
import os
from pathlib import Path
from sqlalchemy.engine import URL

ROOT = Path(__file__).resolve().parents[1]

# 返回读取的配置字典，并把明确配置项写到os.environ；文件不存在则保留纯环境变量启动方式。
def load_local_config():
    """读取固定配置文件；显式本地配置优先于旧终端遗留的环境变量。"""
    # 云端可用 NMPA_SETTINGS_FILE 指向 /etc/nmpa/settings.json；两进程必须相同。
    # 文件中明确配置的字段优先于环境变量；使用纯环境变量部署时不要留下旧 JSON。
    path = Path(os.getenv('NMPA_SETTINGS_FILE', str(ROOT / 'config/settings.local.json')))
    if not path.exists():
        return {}  # 保留原有环境变量启动方式，方便测试和未来部署。
    try:
        config = json.loads(path.read_text(encoding='utf-8-sig'))
    except (ValueError, OSError) as exc:
        raise RuntimeError('本地配置无法读取，请检查 settings.local.json 的 JSON 格式') from exc
    if not isinstance(config, dict):
        raise ValueError('配置必须是 JSON 对象')
    # api_key 是服务器之间的鉴权密钥；host 是监听地址，不是 Java 请求地址。
    # 云端同机反向代理可监听 127.0.0.1；跨机直连才需要对应网卡或 0.0.0.0。
    variables = {'api_key':'NMPA_API_KEY','table':'NMPA_TABLE','mysqldump':'NMPA_MYSQLDUMP',
                 'daily_enabled':'NMPA_DAILY_ENABLED',
                 'daily_max_attempts':'NMPA_DAILY_MAX_ATTEMPTS','daily_retry_seconds':'NMPA_DAILY_RETRY_SECONDS',
                 'daily_retry_max_seconds':'NMPA_DAILY_RETRY_MAX_SECONDS',
                 'host':'NMPA_HOST','port':'NMPA_PORT','node_executable':'NMPA_NODE',
                 'npm_executable':'NMPA_NPM'}
    for field, name in variables.items():
        if field in config:
            os.environ[name] = str(config[field])
    # 相对data_dir以项目根目录计算，而不是以IDE当前工作目录计算，避免API与worker写到不同地方。
    if 'data_dir' in config:
        data = Path(config['data_dir']).expanduser()
        os.environ['NMPA_DATA_DIR'] = str(data if data.is_absolute() else (ROOT / data).resolve())
    # 迁移 Linux 后，mysql.host=127.0.0.1 指 Linux 本机，不再是 Windows MySQL。
    # 修改数据库地址/密码后重启 Worker；API 与 Worker 都要使用这份配置。
    if 'mysql' in config:
        mysql = config['mysql']
        # URL.create 正确处理密码中的 @、# 等符号；不拼接也不打印密码。
        if mysql:
            url = URL.create('mysql+pymysql', username=mysql['username'], password=mysql['password'],
                             host=mysql.get('host','127.0.0.1'), port=int(mysql.get('port',3306)),
                             database=mysql['database'], query={'charset':'utf8mb4'})
            os.environ['NMPA_DATABASE_URL'] = url.render_as_string(hide_password=False)
        else:
            os.environ['NMPA_DATABASE_URL'] = ''  # mysql:null 时只允许预览。
    return config
