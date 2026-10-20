"""首次安装 JavaScript 依赖用的 IDE 入口，只需在新机器上运行一次。"""
import os
import shutil
import subprocess
from pathlib import Path
from api.local_config import load_local_config

if __name__ == '__main__':
    load_local_config()
    npm = shutil.which(os.getenv('NMPA_NPM', 'npm'))
    if not npm:
        raise RuntimeError('找不到 npm，请先安装 Node.js 24 并重启 PyCharm')
    # Windows 的 npm.cmd 由 cmd.exe 执行。路径来自本地配置，不拼接 shell 字符串。
    command = ['cmd.exe','/d','/c',npm,'ci','--omit=dev'] if os.name=='nt' else [npm,'ci','--omit=dev']
    subprocess.run(command, cwd=Path(__file__).resolve().parent, check=True)
    print('Node 依赖安装完成；下一步运行 check_runtime.py。')
