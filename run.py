# 阅读说明：命令行采集/导出入口，不是FastAPI服务启动入口。
# collect/export转给pipeline；其他子命令转给nmpa.__main__。执行采集命令会访问来源网站。
# 本文件只增加解释注释，业务代码保持原样。

"""Launch from this folder using the selected Python environment."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)

if __name__ == '__main__':
    if len(sys.argv) == 1 and not sys.stdin.isatty():
        sys.argv.extend(['collect'])
    elif len(sys.argv) == 1:
        sys.argv.extend(['collect', '--keywords', '313'])
    if sys.argv[1] in ('collect', 'export'):
        from pipeline import main
    else:
        from nmpa.__main__ import main
    raise SystemExit(main())
