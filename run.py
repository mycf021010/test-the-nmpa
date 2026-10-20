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
        try:
            keywords = (input('请输入部位关键词，空格分隔（回车使用“膝 髋”）：').strip() or '膝 髋').split()
        except (EOFError, KeyboardInterrupt):
            raise SystemExit(130)
        sys.argv.extend(['collect', '--keywords', *keywords])
    if sys.argv[1] in ('collect', 'export'):
        from pipeline import main
    else:
        from nmpa.__main__ import main
    raise SystemExit(main())
