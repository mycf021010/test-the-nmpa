"""PyCharm 入库重试入口：先停止 Worker，填写失败任务编号，再右键运行本文件。"""
import json
from api.config import Settings
from retry_failed_import import retry

# 从页面“当前任务”复制完整编号，粘贴到引号里；不是 API 密钥。
# 只接受失败/中断且没有成功入库结果的任务；试采必须已经通过页面确认过入库。
JOB_ID = ""

if __name__ == '__main__':
    if not JOB_ID.strip():
        raise SystemExit('请先填写本文件 JOB_ID 中的任务编号，再点击 Run。')
    try:
        result = retry(Settings.from_env(), JOB_ID.strip())
        print('原采集结果入库重试完成；没有重新执行爬虫。')
        print(json.dumps(result,ensure_ascii=False,indent=2))
    except FileExistsError:
        raise SystemExit('请先在 PyCharm 停止 Worker，API 和 Java 可以继续运行。') from None
    except Exception as exc:
        # 只输出安全提示和数据库错误码，不显示原始 SQL、URL 或凭据。
        code = getattr(getattr(exc,'orig',None),'args',(None,))[0]
        if isinstance(exc,(ValueError,RuntimeError)):
            print('重试未完成：',str(exc))
        elif isinstance(code,int):
            print('重试未完成，数据库错误码：',code)
        elif isinstance(exc,OSError):
            print('重试未完成，系统错误码：',getattr(exc,'winerror',None) or exc.errno)
        else:
            print('重试未完成，异常类型：',type(exc).__name__)
        raise SystemExit(1) from None
