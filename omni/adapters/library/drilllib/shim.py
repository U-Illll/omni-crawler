#!/usr/bin/env python3
"""drilllib/shim.py —— 演练台装载器：把「沙箱副本 scrape」装成生产等价进程并跑它的 _cli()。

为什么需要它（这是测试布局，不是改被测行为）：
  1. 生产入口 `python3 scrape.py` 会让被测模块的 __name__ 出现在 sys.modules 里；
     importlib 装载不会自动注册，而 scrape.py 的 memguard 接线用
     `install_gate(sys.modules[__name__])` —— 缺注册会抛 KeyError 并被 fail-open 吞掉
     （静默降级成「有采样无闸门」，R2 p4/merge 移交 #11）。这里补注册，使沙箱布局与生产等价。
  2. scrape.py 的 HOST 是硬编码真实域名。演练台**必须**把它指向 loopback mock，
     否则会打真实图书馆域名（红线）。只在装载后改这一个常量，不改任何逻辑。
  3. 心跳的 LIMITER 采集走 __main__（生产等价：`python3 scrape.py` 时 LIMITER 挂在 __main__）。

安全：走 SCRAPE_SANDBOX_BASE / DRILL_OUT_DIR 双断言，任何越界直接 exit 9（fail-closed）。
"""
import importlib.util
import os
import sys
from pathlib import Path


def _die(msg):
    sys.stderr.write("SHIM-REFUSE: %s\n" % msg)
    sys.exit(9)


def main():
    target = os.environ.get("DRILL_SCRAPE")
    sandbox_base = os.environ.get("SCRAPE_SANDBOX_BASE")
    out_dir = os.environ.get("DRILL_OUT_DIR") or os.environ.get("SCRAPE_OUT_DIR")
    sandbox_root = os.environ.get("DRILL_SANDBOX_ROOT")
    mock_host = os.environ.get("DRILL_MOCK_HOST")

    for name, val in (("DRILL_SCRAPE", target), ("SCRAPE_SANDBOX_BASE", sandbox_base),
                      ("DRILL_SANDBOX_ROOT", sandbox_root), ("DRILL_MOCK_HOST", mock_host),
                      ("DRILL_OUT_DIR", out_dir)):
        if not val:
            _die("缺环境变量 %s" % name)

    # --- fail-closed：脚本 / 输出 / 沙箱根都必须落在演练沙箱内，且 HOST 必须是 loopback
    if "/tmp/library-scrape" in target or "/tmp/library-scrape" in out_dir:
        _die("路径含生产根 /tmp/library-scrape：%s %s" % (target, out_dir))
    for p in (target, sandbox_base, out_dir, os.path.dirname(target)):
        if not str(Path(p).resolve()).startswith(str(Path(sandbox_root).resolve())):
            _die("路径越界：%s 不在 %s 内" % (p, sandbox_root))
    if not (mock_host.startswith("http://127.0.0.1:") or mock_host.startswith("http://localhost:")):
        _die("DRILL_MOCK_HOST 不是 loopback：%r" % mock_host)

    src_dir = str(Path(target).resolve().parent)
    sys.path.insert(0, src_dir)                 # memguard.py / heartbeat.py 与被测副本同目录

    spec = importlib.util.spec_from_file_location("scrape_under_test", target)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["scrape_under_test"] = mod      # ← 生产等价性（memguard.install_gate 依赖它）
    spec.loader.exec_module(mod)

    mod.HOST = mock_host                        # ← 唯一的行为改动：出口指向 loopback mock
    try:
        mod.SESSION.trust_env = False
    except Exception:
        pass

    try:                                        # 生产等价：LIMITER 在 __main__ 上（心跳采集用）
        import __main__ as _m
        if not hasattr(_m, "LIMITER"):
            _m.LIMITER = mod.LIMITER
    except Exception:
        pass

    sys.stderr.write("SHIM-READY pid=%d scrape=%s out=%s host=%s\n"
                     % (os.getpid(), target, mod.OUT, mod.HOST))
    sys.stderr.flush()
    return mod._cli()


if __name__ == "__main__":
    sys.exit(main() or 0)
