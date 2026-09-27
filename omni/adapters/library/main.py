#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""library adapter CLI。

用法：
  # 框架托管（推荐；等价 keeper：状态观测 + 必要时启动子进程）
  python3 main.py managed [--once|--loop] [--site DIR] [--interval 60] [--site ...]
  # 运维直跑（转发 scrape.py 原参数；不与托管并发）
  python3 main.py scrape [scrape.py 原参数...]
  # 状态查看
  python3 main.py status [--site DIR]
"""
import argparse
import os
import subprocess
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(_HERE)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")

    p_m = sub.add_parser("managed")
    p_m.add_argument("--once", action="store_true", help="单轮（iter_once 一次）")
    p_m.add_argument("--loop", action="store_true", help="长程循环")
    p_m.add_argument("--site", default=None, help="现场目录（默认 adapters/library/site）")
    p_m.add_argument("--seeds", default=None, help="种子字母（逗号分隔，如 Z 或 Q,Z；默认全量）")
    p_m.add_argument("--interval", type=float, default=60, help="检查节拍秒")
    p_m.add_argument("--max-iter", type=int, default=200)
    p_m.add_argument("--rest", type=float, default=None, help="收敛后休整秒（默认 900）")

    p_s = sub.add_parser("scrape")
    p_s.add_argument("rest", nargs=argparse.REMAINDER)

    p_st = sub.add_parser("status")
    p_st.add_argument("--site", default=None)

    args = ap.parse_args()
    if args.cmd == "scrape":
        cmd = [sys.executable, os.path.join(_HERE, "scrape.py")] + (args.rest or [])
        return subprocess.call(cmd, cwd=_HERE)
    if args.cmd == "status":
        from omni.adapters.library.adapter import LibraryAdapter
        a = LibraryAdapter()
        a.site = os.path.abspath(args.site or os.environ.get("OMNI_LIB_SITE")
                                 or os.path.join(a.root(), "site"))
        a._booted = True
        import json as _json
        print(_json.dumps(a.snapshot(), ensure_ascii=False, indent=1))
        return 0
    if args.cmd == "managed":
        from omni.core.engine import Engine
        from omni.adapters.library.adapter import LibraryAdapter
        a = LibraryAdapter()
        e = Engine(a, args=args)
        if args.once:
            e.run_once()
            return 0
        return 0 if e.loop(max_iter=args.max_iter) in ("converged", "max_iter", "stalled") else 1
    ap.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
