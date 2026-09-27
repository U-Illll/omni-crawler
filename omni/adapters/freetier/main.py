#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""freetier adapter CLI。

用法（在 omni-crawler 根目录）：
  python3 -m omni.adapters.freetier.main baseline
  python3 -m omni.adapters.freetier.main refresh
  python3 -m omni.adapters.freetier.main status
  python3 -m omni.adapters.freetier.main report

或直接路径执行（自动补 sys.path）：
  python3 omni/adapters/freetier/main.py baseline
"""
import argparse
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_PKG_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(_HERE)))
if _PKG_ROOT not in sys.path:
    sys.path.insert(0, _PKG_ROOT)

from omni.adapters.freetier.adapter import FreeTierAdapter  # noqa: E402
from omni.core.engine import Engine  # noqa: E402
from omni.core.log import init as log_init, log, ts_iso  # noqa: E402


def build_parser():
    ap = argparse.ArgumentParser(prog="freetier",
                                 description="免费机制基线采集与变化监控")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, h in (("baseline", "建立基线（幂等；已有快照的站点跳过）"),
                    ("refresh", "复核所有站点，有变化才产出")):
        p = sub.add_parser(name, help=h)
        p.add_argument("--max-iter", type=int, default=200,
                       help="最大迭代（保护用；默认 200）")
    sub.add_parser("status", help="站点状态表")
    sub.add_parser("report", help="重新生成 runs/report.md 并打印")
    return ap


def cmd_run(args):
    adapter = FreeTierAdapter(mode=args.cmd)
    args.mode = args.cmd   # 供 ctx.args.mode 读取（与构造参数一致）
    eng = Engine(adapter, args=args)
    it = 0
    stall = 0
    status = "max_iter"
    while it < args.max_iter:
        it += 1
        progressed = eng.run_once()
        if adapter.converged(eng.ctx):
            status = "converged"
            log(f"[freetier] 收敛于迭代 {it}")
            break
        if not progressed:
            stall += 1
            if stall >= 3:
                status = "stalled"
                log(f"[freetier] 连续 {stall} 轮无进展 → 退出")
                break
        else:
            stall = 0
    out = adapter.report(eng.ctx)
    log(f"[freetier] {status} | store: {eng.store.summary()} | 报告: {out}")
    print(f"\n[freetier] {status} | store: {eng.store.summary()}")
    return 0 if status == "converged" else 1


def cmd_status(args):
    adapter = FreeTierAdapter()
    log_init(adapter.root())
    print(f"{'id':38s} {'状态':8s} {'版本':>3s} {'sha12':12s} 最近检查")
    for s in adapter.sites:
        meta = adapter._load_meta(s["id"])
        st = meta.get("status", "missing")
        ck = ts_iso(meta["last_checked_ts"]) if meta.get("last_checked_ts") else "-"
        print(f"{s['id']:38s} {st:8s} {meta.get('versions', 0):>3} "
              f"{(meta.get('last_sha') or '')[:12]:12s} {ck}")
    return 0


def cmd_report(args):
    adapter = FreeTierAdapter()
    log_init(adapter.root())
    out = adapter.report(None)
    with open(out, encoding="utf-8") as f:
        print(f.read())
    return 0


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.cmd in ("baseline", "refresh"):
        return cmd_run(args)
    if args.cmd == "status":
        return cmd_status(args)
    if args.cmd == "report":
        return cmd_report(args)
    return 2


if __name__ == "__main__":
    sys.exit(main())
