#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""红队：限速器（硬上限 / 自适应 / 事件纪律）。

对标 DR 教训：任何构造参数不可使生效间隔低于硬下限（不可放宽红线）。
"""
import json
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def main():
    tmp = tempfile.mkdtemp(prefix="omni-limiter-")
    from omni.core import log as olog
    olog.init(tmp, writer="redteam")
    from omni.core.config import MIN_INTERVAL_HARD
    from omni.core.limiter import RateLimiter

    def adjust_events():
        p = os.path.join(tmp, "logs", "audit.jsonl")
        if not os.path.exists(p):
            return []
        return [json.loads(l) for l in open(p) if '"limiter_adjust"' in l]

    try:
        # 1) DR 教训：base=0.01 试图放宽 → 被钳到硬下限
        rl = RateLimiter(base=0.01, jitter=0.0)
        check("base 被钳到硬下限", abs(rl.base - MIN_INTERVAL_HARD) < 1e-9,
              f"base={rl.base}")

        # 2) 实际等待 ≥ 硬下限
        t0 = time.time()
        rl.wait("d1")
        rl.wait("d1")   # 第二次应等待 ~0.2s
        gap = time.time() - t0
        check("两次间隔 >= 硬下限", gap >= MIN_INTERVAL_HARD - 0.02, f"gap={gap:.3f}")

        # 3) 自适应：limited 翻倍，封顶 max_interval
        rl2 = RateLimiter(base=1.0, jitter=0.0, max_interval=5.0)
        rl2.on_limited("d2")
        i1 = rl2.interval_of("d2")
        rl2.on_limited("d2")
        i2 = rl2.interval_of("d2")
        for _ in range(10):
            rl2.on_limited("d2")
        i_max = rl2.interval_of("d2")
        check("limited 翻倍", i1 > 1.0 and i2 > i1, f"{i1} -> {i2}")
        check("翻倍封顶", i_max <= 5.0, f"i_max={i_max}")

        # 4) 成功缓慢恢复（×0.9），不低于 base
        rl2.on_success("d2")
        check("成功恢复一步", rl2.interval_of("d2") < i_max)
        for _ in range(200):
            rl2.on_success("d2")
        check("恢复不低于 base", rl2.interval_of("d2") >= 1.0)

        # 5) 事件纪律：调整事件数 < 调用数（恢复期大量 no-op 不写事件）
        evs = [e for e in adjust_events() if e["domain"] == "d2"]
        check("事件只在变化时写", len(evs) <= 11, f"events={len(evs)}")

        # 6) blocked 也增加间隔
        rl3 = RateLimiter(base=1.0, jitter=0.0)
        rl3.on_blocked("d3")
        check("blocked 提高间隔", rl3.interval_of("d3") > 1.0)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n[redteam_limiter] PASS={PASS} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
