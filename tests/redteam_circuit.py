#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""红队：熔断状态机。

对标 DR 教训：
- 事件必须伴随状态变化（禁止空转事件）→ 计数审计里 circuit 事件数 == 真实迁移数；
- fatal 不喂熔断（404×30 → 状态仍 closed、streak=0）；
- half_open 探测失败 → 重新 open。
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
    tmp = tempfile.mkdtemp(prefix="omni-circuit-")
    from omni.core import log as olog
    olog.init(tmp, writer="redteam")

    from omni.core.circuit import (CircuitBreaker, CircuitOpenError,
                                   STATE_CLOSED, STATE_HALF_OPEN, STATE_OPEN)

    def circuit_events():
        p = os.path.join(tmp, "logs", "audit.jsonl")
        if not os.path.exists(p):
            return []
        return [json.loads(l) for l in open(p) if '"circuit"' in l]

    try:
        # 1) 阈值行为：4 次不 open，第 5 次 open
        cb = CircuitBreaker(threshold=5, cooldown=60)
        d = "a.example"
        for i in range(4):
            cb.record_failure(d, "transient")
        check("4 次未 open", cb.state_of(d) == STATE_CLOSED)
        check("空转事件未写", len(circuit_events()) == 0,
              f"events={len(circuit_events())}")
        cb.record_failure(d, "transient")
        check("第 5 次 open", cb.state_of(d) == STATE_OPEN)
        evs = circuit_events()
        check("open 事件 1 条（带读数）",
              len(evs) == 1 and evs[0]["state_from"] == "closed"
              and evs[0]["state_to"] == "open" and evs[0].get("streak") == 5,
              f"evs={evs}")

        # 2) open 期间拒绝
        try:
            cb.check(d)
            check("open 期间 check 抛错", False)
        except CircuitOpenError:
            check("open 期间 check 抛错", True)

        # 3) DR 教训负对照：fatal 404×30 不触发熔断、不产生任何事件
        d2 = "fatal.example"
        for _ in range(30):
            cb.record_failure(d2, "fatal")
        check("fatal×30 状态仍 closed", cb.state_of(d2) == STATE_CLOSED)
        check("fatal 不增加 streak", cb.snapshot()["domains"].get(d2, {}).get("streak", 0) == 0)
        check("fatal 无事件", len([e for e in circuit_events() if e["domain"] == d2]) == 0)

        # 4) cooldown 到期 → half_open → 成功 → closed
        cb._until[d] = time.time() - 1  # 手动过期（测试语义）
        st = cb.check(d)
        check("到期转 half_open", st == STATE_HALF_OPEN)
        cb.record_success(d)
        check("探测成功 → closed", cb.state_of(d) == STATE_CLOSED)
        closed_evs = [e for e in circuit_events() if e["domain"] == d
                      and e["state_to"] == "closed"]
        check("closed 迁移事件存在", len(closed_evs) == 1)

        # 5) half_open 探测失败 → 重新 open
        d3 = "b.example"
        for _ in range(5):
            cb.record_failure(d3, "blocked")
        check("blocked 5 次 open", cb.state_of(d3) == STATE_OPEN)
        cb._until[d3] = time.time() - 1
        cb.check(d3)  # 转 half_open
        cb.record_failure(d3, "transient")
        check("探测失败 → 重新 open", cb.state_of(d3) == STATE_OPEN)

        # 6) 迁移计数对账：d 的迁移 = closed→open, open→half_open, half_open→closed = 3
        d_evs = [e for e in circuit_events() if e["domain"] == d]
        check("d 的迁移事件 = 3 条", len(d_evs) == 3, f"got {len(d_evs)}: {d_evs}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n[redteam_circuit] PASS={PASS} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
