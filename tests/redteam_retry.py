#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""红队：错误分类与重试计划。

对标：C1（重试≥3+退避）+ DR 教训（fatal 与限速解耦）。
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from omni.core import retry as R

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
    # 1) 状态分类
    check("200 -> ok", R.classify_status(200) == "ok")
    check("204 -> ok", R.classify_status(204) == "ok")
    check("429 -> limited", R.classify_status(429) == "limited")
    check("503 -> transient", R.classify_status(503) == "transient")
    for s in (400, 401, 403, 404, 410, 422):
        check(f"{s} -> fatal", R.classify_status(s) == "fatal")

    # 2) 异常分类
    class Timeout(Exception):
        pass

    class ConnectionResetError_(Exception):
        pass

    check("Timeout -> transient", R.classify_exception(Timeout("x")) == "transient")
    check("ConnectionReset -> transient",
          R.classify_exception(ConnectionResetError_("reset")) == "transient")
    check("ValueError -> unknown", R.classify_exception(ValueError("x")) == "unknown")

    # 3) 重试决策（DR 教训：fatal 不重试）
    check("fatal 不重试", R.should_retry("fatal") is False)
    check("blocked 不重试", R.should_retry("blocked") is False)
    check("transient 重试", R.should_retry("transient") is True)
    check("fatal wait_plan None", R.wait_plan("fatal", 1) is None)

    # 4) 退避界（equal jitter 断言：首档 ≤ base；任意档 ≤ cap）
    import random
    random.seed(7)
    base = R.BACKOFF_BASE
    for _ in range(200):
        w = R.wait_plan("transient", 1)
        assert w <= base, (w, base)
    check("首档 wait <= base", True)
    mx = 0.0
    for att in range(1, 12):
        for _ in range(100):
            w = R.wait_plan("transient", att)
            mx = max(mx, w)
    check("任意档 wait <= cap(60)", mx <= R.MAX_BACKOFF + 1e-9, f"max={mx}")

    # 5) 单调性（期望值非递减）
    exp = []
    for att in range(1, 6):
        ws = [R.wait_plan("transient", att) for _ in range(2000)]
        exp.append(sum(ws) / len(ws))
    check("退避期望非递减", all(exp[i] <= exp[i + 1] + 1e-9 for i in range(len(exp) - 1)),
          f"expectations={['%.2f' % e for e in exp]}")

    print(f"\n[redteam_retry] PASS={PASS} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
