#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""红队：升级链决策（escalate）。

验证各结果类别 → 动作映射正确；空壳页升级；逆向任务单落盘。
"""
import os
import shutil
import sys
import tempfile

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
    tmp = tempfile.mkdtemp(prefix="omni-escalate-")
    from omni.core import log as olog
    olog.init(tmp, writer="redteam")
    from omni.fetch.http import FetchResult
    from omni.fetch.escalate import (plan_from_result, write_reverse_ticket)

    try:
        # 1) 基本映射
        cases = [
            (FetchResult(200, "<html>" + "x" * 4000 + "</html>", None, "ok"), "ok"),
            (FetchResult(None, None, "need proxy", "channel"), "switch_egress"),
            (FetchResult(None, None, "circuit open", "circuit_open"), "retry_later"),
            (FetchResult(503, None, "HTTP 503", "transient"), "retry_later"),
            (FetchResult(429, None, "HTTP 429", "limited"), "retry_later"),
            (FetchResult(404, None, "HTTP 404", "fatal"), "give_up"),
            (FetchResult(403, None, "HTTP 403 (blockish)", "fatal"), "escalate_browser"),
            (FetchResult(200, "百度安全验证", "BLOCK_SIGN", "blocked"), "retry_later"),
            (FetchResult(200, "cf-challenge", "CHALLENGE", "blocked"), "escalate_browser"),
        ]
        for r, want in cases:
            got = plan_from_result(r)["action"]
            check(f"{r.cls}/{r.err or 'ok'} -> {want}", got == want, f"got={got}")

        # 2) 空壳页 → escalate_browser
        r = FetchResult(200, "<html><div id=app></div><script src=x.js></script></html>",
                        None, "ok")
        check("空壳页 -> escalate_browser",
              plan_from_result(r)["action"] == "escalate_browser")

        # 3) 签名壁垒 → needs_reverse
        r = FetchResult(200, '{"ok":false,"error":"sign invalid"}', None, "ok")
        check("签名错误 -> needs_reverse",
              plan_from_result(r, signature_error=True)["action"] == "needs_reverse")

        # 4) 逆向任务单落盘
        p = write_reverse_ticket("https://x.example/api", "signature_error_marker",
                                 "resp sign invalid", {"sample": 1})
        check("任务单落盘", os.path.exists(p) and "param-blueprint" in open(p, encoding="utf-8").read())
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n[redteam_escalate] PASS={PASS} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
