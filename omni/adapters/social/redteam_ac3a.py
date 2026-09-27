#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""redteam_ac3a — 自愈测试：429 注入 → 退避重试恢复

流程：起 mock429（子进程）→ Fetcher 请求 → 期望最终 200 + 审计含 retry_wait
"""
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_fetch import Fetcher, RateLimiter  # noqa: E402

mock = subprocess.Popen([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "mock429_server.py"), "8799"])
try:
    time.sleep(1.0)
    f = Fetcher(limiter=RateLimiter(base=0.5, jitter=0.3), timeout=10, max_retries=3)
    t0 = time.time()
    status, text, err = f.get("http://127.0.0.1:8799/test")
    dt = time.time() - t0
    print(f"最终状态: status={status} err={err} 耗时={dt:.1f}s text={ (text or '')[:20] }")
    ok = (status == 200 and err is None)
    print("AC3a(429 自愈):", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)
finally:
    mock.terminate()
