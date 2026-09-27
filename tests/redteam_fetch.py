#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""红队：HTTP 执行体端到端（本地 mock 服务器）。

验证：成功路径 / fatal 不重试 / 503 重试后成功 / 429 降速 / 验证页 blocked /
审计事件完整性。
"""
import json
import os
import shutil
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS = FAIL = 0
COUNTS = {}


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):  # 静音
        pass

    def do_GET(self):
        path = self.path
        COUNTS[path] = COUNTS.get(path, 0) + 1
        n = COUNTS[path]
        if path == "/ok":
            body, code = b"hello world", 200
        elif path == "/404":
            body, code = b"not found", 404
        elif path == "/503":
            body, code = (b"try later", 503) if n < 2 else (b"recovered", 200)
        elif path == "/429":
            body, code = (b"slow down", 429) if n < 2 else (b"recovered", 200)
        elif path == "/block":
            body, code = "百度安全验证：请输入验证码".encode("utf-8"), 200
        elif path == "/count503":
            body, code = str(n).encode(), 200
        else:
            body, code = b"?", 404
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    tmp = tempfile.mkdtemp(prefix="omni-fetch-")
    from omni.core import log as olog
    olog.init(tmp, writer="redteam")
    from omni.core.limiter import RateLimiter
    from omni.core.circuit import CircuitBreaker
    from omni.channel.manager import ChannelManager
    from omni.fetch.http import HttpFetcher

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    port = srv.server_address[1]
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    base = f"http://127.0.0.1:{port}"

    try:
        fetcher = HttpFetcher(
            limiter=RateLimiter(base=0.2, jitter=0.0),
            breaker=CircuitBreaker(threshold=5, cooldown=60),
            channel=ChannelManager(probe_enabled=False),
            timeout=10, max_retries=3,
        )

        # 1) 成功路径
        r = fetcher.get(f"{base}/ok", env="AUTO")
        check("ok 请求成功", r.ok() and "hello" in r.text, f"r={r}")

        # 2) fatal 404：不重试（请求计数 = 1）
        r = fetcher.get(f"{base}/404", env="AUTO")
        check("404 分类 fatal", r.cls == "fatal", f"cls={r.cls}")
        check("404 不重试（计数=1）", COUNTS["/404"] == 1, f"n={COUNTS['/404']}")

        # 3) 503 重试后成功
        r = fetcher.get(f"{base}/503", env="AUTO")
        check("503 重试后成功", r.ok(), f"r={r}")
        check("503 重试计数>=2", COUNTS["/503"] >= 2, f"n={COUNTS['/503']}")

        # 4) 429 重试 + 限速器降速
        lim_before = fetcher.limiter.interval_of("127.0.0.1")
        r = fetcher.get(f"{base}/429", env="AUTO")
        lim_after = fetcher.limiter.interval_of("127.0.0.1")
        check("429 重试后成功", r.ok(), f"r={r}")
        check("429 触发限速器降速", lim_after > lim_before,
              f"{lim_before} -> {lim_after}")

        # 5) 验证页 blocked：不重试
        r = fetcher.get(f"{base}/block", env="AUTO")
        check("验证页 blocked", r.cls == "blocked", f"cls={r.cls}")
        check("blocked 不重试（计数=1）", COUNTS["/block"] == 1, f"n={COUNTS['/block']}")

        # 6) 审计事件完整性
        ap = os.path.join(tmp, "logs", "audit.jsonl")
        evs = [json.loads(l) for l in open(ap)]
        kinds = {e["kind"] for e in evs}
        check("fetch 审计存在", "fetch" in kinds)
        check("retry_scheduled 审计存在", "retry_scheduled" in kinds)
        check("limiter_adjust 审计存在", "limiter_adjust" in kinds)
        fetch_evs = [e for e in evs if e["kind"] == "fetch"]
        check("fetch 事件含 env/egress/cls 读数",
              all(("env" in e and "egress" in e and "cls" in e) for e in fetch_evs))
    finally:
        srv.shutdown()
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n[redteam_fetch] PASS={PASS} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
