#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.core.retry — 错误分类与重试计划。

纪律（DR 终审教训）：
- **fatal 与 transient/limited 严格分离**：认证失败 / 不存在（401/403/404 等）不重试、
  不进入熔断计数（防"fatal 污染限速器/熔断器"）；限流（429）与网络抖动（5xx/超时）
  才重试。语义冲突由目标卡显式定义（本文件即定义处）。
- 退避：指数 + 抖动，封顶 60s（对标 scrapy/crawl4ai）。

分类取值：ok / transient / limited / blocked / fatal / unknown
"""
import random

FATAL_STATUSES = {400, 401, 403, 404, 405, 410, 422}
TRANSIENT_STATUSES = {408, 500, 502, 503, 504, 522, 524}
LIMITED_STATUSES = {429}

MAX_BACKOFF = 60.0
BACKOFF_BASE = 1.5


def classify_status(status):
    """按 HTTP 状态码分类。"""
    if status == 200 or (status is not None and 200 <= status < 300):
        return "ok"
    if status in LIMITED_STATUSES:
        return "limited"
    if status in TRANSIENT_STATUSES:
        return "transient"
    if status in FATAL_STATUSES:
        return "fatal"
    if status is not None and 400 <= status < 500:
        return "fatal"          # 其它 4xx 默认 fatal（保守：不乱重试）
    if status is not None and status >= 500:
        return "transient"      # 其它 5xx 视为可重试
    return "unknown"


def classify_exception(exc):
    """按异常类型分类（超时/连接类 → transient；其余 → unknown）。"""
    name = type(exc).__name__.lower()
    msg = str(exc).lower()
    if any(k in name for k in ("timeout", "timedout", "connection", "socket", "ssl", "eof")):
        return "transient"
    if any(k in msg for k in ("timed out", "timeout", "connection reset", "connection aborted",
                              "remote end closed", "temporary failure")):
        return "transient"
    return "unknown"


def should_retry(cls):
    return cls in ("transient", "limited", "unknown")


def wait_plan(cls, attempt):
    """返回本次重试的等待秒数；不重试返回 None。
    attempt 从 1 开始（第 1 次重试前）。
    退避式（R3 教训：equal jitter）：d = min(cap, base^n)；wait = d/2 + U(0, d/2)。
    - max(wait) ≤ cap 恒成立（R-B2 断言①）；
    - 首档 wait ≤ base（断言②）。"""
    if not should_retry(cls):
        return None
    d = min(MAX_BACKOFF, BACKOFF_BASE ** attempt)
    return d / 2.0 + random.uniform(0.0, d / 2.0)
