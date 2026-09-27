#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sm_common — 公共常量与工具【omni-crawler 适配版】。

迁移说明（2026-09-27）：本文件是 sm-recon 原 sm_common.py 的框架适配层——
保持全部对外接口不变，内部改为调用 omni.core.log（统一日志/审计/心跳）。
"""
import hashlib
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))              # .../omni/adapters/social
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(_HERE)))  # omni-crawler 根
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from omni.core import log as _core_log  # noqa: E402

BASE = _HERE
RUNS = os.path.join(BASE, "runs")
LOGS = os.path.join(BASE, "logs")
os.makedirs(RUNS, exist_ok=True)
os.makedirs(LOGS, exist_ok=True)
_core_log.init(BASE, writer="social")

CN_TZ = timezone(timedelta(hours=8))

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
]


def ua():
    return random.choice(UA_POOL)


def now_ts():
    return int(time.time())


def ts_iso(t=None):
    return datetime.fromtimestamp(t or time.time(), CN_TZ).isoformat(timespec="seconds")


def sha1(s):
    return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()


def jitter_sleep(base, j=1.0):
    """基础间隔 + 抖动（风控礼貌节流）"""
    time.sleep(base + random.uniform(0, j))


def audit(event, log_name="audit.jsonl"):
    """结构化审计（每行一条 JSON）——转 omni.core.log.audit"""
    _core_log.audit(event, log_name=log_name)


def log(msg, log_name="sm.log"):
    _core_log.log(msg, log_name=log_name)


def heartbeat():
    _core_log.heartbeat()
