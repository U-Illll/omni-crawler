#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.core.log — 日志 / 审计 / 心跳（统一出口）。

设计（融合 sm_common 与 r3-audit-v2 契约要点）：
- 模块级 init(root) 绑定实例目录（多任务共存时各自独立）。
- log(msg)：人类日志 + 心跳（供 keeper 停滞看门狗检测）。
- audit(event)：结构化审计 JSONL —— 每行含 ts / ts_iso / writer / pid / kind。
  writer 分组字段（R3 P4.5 教训：同一进程多写者需分组），默认按 pid 派生，可经
  OMNI_WRITER 环境变量覆盖。
- 审计行必须携带事实读数（事件只在状态变化时写；禁止空转事件——DR 教训）。
"""
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

CN_TZ = timezone(timedelta(hours=8))

_BASE = None       # 实例根目录
_RUNS = None       # 运行时数据目录
_LOGS = None       # 日志目录
_WRITER = None     # 写者标识


def init(root, writer=None):
    """绑定实例目录。root 通常为 adapter 的运行根（如 ~/go/omni-crawler 或任务目录）。"""
    global _BASE, _RUNS, _LOGS, _WRITER
    _BASE = os.path.abspath(root)
    _RUNS = os.path.join(_BASE, "runs")
    _LOGS = os.path.join(_BASE, "logs")
    os.makedirs(_RUNS, exist_ok=True)
    os.makedirs(_LOGS, exist_ok=True)
    _WRITER = writer or os.environ.get("OMNI_WRITER") or f"pid{os.getpid()}"
    return _BASE


def base():
    if _BASE is None:
        init(os.getcwd())
    return _BASE


def runs_dir():
    base()
    return _RUNS


def logs_dir():
    base()
    return _LOGS


def writer():
    base()
    return _WRITER


def now_ts():
    return int(time.time())


def ts_iso(t=None):
    return datetime.fromtimestamp(t or time.time(), CN_TZ).isoformat(timespec="seconds")


def heartbeat():
    """主动心跳（长操作前调用，避免看门狗误判停滞）。"""
    base()
    try:
        with open(os.path.join(_RUNS, ".heartbeat"), "w", encoding="utf-8") as f:
            f.write(str(time.time()))
    except Exception:  # noqa: BLE001
        pass


def log(msg, log_name="omni.log", quiet=False):
    base()
    line = f"[{ts_iso()}] {msg}"
    if not quiet:
        print(line, flush=True)
    try:
        with open(os.path.join(_LOGS, log_name), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:  # noqa: BLE001
        pass
    heartbeat()


def audit(event, log_name="audit.jsonl"):
    """结构化审计（每行一条 JSON）。所有事件必须带事实读数。"""
    base()
    ev = dict(event)
    ev.setdefault("ts", now_ts())
    ev.setdefault("ts_iso", ts_iso())
    ev.setdefault("writer", _WRITER)
    ev.setdefault("pid", os.getpid())
    try:
        with open(os.path.join(_LOGS, log_name), "a", encoding="utf-8") as f:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    except Exception:  # noqa: BLE001
        pass
