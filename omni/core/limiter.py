#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.core.limiter — 域级限速器（自适应 + 硬上限）。

融合来源：
- sm-recon sm_fetch.RateLimiter（域级 base+jitter）
- 图书馆 R3 RateLimiter（自适应思想：按响应反馈调整）

纪律（DR 教训）：
- 硬下限 MIN_INTERVAL_HARD(0.2s) 为编译期常量，构造参数**无法**放宽（防御性钳制）。
- 自适应只允许向"更保守"方向快速调整（限流/封锁 → 间隔翻倍），向"更快"方向缓慢恢复。
- 快照 snapshot() 供审计/监控（含当前间隔/调整计数），禁止自报覆盖实测。
"""
import threading
import time

from .config import (DEFAULT_BASE_INTERVAL, DEFAULT_JITTER, DEFAULT_MAX_INTERVAL,
                     MIN_INTERVAL_HARD, assert_interval)
from .log import audit


class RateLimiter:
    """按域限速（线程安全）。"""

    def __init__(self, base=DEFAULT_BASE_INTERVAL, jitter=DEFAULT_JITTER,
                 max_interval=DEFAULT_MAX_INTERVAL):
        # 防御：任何参数不得使生效间隔低于硬下限
        self.base = assert_interval(float(base))
        self.jitter = max(0.0, float(jitter))
        self.max_interval = max(self.base, float(max_interval))
        self._interval = {}      # domain -> 当前生效间隔
        self._last = {}          # domain -> 上次请求 ts
        self._adjust = {}        # domain -> 调整计数（用于快照/审计）
        self._lock = threading.Lock()

    # ---------- 核心 ----------
    def interval_of(self, domain):
        return self._interval.get(domain, self.base)

    def wait(self, domain):
        """阻塞直到满足该域最小间隔+抖动。"""
        with self._lock:
            now = time.time()
            interval = self._interval.get(domain, self.base)
            need = interval + (self.jitter * ((now * 7919) % 100) / 100.0)
            last = self._last.get(domain, 0.0)
            gap = now - last
            sleep_s = need - gap
            if sleep_s > 0:
                time.sleep(sleep_s)
            self._last[domain] = time.time()

    # ---------- 反馈（自适应） ----------
    def on_success(self, domain):
        """成功：缓慢恢复（×0.9，不低于 base）。"""
        with self._lock:
            cur = self._interval.get(domain, self.base)
            if cur > self.base:
                nxt = max(self.base, cur * 0.9)
                if nxt != cur:
                    self._interval[domain] = nxt
                    self._adjust[domain] = self._adjust.get(domain, 0) + 1

    def on_limited(self, domain):
        """429 / 软限流：间隔翻倍（封顶 max_interval）。"""
        self._bump(domain, "limited")

    def on_blocked(self, domain):
        """验证页 / 硬封锁：间隔翻倍 + 立即冷却交给 circuit 处理。"""
        self._bump(domain, "blocked")

    def _bump(self, domain, why):
        with self._lock:
            cur = self._interval.get(domain, self.base)
            nxt = min(self.max_interval, max(cur * 2, self.base * 2))
            changed = nxt != cur
            self._interval[domain] = nxt
            self._adjust[domain] = self._adjust.get(domain, 0) + 1
        # 事件只在真实变化时写（DR 教训：禁止空转事件）
        if changed:
            audit({"kind": "limiter_adjust", "why": why, "domain": domain,
                   "interval_from": round(cur, 3), "interval_to": round(nxt, 3)})

    def snapshot(self):
        with self._lock:
            return {
                "base": self.base,
                "domains": {d: {"interval": round(i, 3),
                                "adjust": self._adjust.get(d, 0)}
                            for d, i in self._interval.items()},
            }


# 硬下限再声明（供外部断言用）
HARD_FLOOR = MIN_INTERVAL_HARD
