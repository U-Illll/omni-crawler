#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.core.circuit — 熔断器（真状态机）。

纪律（DR 终审教训对策）：
- 事件必须伴随**可验证的状态变化**（closed→open→half_open→closed 每步带读数）；
  禁止"空转事件"（状态没变却写 circuit_open）。
- **fatal 类失败不计入熔断**（认证/不存在是业务语义，不是网络健康问题）；
  只有 transient / limited / blocked 参与计数。
- half_open 只放行探测；探测失败 → 重新 open（重新计时）。
"""
import threading
import time

from .config import BLOCK_COOLDOWN, CIRCUIT_COOLDOWN, CIRCUIT_THRESHOLD
from .log import audit

STATE_CLOSED = "closed"
STATE_OPEN = "open"
STATE_HALF_OPEN = "half_open"

# 参与熔断计数的失败类别（fatal/unknown 不参与；unknown 由上层决定是否传 blocked）
COUNTED_CLASSES = {"transient", "limited", "blocked"}


class CircuitOpenError(Exception):
    """熔断打开期间的请求被拒绝。"""


class CircuitBreaker:
    def __init__(self, threshold=CIRCUIT_THRESHOLD, cooldown=CIRCUIT_COOLDOWN):
        self.threshold = int(threshold)
        self.cooldown = float(cooldown)
        self._state = {}       # domain -> state
        self._streak = {}      # domain -> 连续失败计数
        self._until = {}       # domain -> open 到期时间
        self._probe_used = {}  # domain -> half_open 探测是否已放行
        self._lock = threading.Lock()

    # ---------- 查询 ----------
    def state_of(self, domain):
        return self._state.get(domain, STATE_CLOSED)

    def check(self, domain):
        """请求前检查。open 且未到期 → 抛 CircuitOpenError；到期 → 转 half_open 放行探测。"""
        with self._lock:
            st = self._state.get(domain, STATE_CLOSED)
            now = time.time()
            if st == STATE_OPEN:
                if now < self._until.get(domain, 0):
                    remain = int(self._until[domain] - now)
                    raise CircuitOpenError(
                        f"circuit open for {domain}, {remain}s remaining")
                # 到期 → half_open（允许一次探测）
                self._transit(domain, STATE_OPEN, STATE_HALF_OPEN,
                              {"cooldown_elapsed": True})
                self._probe_used[domain] = True
                return STATE_HALF_OPEN
            if st == STATE_HALF_OPEN:
                if self._probe_used.get(domain):
                    raise CircuitOpenError(f"circuit half_open probe already in flight for {domain}")
                self._probe_used[domain] = True
                return STATE_HALF_OPEN
            return STATE_CLOSED

    # ---------- 记录 ----------
    def record_success(self, domain):
        with self._lock:
            st = self._state.get(domain, STATE_CLOSED)
            self._streak[domain] = 0
            self._probe_used[domain] = False
            if st in (STATE_HALF_OPEN, STATE_OPEN):
                self._transit(domain, st, STATE_CLOSED, {"probe_ok": True})

    def record_failure(self, domain, cls):
        """cls: transient / limited / blocked / fatal / unknown。fatal 不参与。"""
        if cls not in COUNTED_CLASSES:
            return
        with self._lock:
            st = self._state.get(domain, STATE_CLOSED)
            streak = self._streak.get(domain, 0) + 1
            self._streak[domain] = streak
            self._probe_used[domain] = False
            if st == STATE_HALF_OPEN:
                # 探测失败 → 重新 open
                self._until[domain] = time.time() + self._open_cool(cls)
                self._transit(domain, STATE_HALF_OPEN, STATE_OPEN,
                              {"streak": streak, "reason": "half_open_probe_failed"})
                return
            if st == STATE_CLOSED and streak >= self.threshold:
                self._until[domain] = time.time() + self._open_cool(cls)
                self._transit(domain, STATE_CLOSED, STATE_OPEN,
                              {"streak": streak, "threshold": self.threshold,
                               "cooldown": round(self._open_cool(cls), 1), "cls": cls})

    def _open_cool(self, cls):
        return BLOCK_COOLDOWN if cls == "blocked" else self.cooldown

    # ---------- 内部 ----------
    def _transit(self, domain, frm, to, extra):
        """状态迁移 + 审计（仅在真实迁移时调用）。"""
        self._state[domain] = to
        ev = {"kind": "circuit", "domain": domain, "state_from": frm, "state_to": to}
        ev.update(extra)
        audit(ev)

    def snapshot(self):
        with self._lock:
            return {
                "domains": {d: {"state": self._state.get(d, STATE_CLOSED),
                                "streak": self._streak.get(d, 0),
                                "until": round(self._until.get(d, 0), 1)}
                            for d in set(self._state) | set(self._streak)},
            }
