#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.core.config — 全局常量与硬上限。

纪律（DR 终审教训对策）：
- 速率硬上限是**编译期常量**，任何 adapter / 配置 / CLI 参数不得放宽。
- 所有限速路径（HTTP/浏览器/探测）共享同一硬下限。
"""

VERSION = "0.1.0"

# ---- 速率硬上限（不可放宽） ----
MAX_REQ_PERSEC_HARD = 5.0                 # 全局最大请求速率（req/s）
MIN_INTERVAL_HARD = 1.0 / MAX_REQ_PERSEC_HARD   # 0.2s —— 任何间隔不得低于此值

# ---- 默认参数（可被 adapter 按目标放宽到更保守，但不能低于硬下限） ----
DEFAULT_BASE_INTERVAL = 2.5    # 基础间隔（秒）
DEFAULT_JITTER = 1.5           # 抖动幅度（秒）
DEFAULT_MAX_INTERVAL = 60.0    # 自适应上限
DEFAULT_TIMEOUT = 25.0         # 单请求超时（秒）

# ---- 重试 / 熔断 ----
DEFAULT_MAX_RETRIES = 3        # 请求级重试次数（C1 标准：>=3）
CIRCUIT_THRESHOLD = 5          # 连续失败 N 次触发熔断
CIRCUIT_COOLDOWN = 120.0       # 熔断冷却（秒）
BLOCK_COOLDOWN = 180.0         # 验证页（风控信号）冷却（秒）

# ---- 通道 ----
CHANNEL_PROBE_TIMEOUT = 8.0    # 出口探针超时
CHANNEL_FALLBACK_BUDGET = 30.0 # 出口 fallback 总预算（秒）——D1 标准：切换 <=30s

# ---- 长程 ----
REST_BETWEEN_ROUNDS = 900.0    # 收敛后休整（keeper STALE_LIMIT 必须大于它）


def assert_interval(interval: float) -> float:
    """断言并钳制间隔不小于硬下限（防御性入口）。"""
    if interval < MIN_INTERVAL_HARD:
        return MIN_INTERVAL_HARD
    return interval
