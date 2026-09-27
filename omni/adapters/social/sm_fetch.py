#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sm_fetch — HTTP 抓取层【omni-crawler 适配版】。

迁移说明（2026-09-27）：保持原 sm_fetch 对外接口
（RateLimiter(base,jitter,min_interval) / Fetcher(...).get(url,...) -> (status,text,err) /
BlockedError / normalize_url / BLOCK_SIGNS），内部改为 omni 框架组件：
- 限速器 → omni.core.limiter.RateLimiter（自适应 + 硬上限）
- 重试/分类 → omni.core.retry（fatal 不重试、equal jitter）
- 熔断 → omni.core.circuit（真状态机）
- 审计/请求 → omni.fetch.http.HttpFetcher（统一出口 + 通道层）
cooldown 期间保持原语义：抛 BlockedError（调用方失败隔离）。
"""
import urllib.parse

from sm_common import RUNS, audit, log  # noqa: F401  （保持原对外导出）

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(_HERE)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from omni.core.limiter import RateLimiter as _OmniLimiter      # noqa: E402
from omni.core.circuit import CircuitBreaker                   # noqa: E402
from omni.channel.manager import ChannelManager                 # noqa: E402
from omni.fetch.http import HttpFetcher                         # noqa: E402

BLOCK_SIGNS = [
    "百度安全验证", "wappass", "验证码", "请输入验证码", "人机验证",
    "滑动验证", "安全检测", "访问过于频繁", "请稍后再试", "出错了",
]

SOFT_LIMIT_SIGNS = ["访问过于频繁", "请稍后再试", "出错了"]


class BlockedError(Exception):
    pass


class RateLimiter(_OmniLimiter):
    """兼容原构造签名 (base, jitter, min_interval)；min_interval 保留但不参与
    （omni 版以硬下限 MIN_INTERVAL_HARD 兜底）。"""

    def __init__(self, base=2.5, jitter=1.5, min_interval=1.0):
        super().__init__(base=base, jitter=jitter)
        self.min_interval = min_interval


class Fetcher:
    """经 omni 框架的统一 HTTP 出口（通道/限速/熔断/重试/审计全接入）。"""

    def __init__(self, limiter=None, timeout=25, max_retries=3):
        self.limiter = limiter or RateLimiter()
        self.timeout = timeout
        self.breaker = CircuitBreaker()
        self.channel = ChannelManager()
        self._http = HttpFetcher(limiter=self.limiter, breaker=self.breaker,
                                 channel=self.channel, timeout=timeout,
                                 max_retries=max_retries)

    @staticmethod
    def domain_of(url):
        return urllib.parse.urlparse(url).hostname or urllib.parse.urlparse(url).netloc

    def get(self, url, headers=None, timeout=None, allow_gzip=True, referer=None):
        """返回 (status, text, err)。err=None 为成功。
        兼容语义：熔断/通道不可用 → 抛 BlockedError（调用方失败隔离）。"""
        r = self._http.get(url, env="AUTO", headers=headers, referer=referer,
                           timeout=timeout)
        if r.cls in ("circuit_open", "channel"):
            raise BlockedError(f"{r.cls}: {r.err}")
        return r.status, r.text, r.err


def normalize_url(u, base=None):
    if not u:
        return None
    u = u.replace("\\/", "/").strip()
    if base:
        u = urllib.parse.urljoin(base, u)
    return u
