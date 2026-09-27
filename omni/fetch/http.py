#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.fetch.http — L0 HTTP 执行体。

把核心组件串成统一请求出口：
- 通道解析（channel.resolve(env) → 出口/代理）
- 熔断检查（circuit.check）
- 域级限速（limiter.wait）
- 请求 + 分类（retry.classify_* / detect）→ 反馈（limiter/circuit/channel）
- 重试循环（equal jitter 退避；fatal/blocked 不重试）
- 每请求审计（含 env/egress/status/cls/attempt/elapsed 读数）

TLS：默认 requests 会话（trust_env=False 防环境代理干扰）；
legacy_tls=True 时挂 R3 LegacyTLSAdapter（老式 TLS 服务器兼容）。
"""
import ssl
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

from ..anticrawl import detect as detect_mod
from ..anticrawl.ua import ua
from ..core import retry as retry_mod
from ..core.log import audit, heartbeat
from ..core.limiter import RateLimiter
from ..core.circuit import CircuitBreaker, CircuitOpenError
from ..channel.manager import ChannelManager, ChannelUnavailable


class LegacyTLSAdapter(HTTPAdapter):
    """兼容需要 OP_LEGACY_SERVER_CONNECT 的老式 TLS 服务器（R3 原样移植）。"""

    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0x4)
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)


class FetchResult:
    __slots__ = ("status", "text", "err", "cls", "meta")

    def __init__(self, status=None, text=None, err=None, cls="unknown", meta=None):
        self.status = status
        self.text = text
        self.err = err
        self.cls = cls
        self.meta = meta or {}

    def ok(self):
        return self.err is None and self.status is not None and 200 <= self.status < 300

    def __repr__(self):
        return (f"FetchResult(status={self.status}, cls={self.cls}, err={self.err!r}, "
                f"meta={self.meta})")


class HttpFetcher:
    def __init__(self, limiter=None, breaker=None, channel=None,
                 timeout=25.0, max_retries=3):
        self.limiter = limiter or RateLimiter()
        self.breaker = breaker or CircuitBreaker()
        self.channel = channel or ChannelManager()
        self.timeout = timeout
        self.max_retries = max_retries
        self._sessions = {}   # (legacy_tls, env) -> requests.Session

    # ---------- session ----------
    def _session(self, legacy_tls):
        key = bool(legacy_tls)
        s = self._sessions.get(key)
        if s is None:
            s = requests.Session()
            s.trust_env = False
            if key:
                s.mount("https://", LegacyTLSAdapter(pool_connections=10, pool_maxsize=10))
            self._sessions[key] = s
        return s

    # ---------- 主入口 ----------
    def get(self, url, env="AUTO", headers=None, referer=None,
            legacy_tls=False, timeout=None, session=None, detect=True,
            allow_redirects=True):
        """返回 FetchResult。所有路径均有审计。

        allow_redirects=False：显式捕获跳转（如 signin 握手，2026-09-27
        freetier 需求）——3xx 响应本身即所需结果（cls 记 ok，不跟随、不重试）。
        """
        import urllib.parse
        # 限速/熔断键 = 主机名（去端口；IPv6 亦经 hostname 归一）
        domain = urllib.parse.urlparse(url).hostname or urllib.parse.urlparse(url).netloc
        # 1) 通道解析
        try:
            egress_name, proxies = self.channel.resolve(env)
        except ChannelUnavailable as e:
            audit({"kind": "fetch", "url": url, "domain": domain, "env": env,
                   "status": None, "cls": "channel", "err": str(e)[:160], "attempt": 0})
            return FetchResult(err=f"ChannelUnavailable: {e}", cls="channel",
                               meta={"env": env})
        # 2) 熔断检查
        try:
            self.breaker.check(domain)
        except CircuitOpenError as e:
            audit({"kind": "fetch", "url": url, "domain": domain, "env": env,
                   "egress": egress_name, "status": None, "cls": "circuit_open",
                   "err": str(e)[:160], "attempt": 0})
            return FetchResult(err=f"CircuitOpenError: {e}", cls="circuit_open",
                               meta={"env": env, "egress": egress_name})

        h = {
            "User-Agent": ua(),   # 教训 2026-09-27：必须默认注入浏览器 UA（否则 WAF 秒拒 403）
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
        if referer:
            h["Referer"] = referer
        if headers:
            h.update(headers)

        attempt = 0
        last = FetchResult(cls="unknown", meta={"env": env, "egress": egress_name})
        while attempt <= self.max_retries:
            heartbeat()
            self.limiter.wait(domain)
            t0 = time.time()
            status, text, err, cls = None, None, None, "unknown"
            try:
                s = session or self._session(legacy_tls)
                r = s.get(url, headers=h, timeout=timeout or self.timeout,
                          proxies=proxies, allow_redirects=allow_redirects)
                status = r.status_code
                text = _decode(r)
                if (not allow_redirects and status is not None
                        and 300 <= status < 400):
                    cls = "ok"   # 显式捕获跳转：响应本身即结果（caller 自处理）
                else:
                    cls = retry_mod.classify_status(status)
                if cls == "ok" and detect and status is not None and 200 <= status < 300:
                    d = detect_mod.detect_content(text)
                    if d == "blocked":
                        cls, err = "blocked", "BLOCK_SIGN"
                    elif d == "challenge":
                        cls, err = "blocked", "CHALLENGE"
                    elif d == "soft_limit":
                        cls, err = "limited", "SOFT_LIMIT"
            except Exception as e:  # noqa: BLE001
                cls = retry_mod.classify_exception(e)
                err = f"{type(e).__name__}: {str(e)[:160]}"

            elapsed = round(time.time() - t0, 3)
            ok = (cls == "ok")
            audit({"kind": "fetch", "url": url, "domain": domain, "env": env,
                   "egress": egress_name, "status": status, "cls": cls, "err": err,
                   "attempt": attempt, "elapsed": elapsed})

            if ok:
                self.limiter.on_success(domain)
                self.breaker.record_success(domain)
                self.channel.mark_success(env, egress_name)
                return FetchResult(status, text, None, "ok",
                                   {"env": env, "egress": egress_name,
                                    "attempt": attempt, "elapsed": elapsed})
            # 失败反馈
            if cls == "limited":
                self.limiter.on_limited(domain)
                self.breaker.record_failure(domain, "limited")
            elif cls == "blocked":
                self.limiter.on_blocked(domain)
                self.breaker.record_failure(domain, "blocked")
            elif cls == "transient":
                self.breaker.record_failure(domain, "transient")
            # fatal：不喂 limiter/circuit（R-B6 语义）

            last = FetchResult(status, text, err or f"HTTP {status}", cls,
                               {"env": env, "egress": egress_name,
                                "attempt": attempt, "elapsed": elapsed})
            if cls == "blocked":
                return last          # 验证页/挑战：不重试（上层决定冷却或升级执行体）
            plan = retry_mod.wait_plan(cls, attempt + 1)
            if plan is None:
                return last
            attempt += 1
            if attempt > self.max_retries:
                last.err = (last.err or "") + " | MAX_RETRIES"
                audit({"kind": "retry_exhausted", "url": url, "domain": domain,
                       "cls": cls, "attempts": attempt})
                return last
            audit({"kind": "retry_scheduled", "url": url, "domain": domain,
                   "cls": cls, "next_attempt": attempt, "wait": round(plan, 2)})
            time.sleep(plan)
        return last

    # ---------- 协议层增强（L0+）：curl_cffi 浏览器指纹 ----------
    def get_impersonated(self, url, impersonate="chrome", env="AUTO",
                         headers=None, timeout=None, allow_redirects=True):
        """curl_cffi 浏览器 TLS/JA3/HTTP2 指纹请求（协议层增强，L0+）。

        用于目标按 TLS 指纹拦截 requests 的场景（2026-09-27 接入：curl_cffi
        实证 groq/住宅出口 200）。返回与 get() 同规 FetchResult；audit
        kind=fetch_impersonate；单次请求（无重试循环，重试由调用方/升级链定）。
        curl_cffi 未安装时返回明确错误（不静默降级）。
        """
        try:
            from curl_cffi import requests as ccr
        except ImportError:
            return FetchResult(err="curl_cffi 未安装（pip install curl_cffi）",
                               cls="fatal", meta={"env": env})
        try:
            egress_name, proxies = self.channel.resolve(env)
        except Exception as e:  # noqa: BLE001  ChannelUnavailable
            return FetchResult(err=f"ChannelUnavailable: {e}", cls="channel",
                               meta={"env": env})
        t0 = time.time()
        status = None
        text, err, cls = "", None, "unknown"
        try:
            r = ccr.get(url, impersonate=impersonate, headers=headers or None,
                        proxies=proxies or None, timeout=timeout or self.timeout,
                        allow_redirects=allow_redirects)
            status = r.status_code
            text = r.text
            cls = retry_mod.classify_status(status)
            if cls == "ok" and status is not None and 200 <= status < 300:
                d = detect_mod.detect_content(text)
                if d == "blocked":
                    cls, err = "blocked", "BLOCK_SIGN"
                elif d == "challenge":
                    cls, err = "blocked", "CHALLENGE"
                elif d == "soft_limit":
                    cls, err = "limited", "SOFT_LIMIT"
        except Exception as e:  # noqa: BLE001
            cls = retry_mod.classify_exception(e)
            err = f"{type(e).__name__}: {str(e)[:160]}"
        elapsed = round(time.time() - t0, 3)
        audit({"kind": "fetch_impersonate", "url": url, "env": env,
               "egress": egress_name, "status": status, "cls": cls,
               "impersonate": impersonate, "err": err, "elapsed": elapsed})
        if cls == "ok":
            self.channel.mark_success(env, egress_name)
            return FetchResult(status, text, None, "ok",
                               {"env": env, "egress": egress_name,
                                "impersonate": impersonate, "elapsed": elapsed})
        return FetchResult(status, text, err or f"HTTP {status}", cls,
                           {"env": env, "egress": egress_name,
                            "impersonate": impersonate, "elapsed": elapsed})


def _decode(r):
    """响应解码：头声明 charset 优先 → utf-8 → gbk → latin-1 兜底。"""
    enc = None
    ct = r.headers.get("Content-Type", "")
    if "charset=" in ct:
        enc = ct.split("charset=", 1)[1].split(";")[0].strip().strip('"').lower()
    for cand in (enc, "utf-8", "gbk"):
        if not cand:
            continue
        try:
            return r.content.decode(cand)
        except Exception:  # noqa: BLE001
            continue
    return r.content.decode("utf-8", "ignore")
