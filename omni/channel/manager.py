#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.channel.manager — 通道层：声明式选路 + 健康探测 + fallback。

对标 D1 标准：网络多通道 fallback 故障切换 ≤30s；切换事件写结构化审计
（发现 / 切换 / 恢复三段读数，DR-B1 教训）。
"""
import time
import urllib.request

from ..core.config import CHANNEL_FALLBACK_BUDGET, CHANNEL_PROBE_TIMEOUT
from ..core.log import audit

# 环境代号
LAN = "LAN"      # 南科大内网
CN = "CN"        # 国内公网
INTL = "INTL"    # 外网
AUTO = "AUTO"    # 自动

# ---- 出口表（可按部署环境注入替换） ----
DEFAULT_EGRESS = {
    "direct": {
        "proxies": None,
        "desc": "直连（校园网 / 云主机本机出口）",
    },
    "mihomo": {
        "proxies": {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"},
        "desc": "本机 mihomo 混合口（机场/自建/ISP 三通道由其内部路由）",
    },
}

# ---- 环境 → 出口偏好（按序尝试） ----
DEFAULT_ENV_PREF = {
    LAN: ["direct"],
    CN: ["direct", "mihomo"],
    INTL: ["mihomo", "direct"],
    AUTO: ["direct", "mihomo"],
}

# ---- 环境探测目标（低频，结果缓存；探测亦属对目标站的请求，故选根路径） ----
DEFAULT_PROBE_URLS = {
    LAN: "https://tis.sustech.edu.cn/",
    CN: "https://www.baidu.com/",
    INTL: "https://search.brave.com/",
}

HEALTH_TTL = 600.0     # 健康结果缓存（秒）
PROBE_DEBOUNCE = 30.0  # 强制重探防抖窗口（秒）——防连续 resolve 骚扰目标站


class ChannelUnavailable(Exception):
    pass


class ChannelManager:
    def __init__(self, egress_table=None, env_pref=None, probe_urls=None,
                 probe_enabled=True):
        self.egress = dict(egress_table or DEFAULT_EGRESS)
        self.env_pref = dict(env_pref or DEFAULT_ENV_PREF)
        self.probe_urls = dict(probe_urls or DEFAULT_PROBE_URLS)
        self.probe_enabled = probe_enabled
        self.health = {}   # (env, egress_name) -> {"ok": bool, "ts": float, "note": str}

    # ---------- 解析 ----------
    def resolve(self, env=AUTO):
        """按环境偏好返回 (egress_name, proxies)。失败抛 ChannelUnavailable。
        切换/失败均写审计（含读数）。"""
        env = env.upper()
        pref = self.env_pref.get(env) or self.env_pref[AUTO]
        t0 = time.time()
        tried = []
        for name in pref:
            if name not in self.egress:
                continue
            if not self._is_healthy(env, name):
                tried.append((name, "unhealthy"))
                continue
            egress = self.egress[name]
            if len(tried):
                audit({"kind": "channel_fallback", "env": env, "chosen": name,
                       "tried": tried, "elapsed": round(time.time() - t0, 3)})
            return name, egress.get("proxies")
        # 全失败：预算内依次强制重探再试一次（带防抖，防连续调用骚扰目标站）
        if self.probe_enabled:
            for name in pref:
                if name not in self.egress:
                    continue
                if self._probe(env, name, force=True):
                    audit({"kind": "channel_recovered", "env": env, "chosen": name,
                           "elapsed": round(time.time() - t0, 3)})
                    return name, self.egress[name].get("proxies")
                if time.time() - t0 > CHANNEL_FALLBACK_BUDGET:
                    break
        audit({"kind": "channel_unavailable", "env": env, "tried": tried})
        raise ChannelUnavailable(f"no healthy egress for env={env} (tried={tried})")

    def _is_healthy(self, env, name):
        key = (env, name)
        h = self.health.get(key)
        if h and (time.time() - h["ts"]) < HEALTH_TTL:
            return h["ok"]
        if not self.probe_enabled:
            return True
        return self._probe(env, name)

    # ---------- 探测 ----------
    def _probe(self, env, name, force=False):
        """探测出口健康。缓存纪律：
        - 非 force：TTL 内直接返回缓存（不重复探测）；
        - force：绕过 TTL，但受 PROBE_DEBOUNCE 防抖（窗口内返回缓存）。
        """
        key = (env, name)
        now = time.time()
        h = self.health.get(key)
        if h:
            age = now - h["ts"]
            if age < HEALTH_TTL and not force:
                return h["ok"]
            if force and age < PROBE_DEBOUNCE:
                return h["ok"]
        url = self.probe_urls.get(env) or self.probe_urls.get(CN)
        proxies = self.egress.get(name, {}).get("proxies")
        ok, note = False, ""
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            if proxies:
                opener = urllib.request.build_opener(
                    urllib.request.ProxyHandler(proxies))
            else:
                opener = urllib.request.build_opener(
                    urllib.request.ProxyHandler({}))   # 显式禁用环境代理
            resp = opener.open(req, timeout=CHANNEL_PROBE_TIMEOUT)
            code = resp.getcode()
            ok = 200 <= code < 400
            note = f"HTTP {code}"
        except Exception as e:  # noqa: BLE001
            note = f"{type(e).__name__}: {str(e)[:80]}"
        self.health[key] = {"ok": ok, "ts": time.time(), "note": note}
        audit({"kind": "channel_probe", "env": env, "egress": name, "ok": ok,
               "note": note, "url": url, "force": force})
        return ok

    # ---------- 反馈 ----------
    def mark_failure(self, env, name, err):
        key = (env, name)
        self.health[key] = {"ok": False, "ts": time.time(),
                            "note": f"fetch_failed: {str(err)[:80]}"}
        audit({"kind": "channel_mark_failure", "env": env, "egress": name,
               "err": str(err)[:120]})

    def mark_success(self, env, name):
        key = (env, name)
        prev = self.health.get(key)
        self.health[key] = {"ok": True, "ts": time.time(), "note": "fetch_ok"}
        if prev and not prev.get("ok"):
            audit({"kind": "channel_mark_success", "env": env, "egress": name})

    def snapshot(self):
        return {f"{e}/{n}": h for (e, n), h in self.health.items()}
