#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.fetch.browser — L1 浏览器执行体（Playwright；Camoufox 优先）。

定位：升级链第一档（HTTP 被指纹/WAF/JS 渲染阻挡时启用）。
- 引擎选择见 `_ensure`（2026-09-27 本机实测定稿：auto → chromium）。
- 代理：经 ChannelManager.resolve(env) 注入（Playwright proxy 参数）。
- 审计：每次 fetch 一行（含 env/engine/status/elapsed）。
- 依赖缺失时给出明确指引（不静默降级）。
- ⚠️ 运行环境（2026-09-27 实测）：本机需用 camoufox-venv 的 python 运行
  （playwright 1.63 + chromium-1243 已装好）；camoufox/Firefox 引擎在本机
  Ubuntu 26.04 暂不可用（135/152 两版 headless 启动卡死已实证，见 _ensure 注释）。
"""
import time

from ..core.log import audit
from ..channel.manager import ChannelManager


class BrowserUnavailable(Exception):
    pass


def _load_playwright():
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
        return sync_playwright
    except ImportError as e:
        raise BrowserUnavailable(
            "playwright 未安装（pip install playwright && playwright install chromium）") from e


def _load_camoufox():
    try:
        from camoufox.sync_api import Camoufox  # noqa: F401
        return Camoufox
    except ImportError:
        return None


class BrowserFetcher:
    def __init__(self, channel=None, engine="auto", headless=True):
        self.channel = channel or ChannelManager()
        self.engine_pref = engine
        self.headless = headless
        self._pw = None
        self._browser = None
        self._engine_used = None

    # ---------- 生命周期 ----------
    def _ensure(self, proxies):
        if self._browser:
            return
        sync_playwright = _load_playwright()
        self._pw = sync_playwright().start()
        pw_proxy = None
        if proxies:
            server = proxies.get("https") or proxies.get("http")
            pw_proxy = {"server": server}
        # 引擎策略（2026-09-27 本机实测定稿）：
        # - "auto"     → 直接 chromium。本机 Ubuntu 26.04 上 camoufox(Firefox 135/152)
        #   headless 启动卡死已实证（glxtest 缺失 + SWGL 失败 + juggler 不握手）；
        #   Playwright Chromium(1243) 同环境验证通过（L1 实测 groq 200）。
        #   Firefox 修复后可将 "auto" 解回 camoufox 优先。
        # - "chromium" → 强制 chromium；"camoufox" → 显式尝试（失败回退 chromium）。
        camouflage = self.engine_pref == "camoufox"
        if camouflage:
            Camoufox = _load_camoufox()
            if Camoufox is not None:
                try:
                    self._browser = Camoufox(headless=self.headless,
                                             proxy=pw_proxy).__enter__()
                    self._engine_used = "camoufox"
                    return
                except Exception as e:  # noqa: BLE001
                    audit({"kind": "browser_engine_fallback", "from": "camoufox",
                           "err": f"{type(e).__name__}: {str(e)[:120]}"})
        if self.engine_pref in ("auto", "chromium", "cloakbrowser", "camoufox"):
            # CloakBrowser 独立二进制（F:\mcps\mcps\CloakBrowser）经 CU 环境时用
            # executable_path 注入；默认用 playwright 自带 chromium。
            launch_kwargs = {"headless": self.headless}
            if pw_proxy:
                launch_kwargs["proxy"] = pw_proxy
            self._browser = self._pw.chromium.launch(**launch_kwargs)
            self._engine_used = "chromium"
            return
        raise BrowserUnavailable(f"不支持的浏览器引擎: {self.engine_pref}")

    def close(self):
        try:
            if self._browser:
                self._browser.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            if self._pw:
                self._pw.stop()
        except Exception:  # noqa: BLE001
            pass
        self._browser = None
        self._pw = None

    # ---------- 抓取 ----------
    def fetch(self, url, env="AUTO", timeout=40000, wait_until="domcontentloaded",
              wait_ms=1500):
        """返回 (status, html, err, meta)。"""
        from ..fetch.http import FetchResult
        try:
            _, proxies = self.channel.resolve(env)
        except Exception as e:  # noqa: BLE001  ChannelUnavailable
            return FetchResult(err=f"ChannelUnavailable: {e}", cls="channel",
                               meta={"env": env})
        t0 = time.time()
        page = None
        try:
            self._ensure(proxies)
            ctx = self._browser.new_context(
                viewport={"width": 1366, "height": 900},
                locale="zh-CN",
            ) if self._engine_used != "camoufox" else self._browser.new_context()
            page = ctx.new_page()
            resp = page.goto(url, timeout=timeout, wait_until=wait_until)
            if wait_ms:
                page.wait_for_timeout(wait_ms)
            html = page.content()
            status = resp.status if resp else None
            elapsed = round(time.time() - t0, 3)
            audit({"kind": "browser_fetch", "url": url, "env": env,
                   "engine": self._engine_used, "status": status, "elapsed": elapsed})
            if status is not None and 200 <= status < 300:
                return FetchResult(status, html, None, "ok",
                                   {"env": env, "engine": self._engine_used,
                                    "elapsed": elapsed})
            return FetchResult(status, html, f"HTTP {status}", "transient" if status and status >= 500 else "fatal",
                               {"env": env, "engine": self._engine_used, "elapsed": elapsed})
        except Exception as e:  # noqa: BLE001
            elapsed = round(time.time() - t0, 3)
            audit({"kind": "browser_fetch", "url": url, "env": env,
                   "engine": self._engine_used, "status": None,
                   "err": f"{type(e).__name__}: {str(e)[:160]}", "elapsed": elapsed})
            return FetchResult(err=f"{type(e).__name__}: {str(e)[:160]}", cls="transient",
                               meta={"env": env, "engine": self._engine_used})
        finally:
            try:
                if page:
                    page.context.close()
            except Exception:  # noqa: BLE001
                pass
