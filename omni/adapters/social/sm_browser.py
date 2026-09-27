#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sm_browser — 浏览器通道（Playwright/chromium）：需 JS 的站点

用途：百度搜索 / 贴吧 / tgstat 等 HTTP 层被拦或需 JS 渲染的页面。
设计：单例浏览器（进程内复用）、每请求新建 page、含超时与反检测基础参数。
依赖：venv 内的 playwright + chromium（chromium-1200 symlink 或真实安装）。
"""
import os
import re
import time

from sm_common import audit, log

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

_pw = None
_browser = None
_ctx = None


def _ensure_browser(headless=True):
    global _pw, _browser, _ctx
    if _browser is not None:
        return _browser
    from playwright.sync_api import sync_playwright
    _pw = sync_playwright().start()
    _browser = _pw.chromium.launch(
        headless=headless,
        args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
    )
    _ctx = _browser.new_context(
        user_agent=_UA, locale="zh-CN", viewport={"width": 1366, "height": 900},
        extra_http_headers={"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"},
    )
    # 轻量反检测：去掉 webdriver 标志
    _ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
    return _browser


def fetch_page(url, timeout=45000, wait_ms=2500, save_shot=None):
    """加载页面返回 (html, final_url, err)"""
    try:
        _ensure_browser()
        page = _ctx.new_page()
        try:
            page.goto(url, timeout=timeout, wait_until="domcontentloaded")
            if wait_ms:
                page.wait_for_timeout(wait_ms)
            html = page.content()
            final = page.url
            if save_shot:
                try:
                    page.screenshot(path=save_shot)
                except Exception:  # noqa: BLE001
                    pass
                audit({"kind": "browser_fetch", "url": url[:200], "len": len(html), "shot": save_shot})
            else:
                audit({"kind": "browser_fetch", "url": url[:200], "len": len(html)})
            return html, final, None
        finally:
            page.close()
    except Exception as e:  # noqa: BLE001
        audit({"kind": "browser_fetch_err", "url": url[:200], "err": str(e)[:150]})
        return None, url, f"{type(e).__name__}: {str(e)[:150]}"


def close():
    global _pw, _browser, _ctx
    try:
        if _browser:
            _browser.close()
        if _pw:
            _pw.stop()
    except Exception:  # noqa: BLE001
        pass
    _pw = _browser = _ctx = None


# ---------------- 百度搜索（浏览器通道） ----------------
BAIDU_RESULT_RE = re.compile(
    r'<h3[^>]*class="[^"]*(?:t|c-title)[^"]*"[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
    re.S,
)


def search_baidu(url_or_kw, save_html=None):
    """百度搜索（浏览器）。返回 (results, html, err)；results=[{title,url}]"""
    if url_or_kw.startswith("http"):
        url = url_or_kw
    else:
        import urllib.parse
        url = "https://www.baidu.com/s?wd=" + urllib.parse.quote(url_or_kw)
    html, final, err = fetch_page(url, wait_ms=3000)
    if err or not html:
        return [], html, err
    if save_html:
        try:
            open(save_html, "w", encoding="utf-8").write(html)
        except Exception:  # noqa: BLE001
            pass
    results = []
    for m in BAIDU_RESULT_RE.finditer(html):
        href, title = m.group(1), re.sub(r"<[^>]+>", "", m.group(2)).strip()
        if href.startswith("http"):
            results.append({"title": title[:120], "url": href[:300]})
    # baidu.com/link?url= 形式的真实链接
    for m in re.finditer(r'<h3[^>]*>.*?<a[^>]*href="(https?://www\.baidu\.com/link\?url=[^"]+)"', html, re.S):
        results.append({"title": "(link)", "url": m.group(1)[:300]})
    return results, html, None


if __name__ == "__main__":
    import sys
    kw = sys.argv[1] if len(sys.argv) > 1 else "南方科技大学 留学生 群"
    res, html, err = search_baidu(kw, save_html="/tmp/sm-recon/recon/baidu_search.html")
    print("err:", err, "| html:", len(html) if html else 0, "| results:", len(res))
    for r in res[:10]:
        print("  -", r["title"][:70], "|", r["url"][:100])
    nums = re.findall(r"群[号]?[^\d]{0,8}(\d{5,11})", html or "")
    print("群号命中:", nums[:10])
    close()
