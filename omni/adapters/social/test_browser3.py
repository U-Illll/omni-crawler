#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""浏览器通道多目标测试：百度 / 贴吧 / tgstat / bing"""
import re
import time

from playwright.sync_api import sync_playwright

TARGETS = [
    ("baidu", "https://www.baidu.com/s?wd=%E5%8D%97%E6%96%B9%E7%A7%91%E6%8A%80%E5%A4%A7%E5%AD%A6%20%E7%95%99%E5%AD%A6%E7%94%9F%20%E7%BE%A4"),
    ("tieba", "https://tieba.baidu.com/f?kw=%E5%8D%97%E6%96%B9%E7%A7%91%E6%8A%80%E5%A4%A7%E5%AD%A6&ie=utf-8"),
    ("tgstat", "https://tgstat.com/search?q=sustech"),
    ("bing-intl", "https://www.bing.com/search?q=SUSTech+telegram+chat"),
]

with sync_playwright() as p:
    b = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
    ctx = b.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        locale="zh-CN", viewport={"width": 1366, "height": 900},
    )
    page = ctx.new_page()
    for name, url in TARGETS:
        t0 = time.time()
        try:
            page.goto(url, timeout=45000, wait_until="domcontentloaded")
            page.wait_for_timeout(3000)
            html = page.content()
            title = page.title()
            shot = f"/tmp/sm-recon/runs/shot-{name}.png"
            try:
                page.screenshot(path=shot)
            except Exception:
                shot = "?"
            nums = re.findall(r"群[号]?[^\d]{0,8}(\d{5,11})", html)[:6]
            tme = sorted(set(re.findall(r"t\.me/(?:joinchat/)?([A-Za-z0-9_+%-]{4,50})", html)))[:6]
            print(f"## {name}: len={len(html)} title={title[:50]!r} ({time.time()-t0:.0f}s)")
            print(f"   shot={shot}")
            if nums:
                print(f"   群号命中: {nums}")
            if tme:
                print(f"   t.me: {tme}")
            # 验证页检测
            for sig in ["安全验证", "验证码", "robot", "captcha", "unusual"]:
                if sig.lower() in html.lower():
                    print(f"   [检测] {sig}")
        except Exception as e:
            print(f"## {name} ERR: {str(e)[:120]}")
    b.close()
print("DONE")
