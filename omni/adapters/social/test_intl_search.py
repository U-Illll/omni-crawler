#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""国际化搜索引擎通道测试（chromium headless）"""
import sys
import time

from playwright.sync_api import sync_playwright

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

TESTS = [
    ("ddg", "https://html.duckduckgo.com/html/?q=SUSTech+international+students+group"),
    ("google", "https://www.google.com/search?q=SUSTech+international+students+WhatsApp"),
    ("brave", "https://search.brave.com/search?q=SUSTech+international+students+telegram"),
]

with sync_playwright() as pw:
    b = pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
    ctx = b.new_context(user_agent=UA, locale="en-US")
    for name, url in TESTS:
        t0 = time.time()
        try:
            page = ctx.new_page()
            page.goto(url, timeout=35000, wait_until="domcontentloaded")
            page.wait_for_timeout(2000)
            html = page.content()
            title = page.title()
            print(f"[{name}] OK len={len(html)} title={title[:60]!r} 用时{time.time()-t0:.0f}s")
            open(f"/tmp/sm-recon/recon/intl_{name}.html", "w", encoding="utf-8").write(html)
            # 快速统计
            for kw in ["t.me", "chat.whatsapp", "discord.gg", "SUSTech"]:
                c = html.count(kw)
                if c:
                    print(f"    {kw}: {c}")
            page.close()
        except Exception as e:  # noqa: BLE001
            print(f"[{name}] FAIL: {type(e).__name__}: {str(e)[:100]}")
    b.close()
print("DONE")
