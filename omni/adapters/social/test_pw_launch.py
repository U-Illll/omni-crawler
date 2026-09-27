#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""playwright + chromium 启动快测（symlink 方案）"""
import re
import time

t0 = time.time()
from playwright.sync_api import sync_playwright  # noqa: E402

print(f"[{time.time()-t0:.1f}s] import", flush=True)
with sync_playwright() as p:
    b = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
    print(f"[{time.time()-t0:.1f}s] launched, version={b.version}", flush=True)
    page = b.new_page()
    try:
        page.goto("https://www.google.com/search?q=site:t.me+sustech&num=20", timeout=40000, wait_until="domcontentloaded")
        page.wait_for_timeout(2500)
        html = page.content()
        links = sorted(set(re.findall(r"t\.me/(?:joinchat/)?([A-Za-z0-9_+%-]{4,60})", html)))
        print(f"[{time.time()-t0:.1f}s] google OK len={len(html)} t.me={links[:8]}", flush=True)
        print("title:", page.title()[:80], flush=True)
    except Exception as e:
        print(f"[{time.time()-t0:.1f}s] google FAIL: {str(e)[:150]}", flush=True)
    b.close()
print("DONE", flush=True)
