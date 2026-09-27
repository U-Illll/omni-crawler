#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""浏览器通道测试 v2：camoufox 抓 Google"""
import re

from camoufox.sync_api import Camoufox

queries = [
    "site:t.me sustech",
    "SUSTech telegram group",
]
with Camoufox(headless=True) as browser:
    page = browser.new_page()
    for q in queries:
        try:
            page.goto("https://www.google.com/search?num=20&q=" + q.replace(" ", "+"),
                      timeout=45000, wait_until="domcontentloaded")
            page.wait_for_timeout(2500)
            html = page.content()
            links = sorted(set(re.findall(r"t\.me/(?:joinchat/)?([A-Za-z0-9_+%-]{4,60})", html)))
            print(f"## {q}  (html {len(html)})")
            if links:
                for l in links[:10]:
                    print("   t.me/" + l)
            else:
                n = html.count("<h3")
                print(f"   无 t.me；h3 数={n}")
                # 检测验证页
                for sig in ["unusual traffic", "not a robot", "captcha", "consent"]:
                    if sig.lower() in html.lower():
                        print("   [检测到]", sig)
        except Exception as e:
            print(f"## {q} ERR: {str(e)[:150]}")
