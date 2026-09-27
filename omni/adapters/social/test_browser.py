#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""浏览器通道测试：Playwright 抓 Google 搜索"""
import re
from playwright.sync_api import sync_playwright

queries = [
    "site:t.me sustech",
    "SUSTech telegram group",
    "南方科技大学 留学生 telegram",
]
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        locale="zh-CN",
    )
    page = ctx.new_page()
    for q in queries:
        try:
            page.goto("https://www.google.com/search?num=20&q=" + q.replace(" ", "+"), timeout=35000, wait_until="domcontentloaded")
            page.wait_for_timeout(2500)
            html = page.content()
            links = sorted(set(re.findall(r"t\.me/(?:joinchat/)?([A-Za-z0-9_+%-]{4,60})", html)))
            print(f"## {q}  (html {len(html)})")
            if links:
                for l in links[:10]:
                    print("   t.me/" + l)
            else:
                # 看有没有搜索结果
                n = html.count("<h3")
                print(f"   无 t.me；h3 数={n}")
        except Exception as e:
            print(f"## {q} ERR: {str(e)[:120]}")
    browser.close()
