#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""渲染 infoadmin 拿完整链接结构"""
import re

from playwright.sync_api import sync_playwright

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

with sync_playwright() as pw:
    b = pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
    ctx = b.new_context(user_agent=UA)
    page = ctx.new_page()
    page.goto("https://infoadmin.sustech.edu.cn/index", timeout=45000, wait_until="domcontentloaded")
    page.wait_for_timeout(3500)
    html = page.content()
    print("len:", len(html))
    open("/tmp/sm-recon/recon/infoadmin_rendered.html", "w", encoding="utf-8").write(html)
    links = set()
    for m in re.finditer(r'href="([^"#]+)"', html):
        u = m.group(1)
        if u.startswith("http") and "sustech" not in u:
            continue
        links.add(u)
    print(f"站内链接 {len(links)} 个:")
    for u in sorted(links)[:40]:
        print(" ", u[:120])
    # 菜单文本
    nav = re.findall(r'<a[^>]*href="([^"]+)"[^>]*>\s*([^<]{2,40})\s*</a>', html)
    print("\n导航样本:")
    seen = set()
    for u, t in nav:
        t = t.strip()
        if t and t not in seen and len(seen) < 30:
            seen.add(t)
            print(f"  {t[:40]:42s} {u[:80]}")
    b.close()
