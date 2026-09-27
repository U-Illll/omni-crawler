#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""渲染 infoadmin 内页（downloads/questions/application/admission）"""
import re
import sys

from playwright.sync_api import sync_playwright

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

PAGES = ["/downloads", "/questions", "/application", "/admission", "/downloads/material", "/downloads/regulations"]

with sync_playwright() as pw:
    b = pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
    ctx = b.new_context(user_agent=UA)
    for p in PAGES:
        try:
            page = ctx.new_page()
            page.goto("https://infoadmin.sustech.edu.cn" + p, timeout=40000, wait_until="domcontentloaded")
            page.wait_for_timeout(2500)
            html = page.content()
            fn = "/tmp/sm-recon/recon/iar_" + p.strip("/").replace("/", "_") + ".html"
            open(fn, "w", encoding="utf-8").write(html)
            text = re.sub(r"<script.*?</script>|<style.*?</style>", "", html, flags=re.S)
            text = re.sub(r"<[^>]+>", " ", text)
            text = re.sub(r"\s+", " ", text)
            print(f"=== {p} ({len(html)}B) ===")
            # 群/联系/PDF 扫描
            for m in re.finditer(r"[^ ]{0,30}(群|wechat|whatsapp|telegram|pdf|download|扫码|contact)[^ ]{0,50}", text, re.I):
                print("  ·", m.group(0)[:110])
            page.close()
        except Exception as e:  # noqa: BLE001
            print(f"=== {p} FAIL: {str(e)[:80]}")
    b.close()
