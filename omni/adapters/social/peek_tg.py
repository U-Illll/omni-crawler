#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""检查 t.me 群链接的上下文（SERP 来源 + tg 页面结构）"""
import glob
import re

print("=== SERP 上下文 ===")
for fn in glob.glob("/tmp/sm-recon/recon/intl_serp_*.html"):
    h = open(fn, encoding="utf-8", errors="ignore").read()
    idx = h.find("DHzf4UNdcqYm")
    if idx > 0:
        ctx = h[max(0, idx - 800):idx + 300]
        ctx = re.sub(r"<[^>]+>", " ", ctx)
        ctx = re.sub(r"\s+", " ", ctx)
        print(fn.split("/")[-1])
        print(ctx[-600:])
        break

print()
print("=== tg 页面结构 ===")
h = open("/tmp/sm-recon/recon/tg_group.html", encoding="utf-8", errors="ignore").read()
for pat in ["tgme_page_title", "tgme_page_description", "tgme_page_extra", "tgme_page_action"]:
    m = re.search(pat + r'[^>]*>(.*?)</', h, re.S)
    if m:
        txt = re.sub(r"<[^>]+>", "", m.group(1))
        print(pat, ":", txt[:150])
# 全页文本
body = re.sub(r"<script.*?</script>", "", h, flags=re.S)
body = re.sub(r"<[^>]+>", " ", body)
body = re.sub(r"\s+", " ", body)
print()
print("页面文本:", body[:400])
