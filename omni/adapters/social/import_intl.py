#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""import_intl — 将国际平台 SERP 结果中的社群链接入库"""
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_store import Store  # noqa: E402

RULES = [
    (re.compile(r"t\.me/"), "tg_link"),
    (re.compile(r"facebook\.com/groups/"), "fb_group"),
    (re.compile(r"facebook\.com/(?!groups)"), "fb_page"),
    (re.compile(r"chat\.whatsapp\.com|wa\.me|whatsapp\.com/channel"), "wa_link"),
    (re.compile(r"discord\.(gg|com/invite)"), "dc_link"),
    (re.compile(r"instagram\.com/"), "ig_page"),
]

INTL_FILE = "/tmp/sm-recon/recon/intl_results.jsonl"
store = Store()
added = 0
scanned = 0
if os.path.exists(INTL_FILE):
    for line in open(INTL_FILE, encoding="utf-8"):
        try:
            rec = json.loads(line)
        except Exception:
            continue
        for r in rec.get("results", []):
            url = r.get("url", "")
            title = r.get("title", "")
            scanned += 1
            for pat, kind in RULES:
                if pat.search(url):
                    if store.add_lead(kind, url[:250], f"[{rec['query'][:60]}] {title[:100]}", "intl:brave", 0.5, 0.5):
                        added += 1
                        print(f"  +{kind}: {url[:100]}")
                    break
store.save()
print(f"扫描 {scanned} 条国际结果 → 新增 {added} 条线索（store 共 {len(store.leads)}）")
