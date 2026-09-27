#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""查看指定群号的来源文章"""
import json
import sys

store_leads = "/tmp/sm-recon/runs/leads.jsonl"
pages = "/tmp/sm-recon/runs/pages.jsonl"
targets = sys.argv[1:]

titles = {}
for line in open(pages, encoding="utf-8"):
    try:
        r = json.loads(line)
        titles[r.get("url")] = r.get("title")
    except Exception:
        pass

for line in open(store_leads, encoding="utf-8"):
    try:
        r = json.loads(line)
    except Exception:
        continue
    if str(r.get("value")) in targets:
        src = r.get("src") or ""
        print(f"{r['value']} | kind={r['kind']} conf={r.get('conf')}")
        print(f"   src: {src[:120]}")
        print(f"   标题: {titles.get(src, '(未找到)')[:100]}")
        print(f"   证据: {(r.get('evidence') or '')[:150]}")
