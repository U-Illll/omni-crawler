#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""查看微信来源的 leads"""
import json
import sys

path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/sm-recon/runs/leads.jsonl"
for line in open(path, encoding="utf-8"):
    try:
        r = json.loads(line)
    except Exception:
        continue
    src = r.get("src") or ""
    if "wechat" in src or "mp.weixin" in src:
        print(f"{r['kind']:8s} | {str(r['value'])[:42]:44s} | {(r.get('evidence') or '')[:100]}")
