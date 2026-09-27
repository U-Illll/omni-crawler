#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""peek_recent — 查看最近新增线索（默认 20 分钟）"""
import json
import sys
import time
from collections import Counter

mins = int(sys.argv[1]) if len(sys.argv) > 1 else 20
path = "/tmp/sm-recon/runs/leads.jsonl"
recent = []
total = 0
now = time.time()
for line in open(path, encoding="utf-8"):
    try:
        r = json.loads(line)
    except Exception:
        continue
    total += 1
    if (r.get("first_ts") or 0) > now - mins * 60:
        recent.append(r)

print(f"总线索: {total} | 最近 {mins} 分钟新增: {len(recent)}")
if recent:
    print("kind 分布:", dict(Counter(r["kind"] for r in recent)))
    for r in recent[-20:]:
        print(f"  {r['kind']:10s} | {str(r['value'])[:60]:62s} | {(r.get('evidence') or '')[:45]}")
