#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据质量自检：验证每条 lead 的 value 确实出现在 evidence 里"""
import json

rows = []
for line in open("/tmp/sm-recon/runs/leads.jsonl", encoding="utf-8"):
    try:
        rows.append(json.loads(line))
    except Exception:
        continue

suspect = 0
for r in rows:
    v = str(r.get("value") or "")
    ev = r.get("evidence") or ""
    ok = v in ev
    if not ok:
        suspect += 1
        if suspect <= 10:
            print(f"SUSPECT: {r['kind']:8s} {v[:30]:32s} | ev: {ev[:120]}")
print(f"\n总计 {len(rows)} 条，可疑 {suspect} 条")
