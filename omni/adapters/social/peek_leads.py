#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""查看 leads.jsonl 采样"""
import json
import sys

limit = int(sys.argv[1]) if len(sys.argv) > 1 else 40
path = "/tmp/sm-recon/runs/leads.jsonl"
rows = []
for line in open(path, encoding="utf-8"):
    try:
        rows.append(json.loads(line))
    except Exception:
        continue
print(f"总线索: {len(rows)}")
rows.sort(key=lambda r: r.get("conf") or 0, reverse=True)
for r in rows[:limit]:
    ev = (r.get("evidence") or "")[:48].replace("\n", " ")
    src = (r.get("src") or "").replace("https://", "")[:50]
    print(f"{r['kind']:8s} {str(r['value'])[:30]:32s} conf={r.get('conf')} | {src} | {ev}")
