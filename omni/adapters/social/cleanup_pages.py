#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""清理 pages.jsonl 中的垃圾条目（so.com 跳转页）"""
import json

path = "/tmp/sm-recon/runs/pages.jsonl"
lines = open(path).readlines()
keep = []
dropped = []
for l in lines:
    try:
        rec = json.loads(l)
    except Exception:
        continue
    if "so.com/link" in rec.get("url", "") or "sogou.com/link" in rec.get("url", ""):
        dropped.append(rec.get("url", "")[:60])
    else:
        keep.append(l)
open(path, "w").writelines(keep)
print(f"保留 {len(keep)} / 删除 {len(dropped)} 行")
for d in dropped[:8]:
    print("  dropped:", d)
