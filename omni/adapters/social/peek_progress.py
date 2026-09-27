#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""查看 progress.json 中的 pending URL 采样"""
import json

p = json.load(open("/tmp/sm-recon/runs/progress.json"))
pend = p.get("pending_urls", [])
print("pending 总数:", len(pend))
for u in pend[:25]:
    print("  ", u[:150])
print()
print("queries_done:", len(p.get("queries_done", [])))
print("stats:", p.get("stats"))
