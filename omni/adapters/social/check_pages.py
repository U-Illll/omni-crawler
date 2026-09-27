#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""诊断 pages.jsonl 行级差异"""
import json
import os

path = "/tmp/sm-recon/runs/pages.jsonl"
lines = open(path, encoding="utf-8").readlines()
print("总行数:", len(lines))
bad = 0
nourl = 0
empty = 0
for i, line in enumerate(lines):
    if not line.strip():
        empty += 1
        continue
    try:
        r = json.loads(line)
    except Exception as e:
        bad += 1
        print(f"  [坏行 {i}] {line[:100]}")
        continue
    if not r.get("url"):
        nourl += 1
        print(f"  [无url {i}] {str(r)[:100]}")
print(f"坏行={bad} 无url={nourl} 空行={empty}")
