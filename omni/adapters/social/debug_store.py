#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""诊断 Store 计数与文件计数差异"""
import json
import sys
import os

sys.path.insert(0, "/tmp/sm-recon/src")
from sm_store import Store  # noqa: E402

s = Store()
print("Store.pages_path:", s.pages_path)
print("Store.dir:", s.dir)
print("summary:", s.summary())
print("len(seen_urls):", len(s.seen_urls))

n = 0
for line in open(s.pages_path, encoding="utf-8"):
    if not line.strip():
        continue
    try:
        r = json.loads(line)
    except Exception as e:
        print("  bad:", str(e)[:60], line[:80])
        continue
    if r.get("url"):
        n += 1
print("手动计数(有url):", n)

# 差异明细：文件里的 url 集 vs seen_urls
file_urls = set()
for line in open(s.pages_path, encoding="utf-8"):
    try:
        r = json.loads(line)
        if r.get("url"):
            file_urls.add(r["url"])
    except Exception:
        pass
print("file unique urls:", len(file_urls))
missing = file_urls - s.seen_urls
print("file 有但 seen 没有:", len(missing))
for u in list(missing)[:10]:
    print("  ", u[:130])
