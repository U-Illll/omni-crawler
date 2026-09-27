#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""清掉微信查询标记（触发带 QR 的重扫）"""
import json
import sys

path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/sm-recon/runs/progress.json"
p = json.load(open(path, encoding="utf-8"))
before = len(p.get("queries_done", []))
p["queries_done"] = [q for q in p.get("queries_done", []) if not q.startswith("wx:")]
after = len(p["queries_done"])
tmp = path + ".tmp"
json.dump(p, open(tmp, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
import os
os.replace(tmp, path)
print(f"queries_done: {before} → {after}（清除 {before - after} 个微信标记）")
