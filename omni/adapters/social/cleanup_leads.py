#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cleanup_leads — 线索清洗：过滤明显误报（wx_hint JS 噪音等）

用法: python3 cleanup_leads.py [leads.jsonl 路径]
"""
import json
import re
import sys

path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/sm-recon/runs/leads.jsonl"

BLACKLIST = {
    "report", "bridge", "function", "script", "window", "document", "return",
    "undefined", "weixinbridge", "weixin", "wechat", "javascript", "html",
}


def is_bad(kind, value):
    v = str(value)
    if kind == "wx_hint":
        if v.startswith("_"):
            return True
        if v.lower() in BLACKLIST:
            return True
        if not re.match(r"^[A-Za-z][A-Za-z0-9_-]{4,29}$", v):
            return True
    if kind == "qq_num":
        if not re.match(r"^\d{5,10}$", v):
            return True
    return False


rows = []
dropped = 0
for line in open(path, encoding="utf-8"):
    try:
        r = json.loads(line)
    except Exception:
        continue
    if is_bad(r.get("kind"), r.get("value")):
        dropped += 1
        print("drop:", r.get("kind"), r.get("value"))
        continue
    rows.append(r)

with open(path, "w", encoding="utf-8") as f:
    for r in rows:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
print(f"清理完成：保留 {len(rows)} / 删除 {dropped}")
