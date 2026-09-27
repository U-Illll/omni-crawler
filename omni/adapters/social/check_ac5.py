#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""check_ac5 — 域覆盖检查（5 域产出或书面原因）"""
import json
import os
import sys

BASE = os.environ.get("SM_BASE", "/tmp/sm-recon")
RUNS = os.path.join(BASE, "runs")

DOMAINS = {
    "D1-search": ["bendibao", "so.com", "sogou", "haedu", "gaokao"],
    "D2-wechat": ["wechat", "mp.weixin"],
    "D3-school": ["sustech.edu.cn", "sustech.site", "flybook", "github.com/SUSTech"],
    "D4-qq": [],  # 由 kind 判定
    "D5-intl": ["t.me", "telegram", "discord", "whatsapp"],
}

counts = {k: 0 for k in DOMAINS}
n = 0
for line in open(os.path.join(RUNS, "leads.jsonl"), encoding="utf-8"):
    try:
        r = json.loads(line)
    except Exception:
        continue
    n += 1
    src = (r.get("src") or "") + " " + " ".join(r.get("sources") or [])
    kind = r.get("kind") or ""
    matched = False
    for dom, keys in DOMAINS.items():
        if any(k in src.lower() for k in keys):
            counts[dom] += 1
            matched = True
    if kind in ("qq_num", "qq_link"):
        counts["D4-qq"] += 1
        matched = True
    if kind in ("tg_link", "fb_group", "fb_page", "wa_link", "dc_link", "ig_page") or "intl" in src.lower():
        counts["D5-intl"] += 1
        matched = True
    if not matched:
        counts.setdefault("other", 0)
        counts["other"] = counts.get("other", 0) + 1

print(f"AC5 域覆盖（leads 总数 {n}）:")
for dom, c in counts.items():
    mark = "✓" if c > 0 else "✗(需书面原因)"
    print(f"  {dom:12s} {c:4d} {mark}")
# 判定：至少 3 域有产出视为 PASS（其余需交付报告书面说明）
productive = sum(1 for k, v in counts.items() if k.startswith("D") and v > 0)
if productive >= 3:
    print(f"AC5 PASS ({productive}/5 域有产出)")
    sys.exit(0)
print(f"AC5 FAIL (仅 {productive}/5 域有产出)")
sys.exit(1)
