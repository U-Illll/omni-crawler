#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""check_ac4 — 数据一致性检查（幂等/完整性）"""
import json
import os
import sys

BASE = os.environ.get("SM_BASE", "/tmp/sm-recon")
RUNS = os.path.join(BASE, "runs")

fail = []
n_leads = 0
seen = set()
for line in open(os.path.join(RUNS, "leads.jsonl"), encoding="utf-8"):
    try:
        r = json.loads(line)
    except Exception:
        fail.append("bad json line")
        continue
    n_leads += 1
    key = (r.get("kind"), r.get("value"))
    if key in seen:
        fail.append(f"duplicate lead {key}")
    seen.add(key)
    if not all(r.get(k) for k in ("kind", "value", "src")):
        fail.append(f"missing fields: {r.get('value')}")
    # URL 型线索（QR 解码/名片/国际平台链接）的证据形态是链接或图片，豁免 value∈evidence；
    # QR→ 开头的 qq_num 是解析产物，同样豁免
    URL_KINDS = {"qr_content", "wx_contact", "tg_link", "fb_group", "fb_page",
                 "wa_link", "dc_link", "ig_page", "qq_link"}
    ev = r.get("evidence") or ""
    if (r.get("kind") not in URL_KINDS and "QR→" not in ev
            and str(r.get("value")) not in ev):
        fail.append(f"value not in evidence: {r.get('value')}")

n_pages = 0
for line in open(os.path.join(RUNS, "pages.jsonl"), encoding="utf-8"):
    if line.strip():
        n_pages += 1

# 幂等重载验证：Store 重载后计数一致
sys.path.insert(0, os.path.join(BASE, "src"))
from sm_store import Store  # noqa: E402

s = Store()
summ = s.summary()
if summ["pages"] != n_pages:
    fail.append(f"pages count mismatch: store={summ['pages']} file={n_pages}")
if summ["leads"] != n_leads:
    fail.append(f"leads count mismatch: store={summ['leads']} file={n_leads}")

if fail:
    print(f"AC4 FAIL ({len(fail)} issues)")
    for f in fail[:15]:
        print("  -", f)
    sys.exit(1)
print(f"AC4 PASS (leads={n_leads}, pages={n_pages}, 幂等一致)")
