#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""process_qr_leads — 将已入库的 qr_content 中的 QQ 群链接解析成群号"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_store import Store  # noqa: E402
import sm_qq  # noqa: E402

store = Store()
qr = [r for r in store.leads.values() if r["kind"] == "qr_content"]
qq_links = [r for r in qr if ("qm.qq.com" in r["value"] or "jq.qq.com" in r["value"])]
print(f"qr_content 总数: {len(qr)} | 含 QQ 群链接: {len(qq_links)}")
n = 0
for r in qq_links:
    res = sm_qq.resolve_any(r["value"])
    if res and res.get("group_uin"):
        src = r.get("src") or ""
        if store.add_lead("qq_num", res["group_uin"],
                          f"QR→{r['value'][:130]}", src, 0.8, 0.9):
            n += 1
            print(f"  新群号: {res['group_uin']} (k={res.get('k')})")
store.save()
print(f"新增 {n} 个群号（store 现有 {len(store.leads)} 条）")
