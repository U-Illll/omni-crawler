#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""upgrade_contacts — 把 qr_content 中的微信名片链接升级为 wx_contact 线索"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_store import Store  # noqa: E402

store = Store()
n = 0
for r in list(store.leads.values()):
    if r["kind"] != "qr_content":
        continue
    v = r.get("value") or ""
    if "weixin.qq.com/r/" in v or "u.wechat.com/" in v:
        if store.add_lead("wx_contact", v[:200], r.get("evidence", ""), r.get("src", ""), 0.5, 0.6):
            n += 1
            print(f"  +wx_contact: {v[:90]}")
store.save()
print(f"新增 {n} 条名片线索（store 共 {len(store.leads)}）")
