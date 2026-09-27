#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""import_leads — 手工发现线索的批量导入"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_store import Store  # noqa: E402

store = Store()
FB = "https://github.com/SUSTech-Application/SUSTechapplication"

items = [
    ("qq_num", "894135698", "南科大北美申请小分队 [QQ群群号]: 894135698（飞跃手册 index.md）", FB, 0.85, 0.88),
    ("qq_num", "391867782", "南科大欧洲申请群[QQ群群号]: 391867782（飞跃手册 index.md）", FB, 0.85, 0.88),
    ("wx_hint", "Nutcracker2020", "坚果钳留学信息分享平台[微信公众号]: Nutcracker2020（飞跃手册 index.md）", FB, 0.7, 0.55),
]
n = 0
for kind, val, ev, src, rel, conf in items:
    if store.add_lead(kind, val, ev, src, rel, conf):
        n += 1
store.save()
print(f"导入 {n} 条新线索（现有 {len(store.leads)} 条）")
