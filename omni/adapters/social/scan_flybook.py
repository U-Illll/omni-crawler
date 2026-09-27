#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""scan_flybook — 对飞跃手册全仓库跑提取器"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_extract import extract_groups, relevance, confidence  # noqa: E402
from sm_store import Store  # noqa: E402

store = Store()
root = "/tmp/sm-recon/recon/flybook"
files_scanned = 0
new_leads = 0
for dirpath, dirs, files in os.walk(root):
    if ".git" in dirpath:
        continue
    for fn in files:
        if not fn.endswith((".md", ".ts", ".json", ".txt", ".yaml", ".yml")):
            continue
        path = os.path.join(dirpath, fn)
        try:
            text = open(path, encoding="utf-8", errors="ignore").read()
        except Exception:  # noqa: BLE001
            continue
        rel = relevance(text)
        src = "flybook:" + os.path.relpath(path, root)
        for ld in extract_groups(text, src):
            if store.add_lead(ld["kind"], ld["value"], ld["evidence"], src, rel,
                              confidence(ld["kind"], ld["evidence"], rel)):
                new_leads += 1
        files_scanned += 1
store.save()
print(f"扫描 {files_scanned} 个文件，新增 {new_leads} 条线索（总 {len(store.leads)}）")
