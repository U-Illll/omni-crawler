#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""提取 PDF 文本 + 扫描群信息"""
import re
import sys

from pypdf import PdfReader

for fn in ["ia_fj_723648c5", "ia_fj_2e65ddcf"]:
    path = f"/tmp/sm-recon/recon/{fn}"
    try:
        r = PdfReader(path)
    except Exception as e:  # noqa: BLE001
        print(f"{fn}: 读取失败 {e}")
        continue
    full = ""
    for p in r.pages:
        try:
            full += (p.extract_text() or "") + "\n"
        except Exception:
            pass
    open(path + ".txt", "w", encoding="utf-8").write(full)
    print(f"=== {fn}: {len(r.pages)} 页, 文本 {len(full)} 字 ===")
    found = False
    for m in re.finditer(r".{0,30}(群|wechat|WeChat|微信|whatsapp|telegram|QQ|扫描|QR|二维码).{0,80}", full, re.I):
        print(" ·", m.group(0).replace("\n", " ")[:150])
        found = True
    if not found:
        print("  （无群相关关键词）")
    # 标题页样本
    print("  首页样本:", full[:200].replace("\n", " ")[:200])
