#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""云端测试：微信文章二维码扫描链路"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_wechat import WechatChannel  # noqa: E402
import sm_qr  # noqa: E402

ch = WechatChannel(interval=4.0)
arts = ch.search("南方科技大学 国际学生")
print(f"搜索: {len(arts)} 篇")
hit = 0
for a in arts[:4]:
    try:
        mp = ch.resolve(a["href"])
        if not mp:
            print(" - 解链失败:", a["title"][:40])
            continue
        art = ch.article(mp)
        if "js_content" not in art["html"]:
            print(" - 非正文页:", a["title"][:40])
            continue
        res = sm_qr.scan_html_for_qr(art["html"], max_images=15)
        print(f" - {a['title'][:44]} → QR {len(res)}")
        for u, p, codes in res:
            hit += 1
            print("     ", codes[:3])
    except Exception as e:  # noqa: BLE001
        print(" - ERR:", type(e).__name__, str(e)[:80])
print("二维码命中:", hit)
