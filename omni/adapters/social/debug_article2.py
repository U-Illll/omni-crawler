#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""调试2：抓不同文章验证正文可达性"""
import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_wechat import WechatChannel  # noqa: E402

ch = WechatChannel(interval=4.0)
arts = ch.search("南方科技大学 留学生")
print(f"搜索: {len(arts)} 篇")
for i, a in enumerate(arts[:6]):
    print(f"  [{i}] {a['title'][:60]} | 号: {a['account'][:20]}")

# 逐篇试（最多 3 篇）
for i in range(min(3, len(arts))):
    a = arts[i]
    try:
        mp = ch.resolve(a["href"])
        if not mp:
            print(f"[{i}] 解链失败")
            continue
        art = ch.article(mp)
        h = art["html"]
        # 状态检测
        status = "ok"
        for sig, st in [("账号已迁移", "migrated"), ("环境异常", "verify"),
                        ("去验证", "verify"), ("该内容已被发布者删除", "deleted"),
                        ("js_content", "ok")]:
            if sig in h:
                status = st
                break
        t = art["title"]
        print(f"[{i}] status={status} title={t[:50]} textlen={len(art['text'])}")
        if status == "ok" and art["text"]:
            nums = re.findall(r"群[号]?[^\d]{0,8}(\d{5,11})", art["text"])
            print(f"      群号: {nums[:8]}")
    except Exception as e:  # noqa: BLE001
        print(f"[{i}] ERR {repr(e)[:100]}")
