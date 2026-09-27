#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""诊断：文章抓取失败的实情（保存 HTML 看特征）"""
import sys
import os
import re

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_wechat import WechatChannel  # noqa: E402

ch = WechatChannel(interval=5.0)
arts = ch.search("跨越山海 南科大 国际")
print(f"搜索: {len(arts)} 篇")
target = None
for a in arts:
    print(" -", a["title"][:60])
    if "跨越山海" in a["title"] or "国际" in a["title"]:
        target = target or a

if target:
    print("\n目标:", target["title"][:60])
    mp = ch.resolve(target["href"])
    print("mp:", (mp or "FAIL")[:130])
    if mp:
        art = ch.article(mp)
        h = art["html"]
        print("HTML len:", len(h))
        open("/tmp/sm-recon/recon/qr_article.html", "w", encoding="utf-8").write(h)
        # 特征检测
        for sig in ["js_content", "activity-name", "环境异常", "去验证", "账号已迁移",
                    "appmsg_content", "rich_media", "verify", "weui-msg", "该内容已被发布者删除",
                    "请在微信客户端打开"]:
            if sig in h:
                i = h.find(sig)
                print(f"  [{sig}] @{i}")
        m = re.search(r"<title[^>]*>(.*?)</title>", h, re.S)
        print("  title:", (m.group(1).strip() if m else "NONE")[:80])
