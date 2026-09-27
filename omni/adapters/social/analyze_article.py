#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""分析 JS 渲染版微信文章的正文数据位置"""
import sys
import re

sys.path.insert(0, "/tmp/sm-recon/src")
from sm_wechat import WechatChannel  # noqa: E402
import urllib.request  # noqa: E402

ch = WechatChannel(interval=5.0)
arts = ch.search("跨越山海 南科大 国际")
target = [a for a in arts if "跨越山海" in a["title"] and "南科大" in a["title"]][0]
mp = ch.resolve(target["href"])
print("mp:", mp[:110])

r = ch.op.open(urllib.request.Request(mp, headers={"Referer": "https://weixin.sogou.com/"}), timeout=30)
h = r.read(3 * 1024 * 1024).decode("utf-8", "ignore")
print("full len:", len(h))
open("/tmp/sm-recon/recon/qr_article_full.html", "w", encoding="utf-8").write(h)

for sig in ["js_content", "activity-name", "跨越山海", "50名", "__INITIAL", "window.__", "appmsg", "群"]:
    idx = h.find(sig)
    print(f"[{sig}] first@{idx}, count={h.count(sig)}")

# 如果"群"在，看看其上下文
i = h.find("群")
if i > 0:
    for m in re.finditer("群", h):
        s = max(0, m.start() - 60)
        ctx = h[s:m.start() + 120].replace("\n", " ")
        print("  群上下文:", ctx[:180])
        break
