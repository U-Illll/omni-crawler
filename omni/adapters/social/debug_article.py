#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""调试：保存文章 HTML 并探测结构"""
import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_wechat import WechatChannel  # noqa: E402

ch = WechatChannel()
arts = ch.search("南方科技大学 留学生 群")
mp = ch.resolve(arts[0]["href"])
art = ch.article(mp)
open("/tmp/sm-recon/mp_article.html", "w", encoding="utf-8").write(art["html"])
print("saved", len(art["html"]))

h = art["html"]
# 标题探测
for pat in [r'<h1[^>]*>', r'id="activity-name"', r'og:title', r"msg_title"]:
    ms = re.findall(pat + r".{0,120}", h)
    print(f"== {pat} 命中 {len(ms)}:")
    for m in ms[:3]:
        print("   ", m.replace("\n", " ")[:130])
# js_content 定位
i = h.find('id="js_content"')
print("js_content pos:", i)
if i > 0:
    print("周围:", h[i-100:i+300].replace("\n", " ")[:400])
