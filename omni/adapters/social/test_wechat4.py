#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""诊断：mp 文章抓取返回的真实内容"""
import re
import sys
import os
import urllib.parse
import http.cookiejar
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
op.addheaders = [("User-Agent", UA), ("Accept-Language", "zh-CN,zh;q=0.9")]
op.open("https://weixin.sogou.com/", timeout=20)

q = "南方科技大学 留学生 群"
url = "https://weixin.sogou.com/weixin?" + urllib.parse.urlencode({"type": "2", "query": q, "ie": "utf8"})
text = op.open(urllib.request.Request(url, headers={"Referer": "https://weixin.sogou.com/"}), timeout=20).read().decode("utf-8", "ignore")
links = re.findall(r'<h3>\s*<a[^>]*href="(/link\?url=[^"]+)"[^>]*>(.*?)</a>', text, re.S)
href = re.sub(r"\s+", "", links[0][0]).replace("&amp;", "&")
body = op.open(urllib.request.Request("https://weixin.sogou.com" + href, headers={"Referer": url}), timeout=20).read().decode("utf-8", "ignore")
frags = re.findall(r"url\s*\+=\s*'([^']*)'", body)
mp_url = "".join(frags)
print("mp_url:", mp_url)
open("/tmp/sm-recon/mp_url.txt", "w").write(mp_url)

# 姿势1：默认（带 cookiejar）
art = op.open(urllib.request.Request(mp_url, headers={"Referer": "https://weixin.sogou.com/"}), timeout=25).read().decode("utf-8", "ignore")
print("== 姿势1 body len:", len(art))
print(art[:600].replace("\n", " "))
print("...")
# 找验证/环境异常标记
for sig in ["环境异常", "去验证", "完成验证", "js_content", "activity-name", "seccheck", "wappoc_appmsg"]:
    if sig in art:
        print("  含标记:", sig)
