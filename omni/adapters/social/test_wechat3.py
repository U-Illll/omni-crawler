#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""搜狗微信完整链路测试 v3：搜索→解link→抓mp文章正文"""
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

# 预热
op.open("https://weixin.sogou.com/", timeout=20)

q = "南方科技大学 留学生 群"
url = "https://weixin.sogou.com/weixin?" + urllib.parse.urlencode({"type": "2", "query": q, "ie": "utf8"})
text = op.open(urllib.request.Request(url, headers={"Referer": "https://weixin.sogou.com/"}), timeout=20).read().decode("utf-8", "ignore")

links = re.findall(r'<h3>\s*<a[^>]*href="(/link\?url=[^"]+)"[^>]*>(.*?)</a>', text, re.S)
print("文章数:", len(links))

if links:
    href, title = links[0]
    href = re.sub(r"\s+", "", href).replace("&amp;", "&")
    t_clean = re.sub("<[^>]+>", "", title).strip()
    full = "https://weixin.sogou.com" + href
    body = op.open(urllib.request.Request(full, headers={"Referer": url}), timeout=20).read().decode("utf-8", "ignore")
    # 解析 url += '...' 拼接
    frags = re.findall(r"url\s*\+=\s*'([^']*)'", body)
    mp_url = "".join(frags)
    print("标题:", t_clean[:80])
    print("mp URL:", mp_url[:150])

    if mp_url.startswith("https://mp.weixin.qq.com"):
        # 抓正文
        art = op.open(urllib.request.Request(mp_url, headers={"Referer": "https://weixin.sogou.com/"}), timeout=25).read().decode("utf-8", "ignore")
        mt = re.search(r'<h1[^>]*id="activity-name"[^>]*>(.*?)</h1>', art, re.S)
        print("文章标题:", re.sub(r"\s+", " ", mt.group(1)).strip()[:80] if mt else "?")
        # 正文文本
        mb = re.search(r'<div[^>]*id="js_content"[^>]*>(.*?)</div>\s*<script', art, re.S)
        content = mb.group(1) if mb else ""
        plain = re.sub(r"<[^>]+>", " ", content)
        plain = re.sub(r"\s+", " ", plain)
        print("正文长度:", len(plain))
        print("正文头 300:", plain[:300])
        # 群号扫描
        nums = re.findall(r"群[号]?[^\d]{0,8}(\d{5,11})", plain)
        print("群号命中:", nums[:10])
        qrs = re.findall(r'(?:qun\.qq\.com|jq\.qq\.com|t\.me)[^"\s<>]{0,80}', art)
        print("链接命中:", qrs[:5])
