#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""搜狗微信跳转解析测试 v2：cookie 预热"""
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

# 预热 1：首页
r1 = op.open("https://weixin.sogou.com/", timeout=20)
print("预热首页:", r1.getcode(), "cookies:", [c.name for c in cj])

# 预热 2：搜索
q = "南方科技大学 留学生"
url = "https://weixin.sogou.com/weixin?" + urllib.parse.urlencode({"type": "2", "query": q, "ie": "utf8"})
r2 = op.open(urllib.request.Request(url, headers={"Referer": "https://weixin.sogou.com/"}), timeout=20)
text = r2.read().decode("utf-8", "ignore")
print("搜索页:", r2.getcode(), "len:", len(text))

links = re.findall(r'<h3>\s*<a[^>]*href="(/link\?url=[^"]+)"[^>]*>(.*?)</a>', text, re.S)
print("文章数:", len(links))
if not links:
    # 尝试其他结构
    links = re.findall(r'href="(/link\?url=[^"]+)"[^>]*>', text)
    print("宽松匹配:", len(links))

if links:
    href = re.sub(r"\s+", "", links[0][0] if isinstance(links[0], tuple) else links[0]).replace("&amp;", "&")
    full = "https://weixin.sogou.com" + href
    try:
        r3 = op.open(urllib.request.Request(full, headers={"Referer": url}), timeout=20)
        body = r3.read(4000).decode("utf-8", "ignore")
        print("跳转:", r3.getcode(), "final:", r3.geturl()[:150])
        print("body:", body[:400].replace("\n", " "))
        print()
        print("cookies 现在:", [(c.name, c.value[:12]) for c in cj])
    except Exception as e:
        print("跳转 ERR:", repr(e)[:200])
