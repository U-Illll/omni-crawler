#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""搜狗微信跳转解析测试：/link?url= → mp.weixin.qq.com"""
import re
import sys
import os
import urllib.parse
import http.cookiejar
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_fetch import Fetcher, RateLimiter  # noqa: E402

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
f = Fetcher(limiter=RateLimiter(base=3.0, jitter=1.5))

# 1. 文章搜索
q = "南方科技大学 留学生"
url = "https://weixin.sogou.com/weixin?" + urllib.parse.urlencode({"type": "2", "query": q, "ie": "utf8"})
status, text, err = f.get(url, referer="https://weixin.sogou.com/")
print("搜索页:", status, "err:", err, "len:", len(text) if text else 0)

links = re.findall(r'<h3>\s*<a[^>]*href="(/link\?url=[^"]+)"[^>]*>(.*?)</a>', text, re.S)
print("文章链接数:", len(links))
for href, title in links[:3]:
    t = re.sub("<[^>]+>", "", title).strip()
    print("  -", t[:60], "|", href[:80])

if links:
    # 2. 解包第一篇
    href = re.sub(r"\s+", "", links[0][0]).replace("&amp;", "&")
    full = "https://weixin.sogou.com" + href
    # 带 cookie 的会话
    cj = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    op.addheaders = [("User-Agent", UA), ("Referer", "https://weixin.sogou.com/")]
    try:
        r = op.open(full, timeout=20)
        body = r.read(3000).decode("utf-8", "ignore")
        print("跳转页:", r.getcode(), "final:", r.geturl()[:120])
        print("body 头 300:", body[:300].replace("\n", " "))
    except Exception as e:
        print("跳转 ERR:", repr(e)[:150])
