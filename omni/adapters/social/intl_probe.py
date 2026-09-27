#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""国际平台与飞跃手册侦察（本机通道）"""
import json
import re
import sys
import time
import urllib.parse
import urllib.request

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
HDR = {"User-Agent": UA, "Accept-Language": "zh-CN,zh;q=0.9"}


def get(url, hdr=None):
    h = dict(HDR)
    if hdr:
        h.update(hdr)
    return urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=25).read().decode("utf-8", "ignore")


print("================ GitHub: 飞跃手册 ================")
for q in ["SUSTech 飞跃", "南科大 飞跃手册", "sustech flybook", "南科飞跃"]:
    try:
        d = json.loads(get("https://api.github.com/search/repositories?q=" + urllib.parse.quote(q), {"Accept": "application/vnd.github+json"}))
        print(f"## q={q} total={d.get('total_count')}")
        for it in (d.get("items") or [])[:5]:
            print("   -", it["full_name"], "|", (it.get("description") or "")[:80], "|", it["html_url"])
    except Exception as e:
        print("## ERR", q, repr(e)[:100])
    time.sleep(1.5)

print()
print("================ Google: 国际平台 ================")
for q in ["SUSTech telegram group", "SUSTech whatsapp group international students", "南方科技大学 留学生 telegram OR whatsapp OR discord"]:
    try:
        html = get("https://www.google.com/search?hl=en&num=15&q=" + urllib.parse.quote(q))
        tme = sorted(set(re.findall(r"t\.me/[A-Za-z0-9_+%-]{4,60}", html)))
        wa = sorted(set(re.findall(r"chat\.whatsapp\.com/[A-Za-z0-9]{10,60}", html)))
        dc = sorted(set(re.findall(r"discord\.(?:gg|com/invite)/[A-Za-z0-9]{4,40}", html)))
        print(f"## {q}")
        if tme:
            print("   t.me:", tme[:6])
        if wa:
            print("   whatsapp:", wa[:4])
        if dc:
            print("   discord:", dc[:4])
        if not (tme or wa or dc):
            print("   （无链接命中）")
    except Exception as e:
        print("## ERR", q, repr(e)[:100])
    time.sleep(2.0)
