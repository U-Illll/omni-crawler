#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""国际平台探测 v2：Telegram/Discord 上的 SUSTech 群组"""
import re
import time
import urllib.parse
import urllib.request

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


def get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8"})
    return urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8", "ignore")


def probe_tme(name):
    """探测 t.me/<name> 页面，返回 (title, extra)"""
    try:
        html = get(f"https://t.me/{name}", timeout=15)
        t = re.search(r'<div class="tgme_page_title"[^>]*>\s*<span[^>]*>([^<]*)', html)
        e = re.search(r'<div class="tgme_page_extra">([^<]*)', html)
        d = re.search(r'<div class="tgme_page_description[^"]*">(.*?)</div>', html, re.S)
        title = (t.group(1).strip() if t else "")
        extra = (e.group(1).strip() if e else "")
        desc = re.sub(r"<[^>]+>", " ", d.group(1)).strip()[:150] if d else ""
        return title, extra, desc
    except Exception as e:  # noqa: BLE001
        return None, None, f"ERR {str(e)[:60]}"


print("======== Google/Bing 搜 t.me 链接 ========")
queries = [
    'site:t.me sustech',
    '"t.me" sustech',
    'SUSTech telegram chat',
    'SUSTech discord',
    '南科大 telegram 群',
]
found = set()
for q in queries:
    try:
        html = get("https://www.google.com/search?num=20&q=" + urllib.parse.quote(q))
        links = re.findall(r"t\.me/(?:joinchat/)?([A-Za-z0-9_+%-]{4,60})", html)
        for l in links:
            found.add(l)
        print(f"## {q} → {len(links)} 命中")
        if links:
            print("   ", sorted(set(links))[:8])
    except Exception as e:  # noqa: BLE001
        print(f"## {q} ERR {str(e)[:80]}")
    time.sleep(1.5)

print()
print("======== t.me 候选探测 ========")
candidates = ["sustech", "sustech_intl", "sustech_international", "SUSTech_Official",
              "sustechunofficial", "joinchat"] + sorted(found)[:10]
seen = set()
for c in candidates:
    if c in seen or len(c) < 4:
        continue
    seen.add(c)
    t, e, d = probe_tme(c)
    if t or (e and "ERR" not in str(e)):
        print(f"  @{c}: title={t} | extra={e} | desc={d[:100]}")
    time.sleep(1.2)
