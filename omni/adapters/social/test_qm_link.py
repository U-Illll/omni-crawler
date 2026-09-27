#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""验证 qm.qq.com 加群链接：浏览器打开 → 群名/群号"""
import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_browser import fetch_page, close  # noqa: E402

LINKS = [
    "https://qm.qq.com/cgi-bin/qm/qr?k=sDNAAhV8xHVbdOHy1NMNJMDABOwZ1QJp&noverify=0",
    "https://qm.qq.com/cgi-bin/qm/qr?k=q9j3WOsR8v4lsR6xB2FOBV89U6K9F6tm&noverify=0",
]

for url in LINKS:
    html, final, err = fetch_page(url, wait_ms=3500, save_shot=f"/tmp/sm-recon/runs/shot-qm-{hash(url) % 10000}.png")
    print("=" * 60)
    print("url:", url[:100])
    print("final:", (final or "")[:150], "| err:", err)
    if html:
        print("len:", len(html))
        # 提取标题与群信息
        t = re.search(r"<title[^>]*>(.*?)</title>", html, re.S)
        print("title:", (t.group(1).strip() if t else "?")[:80])
        # 常见字段
        for pat, name in [
            (r'"group_name"\s*:\s*"([^"]+)"', "group_name"),
            (r'"groupName"\s*:\s*"([^"]+)"', "groupName"),
            (r'群名称[：:]\s*([^<\n]{2,40})', "群名称"),
            (r'"group_code"\s*:\s*"?(\d+)', "group_code"),
            (r'群号[：:]\s*(\d{5,11})', "群号"),
            (r'"member_num"\s*:\s*(\d+)', "member_num"),
        ]:
            m = re.search(pat, html)
            if m:
                print(f"  [{name}] {m.group(1)[:60]}")
        # 文本掠影
        txt = re.sub(r"<script.*?</script>|<style.*?</style>", " ", html, flags=re.S)
        txt = re.sub(r"<[^>]+>", " ", txt)
        txt = re.sub(r"\s+", " ", txt).strip()
        print("text:", txt[:300])
close()
