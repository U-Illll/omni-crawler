#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sm_bili — B 站搜索通道（视频标题+描述提取群线索）

用法: python3 sm_bili.py "查询1" "查询2" ...
"""
import json
import os
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_store import Store  # noqa: E402
from sm_extract import extract_groups, relevance  # noqa: E402
from sm_common import log, audit, ua  # noqa: E402

API = "https://api.bilibili.com/x/web-interface/search/type?search_type=video&keyword={kw}&page={pg}"


def strip_em(s):
    return (s or "").replace('<em class="keyword">', "").replace("</em>", "")


def search_bili(query, page=1, timeout=20):
    url = API.format(kw=urllib.parse.quote(query), pg=page)
    req = urllib.request.Request(url, headers={
        "User-Agent": ua(),
        "Referer": "https://www.bilibili.com/",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        d = json.loads(r.read().decode("utf-8", "ignore"))
    if d.get("code") != 0:
        raise RuntimeError(f"bili api code={d.get('code')} msg={d.get('message')}")
    res = (d.get("data") or {}).get("result") or []
    out = []
    for v in res:
        bvid = v.get("bvid") or ""
        out.append({
            "title": strip_em(v.get("title")),
            "desc": strip_em(v.get("description")),
            "author": v.get("author") or "",
            "url": f"https://www.bilibili.com/video/{bvid}" if bvid else "",
        })
    return out


def main():
    queries = sys.argv[1:]
    if not queries:
        queries = ["南科大 留学生", "南方科技大学 国际学生", "SUSTech 留学生"]
    store = Store()
    n_new = 0
    for q in queries:
        try:
            vids = search_bili(q)
        except Exception as e:  # noqa: BLE001
            log(f"[bili] {q} 失败: {str(e)[:80]}")
            audit({"kind": "bili_err", "q": q, "err": str(e)[:120]})
            time.sleep(3)
            continue
        hits = 0
        for v in vids:
            txt = f"{v['title']} {v['desc']} {v['author']}"
            if not v["url"]:
                continue
            rel = relevance(txt)
            for l in extract_groups(txt, v["url"]):
                ev = f"[bili:{q[:40]}] {v['title'][:60]} | {v['desc'][:80]}"
                conf = l.get("conf") or 0.5
                if store.add_lead(l["kind"], l["value"], ev, v["url"], max(rel, 0.4), conf):
                    n_new += 1
                    hits += 1
                    print(f"  +{l['kind']}: {l['value'][:60]} ← {v['title'][:50]}")
        log(f"[bili] {q} → {len(vids)} 视频（新增 {hits}）")
        time.sleep(6)
    store.save()
    print(f"B站通道完成：新增 {n_new}（store 共 {len(store.leads)}）")


if __name__ == "__main__":
    main()
