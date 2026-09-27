#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sm_intl — 国际平台搜索采集（Brave Search via chromium）

用法: camoufox-venv/bin/python sm_intl.py "query1" "query2" ...
输出: recon/intl_results.jsonl（每行 {query, results:[{title,url}], ts}）
"""
import json
import re
import sys
import time
import urllib.parse

from playwright.sync_api import sync_playwright

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

OUT = "/tmp/sm-recon/recon/intl_results.jsonl"


def collect_brave(ctx, query):
    url = "https://search.brave.com/search?q=" + urllib.parse.quote(query)
    page = ctx.new_page()
    try:
        page.goto(url, timeout=40000, wait_until="domcontentloaded")
        page.wait_for_timeout(2500)
        html = page.content()
        results = []
        # Brave 结果块：#results a[href]
        for m in re.finditer(r'<a[^>]+href="(https?://[^"]+)"[^>]*>(.*?)</a>', html, re.S):
            href, inner = m.group(1), m.group(2)
            if any(d in href for d in ["brave.com", "search.brave", "javascript:"]):
                continue
            t = re.sub(r"<[^>]+>", " ", inner)
            t = re.sub(r"\s+", " ", t).strip()
            if href.startswith("http") and len(t) > 3:
                results.append({"title": t[:150], "url": href[:400]})
        # 去重
        seen = set()
        uniq = []
        for r in results:
            if r["url"] in seen:
                continue
            seen.add(r["url"])
            uniq.append(r)
        return uniq, html
    finally:
        page.close()


def main():
    queries = sys.argv[1:]
    if not queries:
        queries = [
            "SUSTech international students telegram group",
            "SUSTech WhatsApp group international students",
            "南方科技大学 留学生 telegram 群",
            "SUSTech exchange students whatsapp",
        ]
    out = open(OUT, "a", encoding="utf-8")
    with sync_playwright() as pw:
        b = pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = b.new_context(user_agent=UA, locale="en-US")
        for q in queries:
            t0 = time.time()
            try:
                res, html = collect_brave(ctx, q)
                rec = {"query": q, "results": res, "ts": int(time.time())}
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                out.flush()
                # 命中的平台链接
                hits = [r for r in res if re.search(
                    r"t\.me|telegram|whatsapp|discord|instagram|facebook", r["url"], re.I)]
                print(f"[{q[:60]}] {len(res)} 结果（平台命中 {len(hits)}）用时{time.time()-t0:.0f}s")
                for r in hits[:5]:
                    print(f"    {r['title'][:80]} | {r['url'][:100]}")
                fn = f"/tmp/sm-recon/recon/intl_serp_{int(time.time())}.html"
                open(fn, "w", encoding="utf-8").write(html)
            except Exception as e:  # noqa: BLE001
                print(f"[{q[:60]}] FAIL: {type(e).__name__}: {str(e)[:100]}")
            time.sleep(4)
        b.close()
    out.close()
    print("DONE")


if __name__ == "__main__":
    main()
