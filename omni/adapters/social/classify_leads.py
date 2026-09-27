#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""classify_leads — 对 qq_num 线索跑 LLM 分类（增量、可反复运行）"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_store import Store  # noqa: E402
from sm_llm import LLMClient, classify_lead  # noqa: E402

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 15

store = Store()
out_path = os.path.join(store.dir, "classified.jsonl")
done = {}
if os.path.exists(out_path):
    for line in open(out_path, encoding="utf-8"):
        try:
            rec = json.loads(line)
            done[rec["value"]] = rec
        except Exception:
            pass

# url → 标题映射（补上下文用）
titles = {}
pages_path = os.path.join(store.dir, "pages.jsonl")
if os.path.exists(pages_path):
    for line in open(pages_path, encoding="utf-8"):
        try:
            rec = json.loads(line)
            if rec.get("url") and rec.get("title"):
                titles[rec["url"]] = rec["title"]
        except Exception:
            pass

llm = LLMClient()
n = 0
for r in sorted(store.leads.values(), key=lambda x: -(x.get("conf") or 0)):
    if n >= LIMIT:
        break
    if r["kind"] != "qq_num":
        continue
    v = str(r["value"])
    if v in done:
        continue
    # 证据增强：附源文章标题
    ev = r.get("evidence") or ""
    src = r.get("src") or ""
    t = titles.get(src, "")
    if t:
        ev = f"【来源文章：{t[:80]}】{ev}"
    try:
        verdict, chan = classify_lead(llm, v, "qq_num", ev)
        rec = {"value": v, "verdict": verdict, "channel": chan, "ts": int(time.time()),
               "src": src[:200]}
        with open(out_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        done[v] = rec
        n += 1
        print(f"[{n}] {v} → {json.dumps(verdict, ensure_ascii=False)[:130]}")
    except Exception as e:  # noqa: BLE001
        print(f"[ERR] {v}: {str(e)[:100]}")
        break
print(f"本轮分类 {n} 个（累计 {len(done)}）")
