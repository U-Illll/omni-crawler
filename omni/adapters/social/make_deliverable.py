#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""make_deliverable — 生成留学生社媒群号交付清单（markdown + csv）

用法: python3 make_deliverable.py
输入: runs/leads.jsonl + runs/classified.jsonl
输出: runs/交付清单-留学生社媒群号.md + runs/leads-full.csv
"""
import csv
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_store import Store  # noqa: E402
from sm_common import ts_iso  # noqa: E402

store = Store()
RUNS = store.dir

# 分类映射
cls = {}
cls_path = os.path.join(RUNS, "classified.jsonl")
if os.path.exists(cls_path):
    for line in open(cls_path, encoding="utf-8"):
        try:
            rec = json.loads(line)
            cls[rec["value"]] = rec
        except Exception:
            pass

# 文章标题映射（补充来源可读性）
titles = {}
for line in open(os.path.join(RUNS, "pages.jsonl"), encoding="utf-8"):
    try:
        rec = json.loads(line)
        if rec.get("url") and rec.get("title"):
            titles[rec["url"]] = rec["title"]
    except Exception:
        pass


def src_desc(src):
    if not src:
        return ""
    t = titles.get(src, "")
    if t:
        return f"{t[:60]}（{src[:60]}）"
    return src[:90]


def cat_of(rec):
    if not rec:
        return "未分类"
    v = rec.get("verdict") or {}
    if isinstance(v, dict):
        return v.get("category") or "未分类"
    return "未分类"


def intl_of(rec):
    if not rec:
        return None
    v = rec.get("verdict") or {}
    if isinstance(v, dict):
        return v.get("intl")
    return None


def note_of(rec):
    if not rec:
        return ""
    v = rec.get("verdict") or {}
    if isinstance(v, dict):
        return v.get("note") or ""
    return ""


leads = []
_m = os.path.join(RUNS, "merged-leads.jsonl")
_src = _m if os.path.exists(_m) else os.path.join(RUNS, "leads.jsonl")
for line in open(_src, encoding="utf-8"):
    try:
        leads.append(json.loads(line))
    except Exception:
        pass
qq = [r for r in leads if r["kind"] == "qq_num"]
contacts = [r for r in leads if r["kind"] == "wx_contact"]
intl_links = [r for r in leads if r["kind"] in ("tg_link", "fb_group", "fb_page", "wa_link", "dc_link", "ig_page", "qr_content")]
hints = [r for r in leads if r["kind"] == "wx_hint"]
others = [r for r in leads if r["kind"] not in ("qq_num", "wx_contact", "wx_hint") and r not in intl_links]

# 分组
core, related, other_qq = [], [], []
for r in qq:
    rec = cls.get(str(r["value"]))
    if intl_of(rec) is True:
        core.append((r, rec))
    elif cat_of(rec) in ("新生", "交换", "国际", "留学申请"):
        related.append((r, rec))
    else:
        other_qq.append((r, rec))

lines = []
lines.append("# 南方科技大学 · 留学生相关社媒群组清单")
lines.append("")
lines.append(f"> 生成时间：{ts_iso()} · 工程：sm-recon（云端 YOUR_SERVER_IP + 本机双端采集）")
lines.append(f"> 统计：QQ 群 {len(qq)} · 微信联系人 {len(contacts)} · 微信线索 {len(hints)} · 国际平台 {len(intl_links)} · 其他 {len(others)}")
lines.append("")

lines.append(f"## A. 留学生/国际相关群（LLM 判定 intl=true）——{len(core)} 个")
lines.append("")
lines.append("| 群号 | 类别 | 说明 | 来源 | 置信 |")
lines.append("|---|---|---|---|---|")
for r, rec in sorted(core, key=lambda x: -(x[0].get("conf") or 0)):
    lines.append(f"| **{r['value']}** | {cat_of(rec)} | {note_of(rec)[:80]} | {src_desc(r.get('src'))[:70]} | {r.get('conf')} |")
lines.append("")

lines.append(f"## B. 相关群（新生/交换/国际项目）——{len(related)} 个")
lines.append("")
lines.append("| 群号 | 类别 | 说明 | 来源 | 置信 |")
lines.append("|---|---|---|---|---|")
for r, rec in sorted(related, key=lambda x: -(x[0].get("conf") or 0)):
    lines.append(f"| {r['value']} | {cat_of(rec)} | {note_of(rec)[:70]} | {src_desc(r.get('src'))[:60]} | {r.get('conf')} |")
lines.append("")

lines.append(f"## C. 其他南科大群（{len(other_qq)} 个）")
lines.append("")
lines.append("| 群号 | 类别 | 说明 |")
lines.append("|---|---|---|")
for r, rec in sorted(other_qq, key=lambda x: -(x[0].get("conf") or 0))[:80]:
    lines.append(f"| {r['value']} | {cat_of(rec)} | {note_of(rec)[:60]} |")
lines.append("")

if contacts:
    lines.append(f"## D. 微信联系人（文章名片）——{len(contacts)} 个")
    lines.append("")
    lines.append("| 链接 | 来源文章 |")
    lines.append("|---|---|")
    for r in contacts[:40]:
        lines.append(f"| {r['value'][:90]} | {src_desc(r.get('src'))[:60]} |")
    lines.append("")

if intl_links:
    lines.append(f"## E. 国际平台链接——{len(intl_links)} 个")
    lines.append("")
    lines.append("| 类型 | 链接 | 说明 |")
    lines.append("|---|---|---|")
    for r in intl_links[:60]:
        lines.append(f"| {r['kind']} | {r['value'][:110]} | {(r.get('evidence') or '')[:60]} |")
    lines.append("")

md = "\n".join(lines)
md_path = os.path.join(RUNS, "交付清单-留学生社媒群号.md")
open(md_path, "w", encoding="utf-8").write(md)
print(f"清单已生成: {md_path} ({len(md)} 字符)")

# CSV 全量
csv_path = os.path.join(RUNS, "leads-full.csv")
with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["kind", "value", "conf", "evidence", "src", "llm_category", "llm_intl", "llm_note"])
    for r in leads:
        rec = cls.get(str(r["value"]))
        w.writerow([r["kind"], r["value"], r.get("conf", ""), (r.get("evidence") or "")[:300],
                    (r.get("src") or "")[:200], cat_of(rec), intl_of(rec), note_of(rec)[:200]])
print(f"CSV 已生成: {csv_path}")
