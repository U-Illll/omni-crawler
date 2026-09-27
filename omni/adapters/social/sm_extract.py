#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sm_extract — 群号/群链接提取 + 相关性评分（v0.1）

提取对象：
- QQ 群号：上下文词（群/QQ/加群）附近的 5-10 位数字
- QQ 加群链接：jq.qq.com / qm.qq.com 的 k= 参数
- Telegram：t.me 链接
- 微信群/二维码：二维码图片 URL / 微信联系方式（弱信号）
"""
import re
import html as html_mod

TAG_RE = re.compile(r"<[^>]+>")
SCRIPT_RE = re.compile(r"<(script|style|noscript)[^>]*>.*?</\1\s*>", re.S | re.I)

# 上下文 + 数字（QQ 群号候选）
QQ_NUM_PATTERNS = [
    re.compile(r"群\s*[号码]?\s*[:：]?\s*(\d{5,10})\b"),
    re.compile(r"[QqＱｑ]\s*[Qq]\s*群\s*[:：]?\s*(\d{5,10})\b"),
    re.compile(r"加\s*群\s*[:：]?\s*(\d{5,10})\b"),
    re.compile(r"(\d{5,10})\s*[（(]?\s*[加入进].{0,4}群"),
    re.compile(r"群\s*[:：]\s*(\d{5,10})\b"),
    re.compile(r"搜索\s*[:：]?\s*(\d{5,10})\s*[加进]"),
]
QQ_LINK_PATTERNS = [
    re.compile(r"(jq\.qq\.com/\?[^\"'\s<>]*?k=[A-Za-z0-9_-]+)"),
    re.compile(r"(qm\.qq\.com/cgi-bin/qm/qr\?[^\"'\s<>]*?k=[A-Za-z0-9_-]+)"),
    re.compile(r"(qun\.qq\.com/[^\"'\s<>]{0,120})"),
]
TG_PATTERNS = [
    re.compile(r"(?:https?://)?t\.me/(?:joinchat/)?([A-Za-z0-9_+%-]{4,60})"),
]
# 微信号：要求明确"微信号："或"添加微信"语境（收紧版 2026-09-23）
WX_HINTS = [
    re.compile(r"微信号?\s*[:：]\s*([A-Za-z][A-Za-z0-9_-]{4,29})"),
    re.compile(r"(?:添加|加)\s*(?:微信|V信|vx|VX)\s*[号]?\s*[:：]?\s*([A-Za-z][A-Za-z0-9_-]{4,29})"),
    re.compile(r"(?:微信|vx|VX)\s*[:：]\s*([A-Za-z][A-Za-z0-9_-]{4,29})"),
]

# 相关性关键词（南科大 + 留学生）
KW_SUBJECT = ["南方科技大学", "南科大", "SUSTech", "sustech", "SUSTC"]
KW_TARGET = [
    "留学生", "国际学生", "国际生", "外国学生", "海外学生", "国际友人",
    "exchange student", "international student", "留学", "交换生", "学伴",
    "新生", "招生", "入学", "录取",
]


def strip_html(text, limit=200000):
    t = text[:limit]
    t = SCRIPT_RE.sub(" ", t)   # 先剥离 script/style/noscript 整块（防 JS 噪音进提取）
    t = TAG_RE.sub(" ", t)
    return html_mod.unescape(t)


def extract_groups(text, url, ctx_window=60):
    """从页面文本提取群线索。返回 list of dict。"""
    out = []
    seen = set()

    def add_ctx(kind, value, pos, page_text):
        key = (kind, value)
        if key in seen:
            return
        seen.add(key)
        s = max(0, pos - ctx_window)
        e = min(len(page_text), pos + len(value) + ctx_window)
        ev = re.sub(r"\s+", " ", page_text[s:e]).strip()
        out.append({"kind": kind, "value": value, "evidence": ev, "src": url})

    plain = strip_html(text)

    for pat in QQ_NUM_PATTERNS:
        for m in pat.finditer(plain):
            val = m.group(1)
            tail = plain[m.start(1):m.start(1) + 24]
            if re.search(r"人\s*(聚集|加入|关注|收藏|浏览|在读|参与)", tail):
                continue  # 豆瓣/社区成员数等误报（如"N 人聚集在这个小组"）
            add_ctx("qq_num", val, m.start(1), plain)

    for pat in QQ_LINK_PATTERNS:
        for m in pat.finditer(text):
            add_ctx("qq_link", m.group(1), m.start(1), text)

    for pat in TG_PATTERNS:
        for m in pat.finditer(text):
            add_ctx("tg", m.group(1), m.start(1), text)

    for pat in WX_HINTS:
        for m in pat.finditer(plain):
            val = m.group(1)
            if val.lower() in ("id", "号", "群", "二维码"):
                continue
            add_ctx("wx_hint", val, m.start(1), plain)

    return out


def relevance(text):
    """0-1 相关性（南科大 × 留学生 语境）"""
    t = strip_html(text[:50000])
    has_subject = any(k in t for k in KW_SUBJECT)
    target_hits = sum(1 for k in KW_TARGET if k in t)
    score = 0.0
    if has_subject:
        score += 0.45
    score += min(0.4, target_hits * 0.08)
    if "群" in t:
        score += 0.15
    return round(min(1.0, score), 2)


def confidence(kind, evidence, rel):
    """单条群线索置信度（启发式）"""
    c = rel * 0.7
    ev = evidence or ""
    if kind == "qq_link":
        c += 0.25
    if kind == "tg":
        c += 0.2
    if re.search(r"(群号|群\s*[:：]|Q\s*群|加群)", ev):
        c += 0.15
    if kind == "wx_hint":
        c -= 0.1
    return round(max(0.05, min(1.0, c)), 2)
