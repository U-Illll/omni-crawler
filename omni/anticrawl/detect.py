#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.anticrawl.detect — 反爬拦截特征检测。

从 sm-recon（BLOCK_SIGNS/SOFT_LIMIT_SIGNS）扩展：
- blocked   验证页 / 人机校验（硬风控信号 → 冷却 + 熔断计数）
- challenge WAF/CF/DataDome 挑战（升级链触发信号）
- soft_limit 软限流提示（降速信号）
- empty_shell 空壳页（SPA 渲染需求信号 → 升级执行体）
"""
import re

BLOCK_SIGNS = [
    "百度安全验证", "wappass", "验证码", "请输入验证码", "人机验证",
    "滑动验证", "安全检测", "访问过于频繁", "请稍后再试", "出错了",
    "security check", "verify you are human", "unusual traffic",
]

CHALLENGE_SIGNS = [
    "challenge-platform", "cf-challenge", "turnstile", "geo.captcha-delivery.com",  # DataDome
    "window._cf_chl", "datadome", "checking your browser", "just a moment",
    # "waf" 已移除（2026-09-27）：纯子串误报——Notion 页 Next.js chunk 文件名
    # "…qwaf.css" 曾触发假阳性（正常定价页被判 challenge）。真 WAF 挑战页由
    # "just a moment" / "checking your browser" 等更强信号覆盖。
    "captcha",
]

SOFT_LIMIT_SIGNS = ["访问过于频繁", "请稍后再试", "出错了", "too many requests", "rate limit"]


def detect_content(text):
    """扫描响应文本。返回 None | blocked | challenge | soft_limit。"""
    if not text:
        return None
    head = text[:8000]
    low = head.lower()
    if any(s in head for s in BLOCK_SIGNS if _is_cjk(s)) or \
       any(s in low for s in BLOCK_SIGNS if not _is_cjk(s)):
        return "blocked"
    if any(s in low for s in CHALLENGE_SIGNS):
        return "challenge"
    if any(s in head for s in SOFT_LIMIT_SIGNS):
        return "soft_limit"
    return None


def like_empty_shell(text, min_visible=200):
    """粗略判定"空壳页"（可见文本过少）——SPA 需要渲染的信号。
    HTML 去标签后可见字符 < min_visible ⇒ empty_shell。"""
    if not text:
        return True
    body = re.sub(r"<script[\s\S]*?</script>", " ", text, flags=re.I)
    body = re.sub(r"<style[\s\S]*?</style>", " ", body, flags=re.I)
    visible = re.sub(r"<[^>]+>", " ", body)
    visible = re.sub(r"\s+", "", visible)
    return len(visible) < min_visible


def _is_cjk(s):
    return any("\u4e00" <= ch <= "\u9fff" for ch in s)
