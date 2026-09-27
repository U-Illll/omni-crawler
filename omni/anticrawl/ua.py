#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.anticrawl.ua — 浏览器 UA 池（全框架统一）。

教训（2026-09-27 真实网络冒烟）：漏设 UA 时 requests 默认 `python-requests/x`
被 WAF 57ms 秒拒 403——HTTP 出口必须默认注入浏览器 UA。
"""
import random

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
]


def ua():
    return random.choice(UA_POOL)
