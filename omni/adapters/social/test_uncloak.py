#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""uncloak 单元测试：360 链接解包"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_fetch import Fetcher, RateLimiter  # noqa: E402
from sm_search import search_360, uncloak  # noqa: E402

f = Fetcher(limiter=RateLimiter(base=1.0, jitter=0.5))
r = search_360(f, "南方科技大学 留学生 新闻")
print("搜索结果:", len(r["results"]))
for item in r["results"][:3]:
    u = item["raw"]
    real = uncloak(f, u)
    print("  标题:", item["title"][:60])
    print("  原始:", u[:70])
    print("  解包:", real[:110])
    print()
