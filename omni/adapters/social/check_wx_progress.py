#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""微信进度检查"""
import json
import sys

sys.path.insert(0, "/tmp/sm-recon/src")
from sm_store import Store
import sm_sources

s = Store()
wx_done = [q for q in s.progress.get("queries_done", []) if q.startswith("wx:")]
total = len(sm_sources.WECHAT_QUERIES)
print(f"微信查询进度: {len(wx_done)}/{total}")
print("未完成:", [q for q in sm_sources.WECHAT_QUERIES if f"wx:{q}" not in wx_done][:10])
