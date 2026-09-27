#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""oc-bridge 单通道细查"""
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_llm import LLMClient  # noqa: E402

client = LLMClient()
ch = client.channels[0]
print("目标通道:", ch["name"], ch["url"], "model:", ch["model"])
try:
    t0 = time.time()
    text = client._call(ch, [{"role": "user", "content": "回复：通道就绪"}], 800, 0.2)
    print(f"OK ({time.time()-t0:.1f}s):", (text or "")[:80])
except Exception as e:
    import traceback
    print("FAIL:", type(e).__name__, str(e)[:300])
    traceback.print_exc()
