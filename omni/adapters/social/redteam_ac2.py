#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""redteam_ac2 — LLM 降级测试：坏主通道 → 自动切降级

机制验证：主通道不可用（假端口）→ 链上下一通道接管。
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_llm import LLMClient, default_channels  # noqa: E402

real = default_channels()
bad = {
    "name": "bad-primary",
    "url": "http://127.0.0.1:9/v1/chat/completions",  # 保留端口，必失败
    "key": "x", "model": "x", "headers": {},
}
client = LLMClient(channels=[bad] + real)
t0 = time.time()
try:
    text, chan = client.chat([{"role": "user", "content": "回复四个字：降级成功"}],
                             max_tokens=900, purpose="ac2")
    dt = time.time() - t0
    ok = chan != "bad-primary"
    print(f"AC2 结果: 实际通道={chan} 耗时={dt:.1f}s 回复={text[:40]}")
    print("AC2(LLM 降级):", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)
except Exception as e:  # noqa: BLE001
    print("AC2 FAIL: 全链失败:", repr(e)[:150])
    sys.exit(1)
