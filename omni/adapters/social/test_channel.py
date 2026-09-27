#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""测试单通道 LLM 连通（参数：通道名）"""
import sys
import time

sys.path.insert(0, "/tmp/sm-recon/src")
from sm_llm import default_channels, LLMClient  # noqa: E402

target = sys.argv[1] if len(sys.argv) > 1 else None
chans = default_channels()
print("可用通道:")
for c in chans:
    print(f"  - {c['name']:16s} model={c['model']}")

if target:
    sel = [c for c in chans if c["name"] == target]
    if not sel:
        print(f"通道 {target} 不在列表（可能缺 key）")
        sys.exit(1)
    client = LLMClient(channels=sel)
    t0 = time.time()
    try:
        text, name = client.chat([{"role": "user", "content": "回复OK两个字"}], max_tokens=100)
        print(f"[{name}] 用时{time.time()-t0:.1f}s: {text[:100]}")
    except Exception as e:  # noqa: BLE001
        print(f"[{target}] 失败({time.time()-t0:.1f}s): {str(e)[:150]}")
