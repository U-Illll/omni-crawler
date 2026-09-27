#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""测试 NIM 上的具体模型"""
import json
import sys
import time
import urllib.request

sys.path.insert(0, "/tmp/sm-recon/src")
from sm_llm import load_env  # noqa: E402

key = load_env().get("PROVIDER_KEY_NVIDIA")
for m in ["deepseek-ai/deepseek-v4.1-flash", "moonshotai/kimi-k3", "nvidia/llama-3.1-nemotron-70b-instruct"]:
    t0 = time.time()
    try:
        body = json.dumps({"model": m, "messages": [{"role": "user", "content": "回复：好"}],
                           "max_tokens": 200}).encode()
        req = urllib.request.Request(
            "https://integrate.api.nvidia.com/v1/chat/completions", data=body,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json",
                     "User-Agent": "Mozilla/5.0"})
        r = urllib.request.urlopen(req, timeout=40)
        d = json.loads(r.read().decode("utf-8", "ignore"))
        c = (d.get("choices") or [{}])[0].get("message", {}).get("content", "")
        print(f"✓ {m} ({time.time()-t0:.1f}s): {c[:60]}")
    except Exception as e:  # noqa: BLE001
        print(f"✗ {m} ({time.time()-t0:.1f}s): {str(e)[:100]}")
