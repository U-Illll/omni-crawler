#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""NVIDIA NIM 模型候选探测"""
import json
import sys
import urllib.request

sys.path.insert(0, "/tmp/sm-recon/src")
from sm_llm import load_env  # noqa: E402

env = load_env()
key = env.get("PROVIDER_KEY_NVIDIA")
print("has key:", bool(key))
if not key:
    sys.exit(1)

MODELS = [
    "meta/llama-4-maverick-17b-128e-instruct",
    "meta/llama-4-scout-17b-16e-instruct",
    "deepseek-ai/deepseek-v3.1",
    "qwen/qwen3-235b-a22b",
    "nvidia/llama-3.3-nemotron-super-49b-v1.5",
    "moonshotai/kimi-k2-instruct",
    "zai-org/glm-4.5",
]
for m in MODELS:
    try:
        body = json.dumps({"model": m, "messages": [{"role": "user", "content": "hi"}],
                           "max_tokens": 8}).encode()
        req = urllib.request.Request(
            "https://integrate.api.nvidia.com/v1/chat/completions", data=body,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json",
                     "User-Agent": "Mozilla/5.0"})
        r = urllib.request.urlopen(req, timeout=25)
        d = json.loads(r.read().decode("utf-8", "ignore"))
        c = (d.get("choices") or [{}])[0].get("message", {}).get("content", "")[:40]
        print(f"  ✓ {m} → {c}")
    except Exception as e:  # noqa: BLE001
        print(f"  ✗ {m} → {str(e)[:90]}")
