#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""列出 NVIDIA NIM 可用模型"""
import json
import sys
import urllib.request

sys.path.insert(0, "/tmp/sm-recon/src")
from sm_llm import load_env  # noqa: E402

key = load_env().get("PROVIDER_KEY_NVIDIA")
req = urllib.request.Request("https://integrate.api.nvidia.com/v1/models",
                             headers={"Authorization": f"Bearer {key}", "User-Agent": "Mozilla/5.0"})
r = urllib.request.urlopen(req, timeout=30)
d = json.loads(r.read().decode("utf-8", "ignore"))
ids = [m.get("id") for m in (d.get("data") or [])]
print(f"共 {len(ids)} 个模型")
# 挑聊天类（含 instruct/chat/deepseek/qwen/llama/kimi/glm/mistral）
kw = ["instruct", "chat", "deepseek", "qwen", "llama", "kimi", "glm", "mistral", "nemotron"]
sel = [i for i in ids if any(k in (i or "").lower() for k in kw)]
for i in sel[:40]:
    print(" ", i)
