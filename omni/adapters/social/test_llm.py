#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""LLM 通道链验证：通道列表 + 真实分类任务"""
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sm_llm import LLMClient, classify_lead  # noqa: E402

client = LLMClient()
print("通道链:", [c["name"] for c in client.channels])

# 测试1：简单对话
try:
    text, chan = client.chat([{"role": "user", "content": "回复：通道就绪"}], max_tokens=800, purpose="smoke")
    print(f"[1] 通道={chan} 回复={text[:60]}")
except Exception as e:
    print("[1] ERR", repr(e)[:150])

# 测试2：分类任务（真实线索）
try:
    v, chan = classify_lead(client, "119408121", "qq_num", "欢迎加入南方科技大学留学生新生群，群号：119408121")
    print(f"[2] 通道={chan} 分类={json.dumps(v, ensure_ascii=False)}")
except Exception as e:
    print("[2] ERR", repr(e)[:150])

# 测试3：反例（不相关线索）
try:
    v, chan = classify_lead(client, "984677876", "qq_num", "四川招生组：QQ群：984677876。咨询电话：0755-8801")
    print(f"[3] 通道={chan} 分类={json.dumps(v, ensure_ascii=False)}")
except Exception as e:
    print("[3] ERR", repr(e)[:150])
