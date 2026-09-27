#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sm_llm — LLM 多通道客户端【omni-crawler 适配版】。

迁移说明（2026-09-27）：实现已并入 omni.llm.client（同名降级链：oc-bridge →
opencode-go → agnes → nvidia → deepseek-official → opencode-dsv4）。
本文件保持原对外接口（load_env / LLMClient / LLMUnavailable / classify_lead）。
"""
import json
import os
import re
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(_HERE)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from omni.llm.client import (  # noqa: E402,F401
    LLMClient,
    LLMUnavailable,
    load_env,
    default_channels,
)


def classify_lead(client, value, kind, evidence):
    """判定单条群线索是否为南科大留学生相关（返回 (dict, channel)）。"""
    prompt = (
        "你是信息分类助手。判断以下群线索是否与『南方科技大学』相关，"
        "且与『留学生/国际学生/留学/交换生』相关。输出 JSON："
        '{"sustech": true/false, "intl": true/false, "category": "官方|国别|生活|留学申请|招生|新生|社团|其他", "note": "简短说明"}。\n'
        f"线索类型: {kind}\n线索值: {value}\n证据上下文: {evidence[:300]}"
    )
    text, chan = client.chat([{"role": "user", "content": prompt}],
                             max_tokens=2200, purpose="classify")
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        raise RuntimeError("no json in response")
    return json.loads(m.group(0)), chan
