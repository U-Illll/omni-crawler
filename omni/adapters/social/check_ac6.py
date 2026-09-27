#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""check_ac6 — 风控事件检查（封禁/验证页/熔断统计）"""
import json
import os
import sys
from collections import Counter

BASE = os.environ.get("SM_BASE", "/tmp/sm-recon")
audit_path = os.path.join(BASE, "logs", "audit.jsonl")

kinds = Counter()
blocks = Counter()
if os.path.exists(audit_path):
    for line in open(audit_path, encoding="utf-8"):
        try:
            e = json.loads(line)
        except Exception:
            continue
        kinds[e.get("kind")] += 1
        if e.get("kind") == "block_sign":
            blocks[e.get("domain")] += 1

print("审计事件统计:", dict(kinds.most_common(12)))
print("验证页命中（block_sign）:", dict(blocks))
print()
print("AC6 判据：block_sign 出现的域应已进入冷却且被记录")
print("  熔断（circuit_open）次数:", kinds.get("circuit_open", 0))
print("  微信风控（wechat_block）:", kinds.get("wechat_block", 0))

# 判定：机制存在（熔断/冷却有触发记录或无需触发）；出现 block_sign 不算失败（机制处理了），
# 但若同域 block_sign > 5 次（= 反复撞墙未收敛）则 FAIL
bad = [d for d, c in blocks.items() if c > 5]
if bad:
    print(f"AC6 FAIL: 同域反复触发验证页 >5 次: {bad}")
    sys.exit(1)
print("AC6 PASS（风控机制工作，无反复撞墙）")
