#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""social adapter 真实网络冒烟（手动跑；仅 1-2 个请求，验证迁移链可用）。

用法: python3 tests/smoke_social_net.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
social = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "omni", "adapters", "social")
sys.path.insert(0, social)


def main():
    from sm_main import make_fetcher
    from sm_search import search_with_failover, uncloak
    f = make_fetcher()
    q = "南方科技大学 留学生 QQ群"
    print(f"[net] 搜索: {q}")
    r = search_with_failover(f, q)
    results = r.get("results") or []
    print(f"[net] engine={r.get('engine')} results={len(results)} err={r.get('err')}")
    if not results:
        print("[net] SMOKE-FAIL: 无搜索结果")
        return 1
    # 依次尝试最多 3 条结果（单条 403/封闭属目标站行为，任一成功即链路验证通过）
    tried = 0
    for item in results:
        if tried >= 3:
            break
        u = item.get("url") or item.get("raw")
        if not u:
            continue
        tried += 1
        real = uncloak(f, u)
        print(f"[net] ({tried}) {item.get('title', '')[:50]} -> {real[:100]}")
        status, text, err = f.get(real, referer="https://www.so.com/")
        n = len(text or "")
        print(f"[net]     status={status} bytes={n} err={err}")
        if err is None and n > 500:
            print("[net] SMOKE-PASS")
            return 0
    print("[net] SMOKE-FAIL: 3 条均未成功（可能均为反爬站点，链路本身已跑通）")
    return 1


if __name__ == "__main__":
    sys.exit(main())
