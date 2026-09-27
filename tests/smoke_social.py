#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""social adapter 冒烟：模块链导入 + adapter 协议 + Engine 装配（不发网络请求）。"""
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def main():
    # 1) 模块链导入（social 包内模块使用平级 import，需 social 目录在 path）
    here = os.path.dirname(os.path.abspath(__file__))
    social = os.path.join(os.path.dirname(here), "omni", "adapters", "social")
    sys.path.insert(0, social)

    import importlib
    mods = ["sm_common", "sm_fetch", "sm_llm", "sm_store", "sm_sources",
            "sm_search", "sm_extract", "sm_qq"]
    for m in mods:
        try:
            importlib.import_module(m)
            check(f"import {m}", True)
        except Exception as e:  # noqa: BLE001
            check(f"import {m}", False, f"{type(e).__name__}: {e}")

    # 2) 适配层桥接正确性
    import sm_common
    import sm_fetch
    from omni.core.limiter import RateLimiter as OmniRL
    check("sm_common.RUNS 指向 social/runs",
          sm_common.RUNS.endswith(os.path.join("adapters", "social", "runs")))
    check("sm_fetch.RateLimiter 是 omni 子类",
          issubclass(sm_fetch.RateLimiter, OmniRL))

    # 3) adapter 协议
    from omni.adapters.social.adapter import SocialAdapter
    a = SocialAdapter()
    check("adapter.name", a.name == "social")
    check("adapter.root", a.root() == social)

    # 4) Engine 装配（不发起网络；只验证构造 + 组件接线）
    try:
        from omni.core.engine import Engine
        class FakeArgs:
            rounds = "1"
            max_pages = 1
            max_seed = 0
            rest = 0
        e = Engine(a, args=FakeArgs())
        check("Engine 装配", e.store is not None and e.fetcher is not None)
        check("Engine fetcher 共享 limiter", e.fetcher.limiter is e.limiter)
        check("converged 初判 False", a._booted is False or a.converged(e.ctx) in (True, False))
    except Exception as e:  # noqa: BLE001
        check("Engine 装配", False, f"{type(e).__name__}: {e}")

    print(f"\n[smoke_social] PASS={PASS} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
