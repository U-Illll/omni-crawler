#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""冒烟：import 全部模块 + 基础自检。"""
import importlib
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MODS = [
    "omni.core.config", "omni.core.log", "omni.core.limiter", "omni.core.retry",
    "omni.core.circuit", "omni.core.checkpoint", "omni.core.store", "omni.core.engine",
    "omni.channel.manager", "omni.fetch.http", "omni.fetch.browser",
    "omni.anticrawl.detect", "omni.llm.client", "omni.adapters.base",
]

def main():
    ok = 0
    for m in MODS:
        try:
            importlib.import_module(m)
            print(f"OK   {m}")
            ok += 1
        except Exception as e:  # noqa: BLE001
            print(f"FAIL {m}: {type(e).__name__}: {e}")
    print(f"\n{ok}/{len(MODS)} imports OK")
    return 0 if ok == len(MODS) else 1

if __name__ == "__main__":
    sys.exit(main())
