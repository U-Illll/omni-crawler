#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""检查 playwright 与浏览器匹配（单 python 环境）"""
import os
import sys

print("python:", sys.executable)
try:
    import importlib.metadata as md
    print("playwright dist version:", md.version("playwright"))
except Exception as e:
    print("dist version ERR:", str(e)[:80])

try:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        try:
            exe = p.chromium.executable_path
            print("chromium executable_path:", exe)
            print("exists:", os.path.exists(exe))
        except Exception as e:
            print("chromium path ERR:", str(e)[:150])
except Exception as e:
    print("playwright ERR:", str(e)[:150])

for d in [os.path.expanduser("~/.cache/ms-playwright")]:
    if os.path.exists(d):
        print("已装浏览器:", sorted(os.listdir(d)))
