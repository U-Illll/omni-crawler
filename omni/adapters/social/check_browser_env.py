#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""浏览器环境检查：camoufox / playwright 可用性"""
import os
import sys
import glob

print("== python ==", sys.executable, sys.version.split()[0])

# camoufox
try:
    import camoufox  # noqa: F401
    print("[camoufox] lib OK")
except Exception as e:
    print("[camoufox] lib FAIL:", e)

# camoufox 二进制目录
for p in [os.path.expanduser("~/.cache/camoufox"), "/root/.cache/camoufox",
          "/home/user/go/camoufox-venv/camoufox-bin"]:
    if os.path.exists(p):
        items = os.listdir(p)[:6]
        print(f"[camoufox-bin] {p}: {items}")

# playwright
try:
    import playwright  # noqa: F401
    from playwright.sync_api import sync_playwright  # noqa: F401
    print("[playwright] lib OK")
except Exception as e:
    print("[playwright] lib FAIL:", str(e)[:80])

# 浏览器缓存目录
for pat in [os.path.expanduser("~/.cache/ms-playwright"), "/root/.cache/ms-playwright"]:
    if os.path.exists(pat):
        print(f"[pw-browsers] {pat}: {os.listdir(pat)[:8]}")

# WSL 检测
print("[wsl]", "yes" if os.path.exists("/mnt/c/Windows") else "no")
