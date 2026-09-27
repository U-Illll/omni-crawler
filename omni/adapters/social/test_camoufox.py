#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""camoufox 启动快测"""
import time

t0 = time.time()
from camoufox.sync_api import Camoufox  # noqa: E402

print(f"[{time.time()-t0:.1f}s] import done", flush=True)
with Camoufox(headless=True) as browser:
    print(f"[{time.time()-t0:.1f}s] browser launched", flush=True)
    page = browser.new_page()
    print(f"[{time.time()-t0:.1f}s] page created", flush=True)
    page.goto("about:blank")
    print(f"[{time.time()-t0:.1f}s] about:blank OK", flush=True)
    try:
        page.goto("https://www.google.com/", timeout=40000, wait_until="domcontentloaded")
        print(f"[{time.time()-t0:.1f}s] google OK, title={page.title()[:60]}", flush=True)
    except Exception as e:
        print(f"[{time.time()-t0:.1f}s] google FAIL: {str(e)[:120]}", flush=True)
print(f"[{time.time()-t0:.1f}s] DONE", flush=True)
