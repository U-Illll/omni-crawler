#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""social adapter CLI（迁移期保真转发）。

用法与原 sm-recon 一致：
  python3 main.py run --rounds 1,2 --max-pages 100 --max-seed 60
  python3 main.py loop --max-iter 50
  python3 main.py report | status
  python3 main.py wechat --max-wx-articles 30 --with-qr

（框架入口 = omni.core.engine.Engine + SocialAdapter；见 docs/runbook.md）
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import sm_main  # noqa: E402

if __name__ == "__main__":
    sm_main.main()
