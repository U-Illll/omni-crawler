#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""冒烟测试：模块导入 + 提取器样例"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import sm_common  # noqa: E402
import sm_fetch  # noqa: E402
import sm_search  # noqa: E402
import sm_extract  # noqa: E402
import sm_store  # noqa: E402
import sm_llm  # noqa: E402
import sm_sources  # noqa: E402

print("模块导入 OK")

text = """欢迎加入南方科技大学留学生新生群，群号：119408121，QQ群：924058731。
微信入群请扫码，或加群链接 https://jq.qq.com/?_wv=1027&k=abc123XYZ
Telegram 群: t.me/sustech_intl
管理员微信：添加 wxid_hello123 拉群"""

res = sm_extract.extract_groups(text, "http://example.com/test")
for r in res:
    print(" 线索:", r["kind"], r["value"], "| conf:",
          sm_extract.confidence(r["kind"], r["evidence"], sm_extract.relevance(text)))
print("相关性:", sm_extract.relevance(text))

# 存储层冒烟（用独立目录）
st = sm_store.Store(run_dir="/tmp/sm-recon/runs/smoke")
st.add_page("http://example.com/a", "test", "页A", 0.5, 2)
st.add_lead("qq_num", "119408121", "群号：119408121", "http://example.com/a", 0.5, 0.6)
st.add_lead("qq_num", "119408121", "群号：119408121", "http://example.com/b", 0.5, 0.6)
print("Store 冒烟:", st.summary())
st2 = sm_store.Store(run_dir="/tmp/sm-recon/runs/smoke")
print("Store 重载(幂等):", st2.summary(), "leads 去重 =", len(st2.leads))
print("ALL-OK")
