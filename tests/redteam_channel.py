#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""红队：通道层（选路 / fallback / 全失败 / 恢复）。

对标 D1：多通道 fallback + 切换事件审计（发现/切换/恢复三段）。
"""
import json
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
    tmp = tempfile.mkdtemp(prefix="omni-channel-")
    from omni.core import log as olog
    olog.init(tmp, writer="redteam")
    from omni.channel.manager import ChannelManager, ChannelUnavailable

    def events(kind):
        p = os.path.join(tmp, "logs", "audit.jsonl")
        if not os.path.exists(p):
            return []
        return [json.loads(l) for l in open(p) if f'"{kind}"' in l]

    egress = {
        "direct": {"proxies": None, "desc": "直连"},
        "bad": {"proxies": {"http": "http://127.0.0.1:1", "https": "http://127.0.0.1:1"},
                "desc": "死端口出口"},
    }
    pref = {"X": ["bad", "direct"]}
    try:
        # 1) probe 关：未知健康视为可用 → 首选 bad（顺序与偏好一致）
        cm = ChannelManager(egress_table=egress, env_pref=pref, probe_enabled=False)
        name, proxies = cm.resolve("X")
        check("偏好顺序首选 bad", name == "bad")

        # 2) bad 失败标记 → fallback 到 direct + 审计事件
        cm.mark_failure("X", "bad", "connect refused")
        name2, _ = cm.resolve("X")
        check("fallback 到 direct", name2 == "direct")
        fb = events("channel_fallback")
        check("fallback 事件写入", len(fb) == 1 and fb[0]["chosen"] == "direct"
              and fb[0]["tried"], f"fb={fb}")

        # 3) 恢复：mark_success → bad 重新健康（下次 resolve 又选 bad）
        cm.mark_success("X", "bad")
        name3, _ = cm.resolve("X")
        check("恢复后回到 bad", name3 == "bad")

        # 4) 全失败（probe 开且探针不可达）→ ChannelUnavailable
        cm2 = ChannelManager(egress_table=egress, env_pref=pref, probe_enabled=True,
                             probe_urls={"X": "http://127.0.0.1:1/"})
        try:
            cm2.resolve("X")
            check("全失败抛 ChannelUnavailable", False, "no raise")
        except ChannelUnavailable:
            check("全失败抛 ChannelUnavailable", True)
        un = events("channel_unavailable")
        check("unavailable 事件写入", len(un) == 1)

        # 5) 探针结果缓存（TTL 内不重复探测）
        cm3 = ChannelManager(egress_table=egress, env_pref={"Y": ["direct"]},
                             probe_enabled=True, probe_urls={"Y": "http://127.0.0.1:1/"})
        cm3.resolve("Y") if False else None
        # direct 探针到死地址：失败 → 记 probe 事件；再 resolve 缓存
        try:
            cm3.resolve("Y")
        except ChannelUnavailable:
            pass
        n1 = len(events("channel_probe"))
        try:
            cm3.resolve("Y")
        except ChannelUnavailable:
            pass
        n2 = len(events("channel_probe"))
        check("探针结果缓存（TTL 内不重复）", n2 == n1, f"n1={n1} n2={n2}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n[redteam_channel] PASS={PASS} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
