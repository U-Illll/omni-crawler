#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""红队：存储层（幂等 / 断点 / 对账 / 撕裂自愈）。

对标 E1（幂等重放）+ sm_store 教训（文件为准、防旧值漂移）。
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
    tmp = tempfile.mkdtemp(prefix="omni-store-")
    from omni.core import log as olog
    olog.init(tmp, writer="redteam")
    from omni.core.store import Store

    try:
        # 1) records 幂等
        s = Store(os.path.join(tmp, "runs"))
        c1 = s.add_record("lead", "q123", src="u1")
        c2 = s.add_record("lead", "q123", src="u2")
        check("同 key 幂等（第二次不新建）", c1 is True and c2 is False)
        check("sources 累积", s.records[("lead", "q123")]["sources"] == ["u1", "u2"])

        # 2) items 幂等
        a1 = s.add_item("https://a.example/1")
        a2 = s.add_item("https://a.example/1")
        check("item 幂等", a1 is True and a2 is False and s.has_item("https://a.example/1"))

        # 3) pending / tasks
        s.add_pending(["u1", "u2", "u1"])
        check("pending 去重", s.progress["pending"] == ["u1", "u2"])
        s.mark_task_done("t1")
        check("task 完成记录", s.task_done("t1"))

        # 4) 持久化（新实例从文件恢复，文件为准）
        s.save()
        s2 = Store(os.path.join(tmp, "runs"))
        check("重启恢复 records", s2.summary()["records"] == 1)
        check("重启恢复 items", s2.has_item("https://a.example/1"))
        check("重启恢复 tasks", s2.task_done("t1"))

        # 5) reconcile：pending 中被处理过的项移除（u2 已处理；u1 未处理应保留）
        s2.add_pending(["u9"])
        s2.add_item("u2")           # u2 已处理
        removed = s2.reconcile()
        check("reconcile 移除已处理", removed == 1 and s2.progress["pending"] == ["u1", "u9"],
              f"pending={s2.progress['pending']}")

        # 6) 撕裂自愈：手工写坏尾行 → 新实例修复且数据守恒
        with open(os.path.join(tmp, "runs", "items.jsonl"), "ab") as f:
            f.write(b'{"key":"half-broken","status":"o')
        s3 = Store(os.path.join(tmp, "runs"))
        check("撕裂后重启仍可用", s3.has_item("https://a.example/1"))
        raw = open(os.path.join(tmp, "runs", "items.jsonl"), "rb").read()
        check("撕裂行被截断", b"half-broken" not in raw)
        # 审计里应有 jsonl_repair
        ap = os.path.join(tmp, "logs", "audit.jsonl")
        has_repair = os.path.exists(ap) and any('"jsonl_repair"' in l for l in open(ap))
        check("修复事件已审计", has_repair)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n[redteam_store] PASS={PASS} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
