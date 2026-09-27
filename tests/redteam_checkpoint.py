#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""红队：断点事务（原子写 / 撕裂修复 / 坏行隔离 / progress 校验）。

对标：图书馆 E1-E3 断点一致性 + R3 repair/reconcile 教训。
"""
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from omni.core.checkpoint import (atomic_write, atomic_write_json, repair_jsonl,
                                  validate_progress)

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
    tmp = tempfile.mkdtemp(prefix="omni-ckpt-")
    try:
        # 1) 原子写
        p = os.path.join(tmp, "a.json")
        ok, err = atomic_write_json(p, {"x": 1})
        check("atomic_write_json ok", ok and err is None)
        check("content roundtrip", json.load(open(p)) == {"x": 1})

        # 2) 尾行撕裂修复（半写行）
        p2 = os.path.join(tmp, "rec.jsonl")
        with open(p2, "wb") as f:
            f.write(b'{"k":1}\n{"k":2}\n{"k":3,"half":')   # 尾行不完整
        rep = repair_jsonl(p2)
        check("repair detects tail tear", rep["repaired"] and rep["truncated_bytes"] > 0)
        lines = open(p2, "rb").read().split(b"\n")
        good = [l for l in lines if l]
        check("good lines kept", len(good) == 2)
        check("tail removed", all(b"half" not in l for l in good))

        # 3) 中段坏行隔离
        p3 = os.path.join(tmp, "rec3.jsonl")
        with open(p3, "wb") as f:
            f.write(b'{"k":1}\nNOT-JSON\n{"k":3}\n')
        rep = repair_jsonl(p3, quarantine_dir=os.path.join(tmp, "q"))
        check("mid bad line quarantined", rep["bad_lines"] == 1 and rep["repaired"])
        check("quarantine file exists",
              os.path.exists(os.path.join(tmp, "q", "rec3.jsonl.bad")))
        kept = [json.loads(l) for l in open(p3) if l.strip()]
        check("survivors intact", [r["k"] for r in kept] == [1, 3])

        # 4) 完好文件不动
        p4 = os.path.join(tmp, "rec4.jsonl")
        with open(p4, "wb") as f:
            f.write(b'{"k":1}\n{"k":2}\n')
        rep = repair_jsonl(p4)
        check("intact file untouched", not rep["repaired"])

        # 5) progress 校验
        okp, probs = validate_progress({"tasks_done": [], "pending": [], "stats": {"a": 1}})
        check("valid progress ok", okp and not probs)
        badp, probs = validate_progress({"tasks_done": "x", "stats": {"a": -1}})
        check("invalid progress flagged", (not badp) and len(probs) >= 2)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n[redteam_checkpoint] PASS={PASS} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
