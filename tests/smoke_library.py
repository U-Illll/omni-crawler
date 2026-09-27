#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""library adapter 冒烟：协议 + 观测 + 收敛判据（空现场，不启动子进程/不发网络）。"""
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
    tmp = tempfile.mkdtemp(prefix="omni-libsmoke-")
    try:
        from omni.adapters.library.adapter import LibraryAdapter

        a = LibraryAdapter()
        a.site = tmp
        a._booted = True
        check("name", a.name == "library")
        snap = a.snapshot()
        check("空现场 snapshot", snap["progress"] is None and snap["convergence"] is None
              and snap["proc"] is None, f"snap={snap}")
        check("空现场未收敛", a.converged(_Ctx(a)) is False)

        # 预置收敛报告 → converged True
        os.makedirs(os.path.join(tmp, "output"), exist_ok=True)
        import json
        with open(os.path.join(tmp, "output", "convergence-report.json"), "w",
                  encoding="utf-8") as f:
            json.dump({"schema": "r3-convergence-v1", "terminal": True,
                       "exit_code": 0, "reason": "no-todo-no-open-gaps",
                       "decided": "done"}, f)
        check("收敛报告驱动 converged True", a.converged(_Ctx(a)) is True)

        # 半收敛（terminal False）→ False
        with open(os.path.join(tmp, "output", "convergence-report.json"), "w",
                  encoding="utf-8") as f:
            json.dump({"terminal": False, "exit_code": 3, "reason": "incomplete"}, f)
        check("未收敛报告 -> False", a.converged(_Ctx(a)) is False)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n[smoke_library] PASS={PASS} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


class _Ctx:
    """轻量 ctx（仅 args=None + adapter 属性；供 converged 调用）。"""

    def __init__(self, adapter):
        self.args = None
        self.adapter = adapter


if __name__ == "__main__":
    sys.exit(main())
