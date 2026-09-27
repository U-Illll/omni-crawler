#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.adapters.library — 图书馆 Primo 爬虫适配器（R3 v2.1 托管封装）。

模式：**托管封装**（低风险保真迁移）。
- scrape.py（R3 v2.1 合成版，4184 行）原样随包；其自带断点事务/限速/自愈/收敛判据。
- 本适配层 = 生命周期总控 + 状态观测：
    iter_once  = 快照现场状态（progress/convergence-report）→ 审计 → 必要时启动子进程
    converged  = convergence-report 显示 terminal 且 exit_code==0（机器判据，非字符串）
    report     = 汇总现场状态写 runs/report.md
- 现场目录：默认 adapters/library/site/（隔离）；生产切换时用 --site 或 OMNI_LIB_SITE
  指向既有现场（如 /tmp/library-scrape），实现无缝续跑。
- 进程守护：本适配层即托管者（简化版 keeper）；托管模式下不要再同时开 keeper-v3.sh
  （二选一，避免双守护）。
"""
import json
import os
import subprocess
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))

from ..base import Adapter  # noqa: E402


class LibraryAdapter(Adapter):
    name = "library"

    def __init__(self):
        self.site = None
        self.check_interval = 60.0
        self.seeds = []
        self._booted = False

    def root(self):
        return _HERE

    # ---------- 初始化 ----------
    def _ensure(self, ctx):
        if self._booted:
            return
        a = getattr(ctx, "args", None)
        site = None
        if a is not None:
            site = getattr(a, "site", None)
        site = site or os.environ.get("OMNI_LIB_SITE") or os.path.join(_HERE, "site")
        self.site = os.path.abspath(site)
        os.makedirs(self.out_dir, exist_ok=True)
        if a is not None:
            self.check_interval = float(getattr(a, "interval", 60) or 60)
            seeds = getattr(a, "seeds", None)
            if seeds:
                self.seeds = [s.strip() for s in str(seeds).split(",") if s.strip()]
        self._booted = True

    @property
    def out_dir(self):
        return os.path.join(self.site, "output")

    @property
    def pid_file(self):
        return os.path.join(self.site, "scrape.pid")

    # ---------- 观测 ----------
    def snapshot(self):
        """现场状态快照（progress + convergence-report；读数事实）。"""
        snap = {"site": self.site, "ts": time.time(),
                "progress": None, "convergence": None, "proc": None}
        pp = os.path.join(self.out_dir, "progress.json")
        if os.path.exists(pp):
            try:
                with open(pp, encoding="utf-8") as f:
                    prog = json.load(f)
                snap["progress"] = {
                    "todo": len(prog.get("todo") or []),
                    "done": len(prog.get("done") or []),
                    "gaps": len(prog.get("gaps") or []),
                    "records": (prog.get("stats") or {}).get("records"),
                    "leaves": (prog.get("stats") or {}).get("leaves"),
                }
            except Exception as e:  # noqa: BLE001
                snap["progress"] = {"error": f"{type(e).__name__}: {str(e)[:80]}"}
        cp = os.path.join(self.out_dir, "convergence-report.json")
        if os.path.exists(cp):
            try:
                with open(cp, encoding="utf-8") as f:
                    rep = json.load(f)
                snap["convergence"] = {
                    "terminal": rep.get("terminal"),
                    "exit_code": rep.get("exit_code"),
                    "reason": rep.get("reason"),
                    "decided": rep.get("decided"),
                }
            except Exception as e:  # noqa: BLE001
                snap["convergence"] = {"error": f"{type(e).__name__}: {str(e)[:80]}"}
        snap["proc"] = self.proc_alive()
        return snap

    def proc_alive(self):
        """PID 文件 + /proc cmdline 核对（防 PID 复用；继承 keeper-v3 [A] 思想简化版）。"""
        try:
            with open(self.pid_file, encoding="utf-8") as f:
                pid = int(f.read().strip())
        except Exception:  # noqa: BLE001
            return None
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmdline = f.read().decode("utf-8", "ignore")
        except Exception:  # noqa: BLE001
            return None
        if "scrape.py" in cmdline:
            return pid
        return None

    # ---------- 生命周期 ----------
    def start(self):
        from omni.core.log import audit
        env = os.environ.copy()
        env["SCRAPE_OUT_DIR"] = self.out_dir
        stdout_path = os.path.join(self.site, "scrape-stdout.log")
        f = open(stdout_path, "a", encoding="utf-8")
        try:
            proc = subprocess.Popen(
                [sys.executable, "scrape.py"] + list(self.seeds), cwd=_HERE, env=env,
                stdout=f, stderr=subprocess.STDOUT, start_new_session=True,
            )
        except Exception as e:  # noqa: BLE001
            f.close()
            audit({"kind": "lib_start_fail", "err": f"{type(e).__name__}: {str(e)[:160]}"})
            return False
        with open(self.pid_file, "w", encoding="utf-8") as pf:
            pf.write(str(proc.pid))
        audit({"kind": "lib_start", "pid": proc.pid, "site": self.site,
               "out": self.out_dir})
        return True

    def iter_once(self, ctx):
        from omni.core.log import audit, log
        self._ensure(ctx)
        snap0 = self.snapshot()
        audit({"kind": "lib_snapshot", **{k: snap0[k] for k in
               ("progress", "convergence", "proc")}})
        # 已收敛：交引擎判定（converged 会返回 True）
        if self.converged(ctx):
            return False
        # 进程不在 → 启动
        if snap0["proc"] is None:
            log(f"[library] 进程不在（site={self.site}）→ 启动 scrape.py")
            return self.start()
        # 进程活跃 → 一个检查节拍后返回（进行中，防 stall 误判）
        time.sleep(self.check_interval)
        return True

    def converged(self, ctx):
        from omni.core.log import audit, log
        self._ensure(ctx)
        snap = self.snapshot()
        conv = snap.get("convergence") or {}
        if conv.get("terminal") is True and conv.get("exit_code") == 0:
            log(f"[library] 收敛确认：{conv}")
            audit({"kind": "lib_converged", "convergence": conv})
            return True
        return False

    def report(self, ctx):
        from omni.core.log import audit, runs_dir
        self._ensure(ctx)
        snap = self.snapshot()
        path = os.path.join(runs_dir(), "report.md")
        lines = [
            "# library adapter · 收敛报告",
            "",
            f"- 现场：`{self.site}`",
            f"- 时间：{time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"- 进程：{snap['proc']}",
            f"- progress：{json.dumps(snap['progress'], ensure_ascii=False)}",
            f"- convergence：{json.dumps(snap['convergence'], ensure_ascii=False)}",
            "",
            f"- 交付/深加工：见现场 output/（records.jsonl）与 process.py 管线（如有）。",
        ]
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        audit({"kind": "lib_report", "path": path})
