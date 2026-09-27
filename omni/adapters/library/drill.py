#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""drill.py —— R3 自愈演练台（故障注入 → 观察 → 断言，一键复跑）

定位（目标卡 §1 C 组 / r3-inputs.md）
------------------------------------
把「系统级自愈」变成可机器判定的演练：在自己目录的沙箱里起一套**影子系统**
（被测 scrape.py 副本 + memguard + heartbeat + keeper），对着 loopback mock 跑抓取，
在运行期注入真实故障（kill -9 / 输出目录消失 / 磁盘写失败 / 网络断 / 429 风暴 /
探测全灭 / progress.json 损毁），然后断言三件事：

  1. **恢复时限**：注入 → 重新干活 ≤ 180s（A2）；
  2. **数据完整性**：unique mms 相对「干净参照跑」零丢失、重复抓取叶子块 ≤1（A2）；
  3. **审计链路**：该场景对应的自愈事件必须出现在审计流里（C1/C2/C3 三级）——
     由 audit-verify.py 判定，本脚本只负责采集与编排。

被注入的系统是**被测系统本身**（默认 = R3 基线 r2candidate），演练台不改它的逻辑：
  · 只把路径常量重写进沙箱、把 HOST 指向 loopback mock（drilllib/sandbox.py + shim.py）；
  · 其余读数全部来自**旁路观测**：mock 的服务端请求日志、keeper 的 audit.jsonl、
    scrape.log、progress.json / records.jsonl、/proc。

用法
----
  python3 drill.py --list-systems
  python3 drill.py --list-scenarios
  python3 drill.py --system baseline-r2cand            # 全场景 + 干净参照跑
  python3 drill.py --system baseline-r2cand --only S1,S6
  python3 drill.py --report                            # 用最近一次 results 重出报告

安全边界（红线）
----------------
  · 只杀「自己沙箱内、cmdline 命中本演练 run 目录」的进程（SAFE_KILL，见 Rig.safe_kill）；
  · mock 只绑 loopback；被测副本重写后若仍含 "/tmp/library-scrape" 字面量则拒绝运行；
  · 全部产物写在 /tmp/lib-opt-work/R3/impl/slot-drill/ 内。
"""
import argparse
import json
import os
import re
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# R3-P4 修复（本文件 = drill.py + audit-unify 槽的 4 项修复；其余逐字节同 R3 版）
#   B(a) **ordering bug**（verification §5.4）：`evidence.json` 的写入提到
#        `run_audit_verify()` **之前** —— 原先校验器读不到注入时间线，时间窗校验从未生效。
#   B(b) `--window` 静默忽略（verification §5.2）：CLI 显式给出时**覆盖**场景声明并打 WARN。
#   B(c) `audit_link` 改为要求 **canonical 硬通过**（audit-verify-v2 的 canonical_pass）：
#        derived-only / 无时间窗一律降级标注并判 FAIL（堵 d1「一句日志过关」）。
#   B(d) evaluate 软/硬分级复查（crit-adversarial d2）：`kill_loop_3rounds` 由 soft 改 hard
#        （kills 不足下限 = 假收敛嫌疑 ⇒ FAIL）。其余 soft 断言的豁免理由见回执。
#   · 校验器解析顺序：audit-verify-v2.py > audit-verify.py（可用 DRILL_VERIFIER 覆盖）
#   · REFS 可用 DRILL_R3_REF_DIR 覆盖（便于在影子目录复跑，仍只读）
# ---------------------------------------------------------------------------
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from drilllib import sandbox as sb                      # noqa: E402
from drilllib import evidence as evd                     # noqa: E402
from drilllib.evidence import (LOG_PATTERNS, log_evidence as _log_evidence,   # noqa: E402
                               mms_set as _mms_set, mock_req_stats as _mock_req_stats,
                               read_text as _read_text)

R3 = HERE.parent.parent                                  # /tmp/lib-opt-work/R3
REFS = Path(os.environ.get("DRILL_R3_REF_DIR") or (R3 / "refs"))
WORK = R3.parent                                         # /tmp/lib-opt-work
SYSTEMS_DIR = HERE / "systems"
RUNS_DIR = HERE / "runs"
RESULTS_DIR = HERE / "results"
MOCK_PY = HERE / "drilllib" / "mock_drill.py"
# R3-P4：统一校验器 v2 优先；v2 缺席时退回 v1（并在报告里标注用的哪一代）
VERIFIER = Path(os.environ.get("DRILL_VERIFIER") or (HERE / "audit-verify-v2.py"))
if not VERIFIER.exists():
    VERIFIER = HERE / "audit-verify.py"
PROD_LITERAL = "/tmp/library-scrape"

# ---------------------------------------------------------------------------
# 演练语料（mock）：满深树（--len-tail 1.0）⇒ 无天然缺口，时间不空耗在复验轮上；
# 「缺口 / 复验 / 兜底轮」这些代码路径由 S6（探测全灭）显式覆盖，不靠语料偶然触发。
# ---------------------------------------------------------------------------
CORPUS = {
    "records": 21600,        # 36 个首字符各 ≈600 条（> LEAF_MAX=490 ⇒ 首层必为分支节点）
    "depth": 2,
    "fan": 3,
    "dist": "zipf",
    "len_tail": "1.0",
    "lat_p50": 0.002,        # 服务端延迟不是本轮变量（B 组在 R2 已收口）→ 压到噪声级
    "lat_sigma": 0.0,
    "profile": "ideal",      # 无服务端限流；限流/熔断由 S5 显式注入
}
SEEDS = ["A", "B", "C", "D", "E", "F"]
DEFAULT_WINDOW = 150         # 单场景观察窗（s）
DEFAULT_DEADLINE = 180       # A2 恢复时限（s）
SEED_NOTE = ("种子数刻意 > W_MAX(3)：R2 并发版把已认领条目**移出** prog['todo']（只活在内存 "
             "inflight 集合里），进程被杀时在飞条目在盘上无痕。若种子数 = W_MAX，第一次 kill 就让"
             "磁盘队列变空 ⇒ 复跑即「无新任务」假收敛，后续 kill 无进程可杀、演练失去观测对象。"
             "6 个种子使队列深度足以承载 3 轮 kill 观测。")


def _now():
    return time.time()


def _iso(ts=None):
    return datetime.fromtimestamp(ts if ts else time.time()).isoformat(timespec="milliseconds")


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _sha256_12(p):
    import hashlib
    try:
        return hashlib.sha256(Path(p).read_bytes()).hexdigest()[:12]
    except OSError:
        return None


# ===========================================================================
# 系统解析（可插拔：R3 各槽产物落位后，加一个 systems/*.json 即可接入）
# ===========================================================================
def load_systems():
    systems = {}
    for p in sorted(SYSTEMS_DIR.glob("*.json")):
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
        except ValueError as e:
            print("[warn] 系统描述无法解析：%s (%s)" % (p, e))
            continue
        d.setdefault("name", p.stem)
        d["_file"] = str(p)
        systems[d["name"]] = d
    return systems


def resolve_system(name, overrides):
    systems = load_systems()
    if name not in systems:
        raise SystemExit("未知系统 %r；可用：%s" % (name, ", ".join(sorted(systems))))
    sysdef = dict(systems[name])
    for key in ("scrape", "memguard", "heartbeat", "keeper"):
        if overrides.get(key):
            sysdef[key] = overrides[key]
    missing, degraded = [], []
    strict_members = set(sysdef.get("strict") or [])
    for key in ("scrape", "memguard", "heartbeat", "keeper"):
        val = sysdef.get(key)
        if not val:
            continue
        if Path(val).exists():
            continue
        fb = (sysdef.get("fallbacks") or {}).get(key)
        # fallback 只对**非定义性**成员生效：strict 成员缺席 ⇒ 该系统的本次运行不成立，
        # 绝不用别的实现冒充（否则"插槽接入"会退化成"其实还在测基线"）。
        if fb and Path(fb).exists() and key not in strict_members:
            degraded.append({"member": key, "wanted": val, "using": fb})
            sysdef[key] = fb
            continue
        missing.append({"member": key, "path": val, "strict": key in strict_members})
    sysdef["_missing"] = missing
    sysdef["_degraded"] = degraded
    sysdef["_ready"] = not missing and bool(sysdef.get("scrape"))
    sysdef["_hashes"] = {k: _sha256_12(sysdef[k]) for k in ("scrape", "memguard", "heartbeat",
                                                            "keeper") if sysdef.get(k)}
    for k in ("scrape", "memguard", "heartbeat", "keeper"):
        v = sysdef.get(k)
        if v and str(v).startswith(PROD_LITERAL):
            raise SystemExit("系统描述指向生产路径，拒绝运行：%s=%s" % (k, v))
    return sysdef


# ===========================================================================
# Rig —— 一次演练的沙箱：mock + keeper + 被看护作业
# ===========================================================================
class Rig:
    def __init__(self, sid, system, run_dir, cfg):
        self.sid = sid
        self.system = system
        self.cfg = cfg
        self.dir = Path(run_dir)
        self.out_dir = self.dir / "out"
        self.logs = self.dir / "logs"
        self.state = self.dir / "state"
        self.mock_proc = None
        self.keeper_proc = None
        self.port = cfg.get("port") or _free_port()
        self.job = system.get("job_name", "v8")
        self.saves_seen = set()
        self.max_records = 0
        self.notes = []
        self.t0 = _now()
        self.mock_restarts = 0

    # ---------------------------------------------------------------- 准备
    def setup(self):
        self.logs.mkdir(parents=True, exist_ok=True)
        paths = sb.prepare(self.dir, self.system, self.cfg.get("seeds", SEEDS), HERE)
        paths.update({
            "audit": str(self.dir / "audit.jsonl"),
            "startup_audit": str(self.dir / "startup-audit.log"),
            "keeper_log": str(self.logs / "keeper.log"),
            "run_id": self.cfg["run_id"],
        })
        self.paths = paths
        self.env = sb.env_for(paths, self.system, self.mock_url, extra=self.cfg.get("env"))
        (self.dir / "sandbox.json").write_text(
            json.dumps({k: v for k, v in paths.items()}, ensure_ascii=False, indent=1),
            encoding="utf-8")
        if not paths["rewrite"]["prod_literal_left"] == 0:
            raise sb.SandboxError("副本仍含生产路径字面量")
        return paths

    # ---------------------------------------------------------------- mock
    @property
    def mock_url(self):
        return "http://127.0.0.1:%d" % self.port

    def start_mock(self, extra_args=None):
        ready = self.dir / "logs" / "mock-ready.json"
        if ready.exists():
            ready.unlink()
        args = [sys.executable, str(MOCK_PY), "--host", "127.0.0.1", "--port", str(self.port),
                "--records", str(CORPUS["records"]), "--depth", str(CORPUS["depth"]),
                "--fan", str(CORPUS["fan"]), "--dist", CORPUS["dist"],
                "--len-tail", CORPUS["len_tail"],
                "--lat-p50", str(CORPUS["lat_p50"]), "--lat-sigma", str(CORPUS["lat_sigma"]),
                "--profile", CORPUS["profile"],
                "--log", str(self.logs / "mock-requests.jsonl"),
                "--stats-file", str(self.logs / "mock-stats.json"),
                "--ready-file", str(ready)]
        args += list(extra_args or [])
        logf = open(self.logs / "mock.out", "ab")
        self.mock_proc = subprocess.Popen(args, stdout=logf, stderr=subprocess.STDOUT,
                                          cwd=str(self.dir))
        for _ in range(200):
            if ready.exists():
                return json.loads(ready.read_text())
            if self.mock_proc.poll() is not None:
                raise RuntimeError("mock 启动失败：%s" % _read_text(self.logs / "mock.out")[-500:])
            time.sleep(0.05)
        raise RuntimeError("mock 启动超时")

    def mock_get(self, path, timeout=3.0):
        import urllib.request
        try:
            with urllib.request.urlopen(self.mock_url + path, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except Exception as e:                      # 网络断注入期间属于预期
            return {"_error": "%s: %s" % (type(e).__name__, e)}

    def mock_fault(self, **kw):
        from urllib.parse import urlencode
        q = dict(kw)
        q["note"] = q.get("note") or "drill"
        return self.mock_get("/__mock/fault?" + urlencode(q))

    def stop_mock(self, restart=False):
        if self.mock_proc is None:
            return
        self.mock_get("/__mock/shutdown")
        try:
            self.mock_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.mock_proc.terminate()
            try:
                self.mock_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.mock_proc.kill()
        self.mock_proc = None
        if restart:
            self.mock_restarts += 1
            self.start_mock()

    # ---------------------------------------------------------------- keeper
    def start_keeper(self):
        keeper = self.system.get("keeper")
        if not keeper or not Path(keeper).exists():
            raise RuntimeError("keeper 缺席：%r（系统 %s）" % (keeper, self.system.get("name")))
        logf = open(self.logs / "keeper.out", "ab")
        self.keeper_proc = subprocess.Popen(
            ["bash", str(keeper), "start"], env=self.env, cwd=str(self.dir),
            stdout=logf, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)

    def stop_keeper(self, timeout=25):
        """优雅停：先 stop 标志（keeper 自己的协议），超时再 SIGTERM/SIGKILL 自己的子进程。"""
        keeper = self.system.get("keeper")
        if keeper and Path(keeper).exists():
            try:
                subprocess.run(["bash", str(keeper), "stop"], env=self.env, cwd=str(self.dir),
                               timeout=10, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception:
                pass
        if self.keeper_proc is not None:
            try:
                self.keeper_proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.keeper_proc.terminate()
                try:
                    self.keeper_proc.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    self.keeper_proc.kill()
                    self.keeper_proc.wait(timeout=8)
            self.keeper_proc = None

    # ---------------------------------------------------------------- 被看护进程
    def pidfile(self):
        return self.state / "pid" / ("%s.pid" % self.job)

    def job_pid(self):
        txt = _read_text(self.pidfile())
        m = re.search(r"pid=(\d+)", txt)
        return int(m.group(1)) if m else None

    def verify_pid(self, pid):
        """SAFE_KILL 前置核对：只认「本演练沙箱内、由本演练 run 目录启动」的进程。"""
        if not pid:
            return {"ok": False, "why": "no_pid"}
        base = Path("/proc/%d" % pid)
        if not base.exists():
            return {"ok": False, "why": "proc_missing", "pid": pid}
        cmdline = _read_text(base / "cmdline").replace("\0", " ").strip()
        cwd = os.readlink(base / "cwd") if (base / "cwd").exists() else None
        start = None
        try:
            stat = _read_text(base / "stat")
            start = stat.rsplit(")", 1)[1].split()[19]     # field 22 = starttime
        except Exception:
            pass
        inside = str(self.dir) in cmdline or str(self.paths["wrapper"]) in cmdline
        # 强判据：argv 里必须出现本演练专属的包装脚本（每个场景 run 目录唯一）
        strong = str(self.paths["wrapper"]) in cmdline
        return {"ok": bool(inside and strong), "pid": pid, "cmdline": cmdline[:400],
                "cwd": cwd, "starttime": start,
                "why": "ok" if (inside and strong) else
                       ("cmdline_not_in_run_dir" if not inside else "wrapper_token_missing")}

    def safe_kill(self, reason, sig=signal.SIGKILL):
        pid = self.job_pid()
        v = self.verify_pid(pid)
        rec = {"ts": _now(), "reason": reason, "pid": pid, "verify": v, "killed": False}
        if not v["ok"]:
            rec["refused"] = True
            self.notes.append("SAFE_KILL 拒绝：%s (%s)" % (pid, v["why"]))
            return rec
        try:
            pgid = os.getpgid(pid)
        except OSError:
            pgid = pid
        try:
            if pgid == pid:                    # keeper 用 setsid 起作业 ⇒ 组长=进程组
                os.killpg(pgid, sig)
            else:
                os.kill(pid, sig)
            rec["killed"] = True
            rec["pgid"] = pgid
            rec["signal"] = int(sig)
        except OSError as e:
            rec["error"] = "%s: %s" % (type(e).__name__, e)
        return rec

    def job_alive(self, ignore_pid=None):
        pid = self.job_pid()
        if not pid or pid == ignore_pid:
            return None
        v = self.verify_pid(pid)
        return v if v["ok"] else None

    # ---------------------------------------------------------------- 观测
    def records_path(self):
        return self.out_dir / "records.jsonl"

    def progress_path(self):
        return self.out_dir / "progress.json"

    def complete(self):
        return "全部完成" in _read_text(self.out_dir / "scrape.log")

    def poll(self):
        """一次轻量采样：progress 保存次数 / records 行数 / 工作日志行数。"""
        p = self.progress_path()
        txt = _read_text(p)
        if txt:
            m = re.search(r'"saved"\s*:\s*"([^"]+)"', txt)
            self.saves_seen.add(m.group(1) if m else "size:%d" % len(txt))
        try:
            self.max_records = max(self.max_records, self.records_path().stat().st_size)
        except OSError:
            pass
        return {"saves": len(self.saves_seen), "records_bytes": self.max_records}

    def work_evidence(self):
        ev = _log_evidence(self.out_dir / "scrape.log")
        work = sum(ev[k]["count"] for k in ("branch", "leaf", "pre_fetch"))
        return work, ev

    def wait_until(self, pred, timeout, poll=0.25, label=""):
        t0 = _now()
        while _now() - t0 < timeout:
            self.poll()
            if pred():
                return {"ok": True, "waited_s": round(_now() - t0, 2), "label": label}
            time.sleep(poll)
        return {"ok": False, "waited_s": round(_now() - t0, 2), "label": label, "timeout": True}

    def wait_progress_saves(self, n, timeout):
        return self.wait_until(lambda: len(self.saves_seen) >= n, timeout,
                               label="progress_saves>=%d" % n)

    def wait_records_bytes(self, nbytes, timeout):
        return self.wait_until(
            lambda: (self.records_path().exists() and self.records_path().stat().st_size >= nbytes),
            timeout, label="records_bytes>=%d" % nbytes)

    def wait_complete(self, timeout):
        return self.wait_until(self.complete, timeout, label="complete")

    def wait_recover(self, t_inject, timeout, killed_pid=None, baseline_work=None,
                     signal="any", saves_ref=None, lines_floor=None):
        """注入后恢复 = 新进程被拉起 **且**重新开始干活（不是只看进程存活）。

        signal 决定「重新干活」的判据（写入点类故障必须用后两种，否则会把活锁读成已恢复）：
          · "any"         → 工作日志行增加 / 完成标记 / records 增长（进程级故障用）
          · "save"        → **progress.json 成功落盘**（写入点被修好）
          · "valid_lines" → records.jsonl 的**有效行**超过 lines_floor（追加真的成功了）

        三种终态分开记录，避免把「假收敛」误读成「恢复」：
          · work_resumed=True          → 真的重新出现工作行（叶子/细分/先抓）
          · completed_after_inject=True 且 work_resumed=False → 复跑直接宣布完成（假收敛嫌疑）
          · timeout=True               → 观察窗内既没有活着的作业、也没有完成标记
        """
        t0 = _now()
        restart_s = None
        new_pid = None
        if saves_ref is None:
            saves_ref = len(self.saves_seen)
        out = {"inject_ts": t_inject, "restart_s": None, "recover_s": None, "new_pid": None,
               "work_resumed": False, "completed_after_inject": False, "timeout": True,
               "signal": signal}
        if baseline_work is None:
            baseline_work, _ = self.work_evidence()
        while _now() - t0 < timeout:
            self.poll()
            alive = self.job_alive(ignore_pid=killed_pid)
            saved = len(self.saves_seen) > saves_ref
            valid_lines = None
            if signal == "valid_lines":
                rr = _mms_set(self.records_path())
                valid_lines = rr["lines"] - rr["invalid_lines"]
            if alive:
                if restart_s is None:
                    restart_s = round(_now() - t_inject, 2)
                    new_pid = alive["pid"]
                work, _ev = self.work_evidence()
                progressed = work > baseline_work
                done = self.complete()
                if signal == "save":
                    ok = saved
                elif signal == "valid_lines":
                    ok = lines_floor is not None and valid_lines > lines_floor
                else:
                    ok = progressed or done or \
                        (self.max_records > self.cfg.get("_records_at_inject", 0))
                if ok:
                    out.update({"restart_s": restart_s, "recover_s": round(_now() - t_inject, 2),
                                "new_pid": new_pid, "timeout": False,
                                "save_after_inject": bool(saved),
                                "valid_lines_after": valid_lines,
                                "work_resumed": bool(progressed),
                                "completed_after_inject": bool(done)})
                    return out
            elif self.complete() and signal == "any":
                # 完成标记已在且没有活着的作业 ⇒ 不会再重启（keeper 守卫语义），提前定性
                out.update({"restart_s": restart_s, "new_pid": new_pid,
                            "no_recovery": "converged_marker_present",
                            "completed_after_inject": True})
                return out
            time.sleep(0.25)
        out.update({"restart_s": restart_s, "new_pid": new_pid})
        return out

    # ---------------------------------------------------------------- 收尾
    def teardown(self):
        try:
            self.stop_keeper()
        finally:
            # 兜底：清掉本沙箱内自己起的作业进程组（严格 SAFE_KILL 判据）
            pid = self.job_pid()
            v = self.verify_pid(pid)
            if v["ok"]:
                try:
                    os.killpg(os.getpgid(pid), signal.SIGKILL)
                except OSError:
                    pass
            self.stop_mock()

    def snapshot(self, phase, extra=None):
        work, ev = self.work_evidence()
        rec = _mms_set(self.records_path())
        req = _mock_req_stats(self.logs / "mock-requests.jsonl")
        prog = None
        try:
            prog = json.loads(_read_text(self.progress_path()) or "null")
        except ValueError:
            prog = None
        snap = {
            "phase": phase, "ts": _now(), "ts_iso": _iso(),
            "job_pid": self.job_pid(),
            "records": {"lines": rec["lines"], "unique": len(rec["unique"]),
                        "invalid_lines": rec["invalid_lines"], "dup_mms": rec["dup_mms"]},
            "progress": ({"saves": len(self.saves_seen),
                          "todo": len(prog.get("todo") or []),
                          "done": len(prog.get("done") or []),
                          "gaps": len(prog.get("gaps") or []),
                          "recovery_notes": len(prog.get("recovery") or []),
                          "torn_requeue": len(prog.get("torn_requeue") or [])} if prog else None),
            "work_lines": work,
            "mock": {k: req[k] for k in ("requests", "probe", "leaf_page",
                                         "dup_leaf_sort_pairs", "status", "reason")},
            "complete": self.complete(),
        }
        if extra:
            snap.update(extra)
        return snap


# ===========================================================================
# 场景实现：每场景 = 注入(inject) → 观察(observe) → 断言(assert)
# ===========================================================================
def _mkdirs(rig):
    rig.out_dir.mkdir(parents=True, exist_ok=True)


def sc_S1(rig):
    """S1 kill -9 循环 ×3：进程级自愈（C3/A2）。"""
    res = {"injections": [], "expect_audit": {"keeper_restart": 3}}
    rig.wait_progress_saves(1, 60)
    rig.wait_records_bytes(20000, 60)          # 至少一次真实 append 落盘后再杀
    for i in range(1, 4):
        rig.poll()
        alive = rig.job_alive()
        if alive is None:
            res["injections"].append({
                "round": i, "skipped": "no_live_job",
                "complete_marker": rig.complete(),
                "note": ("完成标记已在：keeper 守卫语义下不会再有重启 ⇒ 后续 kill 无观测对象"
                         if rig.complete() else "pidfile 无有效作业（等待下一次巡检）")})
            continue
        base_work, _ = rig.work_evidence()
        rig.cfg["_records_at_inject"] = rig.max_records
        rec = rig.safe_kill("S1 kill#%d（kill -9 循环）" % i)
        if not rec.get("killed"):
            rec["round"] = i
            res["injections"].append(rec)
            continue
        recov = rig.wait_recover(rec["ts"], rig.cfg["deadline"], killed_pid=rec["pid"],
                                 baseline_work=base_work)
        recov["round"] = i
        rec.update(recov)
        res["injections"].append(rec)
        # 下一轮前给系统一点推进时间（也避免把「秒死循环」当成正常路径）
        if recov.get("work_resumed"):
            rig.wait_progress_saves(len(rig.saves_seen) + 1, 30)
    return res


def sc_S2(rig):
    """S2 运行中 rm -rf 输出目录（F3：目录消失即死）。

    观察点：records.jsonl 是**常开 fd**（v10 行级原子追加），unlink 目录不会让 fd 失效 ⇒
    进程可能继续把记录写进「已删除的 inode」（数据黑洞），直到下一次 progress 原子替换才崩。
    这里显式记录这个窗口，避免把「还活着」误读成「没事」。
    """
    res = {"injections": [], "expect_audit": {"keeper_restart": 1}}
    rig.wait_progress_saves(1, 60)
    rig.wait_records_bytes(20000, 60)
    rig.cfg["_records_at_inject"] = rig.max_records
    base_work, _ = rig.work_evidence()
    t = _now()
    import shutil
    shutil.rmtree(str(rig.out_dir), ignore_errors=True)
    rec = {"ts": t, "kind": "rm_rf_out_dir", "path": str(rig.out_dir),
           "existed_after": rig.out_dir.exists(),
           "work_lines_at_inject": base_work}
    rig.wait_until(lambda: rig.job_alive() is None, 60, label="crash_after_rm_rf")
    rec.update({"job_died": rig.job_alive() is None,
                "ghost_window_s": round(_now() - t, 2),
                "records_invisible_bytes": rig.max_records})
    rec.update(rig.wait_recover(t, rig.cfg["deadline"], baseline_work=base_work))
    res["injections"].append(rec)
    return res


def _valid_lines(records_path):
    r = _mms_set(records_path)
    return r["lines"] - r["invalid_lines"], r["invalid_lines"]


def sc_S3a(rig):
    """S3a 磁盘写失败（RLIMIT_FSIZE 短写）→ 活锁 + 撕裂行 → 重启修复。

    实测得到的三条关键事实（第一、二版演练踩出来的）：
      1. 限值 = **当前 records 大小 + 8KB**：文件已超限时任何写入都直接失败 ⇒ 不产生半写；
         +8KB 才逼出「写入先落一部分再跨界」= 真正的短写 + 撕裂行。
      2. 限值必须在**下一次 spawn** 才生效（包装脚本每次启动读 `drill-env/fsize_kb`）⇒ 设完限值
         必须换进程，否则跑的是没有限值的老进程。
      3. **CPython 默认忽略 SIGXFSZ**（实测 `signal.getsignal(SIGXFSZ)==1`）⇒ RLIMIT_FSIZE 不会
         打死 Python 进程，`os.write` 只返回短写 + `OSError [Errno 27] File too large`。于是基线
         把它走成「条目异常 → 回队尾」的**活锁**（进程不死、keeper 看不见、撕裂行也不修）。
         因此恢复动作 = 解除限值 **并且**换进程（人工修好磁盘后重启的等价物）。
    """
    res = {"injections": [], "expect_audit": {"keeper_restart": 1}}
    rig.wait_progress_saves(1, 60)
    rig.wait_records_bytes(60000, 60)
    fsize = Path(rig.paths["fsize_file"])
    cur = rig.records_path().stat().st_size
    kb = cur // 1024 + 8
    fsize.write_text("%d\n" % kb, encoding="utf-8")
    base_work, _ = rig.work_evidence()
    t = _now()
    rec = {"ts": t, "kind": "rlimit_fsize", "kb": kb, "records_bytes": cur,
           "limit_bytes": kb * 1024, "limit_applies_at": "next_spawn"}
    res["injections"].append(rec)
    kill = rig.safe_kill("S3a 施加写限值后换进程（新 spawn 才带 ulimit -f %d）" % kb)
    rec["kill"] = {k: kill.get(k) for k in ("pid", "killed", "pgid")}
    # 等撕裂：文件恰好在限值处停住 + 末行残缺
    def torn():
        p = rig.records_path()
        if not p.exists():
            return False
        return p.stat().st_size >= kb * 1024 and _torn_tail(p).get("torn")
    got = rig.wait_until(torn, 60, label="short_write_torn_tail")
    rec["torn_observed"] = bool(got.get("ok"))
    rec["torn_tail"] = _torn_tail(rig.records_path())
    rec["records_bytes_after_tear"] = (rig.records_path().stat().st_size
                                       if rig.records_path().exists() else None)
    lines_at_tear, _inv = _valid_lines(rig.records_path())
    rec["valid_lines_at_tear"] = lines_at_tear
    # 观察活锁：进程是否还活着 / 条目异常刷了多少
    # （条目异常要等该条目「第一段」跑完才出现：分支节点要先做完 36 次探测 ≈22s ⇒ 给足窗口，
    #  否则会把「还卡在第一段」误读成「没有活锁」）
    def item_errors():
        e = _log_evidence(rig.out_dir / "scrape.log")
        return e.get("conc_item_error", {}).get("count", 0)
    rig.wait_until(lambda: item_errors() >= 3, 45, poll=1.0, label="livelock_visible")
    time.sleep(2.0)
    ev = _log_evidence(rig.out_dir / "scrape.log")
    after6 = (rig.records_path().stat().st_size if rig.records_path().exists() else None)
    rec["livelock"] = {
        "item_errors": ev.get("conc_item_error", {}).get("count", 0),
        "item_error_samples": ev.get("conc_item_error", {}).get("samples", [])[:3],
        "alive_during_fault": rig.job_alive() is not None,
        "progress_during_fault": bool(after6 and after6 > kb * 1024),
        "records_bytes_during_fault": after6,
        "limit_bytes": kb * 1024,
    }
    # 恢复动作 = 修好写点（解除限值）+ 换进程（限值是进程属性，不换进程就一直被卡住）
    fsize.write_text("0\n", encoding="utf-8")
    rig.poll()
    saves_ref = len(rig.saves_seen)
    lift_t = _now()
    kill2 = rig.safe_kill("S3a 解除限值后换进程（等价于人工修好磁盘后重启）")
    lift = {"ts": lift_t, "kind": "rlimit_lifted", "kb": 0, "inject_ts": t,
            "kill": {k: kill2.get(k) for k in ("pid", "killed", "pgid")}}
    lift.update(rig.wait_recover(lift_t, rig.cfg["deadline"], baseline_work=base_work,
                                 signal="valid_lines", saves_ref=saves_ref,
                                 lines_floor=lines_at_tear))
    res["injections"].append(lift)
    return res


def _torn_tail(records_path):
    """末行是否为「无换行结尾的残缺行」（撕裂写入的直接读数）。"""
    try:
        data = Path(records_path).read_bytes()
    except OSError:
        return {"exists": False}
    if not data:
        return {"exists": True, "bytes": 0, "torn": False}
    tail = data.rsplit(b"\n", 1)[-1]
    ok = False
    if tail and tail != data:
        try:
            json.loads(tail.decode("utf-8"))
            ok = True
        except Exception:
            ok = False
    return {"exists": True, "bytes": len(data), "torn": bool(tail and not ok),
            "tail_len": len(tail)}


def sc_S3b(rig):
    """S3b 只读写入点：输出目录 chmod 555 ⇒ progress 原子替换失败。

    基线实测形态（第一版演练发现）：**不是崩溃，是活锁** —— `save_progress` 抛
    PermissionError，被 `_conc_worker` 的 `except Exception` 兜住 → 条目「回队尾」→ 重抓 →
    再抛 …… 进程一直活着、keeper 永远看不到异常、progress 永不落盘、重复抓取不断累积。
    因此本场景额外记录「活锁读数」：故障窗内的条目异常次数 / 成功存档次数 / 是否死亡。
    """
    res = {"injections": [], "expect_audit": {"keeper_restart": 1}}
    rig.wait_progress_saves(1, 60)
    rig.wait_records_bytes(20000, 60)
    base_work, _ = rig.work_evidence()
    saves_ref = len(rig.saves_seen)
    t = _now()
    os.chmod(str(rig.out_dir), 0o555)
    ro = {"ts": t, "kind": "ro_out_dir", "mode": "0555"}
    died = rig.wait_until(lambda: rig.job_alive() is None, 45, label="crash_on_progress_write")
    ro["died_within_s"] = died.get("waited_s") if died.get("ok") else None
    def item_errors():
        e = _log_evidence(rig.out_dir / "scrape.log")
        return e.get("conc_item_error", {}).get("count", 0)
    rig.wait_until(lambda: item_errors() >= 3, 30, poll=1.0, label="livelock_visible")
    time.sleep(2.0)
    ev = _log_evidence(rig.out_dir / "scrape.log")
    ro["livelock"] = {
        "item_errors": ev.get("conc_item_error", {}).get("count", 0),
        "item_error_samples": ev.get("conc_item_error", {}).get("samples", [])[:3],
        "alive_during_fault": rig.job_alive() is not None,
        "progress_during_fault": bool(len(rig.saves_seen) - saves_ref > 0),
        "saves_during_fault": len(rig.saves_seen) - saves_ref,
    }
    ro["mode_before_lift"] = oct(rig.out_dir.stat().st_mode & 0o777)
    os.chmod(str(rig.out_dir), 0o755)
    rig.poll()
    saves_ref = len(rig.saves_seen)      # 基准取「修好写入点的那一刻」
    lift_t = _now()
    ro.update({"lift_ts": lift_t, "crashed": bool(ro["died_within_s"] is not None)})
    ro.update(rig.wait_recover(lift_t, rig.cfg["deadline"], baseline_work=base_work,
                               signal="save", saves_ref=saves_ref))
    ro["inject_ts"] = t
    res["injections"].append(ro)
    return res


def sc_S4(rig):
    """S4 网络断：mock 真关停（连接拒绝）→ 6s 后同端口恢复 → 请求级重试/退避。"""
    res = {"injections": [], "expect_audit": {"retry_scheduled": 1}}
    rig.wait_progress_saves(1, 60)
    rig.wait_records_bytes(20000, 60)
    base_work, _ = rig.work_evidence()
    rig.cfg["_records_at_inject"] = rig.max_records
    t = _now()
    rig.stop_mock()                              # 真·连接拒绝（监听 socket 关闭）
    down = {"ts": t, "kind": "mock_shutdown", "down_s": 6.0}
    res["injections"].append(down)
    time.sleep(6.0)
    rig.start_mock()                             # 同端口恢复
    up = {"ts": _now(), "kind": "mock_restored", "port": rig.port}
    res["injections"].append(up)
    up.update(rig.wait_recover(t, rig.cfg["deadline"], baseline_work=base_work))
    down["recovered_at"] = up.get("recover_s")
    return res


def sc_S5(rig):
    """S5 429 风暴：mock 全量 429 持续 6s → 熔断/降速与恢复（F11/B3/C1）。"""
    res = {"injections": [], "expect_audit": {"circuit_open": 1}}
    rig.wait_progress_saves(1, 60)
    rig.wait_records_bytes(20000, 60)
    base_work, _ = rig.work_evidence()
    rig.cfg["_records_at_inject"] = rig.max_records
    t = _now()
    rig.mock_fault(err429=1.0, note="S5-429-storm")
    storm = {"ts": t, "kind": "err429_storm", "ratio": 1.0, "duration_s": 6.0}
    res["injections"].append(storm)
    time.sleep(6.0)
    rig.mock_fault(clear=1, note="S5-storm-cleared")
    clear = {"ts": _now(), "kind": "storm_cleared"}
    res["injections"].append(clear)
    clear.update(rig.wait_recover(t, rig.cfg["deadline"], baseline_work=base_work))
    storm["recovered_at"] = clear.get("recover_s")
    return res


def sc_S6(rig):
    """S6 探测全灭（**持续**）：mock 对所有探测请求（limit<=1）返回 404 → 块级兜底（C2/F1）。

    为什么持续到收尾（第一版是 8s 后恢复，结论不够）：
      · 只掐 8s：probe_children 的「3 轮 × 30s 退避」就地重试就能恢复（实测零丢失）——
        那是**瞬时**故障的正解，测不到 F1（假收敛）这条 R3 目标路径。
      · 持续掐死：3 轮退避全败 ⇒ 子块列表为空 ⇒ 覆盖=0、缺口=全量 ⇒ 大缺口冷却 → 当场复验
        → 终局修复轮 → 仍无解 ⇒ 基线写「全部完成」并以 exit 0 收尾（**假收敛**，整棵子树静默消失）。
    两个变体都保留：本场景（持续，window 放宽到 170s）与 S6b（瞬时 8s，回归「正解」读数）。
    """
    res = {"injections": [], "expect_audit": {"probe_all_dead": 1, "repair_round_enter": 1}}
    rig.wait_records_bytes(20000, 60)            # 首层「先抓」落盘后再掐探测
    base_work, _ = rig.work_evidence()
    rig.cfg["_records_at_inject"] = rig.max_records
    t = _now()
    rig.mock_fault(probe_status=404, probe_limit=1, note="S6-probe-dead-persistent")
    down = {"ts": t, "kind": "probe_dead_persistent", "status": 404, "restored": False}
    res["injections"].append(down)
    # 等到「块级兜底链条」走完（大缺口冷却/终局修复轮出现），或最多 150s
    def chain():
        e = _log_evidence(rig.out_dir / "scrape.log")
        return (e.get("probe_retry", {}).get("count", 0) >= 3
                and (e.get("big_gap", {}).get("count", 0) >= 1
                     or e.get("final_repair_round", {}).get("count", 0) >= 1))
    rig.wait_until(chain, 150, poll=2.0, label="block_level_fallback_chain")
    ev = _log_evidence(rig.out_dir / "scrape.log")
    down["probe_retry_lines"] = ev.get("probe_retry", {}).get("count", 0)
    down["big_gap_lines"] = ev.get("big_gap", {}).get("count", 0)
    down["final_repair_rounds"] = ev.get("final_repair_round", {}).get("count", 0)
    down["coverage_zero_branches"] = len([s for s in ev.get("branch", {}).get("samples", [])
                                          if str(s[2]) == "0"])
    r = rig.wait_recover(t, rig.cfg["deadline"], baseline_work=base_work)
    r["inject_ts"] = t
    res["injections"].append(r)
    return res


def sc_S6b(rig):
    """S6b 探测全灭（**瞬时** 8s）：验证「就地重试 3 轮 × 30s 退避」这条恢复路径。"""
    res = {"injections": [], "expect_audit": {"probe_all_dead": 1}}
    rig.wait_records_bytes(20000, 60)
    base_work, _ = rig.work_evidence()
    rig.cfg["_records_at_inject"] = rig.max_records
    t = _now()
    rig.mock_fault(probe_status=404, probe_limit=1, note="S6b-probe-dead-transient")
    down = {"ts": t, "kind": "probe_dead", "status": 404, "duration_s": 8.0}
    res["injections"].append(down)
    time.sleep(8.0)
    rig.mock_fault(clear=1, note="S6b-probe-restored")
    up = {"ts": _now(), "kind": "probe_restored"}
    res["injections"].append(up)
    up.update(rig.wait_recover(t, rig.cfg["deadline"], baseline_work=base_work))
    down["recovered_at"] = up.get("recover_s")
    return res


def _torn(rig, path, frac):
    """把 progress 文件截断成「半写」形态（保留前 frac 比例字节）。"""
    p = Path(path)
    if not p.exists():
        return {"path": str(p), "existed": False}
    data = p.read_bytes()
    keep = max(1, int(len(data) * frac))
    p.write_bytes(data[:keep])
    return {"path": str(p), "existed": True, "bytes_before": len(data), "bytes_after": keep}


def sc_S7a(rig):
    """S7a progress.json 半写（.bak 完好）⇒ 应回退备份槽（A3）。"""
    res = {"injections": [], "expect_audit": {"progress_recovered": 1}}
    rig.wait_progress_saves(2, 90)               # 至少两次保存 ⇒ .bak 槽里有一份好状态
    rig.wait_records_bytes(20000, 60)
    base_work, _ = rig.work_evidence()
    rig.cfg["_records_at_inject"] = rig.max_records
    kill = rig.safe_kill("S7a 截断前先停机（避免在跑的进程立刻覆写好状态）")
    torn = _torn(rig, rig.progress_path(), 0.6)
    t = _now()
    res["injections"].append({"ts": t, "kind": "torn_progress", "kill": kill, "torn": torn})
    r = rig.wait_recover(t, rig.cfg["deadline"], killed_pid=kill.get("pid"),
                         baseline_work=base_work)
    r["inject_ts"] = t
    res["injections"].append(r)
    return res


def sc_S7b(rig):
    """S7b progress.json + .bak 双损 ⇒ 应从 records.jsonl 全量反查重建（A3/E1）。"""
    res = {"injections": [], "expect_audit": {"progress_recovered": 1}}
    rig.wait_progress_saves(2, 90)
    rig.wait_records_bytes(20000, 60)
    base_work, _ = rig.work_evidence()
    rig.cfg["_records_at_inject"] = rig.max_records
    kill = rig.safe_kill("S7b 截断前先停机")
    torn1 = _torn(rig, rig.progress_path(), 0.5)
    torn2 = _torn(rig, rig.out_dir / "progress.json.bak", 0.5)
    t = _now()
    res["injections"].append({"ts": t, "kind": "torn_progress_and_bak", "kill": kill,
                              "torn": [torn1, torn2]})
    r = rig.wait_recover(t, rig.cfg["deadline"], killed_pid=kill.get("pid"),
                         baseline_work=base_work)
    r["inject_ts"] = t
    res["injections"].append(r)
    return res


SCENARIOS = {
    "REF": {"title": "干净参照跑（无故障）：作为 unique 零丢失的黄金基线",
            "run": None, "expect_audit": {}},
    "S1": {"title": "kill -9 循环 ×3 → 恢复时限 + 数据完整性（C3/A2）", "run": sc_S1,
           "inject_kind": "kill -9", "expect_audit": {"keeper_restart": 3}},
    "S2": {"title": "运行中 rm -rf 输出目录 → 目录自愈（F3）", "run": sc_S2,
           "inject_kind": "rm -rf", "expect_audit": {"keeper_restart": 1}},
    "S3a": {"title": "磁盘写失败 RLIMIT_FSIZE → 短写处理与恢复（F3 邻域）", "run": sc_S3a,
            "inject_kind": "rlimit_fsize", "expect_audit": {"keeper_restart": 1}},
    "S3b": {"title": "输出目录只读（写入点不可用）→ 崩溃后恢复", "run": sc_S3b,
            "inject_kind": "ro_dir", "expect_audit": {"keeper_restart": 1}},
    "S4": {"title": "mock 关停/连接拒绝 → 请求级重试与恢复（C1）", "run": sc_S4,
           "inject_kind": "network_down", "expect_audit": {"retry_scheduled": 1}},
    "S5": {"title": "429 风暴 → 熔断/降速与恢复（C1/F11/B3）", "run": sc_S5,
           "inject_kind": "err429_storm", "expect_audit": {"circuit_open": 1}},
    "S6": {"title": "探测全灭（持续）→ 块级兜底与不得假收敛（C2/F1）", "run": sc_S6,
           "inject_kind": "probe_all_dead_persistent", "window": 170,
           # R3-P4：审计窗（契约 §5.2 的 deadline 项）。S6 的探测阶梯 = PROBE_ROUNDS(3)×
           # PROBE_DEAD_DELAY_S(30) × 重试上限，修复轮**必然晚于** 180s 恢复时限；
           # 若沿用 deadline=180 会把真实发生的 repair_round_enter 判成"窗外"
           # （verification §5.4 明确建议给 S6 单列更宽的 audit window）。
           "audit_window_s": 900,
           "expect_audit": {"probe_all_dead": 1, "repair_round_enter": 1}},
    "S6b": {"title": "探测全灭（瞬时 8s）→ 就地重试 3 轮 × 30s 退避（C2）", "run": sc_S6b,
            "inject_kind": "probe_all_dead", "window": 130,
            "expect_audit": {"probe_all_dead": 1}},
    "S7a": {"title": "progress.json 半写（.bak 完好）→ 备份槽回退（A3）", "run": sc_S7a,
            "inject_kind": "torn_progress", "expect_audit": {"progress_recovered": 1}},
    "S7b": {"title": "progress.json + .bak 双损 → records 全量反查重建（A3/E1）", "run": sc_S7b,
            "inject_kind": "torn_progress", "expect_audit": {"progress_recovered": 1}},
}


# ===========================================================================
# 断言
# ===========================================================================
def _a(name, ok, value=None, limit=None, detail="", soft=False):
    return {"name": name, "ok": bool(ok), "value": value, "limit": limit,
            "detail": detail, "soft": bool(soft)}


def evaluate(rig, sid, result, reference, verifier_verdict):
    A = []
    injs = result.get("injections") or []
    recs = [i for i in injs if i.get("recover_s") is not None]
    deadline = rig.cfg["deadline"]

    # 1) 恢复时限
    if sid == "S1":
        performed = [i for i in injs if i.get("killed")]
        recovered = [i for i in performed if i.get("recover_s") is not None]
        worst = max([i["recover_s"] for i in recovered], default=None)
        ok = bool(performed) and len(recovered) == len(performed) and \
            all(i["recover_s"] <= deadline for i in recovered)
        A.append(_a("recovery_within_deadline", ok,
                    value=[i.get("recover_s") for i in performed],
                    limit=deadline,
                    detail="每一次**实际做过的** kill -9 后「重新干活」耗时（s）；"
                           "做过 %d 次、成功恢复 %d 次" % (len(performed), len(recovered))))
        # R3-P4·B(d)：原为 soft ⇒「3 次 kill 降到 1 次仍判 PASS」（crit-adversarial d2）。
        # 现按「不达下限即 FAIL」：不足 3 轮 = 系统中途已无作业可杀 = 假收敛嫌疑。
        A.append(_a("kill_loop_3rounds", len(performed) >= 3, value=len(performed), limit=3,
                    detail="要求的 3 轮 kill 是否都打到了活着的进程；不足 3 轮说明系统在中途"
                           "已无作业可杀（往往是假收敛）—— 硬判据（不达下限即 FAIL）"))
        # d3：注入后「直接宣布完成」而无重新干活的证据 = 假收敛嫌疑（只记录不判死，理由见回执：
        # 合法早收敛场景与假收敛在此字段上不可区分，硬判会误杀干净参照跑）
        suspect = [_i for _i in performed
                   if _i.get("completed_after_inject") and _i.get("recover_s") is None]
        A.append(_a("recovery_no_false_convergence_suspect", not suspect,
                    value=len(suspect), limit=0, soft=True,
                    detail="kill 后未观测到重新干活、却已出现完成标记的注入次数（假收敛嫌疑；"
                           "软读数：硬面由 kill_loop_3rounds + no_unique_loss 承担）"))
    elif recs:
        worst = max(i["recover_s"] for i in recs)
        A.append(_a("recovery_within_deadline", worst <= deadline, value=worst, limit=deadline,
                    detail="注入 → 恢复干活耗时（s）"))
    else:
        A.append(_a("recovery_within_deadline", False, value=None, limit=deadline,
                    detail="未观测到恢复（观察窗内没有重新干活）"))

    # 2) 数据完整性（相对干净参照跑）
    integ = result["integrity"]
    lost = integ["unique_loss"]
    A.append(_a("no_unique_loss", lost == 0, value=lost, limit=0,
                detail="相对参照跑缺失的 unique mms 数（examples=%s）"
                       % (integ.get("lost_examples") or [])[:3]))
    A.append(_a("dup_leaves_within_tolerance", integ["dup_leaf_sort_pairs"] <= 1,
                value=integ["dup_leaf_sort_pairs"], limit=1,
                detail="重复抓取的叶子块（mock 侧 (prefix,sort) 整页请求 >1 次的对数）"))
    A.append(_a("records_wellformed", integ["invalid_lines"] == 0,
                value=integ["invalid_lines"], limit=0, detail="records.jsonl 末态损坏行数"))
    A.append(_a("run_completed", bool(result["final_snapshot"]["complete"]), value=None,
                limit=None, soft=(sid in ("S6", "S6b")),
                detail="scrape.log 出现完成标记（S6/S6b 单列为软读数：跑完≠收敛对，"
                       "假收敛由 no_unique_loss 判）"))

    # 2b) 写入点类故障：**不得活锁**（进程不死、keeper 看不见、永不落盘、撕裂行不修）
    if sid in ("S3a", "S3b"):
        ll = next((i.get("livelock") for i in injs if i.get("livelock")), {}) or {}
        errs = ll.get("item_errors", 0)
        alive = ll.get("alive_during_fault")
        prog = ll.get("progress_during_fault")
        livelock = bool(errs and alive and not prog)
        A.append(_a("no_silent_livelock", not livelock,
                    value={"item_errors": errs, "alive": alive, "progress": prog,
                           "samples": ll.get("item_error_samples")},
                    limit={"item_errors": 0, "alive": True, "progress": True},
                    detail="故障窗内：条目异常次数 / 进程是否存活 / 是否仍有落盘进展。"
                           "「异常刷屏 + 进程存活 + 零进展」= 活锁（keeper 与审计全程无感）"))

    # 3) 审计链路（audit-verify v2 判定）
    #    R3-P4·B(c)：原判据 = bool(vv["pass"])，而 v1 允许 derived 通道顶替 ⇒ 真实 S1 里
    #    canonical 审计 0 条、strict_pass=False 仍 audit_link=True（crit-adversarial d1）。
    #    现改为消费 v2 的 canonical_pass（= 全部链路断言由 audit 通道在**时间窗内**满足）。
    vv = verifier_verdict or {}
    is_v2 = str(vv.get("verifier") or "").startswith("audit-verify-v2")
    canon_ok = bool(vv.get("canonical_pass")) if is_v2 else bool(vv.get("pass"))
    win_obj = vv.get("window") or {}
    A.append(_a("audit_link", canon_ok, value=vv.get("pass_count"),
                limit=vv.get("check_count"),
                detail="audit-verify%s：canonical 通道硬通过；channel=%s；window_enforced=%s；%s"
                       % ("-v2" if is_v2 else "-v1(降级)", vv.get("channel"),
                          win_obj.get("enforced"),
                          "; ".join(vv.get("fail_reasons") or [])[:400])))
    A.append(_a("audit_channel_audit_only", bool(vv.get("audit_only")), value=vv.get("channel"),
                limit="audit", soft=True,
                detail="链路断言是否全部由结构化审计通道满足；derived-only 会被标注"
                       "channel=derived / evidence_level=low 并降级"))
    A.append(_a("audit_schema_strict",
                (bool((vv.get("schema") or {}).get("strict_pass")) if is_v2
                 else bool((vv.get("schema") or {}).get("strict_pass"))),
                value=((vv.get("schema") or {}).get("v2_records")
                       if is_v2 else (vv.get("schema") or {}).get("deviation_count")),
                limit=0, soft=True,
                detail="R3 契约 v2 严格布局校验（schema=r3-audit-v2 / ts_epoch / pid / "
                       "seq 从 1 连续 / level / event / detail）"))

    # 4) 场景专属读数（人读用，不参与判定）
    ev = result.get("log_evidence") or {}
    result["scenario_readings"] = {
        "probe_retry_lines": ev.get("probe_retry", {}).get("count"),
        "zero_child_lines": len([s for s in ev.get("branch", {}).get("samples", [])
                                 if str(s[2]) == "0"]),
        "branch_lines": ev.get("branch", {}).get("count"),
        "throttle_adjust_lines": ev.get("throttle_adjust", {}).get("count"),
        "http_retry_lines": ev.get("http_retry", {}).get("count"),
        "exc_lines": ev.get("exc", {}).get("count"),
        "progress_fallback_lines": ev.get("progress_fallback", {}).get("count"),
        "records_repair_lines": ev.get("records_repair", {}).get("count"),
        "records_rebuild_lines": ev.get("records_rebuild", {}).get("count"),
        "final_repair_rounds": ev.get("final_repair_round", {}).get("count"),
        "conc_item_errors": ev.get("conc_item_error", {}).get("count"),
        "torn_requeue_entries": (result["final_snapshot"].get("progress") or {}
                                 ).get("torn_requeue"),
    }
    return A


# ===========================================================================
# 单场景执行
# ===========================================================================
def run_scenario(sid, system, cfg):
    spec = SCENARIOS[sid]
    run_dir = Path(cfg["run_root"]) / sid
    rig = Rig(sid, system, run_dir, cfg)
    _ACTIVE["rig"] = rig                      # 断电/中断时由信号处理器收尾
    result = {"scenario": sid, "title": spec["title"], "system": system["name"],
              "run_id": cfg["run_id"], "started": _iso(), "t0": _now(),
              "window_s": cfg["window"], "deadline": cfg["deadline"],
              "sandbox": None, "injections": [], "notes": [], "errors": []}
    try:
        paths = rig.setup()
        result["sandbox"] = {k: v for k, v in paths.items() if k != "rewrite"}
        result["sandbox"]["rewrite"] = paths["rewrite"]
        _mkdirs(rig)
        result["mock"] = rig.start_mock()
        result["snapshots"] = {"pre": rig.snapshot("pre-keeper")}
        rig.start_keeper()
        result["snapshots"]["keeper_up"] = rig.snapshot("keeper-up")
        if spec["run"] is not None:
            obs = spec["run"](rig) or {}
            result["observations"] = obs
            # 场景函数返回的注入时间线直接进入顶层字段：断言 / 审计链路窗口 / 报告都读它
            result["injections"] = obs.get("injections") or []
            if obs.get("expect_audit"):
                result["expect_audit"] = obs["expect_audit"]
        # R3-P4·B(b)：CLI 显式 --window 优先于场景声明（原实现静默忽略 CLI，见 verification §5.2）
        spec_win = spec.get("window")
        if spec_win and cfg.get("window_explicit") and float(spec_win) != float(cfg["window"]):
            print("WARN: 场景 %s 自带 window=%ss，被 CLI --window %ss 覆盖"
                  % (sid, spec_win, cfg["window"]), flush=True)
            win = float(cfg["window"])
        else:
            win = float(spec_win or cfg["window"])         # 场景可自带更宽的观察窗
        result["window_s"] = win
        result["window_source"] = ("cli" if cfg.get("window_explicit") else
                                   ("spec" if spec_win else "default"))
        done = rig.wait_complete(win)
        result["complete_wait"] = done
        if not done["ok"]:
            result["errors"].append("观察窗内未出现完成标记（window=%ss）" % win)
        result["snapshots"]["final"] = rig.snapshot("final")
    except Exception as e:                                     # noqa: BLE001
        import traceback
        result["errors"].append("%s: %s" % (type(e).__name__, e))
        result["traceback"] = traceback.format_exc()[-2000:]
    finally:
        try:
            rig.teardown()
        except Exception as e:                                  # noqa: BLE001
            result["errors"].append("teardown: %s: %s" % (type(e).__name__, e))
        result["notes"] = rig.notes
        result["ended"] = _iso()
        result["wall_s"] = round(_now() - result["t0"], 2)
        result["final_snapshot"] = rig.snapshot("final-after-teardown")
        result["log_evidence"] = _log_evidence(rig.out_dir / "scrape.log")
        try:
            result["mock_stats_file"] = json.loads(
                _read_text(rig.logs / "mock-stats.json") or "null")
        except ValueError:
            result["mock_stats_file"] = None
        result["audit_files"] = [str(p) for p in
                                 [run_dir / "audit.jsonl", rig.out_dir / "audit.jsonl",
                                  rig.out_dir / "memguard.jsonl"] if Path(p).exists()]
        result["_rig_dir"] = str(run_dir)
    _ACTIVE["rig"] = None
    return result, rig


def _integrity(result, reference, rig):
    """relative-to-reference 完整性读数（旁路；不依赖被测自报）。"""
    run_mms = _mms_set(Path(rig.out_dir) / "records.jsonl")
    ref = set(reference.get("unique_mms") or [])
    cur = set(run_mms["unique"])
    lost = sorted(ref - cur)
    req = _mock_req_stats(Path(rig.logs) / "mock-requests.jsonl")
    ref_req = reference.get("mock") or {}
    return {
        "reference_unique": len(ref), "unique": len(cur), "unique_loss": len(lost),
        "lost_examples": lost[:10],
        "extra_mms": len(cur - ref),
        "dup_mms": run_mms["dup_mms"], "lines": run_mms["lines"],
        "invalid_lines": run_mms["invalid_lines"],
        "dup_leaf_sort_pairs": req["dup_leaf_sort_pairs"],
        "dup_leaf_examples": req["dup_examples"],
        "requests_total": req["requests"],
        "requests_vs_reference": (round(req["requests"] / ref_req["requests"], 3)
                                  if ref_req.get("requests") else None),
        "probe_requests": req["probe"], "leaf_page_requests": req["leaf_page"],
        "http_status": req["status"], "reasons": req["reason"],
    }


def _evidence_payload(res):
    """构造 evidence.json 内容（= result 去掉 log_evidence 大表 + 计数摘要）。

    R3-P4·B(a)：**注入时间线必须在校验器读取前落盘**。另按契约 §5.2 声明本场景的
    审计窗（`audit_window_s`）：探测阶梯类场景（S6/S6b）的修复轮必然晚于恢复时限，
    沿用 deadline 会把真实事件判成"窗外"。
    """
    ev = {k: v for k, v in res.items() if k != "log_evidence"}
    ev["log_evidence_counts"] = {k: v["count"] for k, v in
                                 (res.get("log_evidence") or {}).items()}
    ev["verifier"] = str(VERIFIER.name)
    spec = SCENARIOS.get(res.get("scenario")) or {}
    win_key = spec.get("audit_window_s")
    if win_key:
        ev["audit_window_s"] = float(win_key)
        ev["audit_window_source"] = "scenario:%s" % res.get("scenario")
    return ev


def write_evidence(res):
    """把 evidence.json 原子写到场景目录（先写临时文件再 replace，避免半写被读到）。"""
    d = Path(res["_rig_dir"])
    payload = _evidence_payload(res)
    tmp = d / "evidence.json.tmp"
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(str(tmp), str(d / "evidence.json"))
    return payload


def run_audit_verify(run_dir, sid, out_json):
    if not VERIFIER.exists():
        return {"pass": False, "fail_reasons": ["audit-verify.py 缺失"]}
    cmd = [sys.executable, str(VERIFIER), "--scenario", sid, "--scenario-dir", str(run_dir),
           "--json", str(out_json)]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        return {"pass": False, "fail_reasons": ["audit-verify 超时"]}
    verdict = {}
    try:
        verdict = json.loads(_read_text(out_json) or "{}")
    except ValueError:
        pass
    verdict.setdefault("exit_code", p.returncode)
    verdict.setdefault("stdout_tail", (p.stdout or "")[-800:])
    if p.returncode != 0:
        verdict["pass"] = False
    return verdict


def _reference(system, cfg, force=False):
    cache = RESULTS_DIR / ("reference-%s.json" % system["name"])
    if cache.exists() and not force:
        try:
            d = json.loads(cache.read_text(encoding="utf-8"))
            if d.get("system") == system["name"] and d.get("mode") == "clean-converged":
                return d
        except ValueError:
            pass
    print("[ref] 干净参照跑（无故障）...")
    res, rig = run_scenario("REF", system, cfg)
    snap = res["final_snapshot"]
    mms = _mms_set(Path(rig.out_dir) / "records.jsonl")
    req = _mock_req_stats(Path(rig.logs) / "mock-requests.jsonl")
    d = {"system": system["name"], "ts": _iso(), "run_id": cfg["run_id"],
         "run_dir": res["_rig_dir"], "wall_s": res["wall_s"],
         "mode": "clean-converged" if snap["complete"] else "clean-unfinished",
         "unique_mms": mms["unique"], "lines": mms["lines"],
         "invalid_lines": mms["invalid_lines"], "dup_mms": mms["dup_mms"],
         "mock": req, "records_bytes": snap["records"],
         "progress": snap["progress"], "log_evidence": {
             k: v["count"] for k, v in res["log_evidence"].items()}}
    cache.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
    return d


# ===========================================================================
# 报告
# ===========================================================================
def default_report_path(system_name):
    """基线系统 → report-baseline.md（验收脚本读这个名）；其它系统 → report-<name>.md。
    否则跑一次别的系统的冒烟演练会把基线报告覆盖掉（实测踩过）。"""
    return HERE / ("report-baseline.md" if system_name == "baseline-r2cand"
                   else "report-%s.md" % system_name)


def default_summary_path(system_name):
    return RESULTS_DIR / ("summary.json" if system_name == "baseline-r2cand"
                          else "summary-%s.json" % system_name)


def write_report(system, reference, results, out_path, run_id):
    L = []
    A = L.append
    A("# 演练基线报告（故障注入 · 修复前对照读数）\n")
    A("> 生成：%s ｜ 演练台：`R3/impl/slot-drill/drill.py` ｜ 校验器：`audit-verify.py` ｜ run_id `%s`\n"
      % (_iso(), run_id))
    A("被测系统：**%s**" % system.get("label", system["name"]))
    A("")
    run_ids = sorted({r.get("run_id") for r in results if r.get("run_id")})
    if len(run_ids) > 1:
        A("> 本轮读数为**分批执行**后合并（宿主侧长跑作业会被回收 ⇒ 拆批跑，`--merge` 合并；"
          "同一系统 / 同一语料 / 同一判据）：")
        for rid in run_ids:
            got = [r["scenario"] for r in results if r.get("run_id") == rid]
            A("> - `%s`：%s" % (rid, ", ".join(got)))
        A("")
    A("| 成员 | 路径 | sha256(12) |")
    A("|---|---|---|")
    for k in ("scrape", "memguard", "heartbeat", "keeper"):
        if system.get(k):
            A("| %s | `%s` | `%s` |" % (k, system[k], (system.get("_hashes") or {}).get(k)))
    for d in system.get("_degraded") or []:
        A("\n> ⚠ 成员降级：%s 期望 `%s`，实际用 `%s`" % (d["member"], d["wanted"], d["using"]))
    for m in system.get("_missing") or []:
        A("\n> ⚠ 成员缺失：%s = `%s`" % (m["member"], m["path"]))
    A("")
    A("## 0. 判定口径")
    A("- **恢复时限**：注入 → 新进程被拉起且**重新出现工作证据**（scrape.log 工作行增加 /"
      " records 增长 / 完成标记）；判据 ≤ %ds（A2）。" % DEFAULT_DEADLINE)
    A("- **unique 零丢失**：场景末态 records.jsonl 的 unique mms ⊇ 干净参照跑集合（旁路比对）。")
    A("- **重复抓取 ≤1 叶子块**：mock 侧统计 `(prefix,sort)` 整页请求（limit≥500）出现 >1 次的对数；"
      "干净跑应为 0。")
    A("- **审计链路**：`audit-verify.py` 按 `refs/r3-audit-schema.md` 校验字段/枚举/seq 单调 +"
      " 场景→事件链路断言。")
    A("")
    # 人工解读（可选文件）：与机器读数分开维护，便于审阅/修订；缺失时报告仍自洽
    findings = HERE / "findings-baseline.md"
    if findings.exists():
        A("")
        A("---")
        A("")
        A(_read_text(findings).rstrip())
        A("")
        A("---")
        A("")
    if reference:
        A("## 1. 干净参照跑（黄金基线）")
        A("")
        A("| 项 | 读数 |")
        A("|---|---|")
        A("| 收敛 | %s |" % reference.get("mode"))
        A("| wall | %ss |" % reference.get("wall_s"))
        A("| unique mms | %s |" % len(reference.get("unique_mms") or []))
        A("| records 行 | %s |" % reference.get("lines"))
        A("| 损坏行 | %s |" % reference.get("invalid_lines"))
        A("| 重复 mms | %s |" % reference.get("dup_mms"))
        A("| mock 请求数 | %s（探测 %s / 整页 %s）|"
          % ((reference.get("mock") or {}).get("requests"),
             (reference.get("mock") or {}).get("probe"),
             (reference.get("mock") or {}).get("leaf_page")))
        A("| 重复叶子块 | %s |" % (reference.get("mock") or {}).get("dup_leaf_sort_pairs"))
        A("")
    A("## 2. 场景总表（修复前）")
    A("")
    A("| 场景 | 注入 | 恢复(s) | 时限 | unique 丢失 | 重复叶子块 | 完成 | 审计链路 | schema 严格 | 判定 |")
    A("|---|---|---|---|---|---|---|---|---|---|")
    for r in results:
        if r["scenario"] == "REF":
            continue
        integ = r.get("integrity") or {}
        rec = [i.get("recover_s") for i in (r.get("injections") or [])
               if i.get("recover_s") is not None]
        recs = ", ".join(str(x) for x in rec) if rec else "—"
        av = (r.get("audit_verify") or {})
        A("| %s | %s | %s | %s | %s | %s | %s | %s | %s | **%s** |" % (
            r["scenario"], (SCENARIOS[r["scenario"]].get("inject_kind") or "—"), recs,
            r["deadline"], integ.get("unique_loss"), integ.get("dup_leaf_sort_pairs"),
            "✓" if (r.get("final_snapshot") or {}).get("complete") else "✗",
            "✓" if av.get("pass") else "✗",
            "✓" if (av.get("schema") or {}).get("strict_pass") else "✗",
            r.get("verdict", "?")))
    A("")
    A("## 3. 逐场景明细")
    for r in results:
        if r["scenario"] == "REF":
            continue
        sid = r["scenario"]
        A("")
        A("### %s · %s" % (sid, SCENARIOS[sid]["title"]))
        A("")
        A("- 判定：**%s**（wall %ss，错误 %s；系统 `%s`，run `%s`）"
          % (r.get("verdict"), r.get("wall_s"), r.get("errors") or "无",
             r.get("system"), r.get("run_id")))
        integ = r.get("integrity") or {}
        A("- 完整性：unique %s/%s（丢失 %s，多余 %s）；损坏行 %s；重复 mms %s；"
          "重复叶子块 %s；mock 请求 %s（相对参照 ×%s）"
          % (integ.get("unique"), integ.get("reference_unique"), integ.get("unique_loss"),
             integ.get("extra_mms"), integ.get("invalid_lines"), integ.get("dup_mms"),
             integ.get("dup_leaf_sort_pairs"), integ.get("requests_total"),
             integ.get("requests_vs_reference")))
        for i in r.get("injections") or []:
            if i.get("kind") == "rlimit_fsize":
                A("- 注入：`RLIMIT_FSIZE=%sKB`（records 已 %s 字节；限值 = 现量+8KB，保证写跨界产生"
                  "**半条记录**）；死亡观测=%s（%ss）；死亡后 records=%s；撕裂末行=%s"
                  % (i.get("kb"), i.get("records_bytes"), i.get("death_observed"),
                     i.get("death_waited_s"), i.get("records_bytes_after_death"),
                     i.get("torn_tail")))
            elif i.get("killed"):
                A("- 注入：kill -9 pid=%s（pgid %s）→ 重启 %ss / 恢复 %ss（新 pid %s）；"
                  "恢复靠干活=%s，靠完成标记=%s"
                  % (i.get("pid"), i.get("pgid"), i.get("restart_s"), i.get("recover_s"),
                     i.get("new_pid"), i.get("work_resumed"), i.get("completed_after_inject")))
            elif i.get("skipped"):
                A("- 注入跳过：%s（%s）" % (i.get("skipped"), i.get("note")))
            elif i.get("kind"):
                extra = ""
                if i.get("kind") == "rm_rf_out_dir":
                    extra = ("；进程在「写已 unlink inode」的静默窗口里活了 %ss，"
                             "黑洞字节 %s（这段工作全部作废）"
                             % (i.get("ghost_window_s"), i.get("records_invisible_bytes")))
                elif i.get("kind") == "ro_out_dir":
                    ll = i.get("livelock") or {}
                    extra = ("；死亡=%s；活锁读数 %s" % (i.get("died_within_s"), ll))
                A("- 注入：%s%s → 恢复 %ss%s"
                  % (i.get("kind"),
                     (" (" + str(i.get("duration_s")) + "s)") if i.get("duration_s") else "",
                     i.get("recover_s"), extra))
            elif i.get("refused"):
                A("- ⚠ SAFE_KILL 拒绝：%s" % (i.get("verify")))
        sr = r.get("scenario_readings") or {}
        A("- 场景读数：`%s`" % json.dumps(sr, ensure_ascii=False))
        av = r.get("audit_verify") or {}
        A("- 审计：文件 %s" % (r.get("audit_files") or "无"))
        A("- 审计判定：%s" % json.dumps(
            {k: av.get(k) for k in ("pass", "pass_count", "check_count", "fail_reasons",
                                    "missing_events", "derived_only")}, ensure_ascii=False))
        A("- 断言：")
        for a in r.get("assertions") or []:
            A("    - [%s] %s：value=%s limit=%s%s — %s"
              % ("PASS" if a["ok"] else "FAIL", a["name"], a["value"], a["limit"],
                 " (soft)" if a.get("soft") else "", a.get("detail")))
        if r.get("errors"):
            A("- 错误：%s" % r["errors"])
    A("")
    A("## 4. 机读产物")
    A("- `results/summary.json` + `%s`（全场景结构化读数；基线用 `summary.json`）"
      % default_summary_path(system["name"]).name)
    A("- 每场景目录：`runs/<run_id>/<SID>/`（sandbox.json / evidence.json / verdict.json / "
      "audit.jsonl / out/ / logs/）")
    Path(out_path).write_text("\n".join(L) + "\n", encoding="utf-8")
    return out_path


# ===========================================================================
# main
# ===========================================================================
def prune_runs(keep=None, current=None):
    keep = int(keep if keep is not None else os.environ.get("DRILL_KEEP_RUNS", "10"))
    if not RUNS_DIR.exists():
        return
    # 只清「有读数」的历史目录：演练台每次调用都会建 run 目录，把 --merge-only/--report
    # 之类的空目录算进来会把真正的现场挤出保留窗口（实测踩过：S6b/S7a/S7b 的证据目录被误删）。
    dirs = sorted([d for d in RUNS_DIR.iterdir()
                   if d.is_dir() and d.name.startswith("drill-")
                   and any(d.glob("*/result.json"))],
                  key=lambda d: d.name)
    for d in dirs[:-keep] if len(dirs) > keep else []:
        if current and d.name == current:
            continue
        import shutil
        shutil.rmtree(d, ignore_errors=True)


def cleanup_stale(verbose=True):
    """清掉**本槽历史演练**遗留的自建进程（断电/中断后的孤儿）。

    红线级保护：只有 /proc/<pid>/cmdline **命中某个 runs/<run_id>/ 目录路径**的进程才会被处理
    —— 这些路径是本槽专属的，别的进程不可能匹配；非自建进程一律不碰，只报告。
    """
    killed, refused = [], []
    if not RUNS_DIR.exists():
        return {"killed": killed, "refused": refused}
    for run_dir in sorted(RUNS_DIR.glob("drill-*")):
        marker = str(run_dir)
        for pf in list((run_dir / "state" / "pid").glob("*.pid")) + \
                list((run_dir / "state").glob("*.pid")):
            m = re.search(r"pid=(\d+)", _read_text(pf))
            if not m:
                continue
            pid = int(m.group(1))
            base = Path("/proc/%d" % pid)
            if not base.exists():
                continue
            cmd = _read_text(base / "cmdline").replace("\0", " ").strip()
            if marker in cmd:
                try:
                    pgid = os.getpgid(pid)
                    os.killpg(pgid if pgid == pid else pid, signal.SIGKILL)
                    killed.append({"pid": pid, "cmd": cmd[:120], "run_dir": marker})
                except OSError as e:
                    refused.append({"pid": pid, "why": str(e)})
            else:
                refused.append({"pid": pid, "why": "cmdline 不含本槽 run 目录，拒绝处理",
                                "cmd": cmd[:120]})
    if verbose and (killed or refused):
        print("[cleanup] 清除自建遗留进程 %d 个；拒绝 %d 个" % (len(killed), len(refused)))
        for k in killed:
            print("    killpg pid=%s %s" % (k["pid"], k["cmd"][:90]))
        for r in refused:
            print("    ⚠ 拒绝 pid=%s（%s）" % (r["pid"], r["why"]))
    return {"killed": killed, "refused": refused}


_ACTIVE = {"rig": None}


def _signal_cleanup(signum, frame):                      # noqa: ARG001
    rig = _ACTIVE.get("rig")
    if rig is not None:
        try:
            rig.teardown()
        except Exception:
            pass
    os._exit(128 + int(signum))


def install_signal_handlers():
    """只接管 SIGINT/SIGTERM（收尾后退出）。

    **不接管 SIGHUP**：后台跑演练时 nohup 依赖 SIGHUP 被忽略；上一版把 SIGHUP 也接管成
    「收尾即退出」，结果父 shell 一结束就把长跑演练带走（实测：S1 跑完、S2 未开始即消失）。
    """
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(s, _signal_cleanup)
        except (ValueError, OSError):
            pass


def main(argv=None):
    ap = argparse.ArgumentParser(description="R3 自愈演练台（故障注入 + 审计校验）")
    ap.add_argument("--system", default="baseline-r2cand")
    ap.add_argument("--only", default=None, help="只跑指定场景（逗号分隔，如 S1,S6）")
    ap.add_argument("--all", action="store_true", help="全场景（默认）")
    ap.add_argument("--no-reference", action="store_true", help="不跑/不复用干净参照跑")
    ap.add_argument("--refresh-reference", action="store_true")
    ap.add_argument("--window", type=float, default=None,
                    help="单场景观察窗（s）。R3-P4：显式给出时**覆盖**场景自带 window "
                         "（默认 %s；原先被场景声明静默忽略，见 verification §5.2）"
                         % DEFAULT_WINDOW)
    ap.add_argument("--deadline", type=float, default=DEFAULT_DEADLINE)
    ap.add_argument("--keeper-tick", type=float, default=None)
    ap.add_argument("--seeds", default="".join(SEEDS))
    ap.add_argument("--scrape", default=None, help="覆盖被测 scrape.py")
    ap.add_argument("--keeper", default=None, help="覆盖 keeper 脚本（如 R3 slot-keeper3 的 keeper-v3.sh）")
    ap.add_argument("--memguard", default=None)
    ap.add_argument("--heartbeat", default=None)
    ap.add_argument("--env", action="append", default=[], help="额外环境变量 K=V（可多次）")
    ap.add_argument("--report", action="store_true", help="只用最近一次 results 重出报告")
    ap.add_argument("--merge", action="store_true",
                    help="与上一次 summary.json 按场景合并（分批跑/单场景复跑时保留其它读数）")
    ap.add_argument("--merge-only", action="store_true",
                    help="不跑场景，只用 --merge/--adopt 的读数重建 summary + 报告")
    ap.add_argument("--reverify", action="store_true",
                    help="只用当前 audit-verify 重判已有读数（校验器判据升级后同步报告，不重跑演练）")
    ap.add_argument("--adopt", action="append", default=[],
                    help="吸收指定历史 run_id 下已完成的 <SID>/result.json（可多次；"
                         "用于长跑被打断后的续接，不重跑已完成的场景）")
    ap.add_argument("--list-systems", action="store_true")
    ap.add_argument("--list-scenarios", action="store_true")
    ap.add_argument("--out", default=None, help="报告输出路径（默认 report-baseline.md）")
    args = ap.parse_args(argv)

    if args.list_scenarios:
        for sid, spec in SCENARIOS.items():
            print("%-5s %s" % (sid, spec["title"]))
        return 0

    systems = load_systems()
    if args.list_systems:
        for name, d in systems.items():
            ok = all(Path(d[k]).exists() for k in ("scrape", "keeper") if d.get(k))
            print("%-22s %-8s %s" % (name, "READY" if ok else "NOT-READY", d.get("label", "")))
            for k in ("scrape", "memguard", "heartbeat", "keeper"):
                if d.get(k):
                    mark = "ok" if Path(d[k]).exists() else "MISSING"
                    print("    %-10s [%s] %s" % (k, mark, d[k]))
        return 0

    if args.report:
        sname = args.system
        summ = default_summary_path(sname)
        if not summ.exists():
            raise SystemExit("没有 %s，先跑一次演练（或 --merge-only）" % summ)
        d = json.loads(summ.read_text(encoding="utf-8"))
        system = resolve_system(d.get("system", {}).get("name", sname), {})
        out = write_report(d["system"], d.get("reference"), d["results"],
                           Path(args.out) if args.out else default_report_path(system["name"]),
                           d["run_id"])
        print("报告已生成：%s" % out)
        return 0

    overrides = {"scrape": args.scrape, "keeper": args.keeper, "memguard": args.memguard,
                 "heartbeat": args.heartbeat}
    system = resolve_system(args.system, overrides)
    if args.keeper_tick:
        system["keeper_tick"] = args.keeper_tick
    if not system["_ready"]:
        raise SystemExit("系统 %s 不完整（缺成员：%s）" % (args.system, system["_missing"]))
    if system["_degraded"]:
        print("[warn] 成员降级：%s" % system["_degraded"])

    env_extra = {}
    for kv in args.env:
        k, _, v = kv.partition("=")
        env_extra[k] = v

    run_id = "drill-%s-%d" % (datetime.now().strftime("%Y%m%dT%H%M%S"), os.getpid())
    run_root = RUNS_DIR / run_id
    install_signal_handlers()
    cleanup_stale()
    prune_runs(current=run_id)   # 默认保留最近 10 次（DRILL_KEEP_RUNS 可调，0=不清理）
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    # 不跑场景的调用（--report / --merge-only / --reverify）不建 run 目录：避免空目录把
    # 有读数的历史现场挤出保留窗口

    seeds = [c for c in (args.seeds or "") if c.strip()]
    # R3-P4·B(b)：--window 缺省为 None ⇒ 显式性可判定（default=None 而非 DEFAULT_WINDOW）
    window_explicit = args.window is not None
    if not window_explicit:
        args.window = DEFAULT_WINDOW
    cfg = {"run_id": run_id, "run_root": str(run_root), "window": args.window,
           "window_explicit": window_explicit,
           "deadline": args.deadline, "seeds": seeds, "env": env_extra}

    sids = ([s.strip() for s in args.only.split(",") if s.strip()] if args.only
            else [s for s in SCENARIOS if s != "REF"])
    bad = [s for s in sids if s not in SCENARIOS]
    if bad:
        raise SystemExit("未知场景：%s（可用：%s）" % (bad, ", ".join(SCENARIOS)))

    print("=== R3 演练台 run_id=%s 系统=%s 场景=%s ===" % (run_id, system["name"], sids))
    reference = None
    if not args.no_reference:
        reference = _reference(system, cfg, force=args.refresh_reference)
        print("[ref] unique=%d wall=%ss complete=%s"
              % (len(reference["unique_mms"]), reference["wall_s"], reference["mode"]))

    # --- 历史读数载入（**在跑之前**快照，不能等到最后再读：增量落盘会覆盖 summary.json，
    #     上一版就是在这里踩坑 —— 先跑的场景被自己的增量写覆盖掉了） ---
    summ_path = default_summary_path(system["name"])
    prev_results = {}
    if (args.merge or args.reverify) and summ_path.exists():
        try:
            prev = json.loads(summ_path.read_text(encoding="utf-8"))
            prev_sys = (prev.get("system") or {}).get("name")
            if prev_sys and prev_sys != system["name"]:
                print("[merge] ⚠ 上一份汇总属于系统 %s，与本次 %s 不同 → 不合并"
                      % (prev_sys, system["name"]))
            else:
                for r in prev.get("results") or []:
                    if r.get("scenario") != "REF":
                        prev_results[r["scenario"]] = r
        except ValueError:
            prev_results = {}
    adopted = {}
    for rid in (args.adopt or []):
        for sid in SCENARIOS:
            if sid == "REF":
                continue
            p = RUNS_DIR / rid / sid / "result.json"
            if p.exists():
                try:
                    adopted[sid] = json.loads(p.read_text(encoding="utf-8"))
                except ValueError:
                    continue
        print("[adopt] 从 %s 吸收场景：%s" % (rid, ", ".join(sorted(
            s for s, r in adopted.items() if r.get("run_id") == rid))))

    results = []

    def _merge_and_write(final=False):
        """把已跑完的场景合并成 summary + 报告。

        **每个场景跑完就写一次**（不是全部跑完才写）：长跑演练会被打断/被杀，增量落盘保证
        任何时刻磁盘上都有一份自洽、可读的中间报告（实测教训：一次整轮跑被宿主回收，
        前面 10 分钟读数全在内存里）。
        优先级：本次跑 > 显式 --adopt（点名 run）> --merge 的上一份 summary。
        【实测教训】反过来写会让上一份汇总里**别的系统**的同名场景覆盖掉 adopt 进来的正确读数。
        """
        # 优先级：本次跑 > 显式 --adopt（点名某个历史 run）> --merge 的上一份汇总
        merged = dict(prev_results)
        merged.update(adopted)
        for r in results:
            merged[r["scenario"]] = r
        ordered = [merged[s] for s in SCENARIOS if s in merged and s != "REF"]
        if final and args.merge and prev_results:
            print("[merge] 合并上一轮读数：%s" % ", ".join(sorted(prev_results)))
        summ = {"run_id": run_id, "ts": _iso(), "system": system, "corpus": CORPUS,
                "seeds": seeds, "window": args.window, "deadline": args.deadline,
                "complete_run": final, "reference": reference, "results": ordered,
                "totals": {"pass": len([r for r in ordered if r["verdict"] == "PASS"]),
                           "fail": len([r for r in ordered if r["verdict"] == "FAIL"]),
                           "count": len(ordered)}}
        default_summary_path(system["name"]).write_text(
            json.dumps(summ, ensure_ascii=False, indent=1), encoding="utf-8")
        p = write_report(system, reference, ordered,
                         Path(args.out) if args.out else default_report_path(system["name"]),
                         run_id)
        # 人读报告的副本与机读汇总同名同处（基线 = results/summary.md），便于成套取用
        md_copy = default_summary_path(system["name"]).with_suffix(".md")
        md_copy.write_text(Path(p).read_text(encoding="utf-8"), encoding="utf-8")
        for stale in (Path(str(p) + ".summary.md"),):
            if stale.exists() and stale != md_copy:
                stale.unlink()
        return summ, p

    if args.reverify:
        # 判据升级后用当前校验器重判（演练不重跑）：审计链路断言随新判据刷新
        n = 0
        for r in prev_results.values():
            d = Path(r.get("_rig_dir") or "")
            if not d.exists():
                continue
            v = run_audit_verify(d, r["scenario"], d / "verdict.json")
            r["audit_verify"] = v
            for a in r.get("assertions") or []:
                if a["name"] == "audit_link":
                    _v2 = str(v.get("verifier") or "").startswith("audit-verify-v2")
                    a["ok"] = bool(v.get("canonical_pass")) if _v2 else bool(v.get("pass"))
                    a["value"] = v.get("pass_count")
                    a["limit"] = v.get("check_count")
                    a["detail"] = ("audit-verify%s：canonical 通道硬通过；channel=%s；%s"
                                   % ("-v2" if _v2 else "-v1(降级)", v.get("channel"),
                                      "; ".join(v.get("fail_reasons") or [])[:400]))
                elif a["name"] == "audit_channel_audit_only":
                    a["ok"] = bool(v.get("audit_only"))
                    a["value"] = v.get("channel")
                elif a["name"] == "audit_schema_strict":
                    a["ok"] = bool((v.get("schema") or {}).get("strict_pass"))
                    a["value"] = (v.get("schema") or {}).get("deviation_count")
            hard = [a for a in r.get("assertions") or [] if not a.get("soft")]
            r["verdict"] = "PASS" if all(a["ok"] for a in hard) else "FAIL"
            n += 1
        print("[reverify] 用当前 audit-verify 重判 %d 个场景" % n)
        args.merge = True

    if args.merge_only:
        args.merge = True          # 只合并 ⇒ 必须读上一份 summary，否则会把读数清空
        # 注意：只有还没读过时才读 —— --reverify 已在内存里改过 prev_results，
        # 这里再读一遍会把重判结果丢掉（实测踩过：报告里的 verdict 一直是旧判据）。
        if not prev_results and summ_path.exists():
            try:
                prev = json.loads(summ_path.read_text(encoding="utf-8"))
                for r in prev.get("results") or []:
                    if r.get("scenario") != "REF":
                        prev_results[r["scenario"]] = r
            except ValueError:
                prev_results = {}
        summ, p = _merge_and_write(final=True)
        print("=== 仅合并：%d 个场景（PASS %d / FAIL %d）；报告 %s ==="
              % (summ["totals"]["count"], summ["totals"]["pass"], summ["totals"]["fail"], p))
        return 0

    run_root.mkdir(parents=True, exist_ok=True)
    for sid in sids:
        print("[run] %s ..." % sid, flush=True)
        res, rig = run_scenario(sid, system, cfg)
        res["integrity"] = _integrity(res, reference or {}, rig)
        # R3-P4·B(a) ordering 修复：evidence.json（含 injections[].ts 注入时间线）**先落盘**，
        # 否则 audit-verify 读不到时间线 ⇒ `no_window` ⇒ 时间窗校验从未生效
        # （verification §5.4 实证：S6 的 repair_round_enter 落在 +516s 仍被判 PASS）。
        write_evidence(res)
        verdict = run_audit_verify(Path(res["_rig_dir"]), sid,
                                   Path(res["_rig_dir"]) / "verdict.json")
        res["audit_verify"] = verdict
        res["assertions"] = evaluate(rig, sid, res, reference, verdict)
        hard = [a for a in res["assertions"] if not a.get("soft")]
        res["verdict"] = "PASS" if all(a["ok"] for a in hard) else "FAIL"
        write_evidence(res)                 # 复查后再落一次（含 audit_verify/assertions）
        Path(res["_rig_dir"], "result.json").write_text(
            json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")
        results.append(res)
        fails = [a["name"] for a in hard if not a["ok"]]
        print("       verdict=%s wall=%ss recover=%s loss=%s audit=%s %s"
              % (res["verdict"], res["wall_s"],
                 [i.get("recover_s") for i in res["injections"] if i.get("recover_s")],
                 res["integrity"]["unique_loss"],
                 "PASS" if verdict.get("pass") else "FAIL",
                 ("fails=%s" % fails) if fails else ""), flush=True)
        _merge_and_write(final=False)      # 每场景增量落盘（被打断也有可读报告）

    out = _merge_and_write(final=True)[1]
    summary = json.loads(default_summary_path(system["name"]).read_text(encoding="utf-8"))
    print("=== 完成：PASS %d / FAIL %d；报告 %s ==="
          % (summary["totals"]["pass"], summary["totals"]["fail"], out))
    return 0 if summary["totals"]["fail"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
