#!/usr/bin/env python3
"""bench.py — R2 mock 压测台：受控、可一键复跑的吞吐/延迟/限流度量基础设施（slot-bench）

一句话：在不碰真实域名、不碰生产路径的前提下，把 scrape 管线跑在本地 Primo mock 上，
量化 B 组指标（B1 吞吐 unique/min、B2 请求 p50/p95、B3 限流红线的出现条件）。

四个场景
--------
  a) 稳态吞吐：完整跑通 scrape.py（mock 化）→ unique/min、req/s、unique/请求、请求构成
  b) 延迟分布：p50/p95（采样点与算法在下面的 §采样口径 里写死）
  c) 限流扫描：① 提供速率阶梯扫描（服务端令牌桶）→ 零错误最高速率 + 黄线
                ② 429 确定性注入比例扫描 → 错误预算
                ③ （full）流水线在注入下的行为：冷却升级/重试/吞吐损失
  d) 版本对照：v10.1 vs v9-prod vs（就绪时）impl/slot-throttle/scrape.py，同一工作负载对比

§采样口径（写死，避免"读数是哪儿来的"扯皮）
--------------------------------------------
  * 传输延迟 http_ms：`scrape.SESSION.get()` 调用前后的 perf_counter 差 —— **不含** LIMITER 排队。
  　　理由：B2 的「请求延迟」指服务端响应时间；把故意加的限速等待混进来会把 p95 抬到秒级，毫无意义。
  * 排队等待 throttle_ms：`LIMITER.acquire()` 的耗时，单独统计（另附端到端 = 排队 + 传输）。
  * **端到端 e2e_ms（P4 增补）**：`http_ms + throttle_ms` 逐请求配对 —— 即 crit-benchmark E4 的
  　　「口径 A（含排队）」。B2 的判定必须两口径并列给出：只报传输口径会得到"余量 5.1×"的假宽松
  　　（crit-correct B2 / crit-benchmark §B2-2：A 口径 p95≈1.28s = 预算 85%、已有 0.34% 越线）。
  　　注意已知口径瑕疵（crit-correct B6）：throttle_ms 挂在线程局部的**下一次** SESSION.get 上，
  　　单请求量级误差很小，但尾部分位会轻微串味；读数里同时给出 n 与 >1.5s 比例以便交叉核对。
  * 分位算法：**最近秩法** nearest-rank，idx = ceil(p/100*n)-1，clamp 到 [0,n-1]，不做插值。
  　　与 mock_primo.py 内置的同一函数一致 → 客户端读数可与服务端自报读数交叉核对。
  * 场景 a 的吞吐窗口 = mod.main() 进入 → 预算/收敛停止；unique 取自 records.jsonl 的去重 mms。
  　　**P4 修正（crit-correct B4）**：这个读数是**整轮平均**（含 reconcile/启动/终局轮/grace），
  　　不是"稳态窗口"。故同时给出两个口径并显式标注：
  　　  - `unique_per_min`          = 整轮平均（scope=whole_run_average，与旧读数逐位一致）
  　　  - `unique_per_min_active`   = 活跃窗口平均（首末请求之间，scope=active_window_average）
  　　  - `startup_s` / `startup_share_pct` = 首个请求之前的启动开销及其占比
  　　短工作负载（如场景 d 的 52s 对照）里启动占比不可忽略，必须两个口径一起看。

§fail-closed 判定（P4 修复 F1，crit-verify §F1）
-----------------------------------------------
  旧行为：变体缺失 → 场景被**静默跳过**；复现性 rows={} 仍打印 `[复现性] PASS`、退出码 0
  （verify 实测 0.046s "假通过"）。新行为：
    · 每个必需场景必须给出**非空真实读数**（请求数>0、unique>0、子进程无 error），否则计入
      `verdict.failures`；
    · 复现性要求 ≥2 次**有效**运行且全部键都可比较，否则 within_5pct=False + reason 说明；
    · 变体缺失：bench 自己声明的可选变体（跨 slot 未就绪）记为 degraded 并显式打印；
      `--variant X` 显式指定或 `--require-variants` 时缺失即 failure；
    · 退出码：4 = failure（必需场景无真实读数）/ 5 = degraded（预算不足未测或可选变体缺失，
      是"没测"不是"通过"）/ 0 = PASS；warning 不影响退出码但显著打印。
    · "预算不足跳过"与"空读数"被显式区分（`skipped_scenarios` 机器可读），避免把没测过的东西
      写成通过 —— 但两者都不会打印 PASS。

§安全设计（fail-closed）
------------------------
  1. 被测 scrape.py 一律**复制到本 slot 的 run/<场景>/ 下**再改（绝不 import 原文件）；
  2. 复制时**文本重写** BASE/OUT 常量 → 指向本 slot；import 后再次断言 OUT/BASE 落在 slot 内，
     否则立即中止（防止 v9-prod 那种 `OUT = BASE/"output"` 直写生产路径）；
  3. 强制 SCRAPE_OUT_DIR / SCRAPE_SANDBOX_BASE 环境变量；SESSION.trust_env=False（不走代理）；
  4. mock 只绑 127.0.0.1；收尾用 /__mock/shutdown 优雅退出，不发信号；
  5. 本进程不 kill 任何外部进程；只等自己起的子进程。

用法
----
  python3 bench.py --quick                 # <60s 快速复跑（acceptance 用）
  python3 bench.py                         # 完整基线（数分钟）
  python3 bench.py --scenarios a,c         # 只跑指定场景
  python3 bench.py --variant /path/to/scrape.py
  python3 bench.py --list-variants
"""
import argparse
import hashlib
import json
import math
import os
import re
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
SLOT = HERE
RUNS = SLOT / "run"
RESULTS = SLOT / "results"
MOCK_PY = SLOT / "mock_primo.py"
CORPUS_REAL = SLOT / "corpus-real-tree.json"
R2 = SLOT.parent.parent
REFS = R2 / "refs"
THROTTLE_SCRAPE = R2 / "impl" / "slot-throttle" / "scrape.py"

BASELINE_V10 = REFS / "scrape-v10.1.py"
BASELINE_V9 = REFS / "scrape-v9-prod.py"

# 场景种子集（来自 calib_real_tree.py 的真实子树成本：A=98 req，E=59，L/M/U/W/Y=2 ...）
SEEDS_QUICK = {"a": "A", "b": "A", "c": "A", "d": "LMUWY"}
SEEDS_FULL = {"a": "AELM NSUVWXZ".replace(" ", ""), "b": "A", "c": "A", "d": "AES"}

PY = sys.executable or "python3"


# ===========================================================================
# 基础设施
# ===========================================================================
def ensure_dirs():
    for d in (RUNS, RESULTS):
        d.mkdir(parents=True, exist_ok=True)


def nearest_rank(sorted_vals, pct):
    """最近秩法（与 mock_primo.nearest_rank 同实现）。不做插值 → 可复现、保守。"""
    n = len(sorted_vals)
    if n == 0:
        return None
    idx = max(0, min(n - 1, -(-n * pct // 100) - 1))
    return sorted_vals[idx]


def pctl(vals, pct):
    return nearest_rank(sorted(vals), pct)


def inside(child, root):
    try:
        return os.path.commonpath([os.path.realpath(child), os.path.realpath(root)]) \
            == os.path.realpath(root)
    except ValueError:
        return False


class Fail(Exception):
    pass


class MockServer:
    """启动/停止一个 loopback mock；收尾走 /__mock/shutdown（不发信号）。"""

    def __init__(self, corpus="real", profile="ideal", seeds=None, records=40000,
                 extra=None, tag="mock"):
        self.tag = tag
        self.ready = RUNS / ("%s.ready.json" % tag)
        self.stats_file = RUNS / ("%s.stats.json" % tag)
        self.out_file = RUNS / ("%s.out" % tag)
        argv = [PY, str(MOCK_PY), "--port", "0", "--host", "127.0.0.1",
                "--profile", profile, "--ready-file", str(self.ready),
                "--stats-file", str(self.stats_file)]
        if corpus == "real":
            argv += ["--corpus-file", str(CORPUS_REAL), "--records", str(records)]
            if seeds:
                argv += ["--seed-subset", ",".join(seeds)]
        else:
            argv += ["--records", str(records)]
        if extra:
            argv += [str(x) for x in extra]
        self.argv = argv
        self.proc = None
        self.port = None

    def start(self):
        if self.ready.exists():
            self.ready.unlink()
        RUNS.mkdir(parents=True, exist_ok=True)
        self.fh = open(self.out_file, "w")
        self.proc = subprocess.Popen(self.argv, stdout=self.fh, stderr=subprocess.STDOUT,
                                     cwd=str(RUNS))
        t0 = time.time()
        while time.time() - t0 < 60:
            if self.ready.exists():
                try:
                    info = json.loads(self.ready.read_text())
                    if info.get("port"):
                        self.port = info["port"]
                        self.info = info
                        return self
                except Exception:
                    pass
            if self.proc.poll() is not None:
                raise Fail("mock 启动失败（exit %s）: %s" % (self.proc.returncode,
                                                            self._tail()))
            time.sleep(0.05)
        raise Fail("mock 启动超时: %s" % self._tail())

    def _tail(self, n=1500):
        try:
            return self.out_file.read_text()[-n:]
        except Exception:
            return "<no output>"

    @property
    def base(self):
        return "http://127.0.0.1:%d" % self.port

    def stats(self):
        import urllib.request
        try:
            with urllib.request.urlopen(self.base + "/__mock/stats", timeout=10) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def config(self):
        import urllib.request
        try:
            with urllib.request.urlopen(self.base + "/__mock/config", timeout=10) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def stop(self):
        import urllib.request
        if self.proc is None:
            return
        try:
            urllib.request.urlopen(self.base + "/__mock/shutdown", timeout=5).read()
        except Exception:
            pass
        try:
            self.proc.wait(timeout=8)        # 只等自己起的子进程，不发信号
        except subprocess.TimeoutExpired:
            self.proc.terminate()            # 兜底：仅限本进程自己起的 mock 子进程
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        try:
            self.fh.close()
        except Exception:
            pass

    def __enter__(self):
        return self.start()

    def __exit__(self, *a):
        self.stop()


def run_child(mode, cfg, timeout_s, tag):
    """跑一个 bench 子进程（worker / sweep），返回 (结果 dict, 子进程 stdout 路径, rc)。"""
    RUNS.mkdir(parents=True, exist_ok=True)
    cfg_path = RUNS / ("%s.cfg.json" % tag)
    res_path = RUNS / ("%s.result.json" % tag)
    out_path = RUNS / ("%s.child.out" % tag)
    cfg = dict(cfg)
    cfg["mode"] = mode
    cfg["result_path"] = str(res_path)
    cfg_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=1))
    if res_path.exists():
        res_path.unlink()
    t0 = time.time()
    with open(out_path, "w") as fh:
        p = subprocess.Popen([PY, str(SLOT / "bench.py"), "_" + mode, "--config", str(cfg_path)],
                             stdout=fh, stderr=subprocess.STDOUT, cwd=str(SLOT))
        try:
            p.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            p.terminate()                    # 兜底：超时只针对自己起的子进程
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
    dt = time.time() - t0
    res = None
    if res_path.exists():
        try:
            res = json.loads(res_path.read_text())
        except Exception as e:
            res = {"ok": False, "error": "result json 解析失败: %s" % e}
    if res is None:
        res = {"ok": False, "error": "子进程未产出结果（rc=%s）" % p.returncode}
    res.setdefault("child_wall_s", round(dt, 2))
    res["child_rc"] = p.returncode
    res["child_out"] = str(out_path)
    return res


# ===========================================================================
# 被测源码沙箱化（fail-closed）
# ===========================================================================
def prepare_source(src, dst, sandbox_base):
    """把被测 scrape.py 复制到 run 目录并**文本重写**路径常量：
       BASE → 沙箱根；OUT → 沙箱输出目录（强制 env）。
    这样即使被测版本不支持 SCRAPE_OUT_DIR（如 v9-prod 的 OUT = BASE/"output"），
    也不可能写到 /tmp/library-scrape。"""
    text = Path(src).read_text(encoding="utf-8")
    if not re.search(r"^\s*import .*\bos\b", text, re.M):
        text = re.sub(r"^(import .*)$", r"\1\nimport os", text, count=1, flags=re.M)
    n_base = n_out = 0
    out_lines = []
    for line in text.splitlines():
        if re.match(r"^BASE\s*=\s*Path\(", line):
            line = 'BASE = Path(os.environ["SCRAPE_SANDBOX_BASE"])'
            n_base += 1
        elif re.match(r"^OUT\s*=", line):
            line = 'OUT = Path(os.environ["SCRAPE_OUT_DIR"])'
            n_out += 1
        out_lines.append(line)
    patched = "\n".join(out_lines) + "\n"
    # 路径兜底桩：任何 Path("/tmp/library-scrape") 字面量也一并改写
    patched = patched.replace('"/tmp/library-scrape"', 'os.environ["SCRAPE_SANDBOX_BASE"]')
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    Path(dst).write_text(patched, encoding="utf-8")
    return {"base_rewritten": n_base, "out_rewritten": n_out}


def load_under_test(cfg):
    """按 cfg 装载被测模块并做安全断言。返回 (mod, guard)。"""
    import importlib.util
    src = Path(cfg["scrape_path"])
    if not src.exists():
        raise Fail("被测文件不存在: %s" % src)
    scenario_dir = RUNS / cfg["tag"]
    if not inside(scenario_dir, SLOT):
        raise Fail("场景目录越界: %s" % scenario_dir)
    if scenario_dir.exists():
        shutil.rmtree(scenario_dir)
    out_dir = scenario_dir / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    dst = scenario_dir / "scrape_under_test.py"
    rw = prepare_source(src, dst, str(scenario_dir / "base"))
    os.environ["SCRAPE_OUT_DIR"] = str(out_dir)
    os.environ["SCRAPE_SANDBOX_BASE"] = str(scenario_dir / "base")
    spec = importlib.util.spec_from_file_location("scrape_under_test", str(dst))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.HOST = cfg["base"]
    try:
        mod.SESSION.trust_env = False
    except Exception:
        pass
    guard = {"src": str(src), "copy": str(dst), "out_dir": str(mod.OUT),
             "base": str(getattr(mod, "BASE", ""))}
    if not inside(str(mod.OUT), str(SLOT)):
        raise Fail("安全断言失败：被测模块 OUT=%s 不在 slot 内 → 中止" % mod.OUT)
    if getattr(mod, "BASE", None) is not None and not inside(str(mod.BASE), str(SLOT)):
        raise Fail("安全断言失败：被测模块 BASE=%s 不在 slot 内 → 中止" % mod.BASE)
    guard["inside_slot"] = True
    guard["rewrites"] = rw
    guard["min_interval"] = getattr(mod, "MIN_INTERVAL", None)
    guard["leaf_max"] = getattr(mod, "LEAF_MAX", None)
    guard["bulk_limit"] = getattr(mod, "BULK_LIMIT", None)
    guard["charset_n"] = len(getattr(mod, "CHARSET", []))
    try:      # 版本指纹：对照实验必须可追溯到确切文件内容
        guard["sha256_12"] = hashlib.sha256(src.read_bytes()).hexdigest()[:12]
    except Exception:
        guard["sha256_12"] = None
    return mod, guard


class TimeShim:
    """只替换被测模块命名空间里的 time.sleep（缩放冷却/暂停），其余属性透传真实 time。

    关键：**限速器内部的节奏 sleep 不能缩放**（否则 MIN_INTERVAL 被一起压缩，
    req/s 会虚高到 8+，读数失去可比性 —— 见 receipts 的 bug#5）。
    用线程局部标志：仅在 LIMITER.acquire() 调用期间标记 in_limiter=True。"""

    def __init__(self, scale, flag):
        self.scale = float(scale)
        self.flag = flag

    def sleep(self, s):
        if getattr(self.flag, "in_limiter", False):
            time.sleep(max(0.0, float(s)))              # 限速节奏：原样
        else:
            time.sleep(max(0.0, float(s) * self.scale))  # 冷却/暂停：压缩
            self.flag.scaled_sleeps = getattr(self.flag, "scaled_sleeps", 0) + 1

    def __getattr__(self, name):
        return getattr(time, name)


def instrument(mod, samples_path, scale_sleep=1.0, scale_cooldown=1.0):
    """给被测模块装计量钩子（SESSION.get / LIMITER.acquire / LIMITER.cooldown）。
    返回 (state, samples, scales)。throttle_ms 用线程局部与「下一次 SESSION.get」配对。"""
    import threading
    state = {"n": 0, "by_status": {}, "by_kind": {}, "throttle_ms": [], "cooldown_log": []}
    samples = []
    lock = threading.Lock()
    tl = threading.local()
    orig_get = mod.SESSION.get
    orig_acquire = mod.LIMITER.acquire
    orig_cooldown = mod.LIMITER.cooldown

    def timed_get(url, *a, **kw):
        params = kw.get("params") or {}
        limit = params.get("limit")
        try:
            li = int(limit)
        except (TypeError, ValueError):
            li = -1
        kind = "bulk" if li >= 500 else ("count" if li == 1 else "other")
        t0 = time.perf_counter()
        status = -1
        try:
            r = orig_get(url, *a, **kw)
            status = r.status_code
            return r
        finally:
            dur = (time.perf_counter() - t0) * 1000.0
            wait = getattr(tl, "wait", 0.0)
            tl.wait = 0.0
            row = {"t": time.time(), "http_ms": dur, "status": status, "kind": kind,
                   "limit": li, "sort": params.get("sort"), "throttle_ms": wait,
                   "prefix": (params.get("q") or "").rsplit(",", 1)[-1]}
            with lock:
                state["n"] += 1
                state["by_status"][str(status)] = state["by_status"].get(str(status), 0) + 1
                state["by_kind"][kind] = state["by_kind"].get(kind, 0) + 1
                samples.append(row)

    def timed_acquire():
        t0 = time.perf_counter()
        tl.in_limiter = True
        try:
            orig_acquire()
        finally:
            tl.in_limiter = False
        wait = (time.perf_counter() - t0) * 1000.0
        tl.wait = getattr(tl, "wait", 0.0) + wait
        with lock:
            state["throttle_ms"].append(wait)

    def scaled_cooldown(sec):
        with lock:
            state["cooldown_log"].append(round(float(sec), 2))
        return orig_cooldown(float(sec) * scale_cooldown)

    mod.SESSION.get = timed_get
    mod.LIMITER.acquire = timed_acquire
    mod.LIMITER.cooldown = scaled_cooldown
    if scale_sleep != 1.0:
        mod.time = TimeShim(scale_sleep, tl)
    return state, samples, {"scale_sleep": scale_sleep, "scale_cooldown": scale_cooldown}


# ===========================================================================
# 子进程 1：pipeline worker（完整跑 scrape 主循环）
# ===========================================================================
def mode_run(cfg):
    import threading
    res_path = Path(cfg["result_path"])
    samples_path = Path(cfg["samples_path"])
    res = {"ok": False, "mode": "pipeline", "tag": cfg["tag"],
           "variant": cfg.get("variant"), "scrape_path": cfg["scrape_path"]}
    stop = {"reason": None}
    try:
        mod, guard = load_under_test(cfg)
        res["guard"] = guard
        state, samples, scales = instrument(
            mod, samples_path, cfg.get("scale_sleep", 1.0), cfg.get("scale_cooldown", 1.0))
        res["scales"] = scales
        budget = int(cfg.get("request_budget", 10 ** 9))
        deadline = float(cfg.get("deadline_s", 1e9))

        def finish(reason, extra=None):
            if stop["reason"]:
                return
            stop["reason"] = reason
            res.update({
                "ok": True, "stop_reason": reason,
                "elapsed_s": round(time.time() - t_start, 3),
                "t_start_epoch": round(t_start, 3), "t_end_epoch": round(time.time(), 3),
                "requests": state["n"], "by_status": dict(state["by_status"]),
                "by_kind": dict(state["by_kind"]),
                "cooldown_events": len(state["cooldown_log"]),
                "cooldown_total_s": round(sum(state["cooldown_log"]), 2),
                "cooldown_seq": state["cooldown_log"][:40],
                "throttle_wait_ms": _summarize(state["throttle_ms"]),
            })
            res.update(count_records(mod.RECORDS_FILE))
            try:
                prog = json.loads(Path(mod.PROGRESS_FILE).read_text())
                res["progress"] = {"stats": prog.get("stats"), "gaps": len(prog.get("gaps") or []),
                                   "todo": len(prog.get("todo") or []),
                                   "baseline": prog.get("baseline")}
            except Exception as e:
                res["progress"] = {"error": str(e)}
            res["http_latency_ms"] = _summarize([s["http_ms"] for s in samples])
            res["log_http_events"] = _scan_log(mod.LOG_FILE)
            write_samples(samples, samples_path)
            if extra:
                res.update(extra)
            res_path.write_text(json.dumps(res, ensure_ascii=False, indent=1))

        sys.argv = ["scrape_under_test"] + list(cfg["seeds"])
        t_start = time.time()

        def watchdog():
            while not stop["reason"]:
                time.sleep(0.05)
                if state["n"] >= budget:
                    time.sleep(cfg.get("grace_s", 1.5))   # 让在途请求落盘后再收
                    finish("request_budget")
                elif time.time() - t_start > deadline:
                    time.sleep(cfg.get("grace_s", 1.5))
                    finish("deadline")

        threading.Thread(target=watchdog, daemon=True).start()
        try:
            mod.main()
            finish("converged")
        except SystemExit:
            finish("converged")
        except Exception as e:
            import traceback
            res["traceback"] = traceback.format_exc()[-2000:]
            finish("error", {"error": "%s: %s" % (type(e).__name__, e)})
        os._exit(0 if res.get("ok") else 3)
    except Fail as e:
        res["error"] = str(e)
        res_path.write_text(json.dumps(res, ensure_ascii=False, indent=1))
        os._exit(2)
    except Exception as e:
        import traceback
        res["error"] = "%s: %s" % (type(e).__name__, e)
        res["traceback"] = traceback.format_exc()[-2000:]
        res_path.write_text(json.dumps(res, ensure_ascii=False, indent=1))
        os._exit(3)


def _summarize(vals):
    if not vals:
        return {"n": 0}
    v = sorted(vals)
    return {"n": len(v), "p50": round(nearest_rank(v, 50), 2),
            "p95": round(nearest_rank(v, 95), 2), "p99": round(nearest_rank(v, 99), 2),
            "mean": round(statistics.fmean(v), 2), "max": round(v[-1], 2)}


def count_records(path):
    """统计 records.jsonl：行数 + **有效**去重 mms（吞吐的唯一口径）。

    P4/F6（crit-correct B3）：旧实现把坏行各记一个 `__bad__N` 的 unique、把缺 mms 的行记成
    None ⇒ unique 随坏行数**虚高**（实测 5 行 / 真 2 unique 会报 4 unique），而 A3 撕裂/半写
    正是本轮刻意制造的场景，且 `records_lines` 同时报出 ⇒ 虚高是**静默**的。
    修法：坏行 / 缺 mms 行 / 重复行分别计数，**绝不进入** unique 集合；unique 只数有效 mms。
    """
    path = Path(path)
    out = {"records_lines": 0, "unique_mms": 0, "valid_lines": 0, "bad_lines": 0,
           "missing_mms_lines": 0, "blank_lines": 0, "duplicate_lines": 0,
           "unique_source": "records.jsonl[mms]（仅计有效行；坏行/缺 mms 行不计入）"}
    if not path.exists():
        return out
    mms = set()
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            raw = line.strip()
            if not raw:
                out["blank_lines"] += 1
                continue
            out["records_lines"] += 1
            try:
                rec = json.loads(raw)
            except Exception:
                out["bad_lines"] += 1
                continue
            if not isinstance(rec, dict):
                out["bad_lines"] += 1
                continue
            v = rec.get("mms")
            if isinstance(v, bool) or v is None or (isinstance(v, str) and not v.strip()) \
                    or not isinstance(v, (str, int)):
                out["missing_mms_lines"] += 1
                continue
            out["valid_lines"] += 1
            if v in mms:
                out["duplicate_lines"] += 1
            mms.add(v)
    out["unique_mms"] = len(mms)
    return out


def read_samples(samples_path):
    """读逐请求样本（坏行跳过并计数）。返回 (rows, bad)。"""
    rows, bad = [], 0
    p = Path(samples_path)
    if not p.exists():
        return rows, bad
    for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except Exception:
            bad += 1
            continue
        if isinstance(r, dict):
            rows.append(r)
        else:
            bad += 1
    return rows, bad


def _summarize_vals(vals, extra=None):
    s = _summarize(vals)
    if extra:
        s.update(extra)
    return s


def end_to_end_latency(samples_path, budget_ms=1500.0):
    """crit-benchmark E4 口径：端到端 = 排队 throttle_ms + 传输 http_ms（逐请求配对）。

    P4/F7：旧报告只给"不含排队"的传输口径（B2 判「余量 5.1×」），从未计算含排队的 A 口径；
    而复算 A 口径 p95≈1.28s（= 预算 85%）、0.34% 请求已越 1.5s ⇒ 口径选择直接改变判定性质。
    这里把两口径并列算出来，并给出 >budget 的比例与分请求类读数（bulk/count 是否被 mock 抹平）。
    """
    rows, bad = read_samples(samples_path)
    out = {"budget_ms": budget_ms, "n": 0, "bad_sample_lines": bad,
           "scope": "e2e = LIMITER 排队 + SESSION.get 传输（逐请求配对，含排队）",
           "http_ms": {}, "throttle_ms": {}, "e2e_ms": {},
           "over_budget_n": 0, "over_budget_pct": None, "by_kind": {}}
    if not rows:
        return out
    http = [float(r.get("http_ms") or 0.0) for r in rows]
    thr = [float(r.get("throttle_ms") or 0.0) for r in rows]
    e2e = [a + b for a, b in zip(http, thr)]
    over = sum(1 for v in e2e if v > budget_ms)
    out["n"] = len(rows)
    out["http_ms"] = _summarize_vals(http)
    out["throttle_ms"] = _summarize_vals(thr)
    out["e2e_ms"] = _summarize_vals(e2e)
    for k, v in out["e2e_ms"].items():        # 扁平便捷键（人读/keeper 直接用）
        out[k] = v
    out["over_budget_n"] = over
    out["over_budget_pct"] = round(100.0 * over / len(e2e), 3)
    out["over_budget_budget_ms"] = budget_ms
    out["e2e_p95_over_budget"] = bool(out["e2e_ms"].get("p95") is not None
                                      and out["e2e_ms"]["p95"] > budget_ms)
    out["http_p95_over_budget"] = bool(out["http_ms"].get("p95") is not None
                                       and out["http_ms"]["p95"] > budget_ms)
    for k in ("bulk", "count", "other", "sample"):
        sub = [float(r.get("http_ms") or 0.0) for r in rows if r.get("kind") == k]
        if sub:
            out["by_kind"][k] = {"n": len(sub), "p50": round(nearest_rank(sorted(sub), 50), 2),
                                 "p95": round(nearest_rank(sorted(sub), 95), 2)}
    return out


def active_window(samples_path, t_start_epoch=None, t_end_epoch=None):
    """活跃窗口读数（P4/B4）：首个请求到末个请求之间的墙钟与启动开销占比。

    为什么需要：`elapsed_s` 是整轮墙钟（含 reconcile/启动/终局修复轮/grace），把它当"稳态吞吐"
    的分母会低估吞吐，且在短工作负载（场景 d 的 52s 对照）里会被启动期主导。
    """
    rows, _bad = read_samples(samples_path)
    ts = sorted(float(r["t"]) for r in rows if isinstance(r.get("t"), (int, float)))
    out = {"n_samples": len(ts), "first_t": None, "last_t": None, "span_s": None,
           "startup_s": None, "tail_s": None,
           "scope": "活跃窗口 = 首个请求 → 末个请求（不含启动/reconcile/收尾）"}
    if not ts:
        return out
    out["first_t"] = round(ts[0], 3)
    out["last_t"] = round(ts[-1], 3)
    out["span_s"] = round(ts[-1] - ts[0], 3)
    if t_start_epoch is not None:
        out["startup_s"] = round(max(0.0, ts[0] - float(t_start_epoch)), 3)
    if t_end_epoch is not None:
        out["tail_s"] = round(max(0.0, float(t_end_epoch) - ts[-1]), 3)
    return out


def _scan_log(log_file):
    """从被测管线自己的日志里提取真实的重试/冷却事件（证明注入被管线感知并处理）。"""
    out = {"http_events": {}, "final_fail": 0, "cooldown_lines": 0}
    try:
        text = Path(log_file).read_text(encoding="utf-8", errors="replace")
    except Exception:
        return out
    for m in re.finditer(r"HTTP (\d{3})", text):
        out["http_events"][m.group(1)] = out["http_events"].get(m.group(1), 0) + 1
    out["final_fail"] = len(re.findall(r"请求最终失败", text))
    out["cooldown_lines"] = len(re.findall(r"cooldown", text))
    return out


def write_samples(samples, path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in samples:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


# ===========================================================================
# 子进程 2：sweep（直接施压客户端：提供速率阶梯 / 注入比例扫描）
# ===========================================================================
def mode_sweep(cfg):
    import urllib.request
    import urllib.parse
    res = {"ok": False, "mode": "sweep", "tag": cfg["tag"]}
    base = cfg["base"]
    params_common = {"vid": "86SUSTC_INST:86SUSTC", "tab": "default_tab",
                     "scope": "MyInstitution", "lang": "zh_CN", "mode": "Basic",
                     "getMore": "0", "inst": "86SUSTC_INST"}
    prefix = cfg.get("prefix", "A")
    rows = []
    try:
        for rate in cfg["rates"]:
            data = _sweep_one(base, params_common, prefix, float(rate),
                              float(cfg.get("seconds", 2.0)), int(cfg.get("preburst", 8)),
                              limit_rps=cfg.get("limit_rps"), burst=cfg.get("burst"),
                              workers=cfg.get("workers"))
            # 服务端视角的拒绝计数（每个速率窗口前后各取一次 → 交叉核对客户端观测到的 400）
            try:
                import urllib.request
                with urllib.request.urlopen(base + "/__mock/stats", timeout=10) as r:
                    data["server_rejected_cum"] = json.loads(r.read().decode()) \
                        .get("ratelimit_rejected")
            except Exception:
                pass
            rows.append(data)
        res.update({"ok": True, "rows": rows})
    except Exception as e:
        import traceback
        res["error"] = "%s: %s" % (type(e).__name__, e)
        res["traceback"] = traceback.format_exc()[-1500:]
        res["rows"] = rows
    Path(cfg["result_path"]).write_text(json.dumps(res, ensure_ascii=False, indent=1))
    os._exit(0 if res.get("ok") else 3)


def _sweep_one(base, common, prefix, rate, seconds, preburst, limit_rps=None, burst=None,
               workers=None):
    """单速率窗口：
      阶段 1 preburst：**并发**打 preburst 个请求（用掉服务端令牌桶的 burst 额度，
                       阈值才能在秒级窗口内可观测）；
      阶段 2 paced  ：workers 个线程各自按 interval=workers/rate 的节奏发送 seconds 秒
                       —— **读数的唯一来源**（顺序单客户端在 ~6 req/s 就饱和，会把阶梯压平：
                       见 receipts 的 bug#1；preburst 不计入速率：bug#2）。
    连接复用：每线程独立 Session + HTTPAdapter 连接池（真实管线同样是复用 Session）。"""
    import threading
    import requests
    from concurrent.futures import ThreadPoolExecutor
    url = base + "/primaws/rest/pub/pnxs"
    q = "holding_call_number,begins_with,%s" % prefix
    counts = {"burst": {"n": 0, "ok": 0, "st": {}}, "paced": {"n": 0, "ok": 0, "st": {}}}
    lock = threading.Lock()

    def new_session():
        se = requests.Session()
        se.trust_env = False
        ad = requests.adapters.HTTPAdapter(pool_connections=8, pool_maxsize=8)
        se.mount("http://", ad)
        return se

    def one(phase, se):
        p = dict(common, q=q, limit="1", offset="0")
        try:
            r = se.get(url, params=p, headers={"Accept": "application/json",
                                               "User-Agent": "Mozilla/5.0"}, timeout=30)
            code = r.status_code
        except Exception:
            code = -1
        c = counts[phase]
        with lock:
            c["n"] += 1
            c["st"][str(code)] = c["st"].get(str(code), 0) + 1
            if code == 200:
                c["ok"] += 1

    nw = int(workers or max(1, min(24, math.ceil(max(rate, 0.001) / 5.0))))
    t_b0 = time.perf_counter()
    if preburst:
        with ThreadPoolExecutor(max_workers=min(nw, max(1, preburst))) as ex:
            futs = [ex.submit(one, "burst", new_session()) for _ in range(preburst)]
            [f.result() for f in futs]
    burst_s = time.perf_counter() - t_b0
    # preburst 会把令牌桶打空；受控窗口必须从「桶回满」的干净状态起跑，
    # 否则窗口开头的赤字会污染「零错误最高速率」的判定（见 receipts 的 bug#3）。
    refill_s = 0.0
    if preburst and limit_rps:
        refill_s = (burst if burst is not None else 0) / float(limit_rps) + 0.25
        time.sleep(refill_s)

    interval = nw / rate if rate > 0 else 0
    stop_at = time.perf_counter() + seconds

    def paced_worker(w):
        se = new_session()
        next_t = time.perf_counter() + w * (interval / max(1, nw))
        while True:
            now = time.perf_counter()
            if next_t > now:
                time.sleep(next_t - now)
            if time.perf_counter() >= stop_at:
                break
            one("paced", se)
            next_t += interval

    t0 = time.perf_counter()
    ths = [threading.Thread(target=paced_worker, args=(w,)) for w in range(nw)]
    [t.start() for t in ths]
    [t.join() for t in ths]
    dt = time.perf_counter() - t0
    n = counts["paced"]["n"]
    ok = counts["paced"]["ok"]
    err = n - ok
    out = {"offered_rps": rate, "window_s": round(dt, 3), "workers": nw,
           "refill_wait_s": round(refill_s, 3),
           "requests": n, "ok": ok, "errors": err,
           "by_status": counts["paced"]["st"],
           "achieved_rps": round(n / dt, 3),
           "achieved_ok_rps": round(ok / dt, 3),
           "error_ratio": round(err / n, 4) if n else 0,
           "preburst": {"n": counts["burst"]["n"], "ok": counts["burst"]["ok"],
                        "errors": counts["burst"]["n"] - counts["burst"]["ok"],
                        "by_status": counts["burst"]["st"], "elapsed_s": round(burst_s, 3)}}
    if limit_rps and burst is not None:
        deficit = max(0.0, (rate - limit_rps) * dt - burst)
        out["deficit_model"] = {"limit_rps": limit_rps, "burst": burst,
                                "predicted_max_errors": round(deficit, 2),
                                "predicted_error_ratio": round(deficit / n, 4) if n else 0}
    return out


# ===========================================================================
# 子进程 3：sample（直接传输延迟采样；**故意绕过 LIMITER**，见 §采样口径）
# ===========================================================================
def mode_sample(cfg):
    import threading
    res = {"ok": False, "mode": "sample", "tag": cfg["tag"]}
    try:
        mod, guard = load_under_test(cfg)
        res["guard"] = guard
        prefix = cfg.get("prefix", "A1")
        params = {"vid": mod.VID, "tab": "default_tab", "scope": "MyInstitution",
                  "q": "holding_call_number,begins_with,%s" % prefix,
                  "limit": "1", "offset": "0", "lang": "zh_CN", "mode": "Basic",
                  "getMore": 0, "inst": mod.INST}
        url = mod.HOST + "/primaws/rest/pub/pnxs"
        n = int(cfg["n"]); conc = int(cfg["conc"])
        per = max(1, n // conc)
        lat = []
        by_status = {}
        lock = threading.Lock()

        def worker():
            for _ in range(per):
                t0 = time.perf_counter()
                try:
                    r = mod.SESSION.get(url, params=params,
                                        headers={"Accept": "application/json",
                                                 "User-Agent": "Mozilla/5.0"}, timeout=30)
                    code = r.status_code
                except Exception:
                    code = -1
                d = (time.perf_counter() - t0) * 1000.0
                with lock:
                    lat.append((d, code))
                    by_status[str(code)] = by_status.get(str(code), 0) + 1

        ths = [threading.Thread(target=worker) for _ in range(conc)]
        t0 = time.time()
        [t.start() for t in ths]
        [t.join() for t in ths]
        dt = time.time() - t0
        samples = [{"t": t0 + i * 1e-6, "http_ms": d, "status": c, "kind": "sample",
                    "limit": 1} for i, (d, c) in enumerate(lat)]
        write_samples(samples, Path(cfg["samples_path"]))
        res.update({"ok": True, "n": len(lat), "concurrency": conc, "wall_s": round(dt, 2),
                    "req_per_s": round(len(lat) / dt, 2), "by_status": by_status,
                    "http_latency_ms": _summarize([d for d, _ in lat]),
                    "limiter_bypassed": True})
    except Fail as e:
        res["error"] = str(e)
    except Exception as e:
        import traceback
        res["error"] = "%s: %s" % (type(e).__name__, e)
        res["traceback"] = traceback.format_exc()[-1500:]
    Path(cfg["result_path"]).write_text(json.dumps(res, ensure_ascii=False, indent=1))
    os._exit(0 if res.get("ok") else 3)


# ===========================================================================
# 父进程：场景编排
# ===========================================================================
def variant_list(extra=None):
    """被测变体清单。`required=False` 表示"bench 自己声明的可选变体"（跨 slot 尚未就绪的产物）：
    缺失时记 warning 并显著打印，但不判 failure；`--variant X`（显式指定）一律 required。"""
    vs = [{"name": "v10.1", "path": str(BASELINE_V10), "note": "当前基线（R2 refs 快照）",
           "required": True},
          {"name": "v9-prod", "path": str(BASELINE_V9), "note": "上一版对照", "required": True}]
    if THROTTLE_SCRAPE.exists():
        vs.append({"name": "slot-throttle", "path": str(THROTTLE_SCRAPE),
                   "note": "R2 自适应限速版（就绪）", "required": True})
    else:
        vs.append({"name": "slot-throttle", "path": None, "required": False,
                   "note": "未就绪：%s 不存在 → 可选变体缺失（warning，非 failure）"
                           % THROTTLE_SCRAPE})
    for e in (extra or []):
        vs.append({"name": Path(e).stem, "path": str(Path(e).resolve()),
                   "note": "自定义（--variant 显式指定 ⇒ 必需）", "required": True})
    return vs


# ---------------------------------------------------------------------------
# fail-closed 判定（P4/F1）
# ---------------------------------------------------------------------------
def _a_readings(r):
    t = (r or {}).get("throughput") or {}
    return t


def check_scenario_a(r):
    """场景 a 的有效性：必须有真实非空读数（请求数>0 且 unique>0，子进程无 error）。"""
    if not r:
        return False, "场景 a 无结果"
    if r.get("skipped"):
        return False, "被跳过：%s" % r.get("skipped")
    run = r.get("run") or {}
    if run.get("error"):
        return False, "子进程 error=%s" % run.get("error")
    if run.get("stop_reason") == "error":
        return False, "子进程 stop_reason=error"
    t = _a_readings(r)
    if not t.get("requests"):
        return False, "requests=0（无真实请求读数）"
    if not t.get("unique_records"):
        return False, "unique_records=0（空读数不得判 PASS）"
    return True, None


def check_scenario_b(r):
    if not r:
        return False, "场景 b 无结果"
    if (r.get("client_latency_ms") or {}).get("n"):
        return True, None
    return False, "客户端延迟样本为空（n=0）"


def check_scenario_c(r):
    if not r:
        return False, "场景 c 无结果"
    rows = ((r.get("c1") or {}).get("rows")) or []
    if not rows:
        return False, "c1 速率阶梯无任何行（读数为空）"
    if not any((row.get("requests") or 0) > 0 for row in rows):
        return False, "c1 全部速率窗口 requests=0（空读数）"
    return True, None


def evaluate_verdict(res, want, require_variants=False):
    """把"跑过了"与"有真实读数"分开（fail-closed）。

    三级判定：
      · failures（退出码 4）：必需的场景**被请求但没有真实读数**（空读数/子进程 error/变体缺失）；
      · degraded（退出码 5）：场景因**预算不足**未跑，或 bench 自声明的可选变体缺失
        —— 不是"通过"，只是"没测"；必须显著打印，不许静默算过；
      · warnings：读数质量提示（如 records 含坏行），不影响退出码。
    """
    failures, degraded, warnings = [], [], []
    skipped = res.get("skipped_scenarios") or {}
    for s in want:
        if s in skipped and not res.get(s):
            degraded.append("场景 %s 未测：%s" % (s, skipped[s]))
            continue
        if s == "a":
            r = res.get("a")
            ok, why = check_scenario_a(r)
            if not ok:
                failures.append("场景 a：%s" % why)
            if r and r.get("throughput"):
                t = r["throughput"]
                if t.get("bad_lines"):
                    warnings.append("场景 a：records.jsonl 含 %s 个坏行（unique 只计有效行，"
                                    "坏行/缺 mms 行已单列）" % t["bad_lines"])
        elif s == "b":
            ok, why = check_scenario_b(res.get("b"))
            if not ok:
                failures.append("场景 b：%s" % why)
        elif s == "c":
            ok, why = check_scenario_c(res.get("c"))
            if not ok:
                failures.append("场景 c：%s" % why)
        elif s == "d":
            d = res.get("d") or {}
            vs = d.get("variants") or []
            if not vs:
                failures.append("场景 d：无变体结果")
            for v in vs:
                if not v.get("skipped"):
                    continue
                req = bool(v.get("required", True)) or bool(require_variants)
                msg = "场景 d 变体 %s 缺失：%s" % (v.get("name"), v.get("note"))
                (failures if req else degraded).append(msg)
        else:
            warnings.append("未知场景 %r（未判定）" % s)
    rep = res.get("reproducibility")
    if rep and not rep.get("rows"):
        if rep.get("budget_limited"):
            degraded.append("复现性未测：%s" % (rep.get("reason") or "预算不足，重复次数不够"))
        elif want_has_a(want):
            failures.append("复现性：%s" % (rep.get("reason") or "无有效读数"))
    return {"ok": not failures and not degraded, "failures": failures,
            "degraded": degraded, "warnings": warnings,
            "require_variants": bool(require_variants),
            "policy": ("fail-closed：必需场景必须有非空真实读数（否则 FAIL/退出码 4）；"
                       "预算不足未测或可选变体缺失记 degraded（退出码 5，不是通过）；"
                       "可用 --require-variants 把可选变体升格为必需")}


def want_has_a(want):
    return "a" in (want or [])


def scenario_a(mode_cfg, seeds, tag, request_budget, deadline, mock_extra=None,
               profile="real-20260914", variant=None, scale_sleep=1.0, scale_cooldown=1.0,
               records=40000):
    """场景 a：稳态吞吐（完整管线）。默认用 real-20260914 档（桶阈值 8.4 req/s，
    管线自限 4.65 req/s → 不会触发限流，正是「稳态」的含义）。"""
    variant = variant or {"name": "v10.1", "path": str(BASELINE_V10)}
    out = {"scenario": "a", "tag": tag, "variant": variant["name"],
           "seeds": list(seeds), "profile": profile}
    if not variant.get("path") or not Path(variant["path"]).exists():
        out["skipped"] = "variant 不存在"
        return out
    with MockServer(corpus="real", profile=profile, seeds=seeds, records=records,
                    extra=mock_extra, tag=tag + "-mock") as mk:
        mcfg = mk.config()
        out["mock"] = {"port": mk.port, "config": mcfg.get("corpus"),
                       "expected_crawl": mcfg.get("expected_crawl")}
        cfg = {"tag": tag, "base": mk.base, "seeds": list(seeds),
               "scrape_path": variant["path"], "variant": variant["name"],
               "request_budget": request_budget, "deadline_s": deadline,
               "samples_path": str(RUNS / (tag + ".samples.jsonl")),
               "scale_sleep": scale_sleep, "scale_cooldown": scale_cooldown}
        res = run_child("run", cfg, timeout_s=deadline + 45, tag=tag)
        out["run"] = res
        out["mock_stats"] = mk.stats()
    elapsed = res.get("elapsed_s") or 0
    uniq = res.get("unique_mms") or 0
    reqs = res.get("requests") or 0
    samples_path = RUNS / (tag + ".samples.jsonl")
    act = active_window(samples_path, res.get("t_start_epoch"), res.get("t_end_epoch"))
    e2e = end_to_end_latency(samples_path)
    span = act.get("span_s")
    out["throughput"] = {
        "unique_records": uniq,
        "records_lines": res.get("records_lines"),
        "elapsed_s": elapsed,
        # P4/B4：明确口径，禁止把"整轮平均"当"稳态"
        "unique_per_min": round(uniq / elapsed * 60, 1) if elapsed else 0,
        "unique_per_min_scope": "whole_run_average（整轮墙钟，含 reconcile/启动/终局轮/grace）",
        "unique_per_min_active": (round(uniq / span * 60, 1)
                                  if span and span > 0 else None),
        "unique_per_min_active_scope": "active_window_average（首个请求→末个请求）",
        "active_span_s": span, "startup_s": act.get("startup_s"), "tail_s": act.get("tail_s"),
        "startup_share_pct": (round(100.0 * (act.get("startup_s") or 0) / elapsed, 1)
                              if elapsed else None),
        "records_quality": {"valid_lines": res.get("valid_lines"),
                            "bad_lines": res.get("bad_lines"),
                            "missing_mms_lines": res.get("missing_mms_lines"),
                            "duplicate_lines": res.get("duplicate_lines"),
                            "unique_source": res.get("unique_source")},
        "requests": reqs,
        "req_per_s": round(reqs / elapsed, 3) if elapsed else 0,
        "unique_per_request": round(uniq / reqs, 3) if reqs else 0,
        "by_kind": res.get("by_kind"),
        "by_status": res.get("by_status"),
        "stop_reason": res.get("stop_reason"),
    }
    pred = (out.get("mock") or {}).get("expected_crawl") or {}
    corpus_records = ((out.get("mock") or {}).get("config") or {}).get("records")
    out["fidelity"] = {
        "corpus_records_for_seeds": corpus_records,
        "predicted_requests": pred.get("predicted_requests"),
        "predicted_unique_leaf_lower_bound": pred.get("predicted_unique"),
        "predicted_unique_per_request": pred.get("unique_per_request"),
        "measured_unique_per_request": out["throughput"]["unique_per_request"],
        "request_prediction_error_pct": (
            round((reqs - pred["predicted_requests"]) / pred["predicted_requests"] * 100, 1)
            if pred.get("predicted_requests") and res.get("stop_reason") == "converged" else None),
        "measured_vs_corpus_pct": (round(uniq / corpus_records * 100, 1)
                                   if corpus_records else None),
        "measured_vs_leaf_bound_pct": (round(uniq / pred["predicted_unique"] * 100, 1)
                                       if pred.get("predicted_unique") else None),
    }
    out["latency"] = {"http_ms": res.get("http_latency_ms"),
                      "throttle_wait_ms": res.get("throttle_wait_ms"),
                      "e2e_ms": e2e,
                      "b2_note": ("B2（p95 ≤1.5s）必须两口径并列：传输口径只看服务端响应时间；"
                                  "端到端口径含限速排队（crit-benchmark E4 建议的 A 口径）")}
    return out


def scenario_b(mode_cfg, tag, n_samples=150, conc=8, lat_p50=None):
    """场景 b：延迟分布。采样阶段用 ideal 档（无限流）→ 纯服务时间分布；
    分位算法 = 最近秩法（写死在 nearest_rank）。"""
    out = {"scenario": "b", "tag": tag, "n_samples": n_samples, "concurrency": conc}
    extra = []
    if lat_p50 is not None:
        extra += ["--lat-p50", str(lat_p50)]
    with MockServer(corpus="real", profile="ideal", seeds=["A"], extra=extra,
                    tag=tag + "-mock") as mk:
        out["mock_port"] = mk.port
        cfg = {"tag": tag, "base": mk.base, "n": n_samples, "conc": conc, "prefix": "A1",
               "scrape_path": str(BASELINE_V10), "variant": "v10.1",
               "samples_path": str(RUNS / (tag + ".samples.jsonl"))}
        res = run_child("sample", cfg, timeout_s=180, tag=tag)
        out["run"] = res
        out["server_latency_ms"] = (mk.stats() or {}).get("server_latency_ms")
    client = res.get("http_latency_ms") or {}
    out["client_latency_ms"] = client
    out["percentile_algorithm"] = "nearest-rank: idx=ceil(p/100*n)-1 (no interpolation)"
    srv = out.get("server_latency_ms") or {}
    if client.get("p50") and srv.get("p50"):
        out["client_overhead_ms_p50"] = round(client["p50"] - srv["p50"], 2)
    return out


def scenario_c(mode_cfg, tag, rates, seconds, inject_ratios, inject_rate, seeds,
               pipeline_injection_n=0):
    """场景 c：限流扫描。
      c1 提供速率阶梯（服务端令牌桶 profile=real-20260914，桶阈值 8.4 req/s、越线回 400）
         → 零错误最高速率 + 黄线
      c2 429 确定性注入比例扫描（--err-every-n，n = 1/比例）→ 错误预算与到达率影响
      c3 （可选）完整管线在注入下的行为（冷却升级 / 重试 / 吞吐损失），冷却与 sleep 按 scale 压缩
    """
    out = {"scenario": "c", "tag": tag}
    # ---- c1 速率阶梯 ----
    with MockServer(corpus="real", profile="sweep-20260914", seeds=["A"],
                    tag=tag + "-c1mock") as mk:
        cfg = {"tag": tag + "-c1", "base": mk.base, "rates": rates, "seconds": seconds,
               "preburst": 12, "prefix": "A", "limit_rps": 8.1, "burst": 2}
        res = run_child("sweep", cfg, timeout_s=len(rates) * (seconds + 3) + 60, tag=tag + "-c1")
        out["c1"] = {"rows": res.get("rows") or [], "error": res.get("error"),
                     "server_stats": mk.stats()}
    rows = out["c1"]["rows"]
    zero_ok = [r for r in rows if r["errors"] == 0]
    bad = [r for r in rows if r["errors"] > 0]
    out["c1"]["zero_error_max_rps"] = max((r["achieved_rps"] for r in zero_ok), default=None)
    out["c1"]["zero_error_max_offered_rps"] = max((r["offered_rps"] for r in zero_ok),
                                                  default=None)
    out["c1"]["first_error_offered_rps"] = min((r["offered_rps"] for r in bad), default=None)
    out["c1"]["error_ratio_by_rate"] = {r["offered_rps"]: r["error_ratio"] for r in rows}
    out["c1"]["deficit_model"] = ("errors ≈ max(0, (offered - 8.1)·T - burst)；"
                                  "real-20260914 档 burst=8：5.5 req/s/30s → 0 错 ✓；"
                                  "8.5 req/s/30s → (0.4·30-8)=4 错 = 实测 4×400 ✓"
                                  "（见 report-baseline.md §限流）")
    out["c1"]["yellow_line"] = out["c1"]["first_error_offered_rps"]
    out["c1"]["profile"] = ("sweep-20260914 (token bucket 8.1 req/s, burst 2, 越线=400)；"
                            "与 real-20260914 同速率上限、burst 更小 → 阈值在秒级窗口即可观测")

    # ---- c2 注入比例扫描 ----
    out["c2"] = []
    for n_every in inject_ratios:
        extra = ["--err-every-n", str(n_every), "--err-every-status", "429"] if n_every else []
        tag2 = "%s-c2n%s" % (tag, n_every)
        with MockServer(corpus="real", profile="ideal", seeds=["A"], extra=extra,
                        tag=tag2 + "-mock") as mk:
            cfg = {"tag": tag2, "base": mk.base, "rates": [inject_rate],
                   "seconds": max(1.2, seconds), "preburst": 0, "prefix": "A"}
            res = run_child("sweep", cfg, timeout_s=60, tag=tag2)
        row = (res.get("rows") or [{}])[0]
        row["inject_every_n"] = n_every
        row["inject_ratio"] = (1.0 / n_every) if n_every else 0.0
        row["mock_by_reason"] = (mk.stats() or {}).get("by_reason")
        out["c2"].append(row)

    # ---- c3 管线在注入下的行为（可选，full 模式）----
    if pipeline_injection_n:
        with MockServer(corpus="real", profile="ideal", seeds=list(seeds),
                        extra=["--err-every-n", str(pipeline_injection_n),
                               "--err-every-status", "429"],
                        tag=tag + "-c3mock") as mk:
            cfg = {"tag": tag + "-c3", "base": mk.base, "seeds": list(seeds),
                   "scrape_path": str(BASELINE_V10), "variant": "v10.1+429注入",
                   "request_budget": 60, "deadline_s": 120,
                   "samples_path": str(RUNS / (tag + ".c3.samples.jsonl")),
                   "scale_sleep": 0.05, "scale_cooldown": 0.05}
            res = run_child("run", cfg, timeout_s=180, tag=tag + "-c3")
            out["c3"] = {"run": res, "mock_stats": mk.stats(),
                         "inject_every_n": pipeline_injection_n,
                         "note": "冷却/暂停按 0.05 压缩（真实等待 15-45s → 0.75-2.25s），"
                                 "请求速率不受影响，用于在可接受时长内观察冷却升级序列"}
            el = res.get("elapsed_s") or 0
            out["c3"]["throughput"] = {
                "unique_records": res.get("unique_mms"),
                "elapsed_s": el,
                "unique_per_min": round((res.get("unique_mms") or 0) / el * 60, 1) if el else 0,
                "requests": res.get("requests"),
                "req_per_s": round((res.get("requests") or 0) / el, 3) if el else 0,
                "cooldown_events": res.get("cooldown_events"),
                "cooldown_total_s": res.get("cooldown_total_s"),
                "cooldown_seq": res.get("cooldown_seq"),
                "log_http_events": res.get("log_http_events"),
            }
    return out


def scenario_d(mode_cfg, variants, seeds, tag, request_budget, deadline, budget_left=None):
    """场景 d：版本对照（同一 mock 语料 + 同一工作负载 + 同一预算）。"""
    out = {"scenario": "d", "tag": tag, "seeds": list(seeds), "variants": []}
    for v in variants:
        name = v["name"]
        if not v.get("path") or not Path(v["path"]).exists():
            out["variants"].append({"name": name, "note": v["note"], "skipped": True,
                                    "required": bool(v.get("required", True))})
            continue
        dl = deadline
        if budget_left is not None:
            dl = max(6.0, min(deadline, budget_left() - 3.0))
        r = scenario_a(mode_cfg, seeds, "%s-%s" % (tag, name), request_budget, dl,
                       variant=v)
        ok, why = check_scenario_a(r)
        out["variants"].append({"name": name, "note": v["note"],
                                "required": bool(v.get("required", True)),
                                "valid": ok, "invalid_reason": why,
                                "throughput": r.get("throughput"),
                                "latency": r.get("latency"),
                                "guard": (r.get("run") or {}).get("guard"),
                                "fidelity": r.get("fidelity")})
    ready = [v for v in out["variants"] if not v.get("skipped")]
    if ready:
        base = next((v for v in ready if v["name"] == "v10.1"), ready[0])
        for v in ready:
            b = (base.get("throughput") or {}).get("unique_per_min") or 0
            m = (v.get("throughput") or {}).get("unique_per_min") or 0
            v["vs_baseline_pct"] = round((m - b) / b * 100, 1) if b else None
            ba = (base.get("throughput") or {}).get("unique_per_min_active") or 0
            ma = (v.get("throughput") or {}).get("unique_per_min_active") or 0
            v["vs_baseline_pct_active"] = round((ma - ba) / ba * 100, 1) if ba else None
    return out


# ===========================================================================
# 人读报告
# ===========================================================================
def format_report(mode, res, elapsed_total):
    L = []
    A = L.append
    A("# R2 mock 压测台读数（bench.py，%s 模式）" % mode)
    A("")
    A("- 生成时间：%s" % res["generated_at"])
    A("- 总耗时：%.1fs；字节级复跑命令见 README.md" % elapsed_total)
    A("- 分位算法：最近秩法 idx=ceil(p/100*n)-1（无插值）；传输延迟采样点=SESSION.get 前后（不含限速排队）")
    A("")
    th = res.get("a") or {}
    if th.get("throughput"):
        t = th["throughput"]
        A("## 场景 a — 吞吐（variant=%s，seeds=%s）" % (th.get("variant"),
                                                      "".join(th.get("seeds") or [])))
        A("")
        A("| 读数 | 值 |")
        A("|---|---|")
        A("| unique 记录 | %s |" % t.get("unique_records"))
        A("| records.jsonl 行 | %s |" % t.get("records_lines"))
        A("| 墙钟（整轮） | %ss |" % t.get("elapsed_s"))
        A("| **吞吐 unique/min（整轮平均口径）** | **%s** |" % t.get("unique_per_min"))
        A("| 吞吐 unique/min（活跃窗口口径） | %s |" % t.get("unique_per_min_active"))
        A("| 启动开销 / 占比 | %ss / %s%% |" % (t.get("startup_s"), t.get("startup_share_pct")))
        A("| 有效请求速率 req/s | %s |" % t.get("req_per_s"))
        A("| unique/请求 | %s |" % t.get("unique_per_request"))
        A("| 停止原因 | %s |" % t.get("stop_reason"))
        A("| 请求构成 | %s |" % json.dumps(t.get("by_kind"), ensure_ascii=False))
        A("| 状态码 | %s |" % json.dumps(t.get("by_status"), ensure_ascii=False))
        A("")
        A("> 口径（P4/B4 修正）：`unique_per_min` = unique ÷ **整轮墙钟**（含 reconcile/启动/终局"
          "修复轮/grace）—— 旧报告把它写成「稳态吞吐」是名不副实的；`unique_per_min_active` "
          "= unique ÷ 活跃窗口（首个请求→末个请求，%ss）。两者在短工作负载下会明显分叉。"
          % t.get("active_span_s"))
        A("")
        rq = t.get("records_quality") or {}
        A("> records 质量（P4/B3）：有效行 %s / 坏行 %s / 缺 mms 行 %s / 重复行 %s —— "
          "**unique 只计有效行**（旧口径把坏行各算一个 unique，unique/min 会随坏行数虚高）。"
          % (rq.get("valid_lines"), rq.get("bad_lines"), rq.get("missing_mms_lines"),
             rq.get("duplicate_lines")))
        A("")
        f = th.get("fidelity") or {}
        A("")
        A("保真度交叉核对：语料 %s 条（= 所选种子的真实前缀树总量）；预测请求 %s / 实测 %s"
          "（差 %s%%，来自 title 排序少返回触发的并行补齐）；实测 unique %s = 语料 %s%%、"
          "=叶子理论下界 %s%%" %
          (f.get("corpus_records_for_seeds"), f.get("predicted_requests"), t.get("requests"),
           f.get("request_prediction_error_pct"), t.get("unique_records"),
           f.get("measured_vs_corpus_pct"), f.get("measured_vs_leaf_bound_pct")))
        la = th.get("latency") or {}
        A("")
        A("流水线内延迟：传输 p50=%sms p95=%sms（n=%s）；限速排队 p50=%sms p95=%sms（n=%s）" % (
            (la.get("http_ms") or {}).get("p50"), (la.get("http_ms") or {}).get("p95"),
            (la.get("http_ms") or {}).get("n"),
            (la.get("throttle_wait_ms") or {}).get("p50"),
            (la.get("throttle_wait_ms") or {}).get("p95"),
            (la.get("throttle_wait_ms") or {}).get("n")))
        e2 = la.get("e2e_ms") or {}
        if e2.get("n"):
            A("")
            A("**B2 双口径（P4/E4 补算）** —— 阈值 p95 ≤ 1500ms：")
            A("")
            A("| 口径 | n | p50 | p95 | p99 | max | >1500ms | 判定 |")
            A("|---|---|---|---|---|---|---|---|")
            A("| B：仅传输（旧报告头条） | %s | %s | %s | %s | %s | %s%% | %s |" % (
                (la.get("http_ms") or {}).get("n"), (la.get("http_ms") or {}).get("p50"),
                (la.get("http_ms") or {}).get("p95"), (la.get("http_ms") or {}).get("p99"),
                (la.get("http_ms") or {}).get("max"),
                e2.get("over_budget_pct"), "PASS" if not e2.get("http_p95_over_budget") else "FAIL"))
            A("| A：端到端（含排队，crit-benchmark E4） | %s | %s | %s | %s | %s | %s%% | %s |" % (
                e2.get("n"), e2.get("p50"), e2.get("p95"), e2.get("p99"), e2.get("max"),
                e2.get("over_budget_pct"), "PASS" if not e2.get("e2e_p95_over_budget") else "FAIL"))
            A("")
            A("> %s" % (la.get("b2_note")))
            A("> 分请求类传输延迟：%s（mock 的延迟与页大小无关 ⇒ bulk 类读数**不可外推**到生产，"
              "见 crit-benchmark §B2-2）" % json.dumps(e2.get("by_kind"), ensure_ascii=False))
            A("")
    b = res.get("b") or {}
    if b.get("client_latency_ms"):
        cl = b["client_latency_ms"]
        sr = b.get("server_latency_ms") or {}
        A("## 场景 b — 延迟分布（ideal 档，无服务端限流；并发 %s，样本 %s）" % (
            b.get("concurrency"), b.get("n_samples")))
        A("")
        A("| 视角 | n | p50 | p95 | p99 | max |")
        A("|---|---|---|---|---|---|")
        A("| 客户端（SESSION.get，B2 口径） | %s | %s ms | %s ms | %s ms | %s ms |" % (
            cl.get("n"), cl.get("p50"), cl.get("p95"), cl.get("p99"), cl.get("max")))
        A("| mock 服务端自报（交叉核对） | %s | %s ms | %s ms | %s ms | %s ms |" % (
            sr.get("n"), sr.get("p50"), sr.get("p95"), sr.get("p99"), sr.get("max")))
        A("")
        A("客户端-服务端 p50 差 = %s ms（Python requests 栈开销，非服务端延迟）" %
          b.get("client_overhead_ms_p50"))
        A("")
    c = res.get("c") or {}
    if c.get("c1"):
        A("## 场景 c1 — 提供速率阶梯扫描（服务端 %s）" % c["c1"].get("profile"))
        A("")
        A("| 提供速率 req/s | 窗口内请求 | 200 | 错误 | 达到速率 req/s | 错误率 | 状态码 |")
        A("|---|---|---|---|---|---|---|")
        for r in c["c1"]["rows"]:
            A("| %s | %s | %s | %s | %s | %s | %s |" % (
                r["offered_rps"], r["requests"], r["ok"], r["errors"],
                r["achieved_rps"], r["error_ratio"], json.dumps(r["by_status"])))
        A("")
        A("- **零错误最高速率（实测）**：%s req/s" % c["c1"].get("zero_error_max_rps"))
        A("- **黄线（首次出现 4xx 的提供速率）**：%s req/s" % c["c1"].get("yellow_line"))
        A("")
    if c.get("c2"):
        A("## 场景 c2 — 429 确定性注入比例扫描（提供速率 %.1f req/s）" % (
            c["c2"][0].get("offered_rps") or 0))
        A("")
        A("| 注入比例 | 每 n 个请求 | 请求数 | 200 | 429 | 观测错误率 | 状态码分解 |")
        A("|---|---|---|---|---|---|---|")
        for r in c["c2"]:
            A("| %.2f%% | %s | %s | %s | %s | %s | %s |" % (
                100 * (r.get("inject_ratio") or 0), r.get("inject_every_n") or "-",
                r.get("requests"), r.get("ok"), (r.get("by_status") or {}).get("429", 0),
                r.get("error_ratio"), json.dumps(r.get("by_status"), ensure_ascii=False)))
        A("")
    if c.get("c3"):
        t = c["c3"]["throughput"]
        A("## 场景 c3 — 管线在 429 注入下的行为（每 %s 个请求错 1 个；冷却压缩 0.05×）"
          % c["c3"].get("inject_every_n"))
        A("")
        A("- unique=%s 墙钟=%ss **unique/min=%s** req/s=%s" % (
            t.get("unique_records"), t.get("elapsed_s"), t.get("unique_per_min"),
            t.get("req_per_s")))
        A("- 冷却事件 %s 次，累计冷却 %ss；冷却序列（前若干）：%s" % (
            t.get("cooldown_events"), t.get("cooldown_total_s"),
            json.dumps(t.get("cooldown_seq") or [])[:200]))
        A("- 管线日志里的 HTTP 事件：%s" % json.dumps(t.get("log_http_events"),
                                                    ensure_ascii=False))
        A("- %s" % c["c3"].get("note"))
        A("")
    d = res.get("d") or {}
    if d.get("variants"):
        A("## 场景 d — 版本对照（seeds=%s）" % "".join(d.get("seeds") or []))
        A("")
        A("| 版本 | 说明 | unique/min | req/s | unique/请求 | 传输 p50/p95 | vs 基线 |")
        A("|---|---|---|---|---|---|---|")
        for v in d["variants"]:
            if v.get("skipped"):
                A("| %s | %s | - | - | - | - | 跳过 |" % (v["name"], v.get("note")))
                continue
            t = v.get("throughput") or {}
            la = (v.get("latency") or {}).get("http_ms") or {}
            A("| %s | %s | %s | %s | %s | %s/%s ms | %s%% |" % (
                v["name"], v.get("note"), t.get("unique_per_min"), t.get("req_per_s"),
                t.get("unique_per_request"), la.get("p50"), la.get("p95"),
                v.get("vs_baseline_pct")))
        A("")
    if res.get("reproducibility"):
        r = res["reproducibility"]
        A("## 复现性（同配置两次运行）")
        A("")
        if not r.get("rows"):
            A("- **FAIL/无效**：%s" % (r.get("reason") or "无有效读数（空读数不得判 PASS）"))
        else:
            A("| 指标 | run1 | run2 | 差异 |")
            A("|---|---|---|---|")
            for k, v in r["rows"].items():
                A("| %s | %s | %s | %s%% |" % (k, v["run1"], v["run2"], v["delta_pct"]))
            A("")
            if r.get("missing_keys"):
                A("- **FAIL/无效**：以下指标不可比较（缺失或为 0）：%s" % r["missing_keys"])
            A("- 结论：**%s**（判据 ≤±5%%，比较 %s 次有效运行）" % (
                "通过" if r.get("within_5pct") else "超出 ±5%", r.get("runs_compared", 2)))
        A("")
    v = res.get("verdict")
    if v:
        A("## fail-closed 判定（P4/F1）")
        A("")
        A("- 结论：**%s**" % ("PASS" if v.get("ok") else
                              ("FAIL" if v.get("failures") else "DEGRADED（未测，不是通过）")))
        if v.get("failures"):
            for f in v["failures"]:
                A("  - FAIL：%s" % f)
        if v.get("degraded"):
            for g in v["degraded"]:
                A("  - DEGRADED：%s" % g)
        if v.get("warnings"):
            for w in v["warnings"]:
                A("  - WARN：%s" % w)
        A("- 口径：%s" % v.get("policy"))
        A("")
    if res.get("notes"):
        A("## 备注")
        A("")
        for n in res["notes"]:
            A("- %s" % n)
        A("")
    return "\n".join(L)


def print_summary(mode, res):
    print("=" * 78)
    print("R2 BENCH SUMMARY (%s) — mock 受控压测台" % mode)
    print("=" * 78)
    a = res.get("a") or {}
    if a.get("throughput"):
        t = a["throughput"]
        la = (a.get("latency") or {}).get("http_ms") or {}
        e2 = (a.get("latency") or {}).get("e2e_ms") or {}
        print("[A 吞吐] variant=%s seeds=%s" % (a.get("variant"),
                                               "".join(a.get("seeds") or [])))
        print("    unique=%s  wall=%ss  >>> unique/min(整轮平均)=%s  unique/min(活跃窗口)=%s  "
              "启动占比=%s%%  req/s=%s  unique/请求=%s" % (
                  t.get("unique_records"), t.get("elapsed_s"), t.get("unique_per_min"),
                  t.get("unique_per_min_active"), t.get("startup_share_pct"),
                  t.get("req_per_s"), t.get("unique_per_request")))
        print("    传输延迟 p50=%sms p95=%sms (n=%s) | 限速排队 p50=%sms p95=%sms | 端到端 p50=%sms "
              "p95=%sms >1500ms=%s%% | 状态码=%s" % (
                  la.get("p50"), la.get("p95"), la.get("n"),
                  ((a.get("latency") or {}).get("throttle_wait_ms") or {}).get("p50"),
                  ((a.get("latency") or {}).get("throttle_wait_ms") or {}).get("p95"),
                  e2.get("p50"), e2.get("p95"), e2.get("over_budget_pct"),
                  json.dumps(t.get("by_status"), ensure_ascii=False)))
        rq = t.get("records_quality") or {}
        if rq.get("bad_lines") or rq.get("missing_mms_lines"):
            print("    records 质量：坏行=%s 缺 mms 行=%s（unique 只计有效行，不虚高）" % (
                rq.get("bad_lines"), rq.get("missing_mms_lines")))
    b = res.get("b") or {}
    if b.get("client_latency_ms"):
        cl = b["client_latency_ms"]
        print("[B 延迟分布] 客户端 p50=%sms p95=%sms p99=%sms (n=%s)；服务端自报 p50=%sms p95=%sms"
              % (cl.get("p50"), cl.get("p95"), cl.get("p99"), cl.get("n"),
                 (b.get("server_latency_ms") or {}).get("p50"),
                 (b.get("server_latency_ms") or {}).get("p95")))
    c = res.get("c") or {}
    if c.get("c1"):
        print("[C1 速率阶梯] 零错误最高速率=%s req/s；黄线（首次 4xx）=%s req/s" % (
            c["c1"].get("zero_error_max_rps"), c["c1"].get("yellow_line")))
        for r in c["c1"]["rows"]:
            print("    offered=%-5s achieved=%-6s ok=%-4s err=%-3s status=%s" % (
                r["offered_rps"], r["achieved_rps"], r["ok"], r["errors"],
                json.dumps(r["by_status"])))
    if c.get("c2"):
        print("[C2 429 注入扫描] " + " | ".join(
            "比例%.0f%%→观测%.1f%%(429=%s)" % (
                100 * (r.get("inject_ratio") or 0), 100 * (r.get("error_ratio") or 0),
                (r.get("by_status") or {}).get("429", 0)) for r in c["c2"]))
    if c.get("c3"):
        t = c["c3"]["throughput"]
        print("[C3 注入下管线] unique/min=%s req/s=%s 冷却事件=%s 累计冷却=%ss" % (
            t.get("unique_per_min"), t.get("req_per_s"), t.get("cooldown_events"),
            t.get("cooldown_total_s")))
    d = res.get("d") or {}
    if d.get("variants"):
        print("[D 版本对照] " + " | ".join(
            ("%s: unique/min=%s(%s%%) 活跃窗=%s(%s%%)" % (
                v["name"], (v.get("throughput") or {}).get("unique_per_min"),
                v.get("vs_baseline_pct"),
                (v.get("throughput") or {}).get("unique_per_min_active"),
                v.get("vs_baseline_pct_active"))
             if not v.get("skipped") else "%s: 跳过(%s)%s" % (
                 v["name"], v.get("note"),
                 "" if v.get("required", True) else " [可选]"))
            for v in d["variants"]))
    if res.get("reproducibility"):
        r = res["reproducibility"]
        if not r.get("rows"):
            print("[复现性] FAIL/无效：%s" % (r.get("reason") or "无有效读数"))
        else:
            print("[复现性] %s" % ("PASS (≤±5%)" if r["within_5pct"] else "FAIL (>±5%)"))
            for k, v in r["rows"].items():
                print("    %s: run1=%s run2=%s Δ=%s%%" % (k, v["run1"], v["run2"], v["delta_pct"]))
            if r.get("missing_keys"):
                print("    不可比较的指标（缺失/为 0）: %s" % r["missing_keys"])
    v = res.get("verdict")
    if v:
        if v.get("failures"):
            for f in v["failures"]:
                print("[FAIL][fail-closed] %s" % f)
        for g in v.get("degraded") or []:
            print("[DEGRADED] %s" % g)
        for w in v.get("warnings") or []:
            print("[WARN] %s" % w)
        print("[判定] fail-closed: %s（failure=%d degraded=%d warning=%d）" % (
            "PASS" if v.get("ok") else ("FAIL" if v.get("failures") else "DEGRADED"),
            len(v.get("failures") or []), len(v.get("degraded") or []),
            len(v.get("warnings") or [])))
    print("结果 JSON: %s" % res.get("json_path"))
    print("人读报告: %s" % res.get("md_path"))


def main(argv=None):
    ap = argparse.ArgumentParser(description="R2 mock 压测台")
    ap.add_argument("--quick", action="store_true", help="快速模式（<60s，供 acceptance 复跑）")
    ap.add_argument("--scenarios", default="a,b,c,d")
    ap.add_argument("--repeat", type=int, default=1, help="场景 a 重复次数（复现性检查）")
    ap.add_argument("--store-baseline", action="store_true",
                    help="把本次场景 a 读数存为 timing 基线，供后续漂移对比")
    ap.add_argument("--variant", action="append", default=[], help="额外对照的被测 scrape.py")
    ap.add_argument("--list-variants", action="store_true")
    ap.add_argument("--json-out", default=None)
    ap.add_argument("--md-out", default=None)
    ap.add_argument("--budget-seconds", type=float, default=None)
    ap.add_argument("--require-variants", action="store_true",
                    help="把 bench 自声明的可选变体也升格为必需（缺失 ⇒ FAIL/退出码 4）")
    ap.add_argument("--lat-p50", type=float, default=None, help="延迟场景的 mock 服务时间中位数")
    args = ap.parse_args(argv)

    if args.list_variants:
        for v in variant_list(args.variant):
            print("%-16s %-8s %s" % (v["name"], "ready" if v.get("path") else "MISSING",
                                     v.get("path") or v["note"]))
        return 0

    ensure_dirs()
    t0 = time.time()
    mode = "quick" if args.quick else "full"
    budget = args.budget_seconds if args.budget_seconds is not None else (60.0 if args.quick else 1800.0)
    want = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    seeds = SEEDS_QUICK if args.quick else SEEDS_FULL
    res = {"mode": mode, "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
           "budget_seconds": budget, "scenarios": want, "notes": [],
           "skipped_scenarios": {}}
    mock_cfg = {"corpus": "real"}

    def left():
        return budget - (time.time() - t0)

    # ---------------- 场景 a（+ 复现性重复）----------------
    if "a" in want:
        runs = []
        for i in range(max(1, args.repeat)):
            if left() < 25 and i > 0:
                # P4/F1：预算不足导致的"没测"必须机器可读（degraded），不得静默算过
                res["skipped_scenarios"]["repeat"] = "预算不足（剩余 %.0fs < 25s），只跑了 %d 次" % (
                    left(), i)
                res["notes"].append("复现性重复因预算不足只跑了 %d 次" % i)
                break
            tag = "a%s" % ("" if i == 0 else str(i + 1))
            r = scenario_a(mock_cfg, seeds["a"], tag,
                           request_budget=(400 if args.quick else 4000),
                           deadline=(45 if args.quick else 500))
            runs.append(r)
        res["a"] = runs[0]
        if args.repeat > 1:
            # P4/F1：空读数不得判 PASS —— 先筛出**有效**运行，再要求 ≥2 次且键齐全
            valids = [(r, check_scenario_a(r)) for r in runs]
            good = [r for r, (ok, _w) in valids if ok]
            if len(good) < 2:
                res["reproducibility"] = {
                    "rows": {}, "within_5pct": False, "valid": False,
                    "reason": ("有效运行 %d 次 < 2：空读数/被跳过的运行不得判 PASS（fail-closed）"
                               % len(good)),
                    "invalid_reasons": [w for _r, (ok, w) in valids if not ok]}
                res["notes"].append("复现性判定 FAIL：%s" % res["reproducibility"]["reason"])
            else:
                keys = ["unique_per_min", "req_per_s", "unique_per_request", "requests"]
                rows = {}
                within = True
                missing = []
                for k in keys:
                    v1 = (good[0].get("throughput") or {}).get(k)
                    v2 = (good[1].get("throughput") or {}).get(k)
                    if v1 in (None, 0) or v2 in (None, 0):
                        missing.append(k)               # P4/F1：缺键 = 不可比较，不是"通过"
                        within = False
                        continue
                    d = abs(v2 - v1) / v1 * 100
                    within &= d <= 5.0
                    rows[k] = {"run1": v1, "run2": v2, "delta_pct": round(d, 2)}
                res["reproducibility"] = {"rows": rows, "within_5pct": bool(within),
                                          "valid": bool(rows) and not missing,
                                          "missing_keys": missing,
                                          "runs_compared": 2,
                                          "runs_total": len(runs)}
        if args.store_baseline:
            (RESULTS / "timing-baseline.json").write_text(
                json.dumps(res["a"].get("throughput"), ensure_ascii=False, indent=1))

    # ---------------- 场景 b ----------------
    if "b" in want:
        if left() < 10:
            res["skipped_scenarios"]["b"] = "预算不足（剩余 %.0fs < 10s）" % left()
            res["notes"].append("场景 b 因预算不足跳过")
        else:
            res["b"] = scenario_b(mock_cfg, "b", n_samples=(100 if args.quick else 400),
                                  conc=8, lat_p50=args.lat_p50)

    # ---------------- 场景 c ----------------
    if "c" in want:
        if left() < 20:
            res["skipped_scenarios"]["c"] = "预算不足（剩余 %.0fs < 20s）" % left()
            res["notes"].append("场景 c 因预算不足跳过")
        else:
            rates = ([4.0, 8.0, 11.0, 15.0] if args.quick
                     else [2.5, 5.5, 8.0, 8.8, 9.5, 11.0, 13.0])
            secs = 1.5 if args.quick else 4.0
            ratios = [0, 8] if args.quick else [0, 50, 20, 10, 5]
            res["c"] = scenario_c(mock_cfg, "c", rates, secs, ratios,
                                  inject_rate=(8.0 if args.quick else 10.0),
                                  seeds=seeds["c"],
                                  pipeline_injection_n=0 if args.quick else 25)

    # ---------------- 场景 d ----------------
    if "d" in want:
        if left() < 14:
            res["skipped_scenarios"]["d"] = "预算不足（剩余 %.0fs < 14s）" % left()
            res["notes"].append("场景 d 因预算不足跳过")
        else:
            vs = variant_list(args.variant)
            res["d"] = scenario_d(mock_cfg, vs, seeds["d"], "d",
                                  request_budget=(200 if args.quick else 4000),
                                  deadline=(30 if args.quick else 400), budget_left=left)

    total = time.time() - t0
    stamp = time.strftime("%Y%m%d-%H%M%S")
    # P4/F1：fail-closed 判定（空读数/被跳过的必需场景 ⇒ FAIL + 退出码 4）
    res["verdict"] = evaluate_verdict(res, want, require_variants=args.require_variants)
    json_path = Path(args.json_out) if args.json_out else RESULTS / ("bench-%s-%s.json" % (mode, stamp))
    md_path = Path(args.md_out) if args.md_out else RESULTS / ("bench-%s-%s.md" % (mode, stamp))
    res["total_seconds"] = round(total, 2)
    res["json_path"] = str(json_path)
    res["md_path"] = str(md_path)
    json_path.write_text(json.dumps(res, ensure_ascii=False, indent=1))
    md_path.write_text(format_report(mode, res, total))
    (RESULTS / ("latest-%s.json" % mode)).write_text(
        json.dumps(res, ensure_ascii=False, indent=1))
    print_summary(mode, res)
    print("总耗时 %.1fs（预算 %.0fs）" % (total, budget))
    v = res["verdict"]
    if v["failures"]:
        print("BENCH VERDICT: FAIL（fail-closed：%d 项必需场景无真实读数；退出码 4）"
              % len(v["failures"]))
        return 4
    if v.get("degraded"):
        print("BENCH VERDICT: DEGRADED（%d 项未测：预算不足/可选变体缺失；退出码 5 —— 不是通过）"
              % len(v["degraded"]))
        return 5
    print("BENCH VERDICT: PASS（所有必需场景均有非空真实读数）")
    return 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ("_run", "_sweep", "_sample"):
        _which = sys.argv[1]
        _ap = argparse.ArgumentParser()
        _ap.add_argument("--config", required=True)
        _a = _ap.parse_args(sys.argv[2:])
        _cfg = json.loads(Path(_a.config).read_text())
        if _which == "_run":
            mode_run(_cfg)
        elif _which == "_sweep":
            mode_sweep(_cfg)
        elif _which == "_sample":
            mode_sample(_cfg)
        else:
            raise SystemExit("unknown subcommand")
    else:
        sys.exit(main())
