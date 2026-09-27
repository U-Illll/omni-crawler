#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""heartbeat.py — 抓取管线「心跳 + 偏移水位」sidecar（R2 · X2 缺口闭合）

X2 缺口（R1 crit-benchmark 判定）：外部只能看 progress.json / records.jsonl 的 mtime，
无法知道「管线处理到哪、是否停滞」。本模块给出一个 O(1) 可读的偏移水位心跳。
心跳文件默认 <OUT>/heartbeat.json（OUT 与 scrape.py 同源：SCRAPE_OUT_DIR 或
/tmp/library-scrape/output），可用 SCRAPE_HEARTBEAT_FILE 直接指定文件路径。

一、心跳文件 schema（heartbeat/v1）
-----------------------------------
{
 "schema": "heartbeat/v1",
 "ts": "2026-09-15T18:20:00.123+08:00",   # 本次心跳写入时间（ISO，本地时区）
 "ts_epoch": 1757931600.123,              # 同上，epoch 秒（读侧算 age 用这个）
 "round": 42,                             # 本进程内第几次心跳（同 boot_id 续号，单调 +1）
 "writes": 42,                            # 心跳文件累计写入次数（跨重启续号）
 "pid": 12345, "boot_id": "12345-17579-ab12cd34", "proc_start_epoch": ...,
 "since_boot_s": 3712.4,                  # 本进程已运行秒数
 "prefix": "A1B2",                        # 当前/最近一次处理的前缀（= save_progress 的 ctx）
 "todo": 118, "done": 4312, "gaps": 3,    # 队列计数快照（取自 progress.json）
 "leaves": 4312, "records": 288451,       # 本次快照计数（**非单调**，重建/回滚时会退）
 "watermark": {                           # ★ 偏移水位（单调不减；读侧判定停滞只看这里）
    "schema": "watermark/v1",
    "leaves": 4312,                       # 历史最大「已成功落盘叶子数」（stats.leaves 的高水位）
    "records": 288451,                    # 历史最大「已成功落盘记录数」（stats.records 的高水位）
    "prefix": "A1B2",                     # 水位最后推进时正在处理的 prefix
    "ts": "...", "ts_epoch": ...,         # 水位最后推进的时间 = 最近一次成功落盘的时刻
    "advanced": true,                     # 本次写入是否推进了水位
    "advance_count": 4312,                # 水位累计推进次数
    "clamped": false, "clamp_events": 0,  # source 侧回退被夹住（progress 被重建/回滚）的审计
    "prev": {"leaves": 4311, "records": 288449, "ts_epoch": ...}   # 上一版水位（审计）
 },
 "prog_saved": "2026-09-15T18:20:00.120000",  # 该次落盘的 progress.json["saved"]（配对校验用）
 "prog_schema": "v10",
 "recovery_events": 12,
 "build_ms": 0.22,                        # 组装耗时（合并水位等，不含 IO）
 "extra": {"writer_lock_held": true,      # 本进程是否持有存活信标（第二个写者为 false）
           "write_gate": true,            # 本次写入是否拿到跨进程写入门
           "cross_process_lock": true}
}
（端到端写耗时读数由 heartbeat._state["last_write_ms"] 暴露，不写进文件以免自我指涉。）

水位（offset watermark）语义 —— 读侧契约
----------------------------------------
W1 单调不减：watermark.leaves / watermark.records 只增不减——即使 progress.json 被
   reconcile 重算、被 .bak 回退、被 rebuild_from_records 重建，或进程重启（新进程读旧
   心跳取 max）。watermark.ts 同样不减（时钟回拨被夹住并记 clock_backward）。
W2 只记录「已成功落盘」：心跳在 save_progress() 完成（fsync + rename + 目录 fsync）之后
   才写，因此 watermark <= 磁盘上 progress.json/records.jsonl 已声明的事实，绝不超前。
W3 推进 = 数据推进：watermark.ts 只在 leaves 或 records 增长时才前进；管线空转
   （只是反复 save_progress）不会推进水位 —— 这正是「假活」与「真推进」的判据。
W4 回退可见：顶层 leaves/records 只是快照；若快照 < 水位（clamped），说明 progress
   出现过回滚，读侧应升级为重量全检。

二、写入时机与原子性
--------------------
- 时机：scrape.py 的 save_progress() 末尾（唯一集成点）；落盘顺序恒为
  records.jsonl → progress.json → heartbeat.json，心跳永不超前于它描述的事实。
- 原子性：唯一 tmp 名（pid.tid.seq）+ fsync(文件) + os.replace + fsync(目录)，
  外加跨进程 flock；任何时刻 kill -9/断电都不会留下半写心跳——读侧要么看到上一版完整
  心跳、要么看到这一版，永不看到半个 JSON。测试缝（mid_tmp_write，双开关默认彻底关闭）
  可在「tmp 已写入一半并 fsync」处停顿，用于把 kill -9 精确打进该窗口做原子性验证。
- 写者存活凭据：写者进程**非阻塞**获取 heartbeat.json.lock 的排他 flock 并持有到退出
  （内核在进程终止时自动释放）。读侧用 LOCK_EX|LOCK_NB 试探即可**精确**判断写者是否还在
  （无 PID 复用误判、不向任何进程发信号）；锁文件不存在时退回 PID+启动时刻启发式。
- 两个锁文件，各司其职（都放在 OUT，随心跳文件一起）：
  * <heartbeat>.lock  = 存活信标：主写者**非阻塞**获取并持有到进程退出；读侧 LOCK_NB 试探
    即可精确判断「写者是否还在」；不可被阻塞等待，因此第二个进程绝不会被它拖死。
  * <heartbeat>.wlock = 写入门：每次写入期间持有「读旧值→合并→rename」整段（**有界等待**
    0.25s，超时则本次心跳跳过）；任何数量的写者都必须过这道门 ⇒ 水位在任何并发下都不回退。
- 部署建议：同一 OUT 目录仍应只有一个写者进程（scrape.py 单实例）。第二个写者拿不到信标
  （心跳里 extra.writer_lock_held=false，并写一行 heartbeat.log），但仍能安全写入：文件永不
  半写、水位不回退 ⇒ keeper 重启时新旧实例短暂并存不会互相拖死，也不会污染水位。

三、读侧 API
------------
read(path=None)         -> dict | None       # 宽容读（缺失/空/损坏 → None，不抛异常）
load(path=None)         -> Heartbeat | None  # 视图对象（.age()/.watermark_age()/.writer_alive()）
is_stalled(now=None, threshold_s=None, path=None, check_pid=True, require_writer=False)
    -> Verdict（dict 子类；bool(v) == v['stalled']；可 json.dumps）
    停滞判定主入口：now 可为 epoch 秒（默认当前），threshold_s 默认 900s
    （= state-check --stale-warn，可用 SCRAPE_HEARTBEAT_STALL_S 覆盖）。
    判据：now - watermark.ts_epoch >= threshold_s ⇒ stalled=True（数据水位停滞）。
    写者进程存活恒以 writer_alive/writer_gone 暴露；默认不并入 stalled（正常收敛退出
    也会留下「写者已死」），keeper 可用 require_writer=True / --require-writer 升级为 WARN。
CLI：python3 heartbeat.py status [--json] [--stall-threshold 900] [--now EPOCH]
     exit 0=OK / 1=WARN（水位曾回滚，或 --require-writer 命中）/ 3=STALL / 2=用法错误

四、对接 state-check：轻量心跳 vs 重量全检
----------------------------------------
轻量（每次巡检，O(1)）：只读 1 个几 KB 的 JSON，不碰 records.jsonl（135MB 级全扫数十秒）。
    keeper/watchdog 每 <=180s 巡检（A2/C3）用：
        python3 heartbeat.py status --json          # exit != 0 → 升级
    判定停滞必须用 watermark.ts_epoch，**不要用 progress.json/records.jsonl 的 mtime**：
    reconcile/空转保存会把 mtime 刷成「刚刚」，而数据一毫米没动（假活）。
重量（升级路径，O(records)）：仅在轻量判定异常时执行，把「停滞」定性为具体缺陷：
        python3 state-check.py <OUT>/progress.json <OUT>/records.jsonl --json /tmp/sc.json
    对应关系：
      watermark_stale / heartbeat_stale → 与 state-check C3（mtime 停滞）同源，但心跳更早更准；
      clamped / snapshot < watermark    → progress 回滚，跑 C1（声称完成却无记录）；
      heartbeat_stale（连心跳都不写）   → 进程卡死/被冻：先看 keeper 与 scrape.log，再跑全检；
      watermark.records 是 C2 records 行数的下界（真实落盘量），可先做量级判断。
    健康形态：水位单调递增、heartbeat age 远小于阈值、prefix 随 DAG 前进而变化；
    一旦 age 触阈值即 stall，与队列是否为空无关。

五、集成与部署（scrape.py 侧只改 1 行）
------------------------------------
- heartbeat.py 与本文件同目录（与 scrape.py 同一目录）。集成行是函数内 import：
  save_progress() 末尾 `import heartbeat as _hb; _hb.write_heartbeat(prog, prefix=ctx)`；
  以 `python3 /path/to/scrape.py` 启动时 sys.path[0] 即脚本目录，故必定解析到同目录的
  heartbeat.py（不依赖 cwd、不改 scrape.py 文件头 import 区）。
- 环境变量（全部可选）：
    SCRAPE_HEARTBEAT_FILE   心跳文件全路径（默认 <SCRAPE_OUT_DIR>/heartbeat.json）
    SCRAPE_HEARTBEAT_STALL_S 停滞阈值秒（默认 900，与 state-check --stale-warn 一致）
    SCRAPE_HEARTBEAT_FSYNC  0=心跳不做 fsync（省 IO；仍永不半写，极端断电最多回退一版）
    SCRAPE_HEARTBEAT_LOG    0=不写 heartbeat.log（默认只在异常时写）
- 写者不开销抓取：write_heartbeat() 内部异常一律吞掉并记 heartbeat.log；
  跨进程写入门有界等待 0.25s，超时只是本次心跳跳过，绝不阻塞 save_progress()。

只读保证：读侧函数（read/read_status/load/is_stalled/status/dump/lock_holder_alive）
不写、不创建任何文件——探活只在锁文件**已存在**时打开它试探（不会因为巡检而新建文件）。
写侧只在心跳文件所在目录写：heartbeat.json（原子替换）、<heartbeat>.lock（存活信标）、
<heartbeat>.wlock（写入门）、以及仅在出错时追加 heartbeat.log。
本模块不创建任何目录、不访问网络、不发送任何信号（进程存活探测使用信号 0，不影响目标进程）。

六、P4 修复（R2 批评/验证实证缺陷；crit-correct H1/H3/H4、crit-adversarial C3/D2）
------------------------------------------------------------------------------------
F1【H1 高】`save_progress` 内 `import heartbeat` 无保护 ⇒ 心跳模块缺席（bench/沙箱的
   "只拷 scrape.py" 布局）时 ModuleNotFoundError 从 8 个调用点抛出 → 抓取主循环崩 →
   keeper 重启 → 再崩（崩溃循环）。修法不在本文件而在集成行：`scrape-heartbeat-guard.patch`
   把该行改成 `try/except` + 单次告警 + 降级继续（与 slot-mem 的 fail-open 一致；
   本文件的 `write_heartbeat()` 本来就永不抛出，缺口只在 import 那一步）。
F2【H3 中】心跳载荷不含限速/退避状态 ⇒ keeper 无法区分「在退避」与「卡死」。
   修法：payload 增加 `runtime` 块（扁平便捷键 + 嵌套明细）：
     gate_open / gate_closed_s / mem_level / delay_s / strikes / backoff_active /
     backoff_reason / last_success_ts_epoch / last_success_age_s，
     明细来自 `memguard.heartbeat_state()` 与 `__main__.LIMITER.snapshot()`（两者都 fail-open，
     缺席时字段为 null，绝不伪造"健康"）。
F3【C3 集成高】闸门冻结时心跳仍报"健康在推进"（crit-adversarial 实证：闸门关闭 79s 期间
   心跳 advance_count 仍在涨、keeper 判 OK）。修法：F2 的 `runtime.gate_open/gate_closed_s`
   参与判定 —— 闸门关闭 ≥ SCRAPE_HEARTBEAT_GATE_WARN_S(30s) ⇒ WARN + escalate；
   ≥ SCRAPE_HEARTBEAT_GATE_FREEZE_S(300s) ⇒ 追加 `gate_frozen`（`frozen=true`，
   escalate=true），并给出 `stall_explained_by` 供 keeper 决策。
F4【D2 中】跑完的正常进程 900s 后被判 STALL（exit 3）⇒ keeper 空转重启环。
   修法：完成态语义。`state=done`（进程正常退出时由 atexit 写的**终态心跳**）或
   「快照 todo==0 && gaps==0 且写者已退出」⇒ verdict=`DONE`(exit 0)、stalled=False、
   escalate=False。`--require-writer` 不再把完成态翻成 STALL。
F5【H4 中】水位回退判据永久挂 WARN（一次回退后每次巡检都 WARN+escalate，要求跑 O(records)
   全检）。修法：watermark 增加 `first_clamp_ts_epoch`（本次"低于水位"**episode 的起点**）
   与 `clamp_episode`；读侧只在 episode 起点后 SCRAPE_HEARTBEAT_CLAMP_WARN_S(900s) 内
   WARN+escalate，之后降级为信息字段 `watermark_below_high`（verdict 不受影响）。
   旧版心跳（无 first_clamp_ts_epoch）一律按信息字段处理，避免版本混跑时永久 WARN。

新增/变更的环境变量：
    SCRAPE_HEARTBEAT_GATE_WARN_S     闸门关闭多久开始 WARN（默认 30）
    SCRAPE_HEARTBEAT_GATE_FREEZE_S   闸门关闭多久标记 gate_frozen（默认 300）
    SCRAPE_HEARTBEAT_CLAMP_WARN_S    水位回退 WARN 的有效期（默认 900）
    SCRAPE_HEARTBEAT_BACKOFF_GRACE_S 退避能把停滞"解释掉"的宽限秒数；0（默认）=不降级
    SCRAPE_HEARTBEAT_TERMINAL        1（默认）=进程正常退出且已收敛时写终态心跳
    SCRAPE_HEARTBEAT_RUNTIME         0=不采集 runtime 块（默认 1）

新增退出码语义（其余不变）：
    DONE（exit 0）：`state=done`，或 todo==0 && gaps==0 且写者已退出 —— 正常收敛完成，
    不是停滞；keeper 不应据此重启。
"""
import argparse
import atexit
import contextlib
import json
import os
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

__version__ = "1.1.0-p4"

SCHEMA = "heartbeat/v1"
WATERMARK_SCHEMA = "watermark/v1"
HB_NAME = "heartbeat.json"
HB_LOG_NAME = "heartbeat.log"
DEFAULT_STALL_S = 900.0        # 与 state-check --stale-warn 默认值对齐
CLOCK_TOL_S = 1.0              # 时钟回拨判定容差（亚毫秒抖动来自 3 位小数舍入）
DROP_NAME = "%s.tmp.%d.%d.%d"  # heartbeat.json.tmp.<pid>.<tid>.<seq>

# P4 阈值（全部可用环境变量/CLI 覆盖）
DEFAULT_GATE_WARN_S = 30.0     # 闸门关闭多久开始 WARN（F3/C3）
DEFAULT_GATE_FREEZE_S = 300.0  # 闸门关闭多久标记 gate_frozen（F3/C3）
DEFAULT_CLAMP_WARN_S = 900.0   # 水位回退 episode 起点后多久内仍 WARN（F5/H4）

EXIT_OK, EXIT_WARN, EXIT_USAGE, EXIT_STALL = 0, 1, 2, 3

_write_lock = threading.RLock()          # 进程内串行化（保证合并-写入不交错）
_state = {"boot_id": None, "boot_ts": None, "round": 0, "last_write_ms": None,
          "snapshot": None, "atexit_hooked": False, "terminal_written": False}
_seq = [0]


# --------------------------------------------------------------------------- 小工具
def _int(v, default=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _float(v, default=None):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _len(v):
    return len(v) if isinstance(v, (list, tuple, dict)) else 0


def _env_float(name, default):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return _float(raw, default) if _float(raw, default) is not None else default


def iso(ts):
    return datetime.fromtimestamp(float(ts)).astimezone().isoformat(timespec="milliseconds")


def default_stall_threshold():
    """停滞阈值（秒）：SCRAPE_HEARTBEAT_STALL_S 覆盖，默认 900s。"""
    v = _env_float("SCRAPE_HEARTBEAT_STALL_S", DEFAULT_STALL_S)
    return float(v) if v and v > 0 else DEFAULT_STALL_S


def _thr(name, default, allow_zero=False):
    """读一个正数阈值（<=0 视为未设置；allow_zero=True 时 0 合法，用于"关闭该判据"）。"""
    v = _env_float(name, default)
    if v is None:
        return default
    if v <= 0 and not allow_zero:
        return default
    return float(v)


def default_gate_warn_s():
    return _thr("SCRAPE_HEARTBEAT_GATE_WARN_S", DEFAULT_GATE_WARN_S, allow_zero=True)


def default_gate_freeze_s():
    return _thr("SCRAPE_HEARTBEAT_GATE_FREEZE_S", DEFAULT_GATE_FREEZE_S, allow_zero=True)


def default_clamp_warn_s():
    return _thr("SCRAPE_HEARTBEAT_CLAMP_WARN_S", DEFAULT_CLAMP_WARN_S, allow_zero=True)


def default_backoff_grace_s():
    return _thr("SCRAPE_HEARTBEAT_BACKOFF_GRACE_S", 0.0, allow_zero=True)


def default_path():
    """心跳文件路径：SCRAPE_HEARTBEAT_FILE 优先；否则 <OUT>/heartbeat.json，
    OUT = SCRAPE_OUT_DIR 或 scrape.py 的默认输出目录。"""
    override = os.environ.get("SCRAPE_HEARTBEAT_FILE")
    if override:
        return Path(override)
    out = os.environ.get("SCRAPE_OUT_DIR") or "/tmp/library-scrape/output"
    return Path(out) / HB_NAME


# --------------------------------------------------------------------------- 进程身份
def _proc_start_epoch(pid=None):
    """进程启动时刻（epoch 秒），用于识别 PID 复用；无法判定时返回 None。"""
    pid = os.getpid() if pid is None else _int(pid)
    try:
        with open("/proc/uptime", "rb") as f:
            uptime = float(f.read().split()[0])
        with open("/proc/%d/stat" % pid, "rb") as f:
            raw = f.read()
        tail = raw[raw.rfind(b")") + 2:].split()
        ticks = int(tail[19])                      # stat 第 22 字段 starttime
        hz = float(os.sysconf("SC_CLK_TCK"))
        return time.time() - uptime + ticks / hz
    except Exception:
        return None


def pid_alive(pid, expect_start_epoch=None, tolerance=0.2):
    """进程存活探测：os.kill(pid, 0) 只做存在性检查，不发送任何信号。
    expect_start_epoch 已知时用于排除「PID 被复用」的误判：/proc 的 starttime 与
    /proc/uptime 各自按 10ms 刻度量化，同一进程两次读数的抖动 <=20ms，
    故取 0.2s 容差（10 倍余量）；PID 复用意味着新进程的启动时刻相差通常 >=0.3s。
    返回 True/False/None（None=无法判定）。注意这是**兜底**路径：正常心跳都带锁文件，
    读侧优先用 flock 精确探活（见 Heartbeat.writer_liveness）。"""
    if pid is None:
        return None
    pid = _int(pid, -1)
    if pid <= 0:
        return None
    if expect_start_epoch:
        cur = _proc_start_epoch(pid)
        if cur is not None and abs(cur - _float(expect_start_epoch, 0.0)) > tolerance:
            return False                           # /proc 里的进程不是当初写心跳的那个
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _boot_id():
    if _state["boot_id"] is None:
        start = _proc_start_epoch() or time.time()
        _state["boot_ts"] = float(start)
        _state["boot_id"] = "%d-%.3f-%s" % (os.getpid(), start, uuid.uuid4().hex[:8])
    return _state["boot_id"]


# --------------------------------------------------------------------------- 原子写
def _write_all(fd, data):
    view = memoryview(data)
    while view:
        n = os.write(fd, view)
        if n <= 0:
            raise OSError("short write to fd %r" % (fd,))
        view = view[n:]


def _fsync_on():
    """心跳的 fsync 开关（SCRAPE_HEARTBEAT_FSYNC=0 可关闭以省 IO）。
    关闭后仍保持「永不半写」（rename 原子性来自 VFS），极端断电下最多回退到上一版心跳。"""
    return os.environ.get("SCRAPE_HEARTBEAT_FSYNC", "1") != "0"


def _fsync(fd):
    if _fsync_on():
        os.fsync(fd)


def _fsync_dir(path):
    if not _fsync_on():
        return
    try:
        dfd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dfd)
    except OSError:
        pass
    finally:
        os.close(dfd)


_lifetime_locks = {}          # lock_path -> fd（存活信标：持有到进程退出，期间不重复 flock）
_degraded_warned = set()       # 已就「未拿到存活信标」告警过的锁文件


@contextlib.contextmanager
def _write_gate(target, timeout_s=0.25, poll_s=0.002):
    """每次写入的跨进程临界区（<heartbeat>.wlock）：把「读旧心跳 → 合并水位 → rename」
    整段互斥，因此**任意写者数量**下水位都不会回退（任何写者都必须先拿到门）。

    带**有界等待**：超时即让本次心跳跳过（yield False）——绝不阻塞抓取主流程。
    与存活信标（<heartbeat>.lock）分开：信标由主写者持有到退出（供读侧精确判断存活），
    门只在单次写入期间持有。两者都不会形成等待环（信标是非阻塞获取）。"""
    fcntl = None
    try:
        import fcntl as _fcntl
        fcntl = _fcntl
    except Exception:
        fcntl = None
    fd, ok = None, False
    if fcntl is not None:
        try:
            fd = os.open(str(target) + ".wlock", os.O_WRONLY | os.O_CREAT, 0o644)
            deadline = time.time() + max(0.0, float(timeout_s))
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    ok = True
                    break
                except OSError:
                    if time.time() >= deadline:
                        break
                    time.sleep(poll_s)
        except Exception:
            ok = False
    else:
        ok = True           # 平台无 fcntl：不做跨进程互斥，但绝不因此停写
    try:
        yield ok
    finally:
        if fd is not None:
            if ok:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except OSError:
                    pass
            try:
                os.close(fd)
            except OSError:
                pass


def _acquire_lifetime_lock(target):
    """尝试获取「持有到进程退出」的排他 flock（heartbeat.json.lock），返回 fd 或 None。

    两个作用：
    (a) 让读侧可以**精确**探测写者是否还活着：写者进程活着就持有着这把 flock，
        进程退出/kill -9 时由内核自动释放（不依赖 PID 复用启发式，也不发任何信号）；
    (b) 确立「同一 OUT 目录只有一个写者」：单写者下「读旧心跳 → 合并水位 → 改名」天然串行，
        水位不会回退。

    **必须用 LOCK_NB（非阻塞）+ 失败即降级**：否则第二个写者（例如 keeper 重启时新旧两个
    实例短暂并存）会在第一次心跳写入处**永久阻塞**，把抓取主流程一起拖死。
    拿不到锁时的语义：本进程降级为「尽力而为写者」——文件仍然永不半写（rename 原子性不依赖
    锁），但跨进程水位单调性不再有保证（每次写都重新读文件取 max，故最多瞬时回退一次并在
    下一次写入自愈）。每个进程每次写都会重试 NB 抢占，因此主写者退出后新的写者会接管。"""
    lock_path = str(target) + ".lock"
    fd = _lifetime_locks.get(lock_path)
    if fd is not None:
        return fd
    try:
        import fcntl
    except Exception:
        return None
    fd = None
    try:
        fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT, 0o644)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        if lock_path not in _degraded_warned:
            _degraded_warned.add(lock_path)
            _hb_log("另一个写者进程正持有 %s → 本进程降级为尽力而为写者"
                    "（文件仍永不半写；同目录应按单写者原则部署，见 heartbeat.py 文档）"
                    % lock_path, target)
        return None
    except Exception:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        return None
    _lifetime_locks[lock_path] = fd
    return fd


def lock_holder_alive(target):
    """读侧探测：心跳锁文件是否存在活着的持有者。返回 True/False/None（None=无法判定）。

    零副作用：只在锁文件**已存在**时以 O_WRONLY 打开（不创建任何文件），
    用 LOCK_EX|LOCK_NB 试探——抢到即说明没有活写者（拿到后立刻释放）。"""
    lock_path = str(target) + ".lock"
    try:
        import fcntl
    except Exception:
        return None
    try:
        if not os.path.exists(lock_path):
            return None
    except OSError:
        return None
    fd = None
    try:
        fd = os.open(lock_path, os.O_WRONLY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return True                       # 有活着的持有者 ⇒ 写者还在
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        return False                          # 抢到 ⇒ 没有活写者
    except OSError:
        return None
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass


def _seam_active(point):
    """测试缝开关（双开关，默认彻底关闭，仿 scrape.py 的 SCRAPE_FAULT 设计）。"""
    return (os.environ.get("SCRAPE_HEARTBEAT_TEST_SEAM") == point
            and os.environ.get("SCRAPE_HEARTBEAT_ALLOW_TEST_SEAMS") == "1")


def _seam(point, tmp=None):
    """测试缝延迟点：仅用于「把 kill -9 精确打进原子替换窗口内」的原子性验证。
    生产路径零成本（未设 env 时只是一次字符串比较）。"""
    if not _seam_active(point):
        return
    delay = _env_float("SCRAPE_HEARTBEAT_SEAM_DELAY", 0.0) or 0.0
    if delay > 0:
        time.sleep(delay)


def _seam_half_point(data):
    """mid_tmp_write 缝：返回「前半长度」以制造真实的半写窗口（fsync 后停顿，
    被 kill 时 tmp 里恰好是半个 JSON 文档，而 heartbeat.json 仍是上一版完整内容）。"""
    if not _seam_active("mid_tmp_write"):
        return None
    return max(1, len(data) // 2)


def _hb_log(msg, target=None):
    """异常/告警日志：只写 heartbeat.log（不污染 scrape.log），失败即忽略。"""
    if os.environ.get("SCRAPE_HEARTBEAT_LOG", "1") == "0":
        return
    try:
        path = Path(target) if target is not None else default_path()
        line = "[%s] %s\n" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg)
        with open(str(path.with_name(HB_LOG_NAME)), "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


def _next_seq():
    with _write_lock:
        _seq[0] += 1
        return _seq[0]


# --------------------------------------------------------------------------- 水位合并
def merge_watermark(prev_wm, leaves, records, prefix, now, prev_ts=None):
    """纯函数：把本次快照合并进上一版水位，返回 (watermark_dict, advanced, clamped)。

    单调不减（W1）：两个维度各取历史最大值；source 侧回退被夹住（clamped=True）并计数，
    绝不把水位改小。时间只在推进时前进，且不因时钟回拨而倒退（clock_backward 记账）。

    P4/F5（H4）：额外记录「低于水位 episode」的起点与长度 ——
      · `first_clamp_ts_epoch` = 本次 episode 里**第一次**被夹住的时间（episode 内保持，
        重新追上水位后清零）；读侧据此只在 episode 起点后的窗口内 WARN，之后降级为信息字段，
        从而修掉"一次回退 → 此后每次巡检都 WARN+escalate"的永久告警。
      · `clamp_episode` = 本次 episode 内连续被夹住的写入次数（0 = 当前未被夹）。
    """
    prev_wm = prev_wm if isinstance(prev_wm, dict) else {}
    prev_l = _int(prev_wm.get("leaves"))
    prev_r = _int(prev_wm.get("records"))
    prev_ts_epoch = _float(prev_wm.get("ts_epoch"))
    if prev_ts_epoch is None:
        prev_ts_epoch = _float(prev_ts)
    leaves = _int(leaves)
    records = _int(records)
    wm_l, wm_r = max(leaves, prev_l), max(records, prev_r)
    clamped = bool(leaves < prev_l or records < prev_r)
    advanced = bool(wm_l > prev_l or wm_r > prev_r)
    if clamped:
        prev_first = _float(prev_wm.get("first_clamp_ts_epoch"))
        prev_ep = _int(prev_wm.get("clamp_episode"))
        first_clamp_ts = prev_first if (prev_ep >= 1 and prev_first is not None) else now
        clamp_episode = prev_ep + 1
    else:
        first_clamp_ts = None
        clamp_episode = 0
    # 时钟回拨：只认 >1s 的真实回拨（亚毫秒抖动来自序列化的 3 位小数舍入）
    clock_backward = bool(prev_ts_epoch is not None and now < prev_ts_epoch - CLOCK_TOL_S)
    if advanced or prev_ts_epoch is None:
        ts_epoch = now
        wm_prefix = prefix if prefix is not None else prev_wm.get("prefix")
    else:
        ts_epoch = prev_ts_epoch
        wm_prefix = prev_wm.get("prefix")
    if prev_ts_epoch is not None and ts_epoch < prev_ts_epoch:
        ts_epoch = prev_ts_epoch                      # 时间同样单调不减（时钟回拨被夹住）
    return ({
        "schema": WATERMARK_SCHEMA,
        "leaves": wm_l,
        "records": wm_r,
        "prefix": wm_prefix,
        "ts": iso(ts_epoch),
        "ts_epoch": round(float(ts_epoch), 3),
        "advanced": advanced,
        "advance_count": _int(prev_wm.get("advance_count")) + (1 if advanced else 0),
        "clamped": clamped,
        "clamp_events": _int(prev_wm.get("clamp_events")) + (1 if clamped else 0),
        "clamp_episode": clamp_episode,
        "first_clamp_ts_epoch": (round(float(first_clamp_ts), 3)
                                 if first_clamp_ts is not None else None),
        "last_clamp_ts_epoch": (round(float(now), 3) if clamped
                                else (prev_wm.get("last_clamp_ts_epoch")
                                      if clamp_episode else None)),
        "clock_backward": clock_backward,
        "prev": {"leaves": prev_l, "records": prev_r, "ts_epoch": prev_ts_epoch},
    }, advanced, clamped)


# --------------------------------------------------------------------------- 写侧
# --------------------------------------------------------------------------- 运行态采集（P4/F2·F3）
def _collect_memguard():
    """采集内存守卫状态（fail-open）：优先已导入模块，其次按同目录 import。

    返回 memguard.heartbeat_state() 的 dict，或 None（守卫缺席/未启动/异常）。"""
    mod = sys.modules.get("memguard")
    if mod is None:
        try:
            import memguard as mod            # 与 scrape.py/heartbeat.py 同目录（sys.path[0]）
        except Exception:
            return None
    fn = getattr(mod, "heartbeat_state", None)
    if not callable(fn):
        return None
    try:
        st = fn()
    except Exception:
        return None
    return st if isinstance(st, dict) else None


def _collect_limiter():
    """采集限速器状态（fail-open）：`__main__.LIMITER.snapshot()`。

    基线 v10.1 的 RateLimiter 没有 snapshot() ⇒ 返回 None（字段为 null，不伪造读数）。
    只取稳定字段 + 最后一条调整事件（T9 指出 event_log 无界，绝不整表搬进心跳）。"""
    mod = sys.modules.get("__main__")
    lim = getattr(mod, "LIMITER", None) if mod is not None else None
    snap = getattr(lim, "snapshot", None)
    if not callable(snap):
        return None
    try:
        s = snap()
    except Exception:
        return None
    if not isinstance(s, dict):
        return None
    hist = s.get("event_log")
    last = None
    if isinstance(hist, list) and hist:
        e = hist[-1]
        if isinstance(e, (list, tuple)) and len(e) >= 4:
            last = {"ts_epoch": round(_float(e[0], 0.0) or 0.0, 3),
                    "interval_s": _float(e[1]), "rate_req_s": _float(e[2]), "why": str(e[3])}
    return {
        "mode": s.get("mode"),
        "interval_s": s.get("interval"),
        "rate_req_s": s.get("rate_req_s"),
        "min_interval_s": s.get("min_interval"),
        "max_interval_s": s.get("max_interval"),
        "err_streak": _int(s.get("err_streak")),
        "clean_streak": _int(s.get("clean_streak")),
        "cooldown_remaining_s": _float(s.get("cooldown_remaining")),
        "err_rate_rlimit": _float(s.get("err_rate_rlimit")),
        "err_rate_other": _float(s.get("err_rate_other")),
        "grow_events": _int(s.get("grow_events")),
        "relax_events": _int(s.get("relax_events")),
        "latency_p80_s": _float(s.get("latency_p80")),
        "n_obs": _int(s.get("n_obs")),
        "n_acquire": _int(s.get("n_acquire")),
        "window_n": _int(s.get("window_n")),
        "last_event": last,
    }


def collect_runtime(now=None, provider=None):
    """P4/F2（crit-correct H3）+ P4/F3（crit-adversarial C3）：心跳里的运行态块。

    为什么必须进心跳：keeper 只看心跳，而「闸门是否关着」「限速器是不是在退避」只存在于
    进程内存里 ⇒ 管线被守卫冻住时心跳仍报"健康在推进"（C3 实证）。本函数把两侧状态压成
    一个稳定 schema 的 dict，供 keeper 区分「退避中 / 闸门冻住 / 卡死」。

    扁平便捷键（人读与 keeper 判据用）：
      gate_open / gate_closed_s / mem_level / delay_s / strikes / backoff_active /
      backoff_reason / last_success_ts_epoch / last_success_age_s
    嵌套明细：mem（memguard 快照）/ limiter（限速器快照）/ collected（各源是否可用）。

    全部 fail-open：任何一侧缺席 → 对应字段 null + collected 里说明，绝不伪造"健康"。"""
    now = time.time() if now is None else float(now)
    if os.environ.get("SCRAPE_HEARTBEAT_RUNTIME", "1") == "0":
        return None
    mem = lim = None
    if provider is not None:
        try:
            got = provider()
            if isinstance(got, dict):
                mem = got.get("mem") if isinstance(got.get("mem"), dict) else None
                lim = got.get("limiter") if isinstance(got.get("limiter"), dict) else None
        except Exception:
            mem = lim = None
    if mem is None and lim is None:
        mem = _collect_memguard()
        lim = _collect_limiter()
    elif mem is None:
        mem = _collect_memguard()
    elif lim is None:
        lim = _collect_limiter()
    gate_open = None
    gate_closed_s = None
    if isinstance(mem, dict):
        gate_open = bool(mem.get("gate_open")) if mem.get("gate_open") is not None else None
        try:
            gate_closed_s = float(mem.get("gate_closed_s") or 0.0)
        except (TypeError, ValueError):
            gate_closed_s = None
    delay = strikes = backoff_reason = None
    last_success_ts = None
    last_success_src = None
    backoff_active = False
    if isinstance(lim, dict):
        delay = lim.get("interval_s")
        strikes = lim.get("err_streak")
        cd = _float(lim.get("cooldown_remaining_s"), 0.0) or 0.0
        min_iv = _float(lim.get("min_interval_s"), 0.0) or 0.0
        ev = lim.get("last_event") or {}
        backoff_active = bool(cd > 0 or (strikes or 0) >= 2
                              or (delay is not None and min_iv > 0 and delay >= 2.0 * min_iv))
        if cd > 0:
            backoff_reason = "cooldown %.1fs（%s）" % (cd, ev.get("why") or "explicit")
        elif (strikes or 0) >= 2:
            backoff_reason = "err_streak=%s（%s）" % (strikes, ev.get("why") or "errors")
        elif backoff_active:
            backoff_reason = "interval %.3fs ≥ 2×下界 %.3fs（%s）" % (
                delay or 0.0, min_iv, ev.get("why") or "raised interval")
        if ev.get("ts_epoch"):
            last_success_ts = float(ev["ts_epoch"])
            last_success_src = "limiter.last_event"
    return {
        "schema": "runtime/v1",
        "collected_at_epoch": round(now, 3),
        "gate_open": gate_open,
        "gate_closed_s": gate_closed_s,
        "mem_level": (mem.get("level") if isinstance(mem, dict) else None),
        "mem_pct": (mem.get("pct") if isinstance(mem, dict) else None),
        "mem_eff_workers": (mem.get("eff_workers") if isinstance(mem, dict) else None),
        "mem_abort_requested": (bool(mem.get("abort_requested"))
                                if isinstance(mem, dict) else None),
        "delay_s": delay,
        "strikes": strikes,
        "backoff_active": backoff_active,
        "backoff_reason": backoff_reason,
        "last_success_ts_epoch": last_success_ts,
        "last_success_age_s": (round(now - last_success_ts, 3)
                               if last_success_ts is not None else None),
        "last_success_source": last_success_src,
        "mem": mem,
        "limiter": lim,
        "collected": {"memguard": isinstance(mem, dict),
                      "limiter": isinstance(lim, dict),
                      "provider": provider is not None},
    }


def build_payload(prog, prefix=None, prev=None, now=None, extra=None, provider=None):
    """构造心跳 payload（纯函数，可单测）。prog 为 progress.json 的内存 dict。"""
    now = time.time() if now is None else float(now)
    prog = prog if isinstance(prog, dict) else {}
    stats = prog.get("stats") if isinstance(prog.get("stats"), dict) else {}
    leaves = _int(stats.get("leaves"))
    records = _int(stats.get("records"))
    prev = prev if isinstance(prev, dict) else {}
    boot = _boot_id()
    same_boot = prev.get("boot_id") == boot
    prev_round = _int(prev.get("round")) if same_boot else 0
    rnd = max(_int(_state["round"]), prev_round) + 1
    _state["round"] = rnd
    boot_ts = _state["boot_ts"] or _proc_start_epoch() or now
    if prefix is None:
        prefix = prev.get("prefix")
    wm, advanced, clamped = (merge_watermark(prev.get("watermark"), leaves, records,
                                            prefix, now, prev.get("ts_epoch")))
    runtime = collect_runtime(now, provider=provider)
    payload = {
        "schema": SCHEMA,
        "state": "running",              # P4/F4：终态心跳写 "done"（见 mark_done/_atexit_terminal）
        "ts": iso(now),
        "ts_epoch": round(now, 3),
        "round": rnd,
        "writes": _int(prev.get("writes")) + 1,
        "pid": os.getpid(),
        "boot_id": boot,
        "proc_start_epoch": round(float(boot_ts), 3),
        "since_boot_s": round(max(0.0, now - float(boot_ts)), 3),
        "prefix": prefix,
        "todo": _len(prog.get("todo")),
        "done": _len(prog.get("done")),
        "gaps": _len(prog.get("gaps")),
        "leaves": leaves,
        "records": records,
        "recovery_events": _len(prog.get("recovery")),
        "prog_saved": prog.get("saved"),
        "prog_schema": prog.get("schema"),
        "watermark": wm,
        "runtime": runtime,
        "extra": dict(extra) if isinstance(extra, dict) else {},
    }
    return payload, advanced, clamped


def write_heartbeat(prog, prefix=None, path=None, now=None, extra=None, provider=None):
    """save_progress() 之后调用：原子写心跳。**永不抛异常**——监控侧故障不得影响抓取。

    返回写入的 payload（dict），任何内部异常都降级为返回 None + 一行 heartbeat.log。
    """
    try:
        return _write_heartbeat(prog, prefix, path, now, extra, provider)
    except Exception as e:                                  # 监控 sidecar 必须无害
        _hb_log("心跳写入失败（已忽略，不影响抓取）：%r" % (e,), path)
        return None


def _write_heartbeat(prog, prefix, path, now, extra, provider=None):
    t0 = time.time()
    target = Path(path) if path else default_path()
    now = time.time() if now is None else float(now)
    parent = target.parent
    if not parent.exists():
        # 不创建目录：心跳只放在管线自己的输出目录（progress.json 同目录）。
        # 目录不存在 = 管线尚未落盘任何状态 → 静默跳过，绝不制造新路径。
        _hb_log("输出目录不存在，跳过心跳写入（不创建目录）：%s" % parent, target)
        return None
    with _write_lock:                       # 进程内串行化：合并-写入不得交错
        locked = bool(_acquire_lifetime_lock(target))   # 存活信标（NB；供读侧精确探测）
        with _write_gate(target) as gate_ok:            # 跨进程临界区（有界等待）
            if not gate_ok:
                _hb_log("写入门 %s.wlock 等待超时 → 本次心跳跳过（不影响抓取主流程）"
                        % target, target)
                return None
            _seam("before_read", target)
            prev, _problem = read_status(target)
            payload, advanced, clamped = build_payload(prog, prefix, prev, now, extra, provider)
            payload["extra"]["writer_lock_held"] = locked
            payload["extra"]["write_gate"] = bool(gate_ok)
            tmp = str(target.with_name(DROP_NAME % (target.name, os.getpid(),
                                                    threading.get_ident(), _next_seq())))
            payload["build_ms"] = round((time.time() - t0) * 1000.0, 2)
            data = json.dumps(payload, ensure_ascii=False, indent=1).encode("utf-8")
            _seam("before_tmp_write", target)
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                cut = _seam_half_point(data)
                if cut is None:
                    _write_all(fd, data)
                else:                       # 仅测试缝：制造真实半写窗口后停顿
                    _write_all(fd, data[:cut])
                    _fsync(fd)
                    _seam("mid_tmp_write", target)
                    _write_all(fd, data[cut:])
                _fsync(fd)      # 必须先落盘再改名：否则断电后改名指向的可能是空文件
            finally:
                os.close(fd)
            _seam("after_tmp_write", target)
            os.replace(tmp, str(target))
            _fsync_dir(parent)
            _seam("after_rename", target)
    _state["last_write_ms"] = round((time.time() - t0) * 1000.0, 2)   # 端到端自耗时读数
    # P4/F4（D2）：记住本次快照是否为「收敛完成」形态，供进程正常退出时写终态心跳。
    try:
        _state["snapshot"] = {"todo": _int(payload.get("todo")), "gaps": _int(payload.get("gaps")),
                              "leaves": _int(payload.get("leaves")),
                              "records": _int(payload.get("records")),
                              "path": str(target),
                              "converged": bool(_int(payload.get("todo")) == 0
                                                and _int(payload.get("gaps")) == 0)}
        if os.environ.get("SCRAPE_HEARTBEAT_TERMINAL", "1") != "0":
            _install_atexit_terminal()
    except Exception:
        pass
    if clamped:
        _hb_log("水位被夹住：快照 leaves=%s records=%s < 水位 leaves=%s records=%s"
                "（progress 侧回退，读侧请升级重量全检）"
                % (payload["leaves"], payload["records"],
                   payload["watermark"]["leaves"], payload["watermark"]["records"]), target)
    return payload


def mark_done(path=None, reason="converged", extra=None, now=None):
    """P4/F4（D2）：写一条**终态心跳**（`state="done"`），供 keeper 区分「正常跑完」与「停滞」。

    为什么需要：进程收敛退出后心跳与水位的 ts 都停止前进，旧版读侧 900s 后必然判 STALL(exit 3)
    并 escalate/重启，而重启后进程无事可做、秒级退出 ⇒ 15 分钟级空转重启环（crit-adversarial D2）。
    有了 `state=done`，is_stalled() 直接判 DONE(exit 0)，且 `--require-writer` 不会再把它翻成 STALL。

    语义约束（避免把停滞伪装成完成）：本函数只**显式**标记，不猜；调用方（harness/集成行/
    heartbeat.py done CLI）负责确认「队列与缺口都已清空」。写侧与普通心跳同一条原子写路径。
    返回写入的 payload（dict）或 None（失败时只记 heartbeat.log，不抛）。"""
    target = Path(path) if path else default_path()
    now = time.time() if now is None else float(now)
    try:
        payload, _problem = read_status(target)
        if payload is None:                     # 没有可续写的基线 → 用一个最小 payload
            payload = {"schema": SCHEMA, "ts": iso(now), "ts_epoch": round(now, 3),
                       "round": _int(_state["round"]), "writes": 0, "pid": os.getpid(),
                       "todo": 0, "done": 0, "gaps": 0, "leaves": 0, "records": 0,
                       "watermark": {"schema": WATERMARK_SCHEMA, "leaves": 0, "records": 0,
                                     "ts": iso(now), "ts_epoch": round(now, 3),
                                     "advance_count": 0, "clamped": False, "clamp_events": 0,
                                     "clamp_episode": 0, "first_clamp_ts_epoch": None}}
        payload = dict(payload)
        payload["state"] = "done"
        payload["writes"] = _int(payload.get("writes")) + 1
        payload["round"] = max(_int(payload.get("round")), _int(_state["round"])) + 1
        _state["round"] = payload["round"]
        payload["ts"] = iso(now)
        payload["ts_epoch"] = round(now, 3)
        payload["terminal"] = {"state": "done", "reason": str(reason), "ts": iso(now),
                               "ts_epoch": round(now, 3), "pid": os.getpid(),
                               "since_boot_s": payload.get("since_boot_s")}
        if isinstance(extra, dict):
            payload["terminal"].update(extra)
        data = json.dumps(payload, ensure_ascii=False, indent=1).encode("utf-8")
        parent = target.parent
        if not parent.exists():
            return None
        with _write_lock:
            with _write_gate(target) as gate_ok:
                if not gate_ok:
                    _hb_log("终态心跳写入门等待超时 → 跳过：%s" % target, target)
                    return None
                tmp = str(target.with_name(DROP_NAME % (target.name, os.getpid(),
                                                        threading.get_ident(), _next_seq())))
                fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
                try:
                    _write_all(fd, data)
                    _fsync(fd)
                finally:
                    os.close(fd)
                os.replace(tmp, str(target))
                _fsync_dir(parent)
        _state["terminal_written"] = True
        return payload
    except Exception as e:
        _hb_log("终态心跳写入失败（已忽略）：%r" % (e,), target)
        return None


def _atexit_terminal():
    """进程正常退出时的收尾心跳（P4/F4）：只在「最后一次快照已收敛」时写 state=done。

    安全性：只写自己目录里的 heartbeat.json（原子替换），失败即记日志；SIGKILL/断电时
    本钩子不会执行 —— 那种情形由「写者已死 + todo==0 && gaps==0」的派生判据兜住。"""
    snap = _state.get("snapshot") or {}
    if not snap or not snap.get("converged") or _state.get("terminal_written"):
        return
    path = snap.get("path")
    if not path:
        return
    mark_done(path, reason="converged_at_exit",
              extra={"leaves": snap.get("leaves"), "records": snap.get("records"),
                     "todo": snap.get("todo"), "gaps": snap.get("gaps")})


def _install_atexit_terminal():
    if _state.get("atexit_hooked"):
        return
    _state["atexit_hooked"] = True
    atexit.register(_atexit_terminal)


def prune_tmp_files(path=None, max_age_s=3600.0, now=None):
    """清理孤儿 tmp 文件（<name>.tmp.<pid>.<tid>.<seq>，只删 mtime 超过 max_age_s 的）。
    返回被删除的文件名列表。仅显式调用（读侧巡检不会自动删）。"""
    target = Path(path) if path else default_path()
    now = time.time() if now is None else float(now)
    removed = []
    try:
        entries = list(target.parent.iterdir())
    except OSError:
        return removed
    prefix = target.name + ".tmp."
    for p in entries:
        try:
            if not p.name.startswith(prefix):
                continue
            if now - p.stat().st_mtime < float(max_age_s):
                continue
            p.unlink()
            removed.append(p.name)
        except OSError:
            continue
    return removed


# --------------------------------------------------------------------------- 读侧
def read_status(path=None):
    """宽容读：返回 (payload|None, problem|None)；problem ∈ missing/empty/corrupt/
    unreadable/not_heartbeat。原子写保证「半写」不会出现——真出现即判 corrupt。"""
    target = Path(path) if path else default_path()
    try:
        if not target.exists():
            return None, "missing"
        if target.stat().st_size == 0:
            return None, "empty"
        raw = target.read_text(encoding="utf-8")
    except OSError:
        return None, "unreadable"
    except UnicodeDecodeError:
        return None, "corrupt"
    try:
        payload = json.loads(raw)
    except ValueError:
        return None, "corrupt"
    if not isinstance(payload, dict) or "ts" not in payload or \
            (payload.get("schema") or "").split("/")[0] != "heartbeat":
        return None, "not_heartbeat"
    return payload, None


def read(path=None):
    """宽容读心跳，返回 dict 或 None（缺失/损坏不抛异常）。"""
    return read_status(path)[0]


class Heartbeat(object):
    """心跳的只读视图（薄包装；不复制 payload）。"""

    def __init__(self, payload, path=None):
        self.payload = payload
        self.path = str(path) if path is not None else None

    def __getitem__(self, key):
        return self.payload[key]

    def get(self, key, default=None):
        return self.payload.get(key, default)

    def __repr__(self):
        return "<Heartbeat round=%s prefix=%r wm_leaves=%s ts=%s>" % (
            self.round, self.prefix, self.watermark_leaves, self.payload.get("ts"))

    @property
    def ts_epoch(self):
        return _float(self.payload.get("ts_epoch"))

    @property
    def watermark(self):
        wm = self.payload.get("watermark")
        return wm if isinstance(wm, dict) else {}

    @property
    def watermark_ts_epoch(self):
        v = _float(self.watermark.get("ts_epoch"))
        return v if v is not None else self.ts_epoch

    @property
    def watermark_leaves(self):
        return _int(self.watermark.get("leaves"))

    @property
    def watermark_records(self):
        return _int(self.watermark.get("records"))

    @property
    def prefix(self):
        return self.payload.get("prefix")

    @property
    def round(self):
        return _int(self.payload.get("round"))

    # ---- P4：运行态 / 完成态视图（旧版心跳没有这些块 → 返回空/None，不伪造） ----
    @property
    def runtime(self):
        rt = self.payload.get("runtime")
        return rt if isinstance(rt, dict) else {}

    @property
    def gate_open(self):
        v = self.runtime.get("gate_open")
        return None if v is None else bool(v)

    @property
    def gate_closed_s(self):
        v = _float(self.runtime.get("gate_closed_s"))
        return None if v is None else float(v)

    @property
    def state(self):
        return str(self.payload.get("state") or "running")

    @property
    def converged(self):
        """快照是否处于收敛形态（队列与缺口都空）——完成态判据的一半。"""
        return _int(self.payload.get("todo")) == 0 and _int(self.payload.get("gaps")) == 0

    @property
    def done(self):
        """显式终态标记（进程正常退出时写）。"""
        return self.state == "done"

    @property
    def pid(self):
        return _int(self.payload.get("pid")) or None

    def age(self, now=None):
        """距本次心跳写入的秒数（心跳新鲜度 = 写者是否还活着）。"""
        now = time.time() if now is None else float(now)
        ts = self.ts_epoch
        return None if ts is None else round(now - ts, 3)

    def watermark_age(self, now=None):
        """距「水位最后推进」的秒数（= 数据已停滞多久）。"""
        now = time.time() if now is None else float(now)
        ts = self.watermark_ts_epoch
        return None if ts is None else round(now - ts, 3)

    def writer_liveness(self):
        """写者是否还活着：{'alive': True/False/None, 'source': 'flock'|'pid'|'none', 'detail':…}

        首选 flock 探测（精确）：写者进程活着就持有 <heartbeat>.lock 到退出为止，进程结束
        （含 kill -9）时由内核释放——不依赖 PID 复用启发式，也不向任何进程发信号。
        锁文件不存在/不可判定时退回 PID + 启动时刻探测（信号 0）。"""
        if self.path:
            st = lock_holder_alive(self.path)
            if st is not None:
                return {"alive": st, "source": "flock",
                        "detail": ("持有 %s.lock 的写者仍在" % self.path) if st
                                  else ("没有活写者持有 %s.lock（进程已退出/被 kill）" % self.path)}
        alive = pid_alive(self.pid, self.payload.get("proc_start_epoch"))
        return {"alive": alive, "source": "pid" if alive is not None else "none",
                "detail": "PID %s + 启动时刻探测（无锁文件可判定）" % self.pid}

    def writer_alive(self):
        """写者进程是否还在（True/False/None=无法判定）。"""
        return self.writer_liveness()["alive"]

    def matches_progress(self, progress_path):
        """心跳是否与磁盘上的 progress.json 同一次落盘（prog_saved 比对）。
        不同步 = 心跳滞后于 progress（例如 kill 死在两者之间）→ 读侧可据此推迟判死。
        注意：progress.json 可能很大，本方法属于「半重量」操作，按需调用。"""
        try:
            with open(str(progress_path), "r", encoding="utf-8") as f:
                prog = json.load(f)
        except Exception:
            return None
        return bool(prog.get("saved") and prog.get("saved") == self.payload.get("prog_saved"))

    def to_dict(self):
        return self.payload


def load(path=None):
    """读心跳并返回 Heartbeat 视图；不可用时返回 None。"""
    payload, _problem = read_status(path)
    if payload is None:
        return None
    target = Path(path) if path else default_path()
    return Heartbeat(payload, target)


class Verdict(dict):
    """判定结果：dict 子类（可 json.dumps），bool(v) == v['stalled']，支持属性访问。"""

    def __bool__(self):
        return bool(self.get("stalled"))

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


def is_stalled(now=None, threshold_s=None, path=None, check_pid=True, require_writer=False,
               gate_warn_s=None, gate_freeze_s=None, clamp_warn_s=None, backoff_grace_s=None):
    """停滞判定（读侧主入口）。

    now            : epoch 秒（默认 time.time()；测试可传「未来」时间做停滞注入）
    threshold_s    : 秒；默认 900（= state-check --stale-warn，env 可覆盖）
    check_pid      : 是否用信号 0 探测写者进程存活（不影响目标进程）
    require_writer : True 时「写者已死」直接判 WARN(exit 1)；默认只作为信息字段暴露
    gate_warn_s / gate_freeze_s / clamp_warn_s / backoff_grace_s : P4 阈值（默认取 env）
    判据（W3）：now - watermark.ts_epoch >= threshold_s ⇒ stalled=True。
    写者已死但水位仍新鲜 ⇒ **不判 stall**（可能是正常收敛退出，或 keeper 尚在 180s 窗口内），
    但 verdict 恒带 writer_alive / writer_gone 字段，keeper 据此自行决定是否重启。

    P4 增补（crit-adversarial C3/D2、crit-correct H3/H4）：
      · 完成态（DONE, exit 0）：`state=done`（正常退出时写的终态心跳），或
        快照收敛（todo==0 && gaps==0）且写者已退出 ⇒ 正常跑完不再被判 STALL；
        `--require-writer` 也不会把它翻成 STALL（重启一个没事可做的进程只会造成重启环）。
      · 闸门冻结（C3）：`runtime.gate_open=false` 且 `gate_closed_s ≥ gate_warn_s` ⇒ WARN+escalate；
        ≥ gate_freeze_s ⇒ 追加 `gate_frozen`（`frozen=true`）—— 守卫把管线冻住时，心跳不再
        报「健康在推进」。
      · 退避可见（H3）：`runtime.backoff_active` ⇒ 附 `backoff_active` 理由与
        `stall_explained_by`；可用 backoff_grace_s>0 把"退避能解释的停滞"降级为 WARN（默认 0=不降级）。
      · 水位回退（H4）：只在「低于水位 episode 起点」后 clamp_warn_s 内 WARN+escalate；
        此前/此后都只是信息字段 `watermark_below_high`，不再永久挂 WARN。
    verdict：DONE(exit 0) / OK(exit 0) / WARN(exit 1) / STALL(exit 3) / 用法错误(exit 2)。
    """
    now = time.time() if now is None else float(now)
    thr = default_stall_threshold() if threshold_s is None else float(threshold_s)
    g_warn = default_gate_warn_s() if gate_warn_s is None else float(gate_warn_s)
    g_freeze = default_gate_freeze_s() if gate_freeze_s is None else float(gate_freeze_s)
    c_warn = default_clamp_warn_s() if clamp_warn_s is None else float(clamp_warn_s)
    b_grace = default_backoff_grace_s() if backoff_grace_s is None else float(backoff_grace_s)
    target = Path(path) if path else default_path()
    payload, problem = read_status(target)
    base = {"threshold_s": thr, "now": iso(now), "now_epoch": round(now, 3),
            "path": str(target), "check_pid": bool(check_pid),
            "require_writer": bool(require_writer), "gate_warn_s": g_warn,
            "gate_freeze_s": g_freeze, "clamp_warn_s": c_warn,
            "backoff_grace_s": b_grace}
    if payload is None:
        detail = {
            "missing": "心跳文件不存在：管线从未落盘状态，或输出目录被清空/路径不一致",
            "empty": "心跳文件为空（0 字节）：不在正常写入序列中，需人工核查",
            "corrupt": "心跳 JSON 无法解析：正常原子写不该产生半写文件，视为严重异常",
            "unreadable": "心跳文件不可读（权限/IO）",
            "not_heartbeat": "文件不是 heartbeat 结构（schema 不符或缺少 ts）",
        }.get(problem, "未知问题：%s" % problem)
        return Verdict(base, **{
            "stalled": True, "severity": "STALL", "reason": problem or "missing",
            "reasons": [problem or "missing"], "detail": detail,
            "heartbeat": None, "watermark": None, "heartbeat_age_s": None,
            "watermark_age_s": None, "writer_alive": None, "escalate": True,
            "escalate_to": "state-check.py（重量全检）+ 检查 keeper/输出目录",
            "frozen": False, "done": False, "gate_open": None, "gate_closed_s": None,
            "backoff_active": None, "stall_explained_by": None,
            "exit_code": EXIT_STALL, "verdict": "STALL"})
    hb = Heartbeat(payload, target)
    hb_age = hb.age(now)
    wm_age = hb.watermark_age(now)
    since_boot = _float(payload.get("since_boot_s"))
    liveness = hb.writer_liveness() if check_pid else {"alive": None, "source": "skipped",
                                                       "detail": "check_pid=False"}
    alive = liveness["alive"]
    reasons = []
    if wm_age is None:
        reasons.append("no_watermark_ts")
    elif wm_age >= thr:
        reasons.append("watermark_stale")
    if hb_age is None:
        reasons.append("no_ts")
    elif hb_age >= thr:
        reasons.append("heartbeat_stale")
    # 启动宽限：水位时间可能继承自上一轮运行（重启后尚未完成一个新叶子），
    # 只要写者还新鲜（hb_age < thr）且本进程运行不足一个阈值窗口，就不判 stall，
    # 只给 WARN —— 否则每次重启都会误报停滞。
    startup_grace = bool(
        "watermark_stale" in reasons and "heartbeat_stale" not in reasons
        and since_boot is not None and since_boot < thr)
    if startup_grace:
        reasons.remove("watermark_stale")
        reasons.append("startup_grace")
    stalled = any(r in ("watermark_stale", "heartbeat_stale", "no_watermark_ts", "no_ts")
                  for r in reasons)
    if alive is False:
        reasons.append("process_gone")
    snap_leaves = _int(payload.get("leaves"))
    snap_records = _int(payload.get("records"))
    # ---- P4/F5（H4）：水位回退 = event + episode 起点，而不是永久状态 ----
    below_high = bool(snap_leaves < hb.watermark_leaves or snap_records < hb.watermark_records)
    clamped_now = bool(hb.watermark.get("clamped"))
    first_clamp = _float(hb.watermark.get("first_clamp_ts_epoch"))
    clamp_age = None if first_clamp is None else (now - float(first_clamp))
    if below_high or clamped_now:
        if first_clamp is not None:
            clamp_recent = bool(clamp_age <= c_warn)
            reasons.append("watermark_clamped" if clamp_recent else "watermark_below_high")
        elif clamped_now and _int(hb.watermark.get("clamp_events")) <= 1:
            # 旧版心跳（无 episode 起点）：用 clamp_events 做粗判 —— 首次被夹（≤1 次）视为
            # "刚发现回退"，仍 WARN+escalate；已经夹过多次则视为"已知的持久状态"，只做信息
            # 字段。这样版本混跑时既不会漏掉首次发现，也不会永久挂 WARN（H4 的缺陷本体）。
            clamp_recent = True
            reasons.append("watermark_clamped")
        else:
            clamp_recent = False
            reasons.append("watermark_below_high")
    else:
        clamp_recent = False
    rolled_back = clamp_recent                       # 只有"新鲜的"回退才升级
    rolled_back_persistent = bool((below_high or clamped_now) and not clamp_recent)
    # ---- P4/F3（C3）：闸门状态进判定 ----
    gate_open = hb.gate_open
    gate_closed_s = hb.gate_closed_s
    gate_closed = bool(gate_open is False)
    # 读侧外推：payload 里的 gate_closed_s 是**写入那一刻**的读数；心跳两次写入之间的空档里，
    # 闸门只会关得更久。用「写入时的已关时长 + 本心跳的年龄」作为下界，避免 keeper 两次巡检
    # 之间把"已经关了很久"看成"刚关"（也让写者停写而闸门已关的情形被检出）。
    gate_age_extra = 0.0
    if gate_closed and hb.ts_epoch is not None:
        gate_age_extra = max(0.0, now - float(hb.ts_epoch))
    gate_closed_eff = round((gate_closed_s or 0.0) + gate_age_extra, 3)
    gate_frozen = bool(gate_closed and g_freeze > 0 and gate_closed_eff >= g_freeze)
    gate_warn = bool(gate_closed and not gate_frozen
                     and g_warn > 0 and gate_closed_eff >= g_warn)
    if gate_closed:
        reasons.append("gate_closed")
    if gate_frozen:
        reasons.append("gate_frozen")
    # ---- P4/F2（H3）：退避可见性 ----
    rt = hb.runtime
    backoff_active = rt.get("backoff_active")
    backoff_reason = rt.get("backoff_reason")
    if backoff_active:
        reasons.append("backoff_active")
    # ---- P4/F4（D2）：完成态 ----
    done = bool(hb.done or (hb.converged and alive is False))
    if done:
        reasons.append("converged_done")
    if not reasons:
        reasons.append("ok")
    writer_gone = alive is False
    stall_explained_by = None
    if stalled:
        if gate_frozen:
            stall_explained_by = "memguard_gate_closed"
        elif backoff_active:
            stall_explained_by = "throttle_backoff"
        elif writer_gone:
            stall_explained_by = "process_gone"
    if done:
        severity, verdict, escalate, stalled = "DONE", "DONE", False, False
    elif stalled:
        # 退避宽限（默认 0=关闭）：只有调用方显式给宽限、且停滞仍在宽限窗口内才降级
        if b_grace > 0 and backoff_active and wm_age is not None and wm_age <= thr + b_grace:
            severity, verdict, escalate = "WARN", "WARN", True
        else:
            severity, verdict, escalate = "STALL", "STALL", True
    elif rolled_back or gate_frozen or (writer_gone and require_writer):
        severity, verdict = "WARN", "WARN"
        escalate = bool(rolled_back or gate_frozen)
    elif gate_warn:
        severity, verdict, escalate = "WARN", "WARN", False
    elif backoff_active and (hb_age is None or hb_age < thr):
        severity, verdict, escalate = "OK", "OK", False     # 退避中但一切新鲜 → 只做标注
    else:
        severity, verdict, escalate = "OK", "OK", False     # 含 startup_grace 与「写者已死但水位新鲜」
    if "heartbeat_stale" in reasons:
        detail = ("连心跳都没写了 %.0fs（阈值 %.0fs）：主循环卡死/进程被冻/未在运行"
                  % (hb_age or 0, thr))
    elif "watermark_stale" in reasons:
        alive_note = "写者仍活（空转：心跳在写但数据没动）" if alive else "写者已不在"
        detail = ("数据水位停滞 %.0fs（阈值 %.0fs，%s）：最近推进=%s prefix=%r"
                  % (wm_age or 0, thr, alive_note,
                     hb.watermark.get("ts"), hb.watermark.get("prefix")))
    elif done:
        detail = ("收敛完成：todo=%s gaps=%s，%s ⇒ 正常终态（不是停滞）"
                  % (payload.get("todo"), payload.get("gaps"),
                     "已写终态心跳 state=done" if hb.done else "写者已退出且队列/缺口为空"))
    elif gate_frozen:
        detail = ("内存闸门已连续关闭 %.0fs（≥%.0fs）：工作单元被守卫挡住 → 判定冻结；"
                  "memguard 等级=%s pct=%s，先看 <OUT>/memguard.jsonl 再决定是否重启"
                  % (gate_closed_eff, g_freeze, rt.get("mem_level"), rt.get("mem_pct")))
    elif gate_warn:
        detail = ("内存闸门关闭 %.0fs（阈值 %.0fs）：并发已被收缩，数据可能只是暂时不动"
                  % (gate_closed_eff, g_warn))
    elif startup_grace:
        detail = ("启动宽限：本进程运行 %.0fs（< 阈值 %.0fs）且心跳新鲜；水位时间继承自"
                  "上一轮（%s），尚不足以判停滞"
                  % (since_boot or 0, thr, hb.watermark.get("ts")))
    elif rolled_back or rolled_back_persistent:
        detail = ("水位未停滞，但 progress 快照低于水位（回滚过，episode 已持续 %.0fs）→ %s"
                  % (clamp_age or 0,
                     "首次发现：建议跑重量全检确认一致性" if rolled_back
                     else "已超出告警窗口，降级为信息字段（不再重复告警）"))
    elif writer_gone:
        detail = ("水位在推进（%.0fs 前）但写者进程已不在（pid=%s；可能正常收敛退出，"
                  "也可能刚崩 —— keeper 用 --require-writer 可升级为 WARN）"
                  % (wm_age or 0, hb.pid))
    else:
        detail = "水位在推进（%.0fs 前）" % (wm_age or 0)
    if backoff_active and backoff_reason and verdict != "DONE":
        detail += " | 限速器在退避：%s（delay=%ss strikes=%s）" % (
            backoff_reason, rt.get("delay_s"), rt.get("strikes"))
    exit_code = EXIT_STALL if stalled else (EXIT_WARN if severity == "WARN" else EXIT_OK)
    return Verdict(base, **{
        "stalled": stalled, "severity": severity, "verdict": verdict,
        "reason": reasons[0], "reasons": reasons, "detail": detail,
        "heartbeat_age_s": hb_age, "watermark_age_s": wm_age, "writer_alive": alive,
        "writer_gone": writer_gone, "writer_alive_source": liveness["source"],
        "writer_detail": liveness["detail"],
        "since_boot_s": since_boot, "startup_grace": startup_grace,
        "escalate": escalate,
        "escalate_to": ("state-check.py <OUT>/progress.json <OUT>/records.jsonl "
                        "--json /tmp/state-check.json") if escalate else None,
        "watermark_clamped": rolled_back,
        "watermark_clamped_persistent": rolled_back_persistent,
        "clamp_age_s": (None if clamp_age is None else round(clamp_age, 3)),
        "done": done, "state": hb.state,
        "frozen": gate_frozen, "gate_open": gate_open, "gate_closed_s": gate_closed_s,
        "gate_closed_s_effective": gate_closed_eff,
        "gate_frozen": gate_frozen, "mem": (rt.get("mem") if isinstance(rt.get("mem"), dict)
                                            else None),
        "delay_s": rt.get("delay_s"), "strikes": rt.get("strikes"),
        "backoff_active": backoff_active, "backoff_reason": backoff_reason,
        "last_success_ts_epoch": rt.get("last_success_ts_epoch"),
        "last_success_age_s": rt.get("last_success_age_s"),
        "stall_explained_by": stall_explained_by,
        "heartbeat": {"ts": payload.get("ts"), "round": hb.round, "writes": _int(payload.get("writes")),
                      "pid": hb.pid, "boot_id": payload.get("boot_id"),
                      "prefix": hb.prefix, "todo": _int(payload.get("todo")),
                      "done": _int(payload.get("done")), "gaps": _int(payload.get("gaps")),
                      "leaves": snap_leaves, "records": snap_records,
                      "state": hb.state,
                      "recovery_events": _int(payload.get("recovery_events"))},
        "watermark": {"leaves": hb.watermark_leaves, "records": hb.watermark_records,
                      "prefix": hb.watermark.get("prefix"),
                      "ts": hb.watermark.get("ts"), "ts_epoch": hb.watermark_ts_epoch,
                      "advance_count": _int(hb.watermark.get("advance_count")),
                      "clamped": bool(hb.watermark.get("clamped")),
                      "clamp_episode": _int(hb.watermark.get("clamp_episode")),
                      "first_clamp_ts_epoch": first_clamp},
        "exit_code": exit_code})


EXPLAIN = """\
[轻量心跳 vs 重量全检]  heartbeat.py 是 O(1) 的轻量探针；state-check.py 是 O(records) 的重量全检。

1) 巡检（keeper/watchdog，每 <=180s，便宜）：
     python3 heartbeat.py status --json                  # exit 0=OK 1=WARN 3=STALL 2=用法
     - 只看 watermark.ts_epoch（数据水位时间），不要看 progress.json/records.jsonl 的 mtime：
       reconcile/空转保存会把 mtime 刷成「刚刚」而数据毫无进展（假活）。
2) 升级（仅当 exit!=0，或 escalate=true，或水位被夹住）：
     python3 state-check.py <OUT>/progress.json <OUT>/records.jsonl --json /tmp/sc.json
     - watermark_stale / heartbeat_stale → 与 state-check C3（mtime 停滞）同源，但心跳更早更准；
     - watermark_clamped / 快照<水位      → progress 回滚过，重点看 C1（声称完成却无记录）；
     - heartbeat_stale（连心跳都不写）   → 先查 keeper 与进程存活，再跑全检；
     - watermark.records 是 C2 records 行数的下界，可先做量级判断（真实落盘量）。
3) 判定口径：stalled 只由「水位停滞」决定，与「队列是否为空」无关——收敛退出后水位不再
   推进，这是正常终态。写者进程是否还在看 writer_alive/writer_gone 字段（默认不并入 exit
   code）；keeper 若要求「必须有人在写」，加 --require-writer → 写者已死即 exit 1。
4) 误报防护：进程重启后的第一个阈值窗口内，旧水位时间仍在文件里 → startup_grace 生效
   （reason=startup_grace，不判 stall），避免每次重启都报停滞。
5) P4 增补的三种终态/异常态（都在 verdict/reasons 里机器可读）：
   - DONE(exit 0)：`state=done`（进程正常收敛退出时自动写的终态心跳，或 heartbeat.py done 手工标记），
     或「todo==0 && gaps==0 且写者已退出」。**正常跑完不再判 STALL**，--require-writer 也不会
     把它翻成 STALL（旧行为会让 keeper 每 900s 重启一个没事可做的进程）。
   - WARN(gate_closed / gate_frozen)：心跳 runtime 块里的闸门状态说明"数据没动是因为内存守卫
     把工作单元挡住了"，而不是卡死。gate_closed_s ≥ 30s → WARN；≥ 300s → 追加 gate_frozen
     （frozen=true）。此时先看 <OUT>/memguard.jsonl 与 memguard 等级，再决定是否重启。
   - backoff_active：限速器在退避（delay/strikes/cooldown/backoff_reason 都在 runtime 里）。
     默认只做标注（不改变 exit code）；keeper 若确认"退避能解释停滞"，可用 --backoff-grace-s
     把停滞降级为 WARN。
   - watermark_clamped（水位回退）只在"低于水位 episode 起点"后 900s 内 WARN+escalate；
     之后降级为信息字段 watermark_below_high，不再永久挂 WARN。
"""


# --------------------------------------------------------------------------- CLI
def _cmd_status(args):
    v = is_stalled(now=args.now, threshold_s=args.stall_threshold, path=args.file,
                   check_pid=not args.no_pid_check, require_writer=args.require_writer,
                   gate_warn_s=args.gate_warn_s, gate_freeze_s=args.gate_freeze_s,
                   clamp_warn_s=args.clamp_warn_s, backoff_grace_s=args.backoff_grace_s)
    if args.progress and v.get("heartbeat"):
        hb = load(args.file)
        m = hb.matches_progress(args.progress) if hb else None
        v["progress_in_sync"] = m
        if m is False:
            v["detail"] += " | 心跳与 progress.json 不是同一次落盘（心跳滞后，属 kill 窗口）"
    if v["reason"] == "missing" and args.allow_missing:
        v["stalled"] = False
        v["severity"] = "OK"
        v["verdict"] = "OK(missing-allowed)"
        v["exit_code"] = EXIT_OK
    if args.json:
        print(json.dumps(v, ensure_ascii=False, indent=1))
        return v["exit_code"]
    if args.quiet:
        print("heartbeat verdict=%s stalled=%s reason=%s exit=%d"
              % (v["verdict"], v["stalled"], v["reason"], v["exit_code"]))
        return v["exit_code"]
    h = v.get("heartbeat") or {}
    w = v.get("watermark") or {}
    print("=" * 74)
    print("heartbeat %s | stalled=%s reason=%s | threshold=%.0fs"
          % (v["verdict"], v["stalled"], ",".join(v["reasons"]), v["threshold_s"]))
    print("-" * 74)
    print("file      : %s" % v["path"])
    print("now       : %s" % v["now"])
    if h:
        print("heartbeat : ts=%s age=%ss round=%s writes=%s pid=%s alive=%s"
              % (h.get("ts"), v["heartbeat_age_s"], h.get("round"), h.get("writes"),
                 h.get("pid"), v["writer_alive"]))
        print("prefix    : %r | todo=%s done=%s gaps=%s leaves=%s records=%s recovery_events=%s"
              % (h.get("prefix"), h.get("todo"), h.get("done"), h.get("gaps"),
                 h.get("leaves"), h.get("records"), h.get("recovery_events")))
        print("watermark : leaves=%s records=%s prefix=%r ts=%s age=%ss advances=%s clamped=%s"
              % (w.get("leaves"), w.get("records"), w.get("prefix"), w.get("ts"),
                 v["watermark_age_s"], w.get("advance_count"), w.get("clamped")))
        # P4：运行态（闸门/限速器）——keeper 区分「退避中」「闸门冻住」「卡死」的依据
        print("runtime   : state=%s gate_open=%s gate_closed_s=%s frozen=%s done=%s"
              % (v.get("state"), v.get("gate_open"), v.get("gate_closed_s"),
                 v.get("frozen"), v.get("done")))
        print("            mem_level=%s pct=%s | delay=%ss strikes=%s backoff=%s(%s)"
              % (((v.get("mem") or {}).get("level")), ((v.get("mem") or {}).get("pct")),
                 v.get("delay_s"), v.get("strikes"), v.get("backoff_active"),
                 v.get("backoff_reason")))
        if v.get("stall_explained_by"):
            print("            stall_explained_by=%s" % v["stall_explained_by"])
    print("detail    : %s" % v["detail"])
    print("require_writer=%s writer_gone=%s writer_probe=%s | %s"
          % (v["require_writer"], v["writer_gone"], v.get("writer_alive_source"),
             v.get("writer_detail")))
    if v.get("escalate"):
        print("escalate  : %s" % v["escalate_to"])
    print("exit_code : %d" % v["exit_code"])
    return v["exit_code"]


def _cmd_done(args):
    """P4/F4：显式写终态心跳（state=done）。供集成层的正常结束路径/运维手工调用。"""
    out = mark_done(args.file, reason=args.reason)
    if out is None:
        print("终态心跳写入失败（见 heartbeat.log）", file=sys.stderr)
        return EXIT_USAGE
    print(json.dumps({"ok": True, "state": out.get("state"),
                      "terminal": out.get("terminal"),
                      "path": str(Path(args.file) if args.file else default_path())},
                     ensure_ascii=False))
    return EXIT_OK


def _cmd_dump(args):
    payload, problem = read_status(args.file)
    if payload is None:
        print(json.dumps({"ok": False, "problem": problem,
                          "path": str(Path(args.file) if args.file else default_path())},
                         ensure_ascii=False), file=sys.stderr)
        return EXIT_USAGE
    print(json.dumps(payload, ensure_ascii=False, indent=1))
    return EXIT_OK


def _cmd_prune(args):
    removed = prune_tmp_files(args.file, args.max_age, now=args.now)
    print(json.dumps({"removed": removed, "count": len(removed),
                      "dir": str((Path(args.file) if args.file else default_path()).parent)},
                     ensure_ascii=False))
    return EXIT_OK


def _cmd_write(args):
    if args.prog:
        try:
            with open(args.prog, "r", encoding="utf-8") as f:
                prog = json.load(f)
        except Exception as e:
            print("无法读取 --prog: %r" % (e,), file=sys.stderr)
            return EXIT_USAGE
    else:
        prog = {"todo": [{"p": "X%d" % i} for i in range(args.todo)],
                "done": [{"p": "D%d" % i} for i in range(args.done)],
                "gaps": [], "recovery": [],
                "stats": {"leaves": args.leaves, "records": args.records},
                "saved": datetime.now().isoformat(), "schema": "v10"}
    out = write_heartbeat(prog, prefix=args.prefix, path=args.file, extra={"cli": True})
    if out is None:
        print("心跳写入被跳过（见 heartbeat.log）", file=sys.stderr)
        return EXIT_USAGE
    print(json.dumps({"ok": True, "path": str(Path(args.file) if args.file else default_path()),
                      "round": out["round"], "writes": out.get("writes"),
                      "watermark": out["watermark"],
                      "build_ms": out.get("build_ms"),
                      "write_ms_e2e": _state.get("last_write_ms")}, ensure_ascii=False))
    return EXIT_OK


def _cmd_explain(_args):
    print(EXPLAIN)
    return EXIT_OK


def build_parser():
    p = argparse.ArgumentParser(
        prog="heartbeat.py", description="抓取管线心跳 + 偏移水位（R2/X2）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--version", action="version", version="heartbeat.py %s" % __version__)
    sub = p.add_subparsers(dest="cmd")

    def common(sp):
        sp.add_argument("--file", default=None, help="心跳文件路径（默认 env/OUT/heartbeat.json）")
        sp.add_argument("--json", action="store_true", help="输出机器可读 JSON")

    sp = sub.add_parser("status", help="停滞判定（读侧主入口）")
    common(sp)
    sp.add_argument("--stall-threshold", type=float, default=None,
                    help="停滞阈值秒（默认 %s / env SCRAPE_HEARTBEAT_STALL_S）" % DEFAULT_STALL_S)
    sp.add_argument("--now", type=float, default=None, help="把当前时间固定为 epoch 秒（测试用）")
    sp.add_argument("--no-pid-check", action="store_true", help="不探测写者进程存活")
    sp.add_argument("--require-writer", action="store_true",
                    help="写者进程已死即判 WARN/exit 1（默认仅作为 writer_alive 字段暴露）")
    sp.add_argument("--allow-missing", action="store_true",
                    help="心跳缺失时返回 OK（用于管线尚未启动的场景）")
    sp.add_argument("--progress", default=None, help="附带与 progress.json 的配对校验（可能较大）")
    sp.add_argument("--quiet", action="store_true", help="只输出一行结论")
    sp.add_argument("--gate-warn-s", type=float, default=None,
                    help="闸门关闭多久开始 WARN（默认 %s / env）" % DEFAULT_GATE_WARN_S)
    sp.add_argument("--gate-freeze-s", type=float, default=None,
                    help="闸门关闭多久标记 gate_frozen（默认 %s / env）" % DEFAULT_GATE_FREEZE_S)
    sp.add_argument("--clamp-warn-s", type=float, default=None,
                    help="水位回退 episode 起点后多久内仍 WARN（默认 %s / env）" % DEFAULT_CLAMP_WARN_S)
    sp.add_argument("--backoff-grace-s", type=float, default=None,
                    help="退避能解释停滞的宽限秒数（0=不降级，默认 0）")
    sp.set_defaults(func=_cmd_status)

    sp = sub.add_parser("done", help="写终态心跳 state=done（正常收敛退出后用，防 keeper 误判 STALL）")
    common(sp)
    sp.add_argument("--reason", default="converged", help="完成原因（写入 terminal.reason）")
    sp.set_defaults(func=_cmd_done)

    sp = sub.add_parser("dump", help="打印原始心跳 JSON")
    common(sp)
    sp.set_defaults(func=_cmd_dump)

    sp = sub.add_parser("prune", help="清理孤儿 tmp 文件")
    common(sp)
    sp.add_argument("--max-age", type=float, default=3600.0, help="只删 mtime 早于该秒数的 tmp")
    sp.add_argument("--now", type=float, default=None, help="固定当前时间（测试用）")
    sp.set_defaults(func=_cmd_prune)

    sp = sub.add_parser("write", help="手工写一次心跳（联调/运维）")
    common(sp)
    sp.add_argument("--prefix", default=None)
    sp.add_argument("--prog", default=None, help="从真实 progress.json 取计数")
    sp.add_argument("--todo", type=int, default=0)
    sp.add_argument("--done", type=int, default=0)
    sp.add_argument("--leaves", type=int, default=0)
    sp.add_argument("--records", type=int, default=0)
    sp.set_defaults(func=_cmd_write)

    sp = sub.add_parser("explain", help="轻量心跳 vs 重量全检的对接说明")
    sp.set_defaults(func=_cmd_explain)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    if not getattr(args, "cmd", None):
        args = build_parser().parse_args(["status"] + (argv or []))
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
