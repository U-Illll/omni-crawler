#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""memguard.py — 长跑进程内存守卫（R2·slot-mem；stdlib-only、离线可用、fail-open）

设计对标：Crawl4AI `MemoryAdaptiveDispatcher`（R1 sandbox pkgs/crawl4ai/async_dispatcher.py L148-215）
  · 后台 monitor 任务周期性读「真实可用内存」→ 百分比；
  · memory_threshold_percent(90) 进 pressure 模式 = 不再派发新任务；
  · critical_threshold_percent(95) 额外动作；
  · recovery_threshold_percent(85) 退出 pressure（滞回，防抖动）；
  · 持续超限超过 memory_wait_timeout(600s) → 抛 MemoryError（交给上层 keeper 重启）。
参数取值与 R2 调研（`R2/analysis/survey-throttle.md` §4.3）的建议一致：
压力 90% / 危急 95% / 恢复 85%（5% 滞回带）/ 巡检 1s / 持续超限 600s；内存口径照抄
`get_true_memory_usage_percent` 语义（以 MemAvailable 计，不用 used/total，避免被 page cache 误导）。
本模块把同一套机制落到 v10.1 抓取管线（同步、线程池、无 asyncio）上，并补齐三点：
  · 双轴水位：进程 RSS/预算 与 系统 MemAvailable 取更严的一侧（长跑 OOM 可能来自自己，
    也可能来自同机邻居）；若存在 cgroup v1/v2 限额，则并入进程轴（容器里 OOM killer 看的是它）。
  · 分级动作 + 可审计：WARN(只观测) → PRESSURE(收缩有效并发) → CRITICAL(关闭任务闸门)
    → SAFE_MODE(持续临界：审计升级；可选 abort 交给 keeper) + 恢复滞回。
  · 对标差异（有意，已在测试中固定）：对标在持续超限时**抛 MemoryError**；本模块默认
    SAFE_MODE = 审计 + 继续关闭闸门（长跑优先不中断，避免宿主压力导致 kill→restart 抖动），
    设 MEMGUARD_ABORT_ON_TIMEOUT=1 即回到对标行为（进程**真正终止**并交 keeper 重启，见下）。

P4 修复（R2 批评/验证实证缺陷；逐条对应 crit-correct M1/M2 与 crit-adversarial C3/E4/E5）
--------------------------------------------------------------------------------------------
F1【M1 高】SAFE_MODE 之后恢复判据失效 → 闸门永久关闭 + 等级 livelock：
   旧实现里 (a) 降级目标写死 `prev_level - 1`（SAFE→CRITICAL），而 (b) `crit_since` 只在
   `lv < CRITICAL` 时清零 ⇒ 从 SAFE 落回 CRITICAL 的那一拍立刻满足「持续临界 ≥ timeout」
   ⇒ 下一拍又升回 SAFE，等级在 4↔3 之间无限翻转、`gate_closed` 恒 True、并发恒 1
   （复现：6s 内 21 升 19 降 / 20 次 trim / 569 行审计/分）。
   修法：① 从 SAFE_MODE 降级**一步到位**落到当前读数对应的 want（非 SAFE 等级仍保持
   "每 hold_s 降一级"的分级滞回，那是设计意图）；② `crit_since` 采用「持续临界 episode」语义 ——
   从 <CRITICAL 升入、或从 SAFE 落回 CRITICAL，都**重新计时**；只有 episode 内部
   （CRITICAL→SAFE）沿用原计时；③ 加 flap 检测（60s 内 ≥20 次等级切换写 flap-warning 审计）
   与 trim 节流（MEMGUARD_TRIM_MIN_INTERVAL_S），使同类病理不再放大成审计风暴。
F2【M2 高】ABORT_ON_TIMEOUT=1 的 fail-loud 是假的：
   旧实现是监控线程内 `raise MemoryError` + `_monitor_loop` 里 `break` ⇒ 线程退出、状态机
   冻结在 SAFE、闸门保持关闭、进程照旧运行（"交给 keeper"并未发生）。
   修法：抛内部信号 `_AbortSignal` → `_fail_loud()`：写 `abort` 审计（os.write 无缓冲，先落盘）
   + 打开闸门 + 记 `abort_requested` → `os._exit(MEMGUARD_ABORT_EXIT_CODE)`（默认 86）。
   **语义契约（测试断言）：ABORT_ON_TIMEOUT=1 时进程必然终止、退出码=86；ABORT=0（默认）时
   进程不终止，改为显式降级：审计 abort-degraded + 闸门保持关闭 + 心跳携带闸门状态
   （见 heartbeat_state()），让 keeper 依据心跳而不是靠猜。**
F3【C3 集成高】闸门冻结对外不可见：新增 `heartbeat_state()`（只读紧凑快照：level/gate_open/
   gate_closed_s/pct/why/eff_workers/abort_requested…），由 heartbeat.py 自动采集写进心跳，
   使 keeper 能区分「闸门冻住」与「健康在推进」。

不变量（长跑安全）：
  I1 fail-open：任何内部异常都被吞掉并写一条 error 审计，绝不让守卫自身中断抓取。
  I2 只读监控：采样只读 /proc（+ 可选 cgroup 文件）；不持有业务锁、不阻塞采集线程。
  I3 动作可逆：effective_workers() 恢复即回到基线并发；闸门重新打开后工作单元照常进入。
  I4 有界：审计 jsonl 按大小轮转；内存内只保留定长环形缓冲；无任何随运行时间增长的容器。
  I5 闸门有上界：等待超过 MEMGUARD_GATE_MAX_WAIT_S 后放行（并发 1）并审计，避免死等。

公开 API（scrape.py 只用到前三个 + install_gate）：
  start(audit_path, base_workers, tag, log_fn) -> status dict   启动后台监控（幂等）
  stop(reason)                                                  停止监控、写 stop 审计
  effective_workers(kind) -> int                                读函数：当前生效并发度（'leaf'/'probe'）
  install_gate(module) / uninstall_gate()                       把 scrape_leaf/probe_children 包一层闸门（可逆）
  gate_wait(tag, timeout) -> 等待秒数                            阻塞直到闸门打开（有上界）
  status() / snapshot() / audit_path()                          诊断与测试
  heartbeat_state()                                             给心跳/keeper 的紧凑运行态（只读）
  set_source(fn)                                                测试缝：注入内存读数（需 MEMGUARD_ALLOW_TEST_SOURCE=1）

环境变量（全部有默认值，生产可零配置）：
  MEMGUARD_ENABLED=1|0                 总开关（0 = 完全惰性，不建线程不写文件）
  MEMGUARD_WARN_PCT=70                 观察级（本工程新增，对标池无此级）
  MEMGUARD_PRESSURE_PCT=90             收缩并发（对标 memory_threshold_percent=90）
  MEMGUARD_CRITICAL_PCT=95             关闭闸门（对标 critical_threshold_percent=95）
  MEMGUARD_EXIT_MARGIN_PCT=5           滞回带宽（对标 recovery=85 ⇒ 90-5 / 95-5）
  MEMGUARD_INTERVAL_S=1.0              采样周期（对标 check_interval=1s）
  MEMGUARD_CONFIRM_SAMPLES=2           升级前需连续命中的采样数（抗瞬时尖峰）
  MEMGUARD_HOLD_S=20                   降级前需在退出阈值下保持的秒数
  MEMGUARD_PROC_BUDGET_MB=<auto>       进程预算（默认 min(cgroup, MemTotal/8, 且≥256MB)）
  MEMGUARD_RSS_LIMIT_MB=<auto>         硬覆盖：只按 RSS 绝对值算进程轴
  MEMGUARD_GATE_MAX_WAIT_S=300         闸门单次等待上界
  MEMGUARD_CRIT_TIMEOUT_S=600          持续临界多久 → SAFE_MODE
  MEMGUARD_ABORT_ON_TIMEOUT=0          1 = SAFE_MODE 时**终止进程**（os._exit，交给 keeper 重启）
  MEMGUARD_ABORT_EXIT_CODE=86          上述 fail-loud 的退出码（keeper 可据此区分自杀与崩溃）
  MEMGUARD_TRIM=1                      高压时 gc.collect()+malloc_trim(0) 归还内存
  MEMGUARD_TRIM_MIN_INTERVAL_S=5       两次 trim 的最小间隔（防等级抖动放大成 trim 风暴）
  MEMGUARD_TRACE_UNITS=0               1 = 每个工作单元写 unit 审计（含 RSS 增量）
  MEMGUARD_SUMMARY_S=60                常态摘要审计周期（0 = 关闭）
  MEMGUARD_AUDIT_MAX_MB=8              审计文件轮转上限
"""
import atexit
import functools
import gc
import json
import os
import sys
import threading
import time
from datetime import datetime

__all__ = ['start', 'stop', 'effective_workers', 'install_gate', 'uninstall_gate',
           'gate_wait', 'gate_open', 'level', 'level_name', 'status', 'snapshot',
           'audit_path', 'set_source', 'trim_now', 'read_memory', 'heartbeat_state']

# ---------------------------------------------------------------------------
# 水位等级
# ---------------------------------------------------------------------------
NORMAL, WARN, PRESSURE, CRITICAL, SAFE = 0, 1, 2, 3, 4
LEVEL_NAMES = {NORMAL: 'NORMAL', WARN: 'WARN', PRESSURE: 'PRESSURE',
               CRITICAL: 'CRITICAL', SAFE: 'SAFE_MODE'}
# 各等级下生效并发的收缩系数（下限恒为 1）
SHRINK = {NORMAL: 1.0, WARN: 1.0, PRESSURE: 0.5, CRITICAL: 0.0, SAFE: 0.0}
ABORT_EXIT_CODE = 86               # fail-loud 退出码（MEMGUARD_ABORT_EXIT_CODE 可覆盖）


class _AbortSignal(MemoryError):
    """内部信号：sustained-critical + ABORT_ON_TIMEOUT=1 → 由监控线程执行 fail-loud 终止。

    继承 MemoryError **是有意的**：① 保持对标契约（crawl4ai 在 memory_wait_timeout 时抛
    MemoryError），既有调用方/测试里 `except MemoryError` 仍然接得住；② 模块内部再用
    isinstance 区分"该退进程"（_monitor_loop/start → _fail_loud → os._exit）与"只是被
    raise 出来"（同步调用 _sample_once 的测试夹具）。"""
    pass


_LOCK = threading.RLock()          # 保护状态与审计写入
_GATE = threading.Condition(_LOCK)  # 闸门：与状态共用一把锁，避免锁序问题
_AUDIT_LOCK = threading.Lock()      # 审计文件独占（不参与状态锁，防审计 IO 拖慢状态读）

_DEFAULT_BASE_WORKERS = {'leaf': 3, 'probe': 4}
_TORCH = object()                   # 哨兵：未启动


class _Cfg(object):
    """采样与动作参数（每次 start() 从环境变量重建，便于测试逐场景改）。"""

    def __init__(self, env=None):
        env = env if env is not None else os.environ
        g = env.get
        self.enabled = _int(g('MEMGUARD_ENABLED'), 1) != 0
        self.warn_pct = _float(g('MEMGUARD_WARN_PCT'), 70.0)
        self.pressure_pct = _float(g('MEMGUARD_PRESSURE_PCT'), 90.0)   # 对标 memory_threshold_percent
        self.critical_pct = _float(g('MEMGUARD_CRITICAL_PCT'), 95.0)   # 对标 critical_threshold_percent
        margin = _float(g('MEMGUARD_EXIT_MARGIN_PCT'), 5.0)            # 对标 95→85 / 90→85 的滞回带
        self.exit_pct = {WARN: max(0.0, self.warn_pct - margin),
                         PRESSURE: max(0.0, self.pressure_pct - margin),
                         CRITICAL: max(0.0, self.critical_pct - margin)}
        self.interval_s = max(0.05, _float(g('MEMGUARD_INTERVAL_S'), 1.0))  # 对标 check_interval=1s
        self.confirm_samples = max(1, _int(g('MEMGUARD_CONFIRM_SAMPLES'), 2))
        self.hold_s = max(0.0, _float(g('MEMGUARD_HOLD_S'), 20.0))
        self.proc_budget_mb = _float(g('MEMGUARD_PROC_BUDGET_MB'), 0.0) or None
        self.rss_limit_mb = _float(g('MEMGUARD_RSS_LIMIT_MB'), 0.0) or None
        self.gate_max_wait_s = max(0.0, _float(g('MEMGUARD_GATE_MAX_WAIT_S'), 300.0))
        self.crit_timeout_s = max(0.0, _float(g('MEMGUARD_CRIT_TIMEOUT_S'), 600.0))
        self.abort_on_timeout = _int(g('MEMGUARD_ABORT_ON_TIMEOUT'), 0) == 1
        self.abort_exit_code = _int(g('MEMGUARD_ABORT_EXIT_CODE'), ABORT_EXIT_CODE)
        self.trim = _int(g('MEMGUARD_TRIM'), 1) != 0
        self.trim_min_interval_s = max(0.0, _float(g('MEMGUARD_TRIM_MIN_INTERVAL_S'), 5.0))
        self.flap_window_s = max(1.0, _float(g('MEMGUARD_FLAP_WINDOW_S'), 60.0))
        self.flap_limit = max(2, _int(g('MEMGUARD_FLAP_LIMIT'), 20))
        self.trace_units = _int(g('MEMGUARD_TRACE_UNITS'), 0) == 1
        self.summary_s = max(0.0, _float(g('MEMGUARD_SUMMARY_S'), 60.0))
        self.audit_max_mb = max(0.1, _float(g('MEMGUARD_AUDIT_MAX_MB'), 8.0))
        self.gate_install = _int(g('MEMGUARD_GATE_INSTALL'), 1) != 0
        self.ring = max(8, _int(g('MEMGUARD_RING'), 120))


def _int(v, default):
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return default


def _float(v, default):
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# 内存读数（只读 /proc；无 psutil 依赖）
# ---------------------------------------------------------------------------
def _read_status_kb(key):
    try:
        with open('/proc/self/status', 'r') as f:
            for line in f:
                if line.startswith(key):
                    return float(line.split()[1])
    except OSError:
        return None
    return None


def _read_pages_bytes():
    """fallback：/proc/self/statm 第 2 列（resident pages）。"""
    try:
        page = os.sysconf('SC_PAGE_SIZE')
        with open('/proc/self/statm', 'r') as f:
            return int(f.read().split()[1]) * page
    except (OSError, ValueError, IndexError):
        return None


def _read_rusage_bytes():
    """最后兜底：getrusage 的 ru_maxrss（Linux 单位 kB）——是**峰值**不是当前值，
    仅在 /proc 不可用时使用，并在读数里用 source 标注，避免把它当成实时值。"""
    try:
        import resource
        return float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024.0
    except Exception:
        return None


def read_memory(source=None):
    """一次采样：进程 RSS/HWM + 系统 MemTotal/MemAvailable + cgroup 现值/限额。

    返回 dict（全部为字节/百分比，缺失字段为 None）；任何异常都返回尽量完整的读数。"""
    out = {'rss': None, 'hwm': None, 'source': None,
           'sys_total': None, 'sys_available': None, 'sys_free': None,
           'cg_limit': None, 'cg_usage': None, 'ts': time.time()}
    if source is not None:                      # 测试缝：脚本化读数
        try:
            out.update(source())
            out['source'] = out.get('source') or 'injected'
            return out
        except Exception:
            pass
    rss = _read_status_kb('VmRSS:')
    if rss is not None:
        out['rss'] = rss * 1024.0
        out['source'] = '/proc/self/status'
    else:
        rss = _read_pages_bytes()
        if rss is not None:
            out['rss'] = float(rss)
            out['source'] = '/proc/self/statm'
        else:
            rss = _read_rusage_bytes()
            out['rss'] = rss
            out['source'] = 'getrusage(maxrss)' if rss is not None else None
    hwm = _read_status_kb('VmHWM:')
    if hwm is not None:
        out['hwm'] = hwm * 1024.0
    elif out['source'] and 'maxrss' in out['source']:
        out['hwm'] = out['rss']
    try:
        with open('/proc/meminfo', 'r') as f:
            for line in f:
                if line.startswith('MemTotal:'):
                    out['sys_total'] = float(line.split()[1]) * 1024.0
                elif line.startswith('MemAvailable:'):
                    out['sys_available'] = float(line.split()[1]) * 1024.0
                elif line.startswith('MemFree:'):
                    out['sys_free'] = float(line.split()[1]) * 1024.0
    except OSError:
        pass
    # cgroup（v2: memory.max/memory.current；v1: memory.limit_in_bytes/usage_in_bytes）
    for lim_p, use_p in (('/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory.current'),
                         ('/sys/fs/cgroup/memory/memory.limit_in_bytes',
                          '/sys/fs/cgroup/memory/memory.usage_in_bytes')):
        try:
            raw = open(lim_p, 'r').read().strip()
            if raw and raw != 'max':
                v = float(raw)
                if 0 < v < (1 << 62):
                    out['cg_limit'] = v
            raw = open(use_p, 'r').read().strip()
            out['cg_usage'] = float(raw)
            if out['cg_limit']:
                break
        except (OSError, ValueError):
            continue
    return out


def _budget_bytes(cfg, mem):
    """进程轴的预算（字节）：RSS 硬覆盖 > cgroup 限额 > 显式预算 > 系统内存的 1/8。
    下限 256MB、上限 8GB——预算只用于把 RSS 折算成百分比，取错只会让守卫更敏感/更迟钝。"""
    if cfg.rss_limit_mb:
        return cfg.rss_limit_mb * 1048576.0
    cand = []
    if mem.get('cg_limit'):
        cand.append(float(mem['cg_limit']))
    if cfg.proc_budget_mb:
        cand.append(cfg.proc_budget_mb * 1048576.0)
    if mem.get('sys_total'):
        cand.append(float(mem['sys_total']) / 8.0)
    if not cand:
        return 256.0 * 1048576.0
    b = min(cand)
    return max(256.0 * 1048576.0, min(b, 8192.0 * 1048576.0))


def _pcts(mem, cfg):
    """(进程轴百分比, 系统轴百分比, 预算字节, 理由串)"""
    budget = _budget_bytes(cfg, mem)
    proc_pct = None
    if mem.get('rss') is not None and budget > 0:
        proc_pct = 100.0 * float(mem['rss']) / budget
        if mem.get('cg_usage') is not None and mem.get('cg_limit'):
            # 容器里 OOM killer 看的是 cgroup 用量（含 page cache），取更严的一侧
            cg_pct = 100.0 * float(mem['cg_usage']) / float(mem['cg_limit'])
            proc_pct = max(proc_pct, cg_pct)
    sys_pct = None
    if mem.get('sys_total') and mem.get('sys_available') is not None:
        sys_pct = 100.0 * (float(mem['sys_total']) - float(mem['sys_available'])) \
            / float(mem['sys_total'])
    vals = [p for p in (proc_pct, sys_pct) if p is not None]
    pct = max(vals) if vals else 0.0
    why = 'proc_rss' if (proc_pct is not None and proc_pct >= (sys_pct or -1)) else 'sys_avail'
    return proc_pct, sys_pct, budget, (why if vals else 'no_data'), pct


def _level_for(pct, cfg):
    if pct >= cfg.critical_pct:
        return CRITICAL
    if pct >= cfg.pressure_pct:
        return PRESSURE
    if pct >= cfg.warn_pct:
        return WARN
    return NORMAL


# ---------------------------------------------------------------------------
# 状态
# ---------------------------------------------------------------------------
_ST = {
    'started': False, 'run_id': None, 'tag': None, 'cfg': None,
    'level': NORMAL, 'pct': 0.0, 'proc_pct': None, 'sys_pct': None,
    'budget': None, 'mem': {}, 'reason': 'init',
    'gate_closed': False, 'gate_waits': 0, 'gate_wait_ms': 0.0, 'gate_timeouts': 0,
    'inflight': 0, 'inflight_max': 0, 'units': 0, 'samples': 0,
    'escalations': 0, 'deescalations': 0, 'trims': 0, 'trim_reclaimed': 0.0,
    'base_workers': dict(_DEFAULT_BASE_WORKERS), 'eff_workers': dict(_DEFAULT_BASE_WORKERS),
    'crit_since': None, 'safe_since': None, 'low_since': None,
    'gate_closed_since': None, 'last_trim_ts': None,
    'abort_requested': False, 'abort_reason': None, 'aborts': 0,
    'level_changes': [], 'last_flap_warn': None, 'flap_warnings': 0,
    'last_sample_ts': None, 'last_summary_ts': None, 'band': [],
    'source': None, 'audit': None, 'errors': 0, 'briefed_units': 0,
}
_MON = {'thread': None, 'stop': None}
_AUDIT = {'path': None, 'fd': None, 'seq': 0, 'bytes': 0, 'rotations': 0}
_GATE_ORIG = {}
_GATE_WRAPPED = {}
_END_REASON = 'stopped'


def audit_path():
    return _AUDIT['path']


def _now_iso():
    return datetime.now().isoformat(timespec='milliseconds')


def _audit(event, level=None, **fields):
    """写一条结构化审计（jsonl，一行一事件）。审计自身失败只计数，绝不抛出。"""
    if _AUDIT['path'] is None:
        return None
    rec = {'seq': _AUDIT['seq'] + 1, 'ts': _now_iso(), 'event': event,
           'pid': os.getpid(), 'run_id': _ST['run_id'], 'tag': _ST['tag']}
    lv = _ST['level'] if level is None else level
    rec['level'] = lv
    rec['level_name'] = LEVEL_NAMES.get(lv, str(lv))
    mem = _ST.get('mem') or {}
    rec['rss_mb'] = _mb(mem.get('rss'))
    rec['hwm_mb'] = _mb(mem.get('hwm'))
    rec['avail_mb'] = _mb(mem.get('sys_available'))
    rec['total_mb'] = _mb(mem.get('sys_total'))
    rec['budget_mb'] = _mb(_ST.get('budget'))
    rec['proc_pct'] = _r(_ST.get('proc_pct'))
    rec['sys_pct'] = _r(_ST.get('sys_pct'))
    rec['pct'] = _r(_ST.get('pct'))
    rec['gate'] = 'closed' if _ST['gate_closed'] else 'open'
    rec['leaf_workers'] = _ST['eff_workers'].get('leaf')
    rec['probe_workers'] = _ST['eff_workers'].get('probe')
    rec['inflight'] = _ST['inflight']
    rec.update(fields)
    line = json.dumps(rec, ensure_ascii=False, sort_keys=False) + '\n'
    try:
        with _AUDIT_LOCK:
            _AUDIT['seq'] += 1
            _rotate_if_needed()
            fd = _AUDIT['fd']
            if fd is None:
                fd = os.open(str(_AUDIT['path']),
                             os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
                _AUDIT['fd'] = fd
            data = line.encode('utf-8')
            os.write(fd, data)
            _AUDIT['bytes'] += len(data)
    except Exception:
        _ST['errors'] = _ST.get('errors', 0) + 1
    return rec


def _rotate_if_needed():
    cfg = _ST.get('cfg')
    if not cfg:
        return
    limit = cfg.audit_max_mb * 1048576.0
    if _AUDIT['bytes'] < limit:
        return
    try:
        if _AUDIT['fd'] is not None:
            os.close(_AUDIT['fd'])
            _AUDIT['fd'] = None
        path = str(_AUDIT['path'])
        try:
            os.replace(path, path + '.1')
        except OSError:
            pass
        _AUDIT['bytes'] = 0
        _AUDIT['rotations'] += 1
    except Exception:
        _ST['errors'] = _ST.get('errors', 0) + 1


def _mb(v):
    return None if v is None else round(float(v) / 1048576.0, 2)


def _r(v):
    return None if v is None else round(float(v), 2)


def _log(msg):
    fn = _ST.get('log_fn')
    text = f"[memguard] {msg}"
    try:
        if fn is not None:
            fn(text)
            return
    except Exception:
        pass
    try:
        sys.stderr.write(text + '\n')
        sys.stderr.flush()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 动作
# ---------------------------------------------------------------------------
def _apply_level(new_level, reason):
    """执行等级切换的全部副作用（审计 → 日志 → 并发重算 → 闸门/内存归还）。

    P4/F1：`crit_since` 采用「持续临界 episode」语义 —— 只在**进入**一个临界 episode
    （从 <CRITICAL 升入，或从 SAFE 落回 CRITICAL）时重新计时；episode 内部
    （CRITICAL→SAFE）沿用原计时。旧实现在 SAFE→CRITICAL 时继承旧计时 ⇒ 下一拍立刻
    又升回 SAFE（等级 livelock、闸门永不开）。"""
    old = _ST['level']
    if new_level == old:
        return
    now = time.time()
    _ST['level'] = new_level
    _ST['reason'] = reason
    _recompute_workers()
    if new_level > old:
        _ST['escalations'] += 1
    else:
        _ST['deescalations'] += 1
    _note_level_change(now, old, new_level, reason)
    if new_level >= CRITICAL:
        if not _ST['gate_closed']:
            _ST['gate_closed'] = True
            _ST['gate_closed_since'] = now
            _audit('gate-close', reason=reason, from_level=old, to_level=new_level)
            _log(f"等级 {LEVEL_NAMES[old]} → {LEVEL_NAMES[new_level]}（{reason}）："
                 f"闸门关闭，暂停新工作单元；生效并发 leaf={_ST['eff_workers']['leaf']} "
                 f"probe={_ST['eff_workers']['probe']}")
        # 新 episode 的两种入口：<CRITICAL 升入、SAFE 落回；其余（CRITICAL→SAFE）不算
        if _ST['crit_since'] is None or old < CRITICAL or old == SAFE:
            _ST['crit_since'] = now
    else:
        if _ST['gate_closed']:
            _ST['gate_closed'] = False
            _audit('gate-open', reason=reason, from_level=old, to_level=new_level)
            _log(f"等级 {LEVEL_NAMES[old]} → {LEVEL_NAMES[new_level]}（{reason}）：闸门打开，恢复作业")
        _ST['gate_closed_since'] = None
        _ST['crit_since'] = None
        _ST['safe_since'] = None
    _audit('level-up' if new_level > old else 'level-down',
           reason=reason, from_level=old, to_level=new_level,
           eff_leaf=_ST['eff_workers']['leaf'], eff_probe=_ST['eff_workers']['probe'])
    if new_level >= PRESSURE and _ST.get('cfg') and _ST['cfg'].trim and new_level != SAFE:
        # P4/F1：trim 节流（旧实现在等级抖动下每拍 gc+malloc_trim 一次 → 20 次/6s）
        cfg = _ST['cfg']
        last = _ST.get('last_trim_ts')
        if last is None or (now - last) >= cfg.trim_min_interval_s:
            trim_now(reason=f'level={LEVEL_NAMES[new_level]}')


def _note_level_change(now, old, new_level, reason):
    """P4/F1 防御性诊断：短时间内等级反复翻转（livelock 形态）写一条 flap-warning 审计。

    只做观测与留痕（不改变控制流），让同类病理在日志里自证，而不是等到 keeper 发现冻结。"""
    cfg = _ST.get('cfg')
    if cfg is None:
        return
    hist = _ST.setdefault('level_changes', [])
    hist.append(now)
    if len(hist) > 256:
        del hist[:-256]
    win = [t for t in hist if now - t <= cfg.flap_window_s]
    if len(win) >= cfg.flap_limit:
        last = _ST.get('last_flap_warn')
        if last is None or (now - last) >= cfg.flap_window_s:
            _ST['last_flap_warn'] = now
            _ST['flap_warnings'] = _ST.get('flap_warnings', 0) + 1
            _audit('flap-warning', reason=reason, changes_in_window=len(win),
                   window_s=cfg.flap_window_s, limit=cfg.flap_limit,
                   from_level=old, to_level=new_level,
                   crit_since=_ST.get('crit_since'), gate_open=not _ST['gate_closed'])
            _log(f"等级在 {cfg.flap_window_s:.0f}s 内切换 {len(win)} 次（≥{cfg.flap_limit}）"
                 f"→ 疑似等级 livelock，已写 flap-warning 审计（当前 {LEVEL_NAMES[new_level]}）")


def _recompute_workers():
    cfg = _ST.get('cfg')
    base = _ST['base_workers']
    fl = 1.0 if cfg is None else SHRINK.get(_ST['level'], 1.0)
    eff = {}
    for k, v in base.items():
        try:
            eff[k] = max(1, int(v * fl)) if fl > 0 else 1
        except Exception:
            eff[k] = 1
    _ST['eff_workers'] = eff


def trim_now(reason='manual'):
    """gc.collect() + libc malloc_trim(0)：把 glibc 已释放的 arena 归还内核。

    长跑实测（measure-mem.py M7）：反复建/销 ThreadPoolExecutor 后 RSS 会棘轮上漂，
    malloc_trim 一次可回收 3MB 级（本例 3216KB）；Python 侧循环垃圾靠 gc.collect()。"""
    released = 0
    try:
        gc.collect()
    except Exception:
        pass
    before = (_ST.get('mem') or {}).get('rss')
    try:
        import ctypes
        libc = ctypes.CDLL('libc.so.6', use_errno=False)
        if hasattr(libc, 'malloc_trim'):
            libc.malloc_trim(0)
    except Exception:
        pass
    fresh = read_memory(_ST.get('source_fn'))
    after = fresh.get('rss')
    if before is not None and after is not None:
        released = max(0.0, float(before) - float(after))
        _ST['trim_reclaimed'] = _ST.get('trim_reclaimed', 0.0) + released
    _ST['trims'] = _ST.get('trims', 0) + 1
    _ST['last_trim_ts'] = time.time()
    _ST['mem'] = fresh
    _audit('trim', reason=reason, remembered_mb=_mb(released))
    return released


def gate_open():
    with _LOCK:
        return not _ST['gate_closed']


def _gate_closed_s(now=None):
    """闸门已连续关闭多少秒（未关闭 → 0.0）。心跳用它区分「刚关一下」与「冻住了」。"""
    since = _ST.get('gate_closed_since')
    if not _ST['gate_closed'] or since is None:
        return 0.0
    now = time.time() if now is None else float(now)
    return round(max(0.0, now - float(since)), 3)


def gate_wait(tag='work', timeout=None):
    """闸门：等级 ≥ CRITICAL 时阻塞新工作单元。返回等待秒数（0 = 直接放行）。

    上界 MEMGUARD_GATE_MAX_WAIT_S（默认 300s）：超时放行并审计 gate-timeout，
    由「并发=1 + 限速器」兜住吞吐，避免长跑因等待而假死（I5）。"""
    cfg = _ST.get('cfg')
    if cfg is None or _ST['started'] is False:
        return 0.0
    limit = cfg.gate_max_wait_s if timeout is None else max(0.0, float(timeout))
    t0 = time.time()
    timed_out = False
    with _GATE:
        if _ST['gate_closed']:
            _ST['gate_waits'] = _ST.get('gate_waits', 0) + 1
            deadline = t0 + limit
            while _ST['gate_closed'] and time.time() < deadline:
                _GATE.wait(min(0.25, max(0.01, deadline - time.time())))
            if _ST['gate_closed']:
                timed_out = True
    waited = time.time() - t0
    if waited > 0.001:
        with _LOCK:
            _ST['gate_wait_ms'] = _ST.get('gate_wait_ms', 0.0) + waited * 1000.0
            if timed_out:
                _ST['gate_timeouts'] = _ST.get('gate_timeouts', 0) + 1
        _audit('gate-timeout' if timed_out else 'gate-pass',
               tag=tag, waited_ms=round(waited * 1000.0, 1))
        if timed_out:
            _log(f"闸门等待超上界 {limit:.0f}s（tag={tag}）→ 放行单并发单元，审计已留痕")
    return waited


# 工作单元入口包装（闸门 + 并发仪表 + 可选单元审计）
def _unit_wrap(fn, name):
    @functools.wraps(fn)
    def wrapper(*a, **kw):
        waited = gate_wait(name)
        with _LOCK:
            _ST['inflight'] = _ST.get('inflight', 0) + 1
            _ST['units'] = _ST.get('units', 0) + 1
            _ST['inflight_max'] = max(_ST.get('inflight_max', 0), _ST['inflight'])
            inflight = _ST['inflight']
            trace = bool(_ST.get('cfg') and _ST['cfg'].trace_units)
            rss0 = (_ST.get('mem') or {}).get('rss')
        t0 = time.time()
        try:
            return fn(*a, **kw)
        finally:
            dt = time.time() - t0
            mem = read_memory(_ST.get('source_fn'))
            with _LOCK:
                _ST['mem'] = mem
                _ST['inflight'] = max(0, _ST.get('inflight', 1) - 1)
                lv = _ST['level']
                brief = _ST.get('briefed_units', 0)
                if (trace or lv >= WARN) and brief < 200:
                    _ST['briefed_units'] = brief + 1
                    _audit('unit', tag=name, waited_ms=round(waited * 1000.0, 1),
                           secs=round(dt, 3), inflight=inflight,
                           rss_delta_mb=_mb(None if (rss0 is None or mem.get('rss') is None)
                                            else mem['rss'] - rss0),
                           args=str(a[0])[:40] if a else '')
    return wrapper


def install_gate(module=None, names=('scrape_leaf', 'probe_children')):
    """把工作单元入口包一层闸门（运行时替换模块属性，可逆）。

    这是「任务闸门暂停新工作」的落地方式：CRITICAL 时 scrape_leaf / probe_children
    在**发起任何请求之前**等待内存回落，因此不仅收缩了并发，还真正暂停了新工作。
    不修改这两个函数的函数体（其他 slot 并行改动互不冲突）。"""
    cfg = _ST.get('cfg')
    if cfg is not None and (not cfg.enabled or not cfg.gate_install):
        return []
    if module is None:
        module = sys.modules.get('__main__')
    wrapped = []
    with _LOCK:
        for name in names:
            fn = getattr(module, name, None)
            if not callable(fn) or name in _GATE_WRAPPED:
                continue
            _GATE_ORIG[name] = fn
            w = _unit_wrap(fn, name)
            setattr(module, name, w)
            _GATE_WRAPPED[name] = (module, w)
            wrapped.append(name)
    if wrapped:
        _audit('gate-install', wrapped=wrapped, module=getattr(module, '__name__', '?'))
    return wrapped


def uninstall_gate():
    """撤销包装；若当前属性已被第三方再次替换，则保留现状并审计（不覆盖别人的接线）。"""
    removed, skipped = [], []
    with _LOCK:
        for name, (module, w) in list(_GATE_WRAPPED.items()):
            if getattr(module, name, None) is w:
                setattr(module, name, _GATE_ORIG[name])
                removed.append(name)
            else:
                skipped.append(name)
            _GATE_WRAPPED.pop(name, None)
            _GATE_ORIG.pop(name, None)
    if removed or skipped:
        _audit('gate-uninstall', removed=removed, skipped=skipped)
    return removed, skipped


# ---------------------------------------------------------------------------
# 采样与状态机
# ---------------------------------------------------------------------------
def _sample_once():
    """一次采样 + 状态机推进（monitor 线程按 interval 调；测试可同步调）。"""
    cfg = _ST['cfg']
    mem = read_memory(_ST.get('source_fn'))
    proc_pct, sys_pct, budget, why, pct = _pcts(mem, cfg)
    now = time.time()
    with _LOCK:
        prev_level = _ST['level']
        _ST['mem'] = mem
        _ST['proc_pct'] = proc_pct
        _ST['sys_pct'] = sys_pct
        _ST['budget'] = budget
        _ST['pct'] = pct
        _ST['samples'] = _ST.get('samples', 0) + 1
        _ST['last_sample_ts'] = now
        band = _ST.setdefault('band', [])
        band.append((round(pct, 2), _level_for(pct, cfg)))
        del band[:-cfg.ring]

        want = _level_for(pct, cfg)
        if want > prev_level:
            need = cfg.confirm_samples if want == prev_level + 1 else cfg.confirm_samples + 1
            tail = [lv for _, lv in band[-need:]]
            if len(tail) >= need and all(lv >= want for lv in tail):
                _apply_level(want, f'{why} {pct:.1f}% ≥ {cfg.critical_pct if want >= CRITICAL else cfg.pressure_pct if want == PRESSURE else cfg.warn_pct:.0f}%'
                             f' (rss={_mb(mem.get("rss"))}MB avail={_mb(mem.get("sys_available"))}MB)')
                _ST['low_since'] = None
        elif want < prev_level:
            exit_thr = cfg.exit_pct.get(prev_level, cfg.exit_pct.get(CRITICAL, 0.0))
            if pct <= exit_thr:
                if _ST['low_since'] is None:
                    _ST['low_since'] = now
                elif now - _ST['low_since'] >= cfg.hold_s:
                    # P4/F1：**从 SAFE_MODE 恢复必须一步到位**（直接落到当前读数对应的 want）。
                    # 旧实现写死 prev_level-1：SAFE(4)→CRITICAL(3)，而 crit_since 又继承旧值
                    # ⇒ 下一拍立刻升回 SAFE ⇒ 闸门永久关闭、等级 4↔3 livelock
                    # （crit-correct M1 / crit-adversarial E5）。非 SAFE 等级保持"每 hold_s 降一级"
                    # 的分级滞回（那是设计意图，也是槽位自带 scenario c 的判据）。
                    target = want if prev_level >= SAFE else (prev_level - 1)
                    _apply_level(target,
                                 f'水位 {pct:.1f}% ≤ {LEVEL_NAMES[prev_level]} 退出阈值 '
                                 f'{exit_thr:.0f}% 且保持 {now - _ST["low_since"]:.0f}s '
                                 f'→ 回落至 {LEVEL_NAMES[target]}'
                                 f'{"（SAFE 解除：一步到位）" if prev_level >= SAFE else ""}'
                                 f'（当前主控轴 {why}）')
                    _ST['low_since'] = None
            else:
                _ST['low_since'] = None
        else:
            _ST['low_since'] = None

        lv = _ST['level']
        abort_now = False
        # 持续临界 → SAFE_MODE（对标 crawl4ai 的 memory_wait_timeout → MemoryError）
        if lv >= CRITICAL:
            if _ST['crit_since'] is None:
                _ST['crit_since'] = now
            elif cfg.crit_timeout_s and (now - _ST['crit_since']) >= cfg.crit_timeout_s \
                    and lv < SAFE:
                _ST['safe_since'] = now
                _apply_level(SAFE, f'持续临界 {now - _ST["crit_since"]:.0f}s ≥ '
                                   f'{cfg.crit_timeout_s:.0f}s（abort_on_timeout='
                                   f'{int(cfg.abort_on_timeout)}）')
                abort_now = bool(cfg.abort_on_timeout)
                if not abort_now:
                    # P4/F2：ABORT=0 是**显式降级**（不是静默冻结）——留一条可被 keeper/
                    # 人工检索的审计；闸门状态同时进心跳（heartbeat_state），不再靠猜。
                    _audit('abort-degraded', reason='sustained-critical',
                           crit_timeout_s=cfg.crit_timeout_s,
                           note='MEMGUARD_ABORT_ON_TIMEOUT=0：不终止进程，闸门保持关闭，'
                                '由心跳携带 gate_open/gate_closed_s 供 keeper 判定')
                    _log(f'持续临界 ≥{cfg.crit_timeout_s:.0f}s 但 abort_on_timeout=0 → '
                         f'保持 SAFE_MODE（闸门关闭）并写 abort-degraded 审计；'
                         f'闸门状态已进心跳（gate_open=False）')
        else:
            _ST['crit_since'] = None
            if lv == SAFE:
                _ST['safe_since'] = None
        # 常态摘要（有界审计的增长速率）
        if cfg.summary_s and (now - (_ST['last_summary_ts'] or 0)) >= cfg.summary_s:
            _ST['last_summary_ts'] = now
            _audit('summary', samples=_ST['samples'], why=why,
                   units=_ST['units'], inflight_max=_ST['inflight_max'],
                   gate_waits=_ST['gate_waits'], trims=_ST['trims'])
    if abort_now:
        _abort_prepare('sustained-critical + MEMGUARD_ABORT_ON_TIMEOUT=1')
        raise _AbortSignal('sustained-critical + MEMGUARD_ABORT_ON_TIMEOUT=1')
    return mem


def _abort_prepare(text):
    """abort 的可观测部分（审计 + 标记 + 开闸），**不终止进程**（幂等，只记一次）。

    拆出来的原因：同步调用 _sample_once() 的调用方（测试夹具）会接住 _AbortSignal 继续跑，
    而生产路径（_monitor_loop / start）接住后立刻 _fail_loud → os._exit。两条路径都必须
    留下同一条 abort 审计与同一份状态。"""
    cfg = _ST.get('cfg')
    code = cfg.abort_exit_code if cfg is not None else ABORT_EXIT_CODE
    with _LOCK:
        first = not _ST.get('abort_requested')
        _ST['abort_requested'] = True
        _ST['abort_reason'] = text
        _ST['gate_closed'] = False
        _ST['gate_closed_since'] = None
        _GATE.notify_all()
    if not first:
        return code
    _audit('abort', reason=text, exit_code=code, pid=os.getpid(),
           crit_timeout_s=(cfg.crit_timeout_s if cfg is not None else None),
           abort_on_timeout=True, level=SAFE,
           note='fail-loud：进程即将以该退出码终止，交给 keeper 重启')
    try:
        with _AUDIT_LOCK:
            if _AUDIT['fd'] is not None:
                try:
                    os.fsync(_AUDIT['fd'])
                except OSError:
                    pass
    except Exception:
        pass
    return code


def _fail_loud(signal=None):
    """P4/F2：把「持续临界 + ABORT_ON_TIMEOUT=1」变成**真正的** fail-loud。

    旧实现只在线程内 `raise MemoryError` 后 `break`：监控线程退出、状态机冻结、
    闸门永不开、进程照旧运行（crit-correct M2 / crit-adversarial E4 实证）。
    正确语义 = 进程终止，交给 keeper ≤180s 重启：
      · 先落盘 `abort` 审计（os.write 无缓冲，进程消失前已在内核页缓存里）；
      · 打开闸门并登记 abort_requested（让任何在闸门里等待的单元立即返回，虽然马上要退）；
      · os._exit(code)：不跑 atexit、不做栈展开 —— 这是"进程级 fail-loud"的本意，
        progress.json/records.jsonl 的原子性由 v10 的持久化层保证（不产生半写状态）。
    """
    text = str(signal) if signal is not None else 'sustained-critical'
    code = _abort_prepare(text)
    with _LOCK:
        _ST['aborts'] = _ST.get('aborts', 0) + 1
    _log(f'持续临界且 abort_on_timeout=1 → **进程终止**（os._exit({code})），'
         f'交给 keeper 重启；abort 审计已落盘')
    os._exit(int(code) if isinstance(code, int) else ABORT_EXIT_CODE)


def _monitor_loop():
    cfg = _ST['cfg']
    stop_evt = _MON['stop']
    while not stop_evt.is_set():
        try:
            _sample_once()
        except _AbortSignal as sig:
            _fail_loud(sig)          # 不返回：os._exit 终止进程
            return
        except Exception as e:                       # I1 fail-open
            with _LOCK:
                _ST['errors'] = _ST.get('errors', 0) + 1
            _audit('error', where='monitor_loop', err=f'{type(e).__name__}: {e}')
        stop_evt.wait(cfg.interval_s)


def start(audit_path=None, base_workers=None, tag=None, log_fn=None, env=None):
    """启动后台内存守卫（幂等）。返回状态快照。

    audit_path: 结构化审计 jsonl 路径（默认 env MEMGUARD_AUDIT 或 ./memguard.jsonl）
    base_workers: 基线并发 {'leaf':LEAF_WORKERS,'probe':PROBE_WORKERS}（由 scrape.main() 传入）
    log_fn: 文本日志函数（scrape.log），用于 WARN+ 的人工可见性
    """
    cfg = _Cfg(env)
    with _LOCK:
        if _ST['started']:
            return status()
        _ST['cfg'] = cfg
        _ST['tag'] = tag
        _ST['log_fn'] = log_fn
        _ST['base_workers'] = dict(base_workers or _DEFAULT_BASE_WORKERS)
        _ST['run_id'] = f"{os.getpid()}-{int(time.time())}"
        _ST['level'] = NORMAL
        _ST['eff_workers'] = dict(_ST['base_workers'])
        if not cfg.enabled:
            _ST['started'] = False
            return status()
        path = audit_path or os.environ.get('MEMGUARD_AUDIT') or 'memguard.jsonl'
        _AUDIT['path'] = str(path)
        try:
            os.makedirs(os.path.dirname(os.path.abspath(str(path))), exist_ok=True)
        except OSError:
            pass
        _ST['started'] = True
        _MON['stop'] = threading.Event()

    _audit('start', tag=tag, base_workers=dict(_ST['base_workers']),
           warn_pct=cfg.warn_pct, pressure_pct=cfg.pressure_pct,
           critical_pct=cfg.critical_pct, exit_pct=dict(cfg.exit_pct),
           interval_s=cfg.interval_s, confirm_samples=cfg.confirm_samples,
           hold_s=cfg.hold_s, budget_mb=_mb(_budget_bytes(cfg, read_memory())),
           gate_max_wait_s=cfg.gate_max_wait_s, crit_timeout_s=cfg.crit_timeout_s,
           trace_units=cfg.trace_units, source=_ST.get('source') or 'proc')
    t = threading.Thread(target=_monitor_loop, name='memguard-monitor', daemon=True)
    _MON['thread'] = t
    t.start()
    try:
        _sample_once()                 # 立即给出一组读数（不必等第一个 interval）
    except _AbortSignal as sig:
        _fail_loud(sig)                # 不返回：与监控线程路径同一 fail-loud 语义
    _log(f"内存守卫启动（审计 {_AUDIT['path']}；基线并发 leaf={_ST['base_workers'].get('leaf')} "
         f"probe={_ST['base_workers'].get('probe')}；阈值 WARN/PRE/CRIT="
         f"{cfg.warn_pct:.0f}/{cfg.pressure_pct:.0f}/{cfg.critical_pct:.0f}%）")
    atexit.register(stop)
    return status()


def stop(reason=None):
    """停止监控：置 stop、唤醒闸门、卸载包装、写 stop 审计并关审计 fd（幂等）。"""
    global _END_REASON
    with _LOCK:
        if not _ST.get('started'):
            return status()
        if reason:
            _END_REASON = reason
        _ST['started'] = False
        _ST['gate_closed'] = False
        _ST['gate_closed_since'] = None
        ev = _MON.get('stop')
        if ev is not None:
            ev.set()
        _GATE.notify_all()
    t = _MON.get('thread')
    if t is not None and t.is_alive() and t is not threading.current_thread():
        try:
            t.join(timeout=2.0)
        except Exception:
            pass
    _MON['thread'] = None
    up, skipped = uninstall_gate()
    _audit('stop', reason=_END_REASON, samples=_ST['samples'], units=_ST['units'],
           escalations=_ST['escalations'], deescalations=_ST['deescalations'],
           gate_waits=_ST['gate_waits'], gate_timeouts=_ST['gate_timeouts'],
           trims=_ST['trims'], trim_reclaimed_mb=_mb(_ST['trim_reclaimed']),
           errors=_ST['errors'], inflight_max=_ST['inflight_max'],
           gate_uninstalled=up, gate_skipped=skipped)
    with _AUDIT_LOCK:
        if _AUDIT['fd'] is not None:
            try:
                os.close(_AUDIT['fd'])
            except OSError:
                pass
            _AUDIT['fd'] = None
    _log(f"内存守卫停止（{_END_REASON}）：采样 {_ST['samples']} 次、升级 {_ST['escalations']} 次、"
         f"闸门等待 {_ST['gate_waits']} 次、归还内存 {_mb(_ST['trim_reclaimed'])}MB")
    return status()


# ---------------------------------------------------------------------------
# 查询 API
# ---------------------------------------------------------------------------
def level():
    return _ST['level']


def level_name():
    return LEVEL_NAMES.get(_ST['level'], str(_ST['level']))


def effective_workers(kind):
    """当前生效并发度（纯读，无阻塞、无 IO）。

    NORMAL/WARN → 基线；PRESSURE → 基线×0.5（下限 1）；CRITICAL/SAFE → 1。
    未启动时返回基线默认值，因此 scrape.py 的调用点在守卫缺席时行为不变。"""
    if not _ST['started']:
        base = _ST.get('base_workers') or _DEFAULT_BASE_WORKERS
        try:
            return max(1, int(base.get(kind, 1)))
        except Exception:
            return 1
    try:
        return int(_ST['eff_workers'].get(kind, 1))
    except Exception:
        return 1


def status():
    cfg = _ST.get('cfg')
    mem = _ST.get('mem') or {}
    return {
        'started': _ST['started'], 'run_id': _ST['run_id'], 'tag': _ST['tag'],
        'level': _ST['level'], 'level_name': LEVEL_NAMES.get(_ST['level']),
        'reason': _ST['reason'], 'pct': _r(_ST['pct']),
        'proc_pct': _r(_ST['proc_pct']), 'sys_pct': _r(_ST['sys_pct']),
        'rss_mb': _mb(mem.get('rss')), 'hwm_mb': _mb(mem.get('hwm')),
        'avail_mb': _mb(mem.get('sys_available')), 'budget_mb': _mb(_ST.get('budget')),
        'mem_source': mem.get('source') or _ST.get('source'),
        'gate_open': not _ST['gate_closed'],
        'base_workers': dict(_ST['base_workers']), 'eff_workers': dict(_ST['eff_workers']),
        'samples': _ST['samples'], 'units': _ST['units'], 'inflight': _ST['inflight'],
        'inflight_max': _ST['inflight_max'], 'escalations': _ST['escalations'],
        'deescalations': _ST['deescalations'], 'gate_waits': _ST['gate_waits'],
        'gate_wait_ms': round(_ST['gate_wait_ms'], 1), 'gate_timeouts': _ST['gate_timeouts'],
        'trims': _ST['trims'], 'trim_reclaimed_mb': _mb(_ST['trim_reclaimed']),
        'errors': _ST['errors'], 'audit': _AUDIT['path'], 'audit_seq': _AUDIT['seq'],
        'audit_rotations': _AUDIT['rotations'],
        'gate_closed_s': _gate_closed_s(),
        'abort_requested': bool(_ST.get('abort_requested')),
        'abort_reason': _ST.get('abort_reason'),
        'flap_warnings': _ST.get('flap_warnings', 0),
        'thresholds': None if cfg is None else {
            'warn': cfg.warn_pct, 'pressure': cfg.pressure_pct,
            'critical': cfg.critical_pct, 'exit': dict(cfg.exit_pct),
            'interval_s': cfg.interval_s, 'confirm_samples': cfg.confirm_samples,
            'hold_s': cfg.hold_s, 'crit_timeout_s': cfg.crit_timeout_s},
    }


def snapshot():
    """无副作用读数（不推进状态机）：给测试/运维看「此刻算出来是几级」。"""
    cfg = _ST.get('cfg') or _Cfg()
    mem = read_memory(_ST.get('source_fn'))
    proc_pct, sys_pct, budget, why, pct = _pcts(mem, cfg)
    return {'rss_mb': _mb(mem.get('rss')), 'hwm_mb': _mb(mem.get('hwm')),
            'avail_mb': _mb(mem.get('sys_available')), 'total_mb': _mb(mem.get('sys_total')),
            'budget_mb': _mb(budget), 'proc_pct': _r(proc_pct), 'sys_pct': _r(sys_pct),
            'pct': _r(pct), 'why': why, 'level_now': _level_for(pct, cfg),
            'level_state': _ST['level'], 'source': mem.get('source'),
            'gate_open': not _ST['gate_closed'],
            'eff_workers': dict(_ST['eff_workers'])}


def heartbeat_state(now=None):
    """P4/F3（crit-adversarial C3）：给心跳/keeper 的紧凑运行态快照（只读、永不抛异常）。

    为什么需要：闸门状态只存在于本模块内存里，keeper 只看心跳 ⇒ 管线被守卫冻住时心跳仍
    报「健康在推进」（C3 实证：闸门关闭 79s 期间心跳 advance_count 仍在涨、keeper 判 OK）。
    本函数把闸门/等级/并发/abort 状态压成一个小 dict，由 heartbeat.write_heartbeat() 自动
    采集写进心跳的 runtime.mem 块。

    返回 None = 守卫未启用/未启动（心跳据此把 gate_open 记为 null，不伪造"闸门开着"）。"""
    try:
        if not _ST['started']:
            return None
        cfg = _ST.get('cfg')
        now = time.time() if now is None else float(now)
        crit_since = _ST.get('crit_since')
        return {
            'run_id': _ST.get('run_id'),
            'tag': _ST.get('tag'),
            'level': LEVEL_NAMES.get(_ST['level'], str(_ST['level'])),
            'level_num': _ST['level'],
            'gate_open': not _ST['gate_closed'],
            'gate_closed_s': _gate_closed_s(now),
            'gate_waits': _ST.get('gate_waits', 0),
            'gate_timeouts': _ST.get('gate_timeouts', 0),
            'pct': _r(_ST.get('pct')),
            'proc_pct': _r(_ST.get('proc_pct')),
            'sys_pct': _r(_ST.get('sys_pct')),
            'why': _ST.get('reason'),
            'rss_mb': _mb((_ST.get('mem') or {}).get('rss')),
            'budget_mb': _mb(_ST.get('budget')),
            'eff_workers': dict(_ST.get('eff_workers') or {}),
            'base_workers': dict(_ST.get('base_workers') or {}),
            'critical_s': (round(now - float(crit_since), 3)
                           if crit_since is not None else None),
            'in_safe_mode': bool(_ST['level'] >= SAFE),
            'samples': _ST.get('samples', 0),
            'sample_age_s': (round(now - _ST['last_sample_ts'], 3)
                             if _ST.get('last_sample_ts') else None),
            'inflight': _ST.get('inflight', 0),
            'abort_requested': bool(_ST.get('abort_requested')),
            'abort_on_timeout': bool(cfg.abort_on_timeout) if cfg is not None else False,
            'flap_warnings': _ST.get('flap_warnings', 0),
            'audit': _AUDIT['path'],
        }
    except Exception:                       # 观测接口绝不抛出
        return None


def set_source(fn):
    """测试缝：注入读数回调（返回 dict，可只给 rss/sys_available/sys_total）。

    双重门禁：必须设 MEMGUARD_ALLOW_TEST_SOURCE=1，否则忽略（避免生产被误注入）。"""
    if os.environ.get('MEMGUARD_ALLOW_TEST_SOURCE') != '1':
        return False
    _ST['source_fn'] = fn
    with _LOCK:
        _ST['source'] = 'injected' if fn is not None else 'proc'
    return True


def selfcheck():
    """自检：读数可用性 + 各等级动作映射 + 闸门/滞回的可达性（不启线程）。"""
    out = {'ok': True, 'problems': []}
    mem = read_memory()
    out['mem'] = {k: (_mb(v) if k in ('rss', 'hwm', 'sys_total', 'sys_available', 'sys_free',
                                      'cg_limit', 'cg_usage') else v) for k, v in mem.items()}
    if mem.get('rss') is None:
        out['ok'] = False
        out['problems'].append('无法读取进程 RSS（/proc/self/status 与 statm 均不可用）')
    if mem.get('sys_available') is None:
        out['problems'].append('无法读取 MemAvailable（系统轴降级为不可用，进程轴仍生效）')
    cfg = _Cfg()
    out['thresholds'] = {'warn': cfg.warn_pct, 'pressure': cfg.pressure_pct,
                         'critical': cfg.critical_pct, 'exit': dict(cfg.exit_pct)}
    out['level_map'] = {name: LEVEL_NAMES.get(_level_for(pct, cfg), '?')
                        for name, pct in (('normal', 1.0), ('warn', cfg.warn_pct),
                                          ('pressure', cfg.pressure_pct),
                                          ('critical', cfg.critical_pct))}
    return out


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser(description='memguard 诊断 CLI（不自启监控线程）')
    ap.add_argument('cmd', nargs='?', default='snapshot',
                    choices=['snapshot', 'read', 'selfcheck'])
    a = ap.parse_args()
    if a.cmd == 'read':
        m = read_memory()
        print(json.dumps({k: (_mb(v) if isinstance(v, float) and k != 'ts' else v)
                          for k, v in m.items()}, ensure_ascii=False))
    elif a.cmd == 'selfcheck':
        print(json.dumps(selfcheck(), ensure_ascii=False, indent=1))
    else:
        print(json.dumps(snapshot(), ensure_ascii=False, indent=1))
