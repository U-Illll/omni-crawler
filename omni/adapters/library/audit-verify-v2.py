#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""audit-verify-v2.py —— R3-P4 统一审计校验器 v2.1（防伪 + 三写者统一布局 + writer 分组）

权威契约：`R3p4/refs/audit-contract-v2.md`（冻结稿 §1–§7，**§8 = v2.1 增量**）。
本文件是该契约 §5/§6/§7/§8 的实现。自我版本：`VERIFIER_VERSION`。

与 v1（`R3/impl/slot-drill/audit-verify.py`）的关系
--------------------------------------------------
保留能力（不丢）：字段/枚举/单调校验、场景→事件链路断言、derived 旁路通道、
                  逐条 evidence 说明、`--json` 机器可读输出。
新增/反转（本契约要求）：
  1. **fail-closed**：未知 scenario / 缺 scenario / 缺产物目录 ⇒ FAIL(exit 1)。
     v1 在 `check_links()` 返回 None 时"零检查 exit 0"，这是 H6/D-1 的一半。
  2. **时间窗真实生效**：`evidence.json` 的 `injections[].ts` ⇒ 注入相关事件必须落在
     `[first_inj - pre, last_inj + deadline]`。拿不到时间线时**不再静默跳过**：
        · 未显式声明降级 ⇒ 硬违规 `no_window`（堵 e4「清空 injections 即绕过」）；
        · `--allow-no-window` 显式声明 ⇒ `window_ok="no_window"` + `evidence_level=low`
          + `canonical_pass=False`（降级可见，绝不静默通过）。
  3. **血缘（anti-fabrication）**：evidence.json + 真实过程证据（records.jsonl/scrape.log）
     + pid 与真实产物可关联 + 审计文件不得在 run 窗口结束后被追加。全手写文件 ⇒ FAIL。
  4. **seq 防伪（v2.1 修订）**：同一文件内按 **(pid, writer)** 分组，组内 seq 必须从 1 起
     严格 +1（缺号/乱序/0 起 ⇒ 硬违规）。堵 e5「换 pid 插入乱序事件」。
     v2.0 只按 pid 分组，隐含假设"一个 pid = 一个写者"；KI-1（p4-verify 在真实 S4 上复现的
     `seq_duplicate pid=8631 seq=1/2/3`）证明该假设不成立：同一进程内 `scrape.py` 的块级
     写者与 `audit.py` 的请求级写者各持**独立**计数器 ⇒ 必须引入 `writer` 分组键（契约 §8）。
  5. **derived 通道收紧**：来源须在场景目录内 + mtime 在 run 窗口内 + 内容含具体读数
     （含数字，纯模板句不算）；derived-only ⇒ `channel=derived` + `evidence_level=low`。
  6. **撕裂行**：仅**末行**撕裂容忍 1 条（warning）；其余坏行硬违规（对齐 e3）。
  7. **退出码分级**：0=全过 / 1=硬违规或链路断言不过 / 2=用法或环境错误。
  8. **`--compat-v1`**：读旧布局（`r3-audit-v1` / keeper-v2 / **v2.0 无 writer 行**）做对照，
     只给 warning 不判死；默认（严格）模式下旧布局行是硬违规 `legacy_layout:*`，
     且 **v2 行缺 `writer` 是硬违规 `writer_missing`**（契约 §8.2/§8.5：新写入必带 writer）。
     compat 模式下缺 writer 的行回退按 `(pid, "")` 分组（契约 §8.2），其 seq 偏差一并降级为
     warning 并登记 `compat_v1_seq_grouping_by_pid` 降级原因 —— 否则"历史对照"永远 exit 1。

用法
----
  python3 audit-verify-v2.py --scenario S1 --scenario-dir runs/<run>/S1
  python3 audit-verify-v2.py --scenario S1 --scenario-dir DIR --json out.json --quiet
  python3 audit-verify-v2.py --scenario S1 --scenario-dir DIR --compat-v1     # 旧产物对照
  python3 audit-verify-v2.py --scenario S1 --scenario-dir DIR --allow-no-window
  python3 audit-verify-v2.py --schema-only --files a/audit.jsonl b/audit.jsonl
  python3 audit-verify-v2.py --self-test
"""
import argparse
import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

SCHEMA_V2 = "r3-audit-v2"
VERIFIER_VERSION = "2.1"                  # v2.1 = 契约 §8（writer 分组键 + keeper 事件表对齐）
CONTRACT_REV = "v2.1"                     # 实现的契约修订版（§1–§7 + §8）
LEVELS = ("request", "block", "process")
WRITER_KEYS = ("retry", "converge", "keeper")   # 契约 §8.1 的具名写者（其它短横线名合法）
TS_MIN = 1735689600.0                     # 2026-01-01T00:00:00Z
TS_FUTURE_TOL = 600.0
PRE_SLACK_S = 10.0                        # 注入线前松弛（窗口下界）
POST_SLACK_S = 300.0                      # run 结束后允许的收尾写入松弛
WINDOW_DEFAULT_S = 180.0                  # evidence.json 未声明 deadline 时的默认值
DERIVED_MTIME_SLACK_S = 120.0
AUDIT_NAME_RE = re.compile(r"^audit\.jsonl(\.\d+)?$", re.I)

# ---------------------------------------------------------------------------
# 事件表（契约 §2：核心表 + 扩展表。canonical 名 = 写入名）
# ---------------------------------------------------------------------------
EVENT_LEVELS = {}


def _reg(levels, names):
    for n in names:
        EVENT_LEVELS.setdefault(n, set()).update(levels)


# 核心表（契约 §2）
_reg({"request"}, ["retry_scheduled", "retry_exhausted", "error_classified"])
_reg({"block"}, ["probe_all_dead", "repair_round_enter", "convergence_decision",
                 "stall_detected", "graceful_stop"])
_reg({"process"}, ["keeper_restart", "keeper_adopt_refused", "singleton_conflict",
                   "heartbeat_stall", "keeper_exit"])
# 扩展表（契约 §2 明列 + 三生产者既发事件名，均已读码确认）
_reg({"request"}, ["rate_limited", "cooldown_enter", "circuit_open", "circuit_close",
                   "request_failed", "throttle_adjust", "throttle_snapshot", "error_recovered"])
_reg({"block"}, ["run_start", "run_end", "probe_incomplete", "probe_partial_dead", "probe_retry",
                 "probe_enumeration_gap", "repair_round_exit", "repair_round_result",
                 "repair_exhausted", "stall_cleared", "gap_recorded", "children_discovered",
                 "output_dir_recreated", "records_write_deferred", "records_write_dropped",
                 "progress_write_failed", "convergence_report_failed", "short_write_detected",
                 "records_repair", "records_tail_repaired", "big_gap"])
_reg({"process"}, ["keeper_start", "keeper_stop", "keeper_abort", "keeper_adopt", "adopt",
                   "adopt_unverified", "singleton_acquired", "singleton_released", "heartbeat",
                   "heartbeat_class", "tolerance_done", "process_dead", "process_alive",
                   "restart_failed", "restart_backoff", "restart_flapping", "restart_dry_run",
                   "guard_suppressed", "detector_ambiguous", "env_probe", "env_probe_fail",
                   "env_changed", "pidfile_stale", "pidfile_untrusted", "deadline_breached",
                   "progress_recovered", "checkpoint_restore", "dir_recreated", "mem_level_change",
                   "gate_close", "gate_open", "mem_abort", "keeper_signal", "stall_kill",
                   "stall_restart_refused", "state_imported", "cycle_aborted",
                   "config_rail_override", "config_rail_refused", "config_warning",
                   "selftest_case", "selftest_result", "run_start", "run_end", "graceful_stop",
                   "stall_detected", "dir_recreate"])
# ---------------------------------------------------------------------------
# R-9 修复（P4.5·v2.1）：keeper 侧 `R3_ALIAS_EVENTS` 的 canonical 名必须与本校验器**同一张表**。
# 病灶：keeper-v3.sh 的别名表把这些名字当 canonical 直接写入 `event`，而本表缺项 ⇒ 校验器把
# 真实 S4 的 `keeper_exit_observed` 记成 `event_unknown` warning（"两侧事件表不对齐"）。
# 裁决（侵入小者）：**改读侧表**（keeper 侧写入名不动）——keeper 的别名表是 wire 兼容映射，
# 改它要动 keeper 的写入路径与历史语义；扩展本表只影响"名字是否被认识"。
# 名单来源（机器核对，非手抄）：`grep '^audit <name>' keeper-v3.sh` ∪ `R3_ALIAS_EVENTS`
# 左端，去掉经 `r3_event_of` 已映射到 canonical 的旧名（如 restart→keeper_restart）。
# 回归护栏：`--self-test` 的 `keeper.event_table_aligned` 直接从 keeper-v3.sh 重新提取并断言。
# ---------------------------------------------------------------------------
_reg({"process"}, ["keeper_exit_observed", "keeper_lock_unavailable", "keeper_lock_rotated",
                   "keeper_job_stop", "keeper_job_halt", "keeper_job_resume",
                   "keeper_terminal_report_stale", "keeper_terminal_channel_conflict",
                   "keeper_restart_budget", "keeper_restart_budget_exhausted",
                   "spawn_refused"])

# 旧名 → canonical（契约 §2「读侧映射」，**单向**，避免 v1 的反向映射陷阱 L7）
ALIASES_V1 = {
    "restart": "keeper_restart",
    "restart_dry_run": "keeper_restart",
    "adopt": "keeper_adopt",
    "deadline_breach": "deadline_breached",
    "probe_incomplete": "probe_partial_dead",
    "output_dir_recreated": "dir_recreated",
    "trim": "mem_abort",
    "level-up": "mem_level_change",
    "level-down": "mem_level_change",
    "gate-close": "gate_close",
    "gate-open": "gate_open",
    "abort": "mem_abort",
    "abort-degraded": "mem_abort",
    "summary": "mem_level_change",
    "unit": "gate_close",
    "gate-timeout": "gate_close",
    "gate-install": "gate_open",
    "gate-uninstall": "gate_open",
    "error": "mem_level_change",
    "stop": "mem_abort",
    "start": "mem_level_change",
}

# ---------------------------------------------------------------------------
# 场景 → 事件链路断言表（沿用 v1；S6/S6b 的审计窗另见 evidence.json.audit_window_s）
# ---------------------------------------------------------------------------
LINK = {
    "S1": [{"id": "S1.keeper_restart_per_kill", "level": "process",
            "events": ["keeper_restart"],
            "min_count": 3, "windowed": True, "per_injection": "killed",
            "derived": [("log", "reconcile", 1)],
            "why": "每一次 kill -9 都必须留下进程级重启事件（C3/A2）"}],
    "S2": [{"id": "S2.dir_heal", "level": "process",
            "events": ["keeper_restart", "dir_recreated", "output_dir_recreated"],
            "min_count": 1, "windowed": True,
            "derived": [("log", "reconcile", 1)],
            "why": "输出目录被删 ⇒ 目录重建 + 进程重启（F3）"}],
    "S3a": [{"id": "S3a.short_write", "level": "process",
             "events": ["keeper_restart"], "min_count": 1, "windowed": True,
             "derived": [("log_any", [("records_repair", 1), ("reconcile", 1)], 1)],
             "why": "写失败（RLIMIT_FSIZE）⇒ 进程重启 + 撕裂尾部修复"}],
    "S3b": [{"id": "S3b.write_point", "level": "process",
             "events": ["keeper_restart"], "min_count": 1, "windowed": True,
             "derived": [("log_any", [("reconcile", 1), ("records_repair", 1)], 1)],
             "why": "写入点不可用 ⇒ 进程崩溃后由 keeper 恢复"}],
    "S4": [{"id": "S4.request_retry", "level": "request",
            "events": ["retry_scheduled", "error_classified", "cooldown_enter"],
            "min_count": 1, "windowed": True,
            "derived": [("log_any", [("exc", 1), ("http_retry", 1)], 1)],
            "why": "网络断 ⇒ 请求级重试/退避事件（C1）"}],
    "S5": [{"id": "S5.rate_limit_backoff", "level": "request",
            "events": ["rate_limited", "cooldown_enter", "circuit_open", "retry_scheduled"],
            "min_count": 1, "windowed": True,
            "derived": [("log_any", [("http_retry", 1), ("throttle_adjust", 1)], 1)],
            "why": "429 风暴 ⇒ 熔断/降速事件（C1/F11/B3）"}],
    "S6": [{"id": "S6.probe_all_dead", "level": "block",
            # probe_incomplete 是 converge 侧对 probe_partial_dead 的既有细化名（契约 §2 扩展表
            # 允许保留；v1 的 ALIASES 也把它算作同一语义）——写入名不改，读侧按语义等价接受。
            "events": ["probe_all_dead", "probe_partial_dead", "probe_incomplete", "probe_retry",
                       "probe_enumeration_gap"],
            "min_count": 1, "windowed": True,
            "derived": [("log_any", [("probe_retry", 1), ("probe_skip", 1)], 1)],
            "why": "探测全灭必须留痕（C2）"},
           {"id": "S6.repair_round", "level": "block", "events": ["repair_round_enter"],
            "min_count": 1, "windowed": True,
            "derived": [("log_any", [("final_repair_round", 1), ("big_gap", 1)], 1)],
            "why": "探测全灭必须进修复轮兜底（C2：不得假收敛）"}],
    "S6b": [{"id": "S6b.probe_all_dead", "level": "block",
             "events": ["probe_all_dead", "probe_partial_dead", "probe_incomplete",
                        "probe_retry"],
             "min_count": 1, "windowed": True,
             "derived": [("log_any", [("probe_retry", 1), ("probe_skip", 1)], 1)],
             "why": "瞬时探测全灭也必须留痕（C2）；就地重试路径不要求修复轮事件"}],
    "S7a": [{"id": "S7a.progress_recovered", "level": "process",
             "events": ["progress_recovered", "checkpoint_restore", "progress_fallback"],
             "min_count": 1, "windowed": True,
             "derived": [("log", "progress_fallback", 1)],
             "why": "progress 半写 ⇒ 备份槽回退事件（A3）"}],
    "S7b": [{"id": "S7b.rebuild_from_records", "level": "process",
             "events": ["progress_recovered", "checkpoint_restore", "records_rebuild"],
             "min_count": 1, "windowed": True,
             "derived": [("log", "records_rebuild", 1)],
             "why": "progress + bak 双损 ⇒ 从 records 全量反推重建（A3/E1）"}],
    "REF": [],
}

# ---------------------------------------------------------------------------
# derived 通道：scrape.log 的可机器解析行（与 drilllib/evidence.py 同表；
# `_selftest` 里断言两表一致，见 self_test 的 table_parity 检查）
# ---------------------------------------------------------------------------
LOG_PATTERNS = {
    "branch": r"细分 '([^']*)' \(total=(\d+)\) -> (\d+)子块 覆盖(\d+) 缺口(\d+)",
    "leaf": r"叶子 '([^']*)' total=(\d+) 唯一=(\d+)",
    "pre_fetch": r"先抓 '([^']*)': (\d+) 条落盘",
    "probe_retry": r"探测失败 (\d+) 字符，30s 后重试（轮 (\d+)）",
    "probe_skip": r"探测失败 '([^']*)'，跳过",
    "big_gap": r"缺口 (\d+)（大），冷却",
    "final_repair_round": r"=== 终局修复轮：(\d+) 个遗留缺口 ===",
    "throttle_snapshot": r"\[throttle\] mode=(\w+) interval=([\d.]+)s",
    "throttle_adjust": r"\[throttle\] (.+?) \| interval=([\d.]+)->([\d.]+)s",
    "http_retry": r"HTTP (\d{3}) q=",
    "exc": r"EXC (\w+):",
    "progress_fallback": r"progress.json 不可用（缺失/空/损坏）→ 回退 progress.json.bak",
    "records_repair": r"records 修复（最小丢弃）：损坏行 (\d+) 行",
    "records_rebuild": r"records 全量反查重建：(\d+) 行有效记录",
    "reconcile": r"一致性自愈：扫描 (\d+) 行有效记录",
    "tolerance": r"容差|tolerance",
    "complete": r"全部完成",
    "converged": r"收敛：无新任务且无修复进展",
    "conc_item_error": r"\[conc\] 条目异常 '([^']*)': (\w+)",
}


def _read_text(p):
    try:
        return Path(p).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _to_epoch(v):
    """ts → epoch float。epoch 数字原样；ISO 无偏移按**本地时间**（与 v1 同口径）。"""
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        s2 = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s2)
        except ValueError:
            dt = None
            for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                        "%Y-%m-%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(s, fmt)
                    break
                except ValueError:
                    dt = None
            if dt is None:
                return None
        return dt.astimezone().timestamp() if dt.tzinfo is None else dt.timestamp()
    return None


def _iso(ts):
    try:
        return datetime.fromtimestamp(ts).isoformat(timespec="milliseconds")
    except (OverflowError, OSError, ValueError):
        return None


# ===========================================================================
# 输入发现
# ===========================================================================
AUX_SINK_NAMES = {"memguard.jsonl"}
DENY_NAMES = {"records.jsonl", "mock-requests.jsonl", "heartbeat.json", "run.log",
              "progress.json", "convergence-report.json"}


def discover_inputs(scenario_dir):
    """场景目录里的审计输入。

    权威 sink（authoritative）：`audit.jsonl` / `out/audit.jsonl` / `logs/audit.jsonl`
    / 任意 `*audit*.jsonl` 及轮转产物 `audit.jsonl.N`。
    辅助 sink（aux，本契约 §4 之外的既有子系统）：`memguard.jsonl` —— 不产生硬违规。
    """
    d = Path(scenario_dir)
    files, roles = [], {}

    def consider(role, p, authoritative=True):
        if not p.exists() or p.stat().st_size == 0:
            return
        if p.name in DENY_NAMES:
            return
        if str(p) in files:
            return
        files.append(str(p))
        roles[str(p)] = {"role": role, "authoritative": bool(authoritative)}

    for role, p in (("r3_audit", d / "out" / "audit.jsonl"),
                    ("keeper_audit", d / "audit.jsonl"),
                    ("keeper_audit", d / "logs" / "audit.jsonl"),
                    ("r3_audit", d / "logs" / "audit.jsonl")):
        consider(role, p)
    for p in sorted(d.glob("*.jsonl")) + sorted((d / "out").glob("*.jsonl")) \
            + sorted((d / "logs").glob("*.jsonl")):
        if AUDIT_NAME_RE.match(p.name) or "audit" in p.name.lower():
            consider("other_audit", p)
    for p in sorted(d.glob("audit.jsonl.*")) + sorted((d / "out").glob("audit.jsonl.*")):
        consider("audit_rotated", p)
    for p in (d / "out" / "memguard.jsonl", d / "memguard.jsonl"):
        consider("memguard", p, authoritative=False)
    return files, roles


def infer_scenario(scenario_dir):
    m = re.match(r"^(S\d[ab]?|REF)", Path(scenario_dir).name)
    return m.group(1) if m else None


# ===========================================================================
# 记录装载 + v2 行布局校验
# ===========================================================================
def _v2_line_errors(d, allow_legacy_ts=False, strict_unknown=False):
    """v2 行布局校验（契约 §1 + §8）。返回 (issues, rec_fields)。"""
    iss = []
    sch = d.get("schema")
    if sch != SCHEMA_V2:
        iss.append(("legacy_layout", "schema=%r（v2 要求 %r）" % (sch, SCHEMA_V2)))
    ts_epoch = d.get("ts_epoch")
    if not isinstance(ts_epoch, (int, float)) or isinstance(ts_epoch, bool):
        iss.append(("ts_epoch_missing_or_not_number", "ts_epoch=%r" % (ts_epoch,)))
        tse = None
    else:
        tse = float(ts_epoch)
        if tse < TS_MIN:
            iss.append(("ts_implausible", "ts_epoch=%s" % tse))
        elif tse > time.time() + TS_FUTURE_TOL:
            iss.append(("ts_in_future", "ts_epoch=%s" % tse))
    # 遗留键名裁决（契约 §1：v2 行不再写入名为 ts 的键）
    if "ts" in d and not allow_legacy_ts:
        iss.append(("legacy_ts_key", "v2 行不得写遗留键 ts=%r" % (d.get("ts"),)))
    elif "ts" in d:
        iss.append(("legacy_ts_key_tolerated", "ts=%r（--allow-legacy-ts）" % (d.get("ts"),)))
    tsi = d.get("ts_iso")
    if not isinstance(tsi, str) or not tsi.strip():
        iss.append(("ts_iso_missing", "ts_iso=%r" % (tsi,)))
        tsi_epoch = None
    else:
        tsi_epoch = _to_epoch(tsi)
        if tsi_epoch is None:
            iss.append(("ts_iso_unparsable", "ts_iso=%r" % (tsi,)))
        elif tse is not None and abs(tsi_epoch - tse) > 5.0:
            iss.append(("ts_iso_mismatch", "ts_iso 与 ts_epoch 相差 %.1fs" % (tsi_epoch - tse)))
    pid = d.get("pid")
    if not isinstance(pid, int) or isinstance(pid, bool):
        iss.append(("pid_missing_or_not_int", "pid=%r" % (pid,)))
        pid = None
    # 契约 §8.1/§8.2（v2.1）：`writer` 是**强制字段**（写者分组键）。
    #   严格模式：缺失/空/非字符串 ⇒ 硬违规 `writer_missing`（§8.2/§8.5 新写入必带 writer）。
    #   compat 模式：调用方把该 kind 降级为 warning，并按 (pid, "") 回退分组（§8.2）。
    w = d.get("writer")
    if not isinstance(w, str) or not w.strip():
        iss.append(("writer_missing", "writer=%r（契约 v2.1 §8.1：v2 行必须写 writer 分组键；"
                                      "§8.5 新写入一律带 writer）" % (w,)))
        w = ""
    else:
        w = w.strip()
    seq = d.get("seq")
    if not isinstance(seq, int) or isinstance(seq, bool):
        iss.append(("seq_missing_or_not_int", "seq=%r" % (seq,)))
        seq = None
    elif seq < 1:
        iss.append(("seq_not_positive", "seq=%d" % seq))
    lvl = d.get("level")
    if lvl not in LEVELS:
        iss.append(("level_not_in_enum", "level=%r" % (lvl,)))
        lvl = None
    ev = d.get("event")
    if not isinstance(ev, str) or not ev:
        iss.append(("event_missing", "event=%r" % (ev,)))
        ev = None
    elif ev not in EVENT_LEVELS:
        iss.append(("strict_unknown_event" if strict_unknown else "event_unknown",
                    "事件名不在表内：%r" % ev))
    elif lvl is not None and lvl not in EVENT_LEVELS[ev]:
        iss.append(("event_level_mismatch", "event=%s 允许 level=%s 实际=%s"
                    % (ev, sorted(EVENT_LEVELS[ev]), lvl)))
    det = d.get("detail")
    if det is None:
        iss.append(("detail_missing", ""))
        det = {}
    elif not isinstance(det, dict):
        iss.append(("detail_not_object", "type=%s" % type(det).__name__))
        det = {"_value": det}
    rec = {"layout": "v2", "ts": tse, "ts_iso": tsi, "pid": pid, "writer": w, "seq": seq,
           "level": lvl, "event": ev, "detail": det,
           "run": d.get("run") or d.get("run_id"), "r3_fields": True,
           "canonical_event": ev, "raw_event": d.get("event")}
    return iss, rec


def _legacy_line(d):
    """旧布局适配（**仅** `--compat-v1` 模式使用）。返回 (issues, rec)。"""
    iss = []
    sch = str(d.get("schema") or "")
    kind = "keeper" if (sch.startswith("keeper-audit") or "keeper_pid" in d
                        or "r3_event" in d) else "canonical-v1"
    ts = None
    if d.get("ts_epoch") is not None:
        ts = _to_epoch(d.get("ts_epoch"))
    if ts is None:
        ts = _to_epoch(d.get("ts"))
    if ts is None:
        iss.append(("legacy_ts_unparsable", "ts=%r ts_epoch=%r" % (d.get("ts"), d.get("ts_epoch"))))
    tsi = d.get("ts_iso")
    if not tsi and isinstance(d.get("ts"), str):
        tsi = d.get("ts")
    pid = d.get("pid")
    if not isinstance(pid, int) or isinstance(pid, bool):
        pid = d.get("keeper_pid") if isinstance(d.get("keeper_pid"), int) else None
    seq = d.get("seq") if isinstance(d.get("seq"), int) else None
    lvl = d.get("level") if d.get("level") in LEVELS else None
    ev_raw = d.get("event")
    if kind == "keeper" and d.get("r3_event"):
        ev_raw = d.get("r3_event") or d.get("event")
    ev = ALIASES_V1.get(ev_raw, ev_raw) if isinstance(ev_raw, str) else None
    det = d.get("detail") if isinstance(d.get("detail"), dict) else None
    if det is None:
        det = d.get("evidence") if isinstance(d.get("evidence"), dict) else {}
    for f, bad in (("ts", ts is None), ("pid", pid is None), ("seq", seq is None),
                   ("level", lvl is None), ("event", not ev)):
        if bad:
            iss.append(("legacy_field_missing:%s" % f, "%s layout 缺 %s" % (kind, f)))
    rec = {"layout": "legacy", "legacy_kind": kind, "ts": ts, "ts_iso": tsi, "pid": pid,
           "writer": (d.get("writer") if isinstance(d.get("writer"), str) else ""),
           "seq": seq, "level": lvl, "event": ev, "detail": dict(det),
           "run": d.get("run") or d.get("run_id"),
           "r3_fields": False, "canonical_event": ev, "raw_event": ev_raw}
    return iss, rec


def _aux_line(d):
    """辅助 sink（memguard）：只解析、不参与硬判定（可加 --strict-aux 升级）。"""
    iss = []
    ts = _to_epoch(d.get("ts"))
    ev = d.get("event") if isinstance(d.get("event"), str) else None
    rec = {"layout": "aux", "legacy_kind": "memguard", "ts": ts, "ts_iso": d.get("ts"),
           "pid": d.get("pid") if isinstance(d.get("pid"), int) else None,
           "writer": (d.get("writer") if isinstance(d.get("writer"), str) else ""),
           "seq": d.get("seq") if isinstance(d.get("seq"), int) else None,
           "level": "process", "event": ALIASES_V1.get(ev, ev), "detail": {},
           "run": d.get("run_id"), "r3_fields": False,
           "canonical_event": ALIASES_V1.get(ev, ev), "raw_event": ev}
    if "level" in d and d.get("level") not in LEVELS:
        iss.append(("aux_level_type_conflict", "memguard level=%r（整数档位）" % (d.get("level"),)))
    return iss, rec


def load_records(files, roles, opts):
    records, stats = [], {"files": [], "lines": 0, "parsed": 0, "bad_lines": 0,
                          "bad_examples": [], "adapters": {}, "bad_tail": False,
                          "bad_tail_line": None}
    for f in files:
        p = Path(f)
        role = (roles or {}).get(str(p)) or {}
        authoritative = role.get("authoritative", True)
        stats["files"].append(str(p))
        text = _read_text(p)
        raw_lines = text.splitlines()
        for i, line in enumerate(raw_lines, 1):
            line = line.strip()
            if not line:
                continue
            stats["lines"] += 1
            try:
                d = json.loads(line)
            except ValueError as e:
                stats["bad_lines"] += 1
                if i == len(raw_lines) and stats["parsed"] > 0:
                    stats["bad_tail"] = True
                    stats["bad_tail_line"] = i
                if len(stats["bad_examples"]) < 5:
                    stats["bad_examples"].append("%s:%d %s" % (p.name, i, str(e)[:70]))
                continue
            if not isinstance(d, dict):
                stats["bad_lines"] += 1
                continue
            if not authoritative:
                iss, rec = _aux_line(d)
                rec["aux"] = True
            elif d.get("schema") != SCHEMA_V2:
                # 旧布局：compat 模式给 warning 级适配；严格模式一条硬违规（不逐字段刷屏，
                # 但仍适配出字段以便链路断言/报告可读）
                iss, rec = _legacy_line(d)
                if not opts["compat_v1"]:
                    rec["layout"] = "legacy"
                    iss = [("legacy_layout", "schema=%r（v2 要求 %r）"
                            % (d.get("schema"), SCHEMA_V2))]
                    if d.get("ts_epoch") is None:
                        iss.append(("ts_epoch_missing_or_not_number",
                                    "ts_epoch=%r（legacy 行）" % (d.get("ts_epoch"),)))
                    if "ts" in d and not opts["allow_legacy_ts"]:
                        iss.append(("legacy_ts_key", "v2 行不得写遗留键 ts=%r" % (d.get("ts"),)))
            else:
                iss, rec = _v2_line_errors(d, opts["allow_legacy_ts"], opts["strict_unknown"])
            rec["origin"] = "%s:%d" % (p.name, i)
            rec["file"] = str(p)
            rec["line"] = i
            rec["file_authoritative"] = bool(authoritative)
            rec["sink_role"] = role.get("role")
            rec["issues"] = iss
            records.append(rec)
            stats["parsed"] += 1
            key = rec.get("legacy_kind") or rec["layout"]
            stats["adapters"][key] = stats["adapters"].get(key, 0) + 1
    return records, stats


# ===========================================================================
# 逐项校验
# ===========================================================================
class Report:
    """收集硬违规 / 警告 / 降级原因。"""

    def __init__(self):
        self.hard = []
        self.warn = []
        self.degraded = []

    def h(self, kind, detail, origin=None):
        self.hard.append({"kind": kind, "detail": detail, "origin": origin})

    def w(self, kind, detail, origin=None):
        self.warn.append({"kind": kind, "detail": detail, "origin": origin})

    def d(self, reason):
        if reason not in self.degraded:
            self.degraded.append(reason)


def check_facts(records, stats, rep, opts):
    """字段/枚举/单调 + seq 分组的防伪校验。"""
    for r in records:
        for kind, ex in r["issues"]:
            if r.get("aux"):
                # 辅助 sink（memguard）：默认只警告，--strict-aux 才硬判
                (rep.h if opts["strict_aux"] else rep.w)(kind, ex, r["origin"])
            elif r["layout"] != "v2":
                # 旧布局：compat 模式只 warning（契约 §6「不判死」）；
                # 严格模式下一律硬违规（契约 §1「新写入一律 v2」）
                (rep.w if opts["compat_v1"] else rep.h)(kind, ex, r["origin"])
            elif opts["compat_v1"] and kind in COMPAT_SOFT_KINDS:
                # 契约 §8.2：compat 模式下缺 writer ⇒ 回退 (pid,"") 分组且**不判死**
                # （v2.0 历史产物"对照读取"的意义；seq 偏差同源 ⇒ 一并降级并登记降级原因）
                rep.w(kind, ex, r["origin"])
                rep.d("compat_v1_seq_grouping_by_pid")
            elif opt_kind_soft(kind, opts):
                rep.w(kind, ex, r["origin"])
            else:
                rep.h(kind, ex, r["origin"])

    # 撕裂行：仅末行容忍 1 条（契约 §5.6）
    if stats["bad_lines"] and stats["bad_tail"]:
        rep.w("torn_tail", "末行撕裂（行 %s）已容忍 1 条" % stats["bad_tail_line"])
        other = stats["bad_lines"] - 1
    else:
        other = stats["bad_lines"]
    if other > 0:
        rep.h("bad_lines", "%d 条 JSON 解析失败行（非末行撕裂）：%s"
              % (other, stats["bad_examples"][:2]))
    elif stats["bad_tail"] and stats["parsed"] == 0:
        rep.h("bad_lines", "整文件只有撕裂行，无可解析记录")

    # seq：同一文件内按 **(pid, writer)** 分组，从 1 起严格 +1（契约 §3/§5.4 + §8.2/§8.3）
    #   v2.1：分组键加入 writer。理由（KI-1）：同一进程内可有多个写者各持独立计数器
    #   （scrape.py 块级 "converge" × audit.py 请求级 "retry"），只按 pid 分组会误判重复。
    #   compat 回退：缺 writer 的行按 writer=""（契约 §8.2）。
    groups = {}
    for r in records:
        if not r["file_authoritative"] or r["layout"] == "aux":
            continue
        if isinstance(r.get("pid"), int) and isinstance(r.get("seq"), int):
            groups.setdefault((r["file"], r["pid"], r.get("writer") or ""), []).append(r)
    seq_groups = []
    # compat 模式下**分组级** seq 偏差同源降级（契约 §6「不判死」+ §8.2）：缺少 writer 的
    # v2.0 历史产物一旦回退按 pid 单键分组，其 seq 必然重复/缺号 —— 那是被读对象的历史形态，
    # 不是本次运行的缺陷；不降级则"历史对照"永远 exit 1。严格模式不受影响（一律硬违规）。
    def _seq_rep(kind, detail, origin=None):
        if opts["compat_v1"]:
            rep.w(kind, detail, origin)
            rep.d("compat_v1_seq_grouping_by_pid")
        else:
            rep.h(kind, detail, origin)

    for (f, pid, w), rs in sorted(groups.items()):
        rs = sorted(rs, key=lambda x: x["line"])
        seqs = [r["seq"] for r in rs]
        expect = list(range(1, len(seqs) + 1))
        ok = seqs == expect
        g = {"file": Path(f).name, "pid": pid, "writer": w, "count": len(rs), "seq_first": seqs[0],
             "seq_last": seqs[-1], "ok": ok, "continuous": seqs == expect,
             "writer_key": bool(w)}
        seq_groups.append(g)
        if ok:
            continue
        _wtag = "writer=%s" % (w or "<missing>")
        if seqs[0] != 1:
            _seq_rep("seq_group_not_from_1", "file=%s pid=%s %s 首条 seq=%s（要求 1）"
                     % (g["file"], pid, _wtag, seqs[0]), "%s:%d" % (g["file"], rs[0]["line"]))
        seen, prev = set(), None
        for r in rs:
            s = r["seq"]
            if s in seen:
                _seq_rep("seq_duplicate", "file=%s pid=%s %s seq=%s 重复"
                         % (g["file"], pid, _wtag, s), r["origin"])
            elif prev is not None and s < prev:
                _seq_rep("seq_reorder", "file=%s pid=%s %s seq=%s < 前一条 %s"
                         % (g["file"], pid, _wtag, s, prev), r["origin"])
            elif prev is not None and s != prev + 1:
                _seq_rep("seq_gap", "file=%s pid=%s %s seq %s→%s 缺号"
                         % (g["file"], pid, _wtag, prev, s), r["origin"])
            seen.add(s)
            prev = s

    # ts 非降（同 (文件, pid, writer) 组内；软违规）
    tgroups = {}
    for r in records:
        if r["file_authoritative"] and not r.get("aux") and r.get("ts"):
            tgroups.setdefault((r["file"], r.get("pid"), r.get("writer") or ""), []).append(r)
    for _k, rs in tgroups.items():
        last = None
        for r in sorted(rs, key=lambda x: x["line"]):
            if last is not None and r["ts"] < last - 1.0:
                rep.w("ts_not_monotonic", "%s ts=%.3f < 前一条 %.3f"
                      % (r["origin"], r["ts"], last), r["origin"])
            last = r["ts"] if last is None else max(last, r["ts"])

    by_level, by_event, by_layout = {}, {}, {}
    for r in records:
        by_level[r.get("level") or "?/aux"] = by_level.get(r.get("level") or "?/aux", 0) + 1
        by_event[r.get("canonical_event") or "?"] = \
            by_event.get(r.get("canonical_event") or "?", 0) + 1
        by_layout[r["layout"]] = by_layout.get(r["layout"], 0) + 1
    v2 = [r for r in records if r["layout"] == "v2"]
    legacy = [r for r in records if r["layout"] == "legacy"]
    writer_counts = {}
    for r in records:
        if r["layout"] == "v2" and not r.get("aux"):
            k = r.get("writer") or "<missing>"
            writer_counts[k] = writer_counts.get(k, 0) + 1
    return {"seq_groups": seq_groups, "by_level": by_level, "by_event": by_event,
            "by_layout": by_layout, "v2_records": len(v2), "legacy_records": len(legacy),
            "writer_counts": writer_counts,
            "writer_groups": len([g for g in seq_groups if g.get("writer_key")]),
            "writerless_groups": len([g for g in seq_groups if not g.get("writer_key")]),
            "authoritative_records": len([r for r in records if r["file_authoritative"]])}


# 契约 §8.2：`--compat-v1` 下"缺 writer ⇒ 回退 (pid, \"\") 且不判死"所覆盖的偏差种类。
# 缺 writer 的 v2.0 产物一旦回退按 pid 单键分组，其 seq 必然出现重复/缺号（这正是 KI-1 的
# 现象本身）⇒ 必须同源降级，否则 compat 模式对历史产物永远 exit 1，"对照读取"名存实亡。
# 严格模式（默认）下这些种类**全部是硬违规**，不受本表影响。
COMPAT_SOFT_KINDS = frozenset([
    "writer_missing",
    "seq_duplicate",
    "seq_gap",
    "seq_reorder",
    "seq_group_not_from_1",
])


def opt_kind_soft(kind, opts):
    """某些偏差在本契约下是 warning（不判死）——列出理由，避免"静默放宽"。"""
    base = kind.split(":")[0]
    SOFT = {
        "event_unknown": "契约 §2：表外事件记 warning（--strict-unknown 可升级）",
        "event_level_mismatch": "事件↔level 配对漂移（链路断言按 level 过滤，不重复判死）",
        "ts_iso_mismatch": "人读时间与权威 epoch 的偏差（时区形态不强制，契约 §1）",
        "ts_iso_unparsable": "ts_iso 不可解析（人读字段，不影响权威时间）",
        "ts_not_monotonic": "跨进程时钟抖动",
        "detail_fields_missing": "detail 建议字段缺失",
        "legacy_ts_key_tolerated": "--allow-legacy-ts 显式容忍",
        "aux_level_type_conflict": "memguard 档位语义（非三生产者 sink）",
    }
    if base in SOFT:
        return True
    if opts["strict_unknown"] and base == "event_unknown":
        return False
    return False


def collect_log_evidence(scrape_log):
    """scrape.log → {pattern: {count, numeric, samples}}；numeric=含具体读数的行数。"""
    ev = {}
    text = _read_text(scrape_log)
    for name, pat in LOG_PATTERNS.items():
        ms = re.findall(pat, text)
        num = 0
        for m in ms:
            parts = m if isinstance(m, tuple) else (m,)
            if any(re.fullmatch(r"[\d.]+", str(x)) for x in parts if x != ""):
                num += 1
        ev[name] = {"count": len(ms), "numeric": num,
                    "samples": [(m if isinstance(m, str) else list(m)) for m in ms[:3]]}
    # 逐行数字读数（用于 derived 的"具体读数"判据，比逐 pattern 更贴近原文）
    ev["_lines_with_digits"] = len([l for l in text.splitlines() if re.search(r"\d", l)])
    return ev


def progress_view(p):
    try:
        d = json.loads(_read_text(p) or "null")
    except ValueError:
        return None
    if not isinstance(d, dict):
        return None
    rec = d.get("recovery") or []
    actions = {}
    for e in rec:
        if isinstance(e, dict):
            a = str(e.get("action") or "?")
            actions[a] = actions.get(a, 0) + 1
    return {"todo": len(d.get("todo") or []), "done": len(d.get("done") or []),
            "gaps": len(d.get("gaps") or []), "recovery_entries": len(rec),
            "recovery_actions": actions, "torn_requeue": len(d.get("torn_requeue") or []),
            "schema": d.get("schema")}


# ===========================================================================
# 血缘（契约 §5.3）
# ===========================================================================
LINEAGE_GLOBS = (("out/records.jsonl", "records"), ("out/scrape.log", "scrape_log"),
                 ("out/run.log", "run_log"), ("out/progress.json", "progress"),
                 ("out/progress.json.bak", "progress_bak"), ("out/heartbeat.json", "heartbeat"),
                 ("out/memguard.jsonl", "memguard"), ("startup-audit.log", "startup_audit"),
                 ("logs/*", "logs"), ("state/**/*", "state"), ("drill-env/*", "drill_env"),
                 ("work/*", "work"), ("bin/*", "bin"), ("src/*", "src"))
LINEAGE_SKIP_NAMES = {"evidence.json", "verdict.json", "result.json", "sandbox.json",
                      "redteam-results.json", "redteam2-results.json", "mock-stats.json",
                      "mock-ready.json"}


def lineage_sources(scenario_dir):
    d = Path(scenario_dir)
    out = []
    for pat, role in LINEAGE_GLOBS:
        for p in sorted(d.glob(pat)):
            try:
                if not p.is_file():
                    continue
            except OSError:
                continue
            if p.name in LINEAGE_SKIP_NAMES or AUDIT_NAME_RE.match(p.name):
                continue
            if "audit" in p.name.lower():
                continue
            out.append((p, role))
    return out


def check_lineage(scenario_dir, records, rep, opts):
    d = Path(scenario_dir)
    info = {"evidence_present": False, "evidence_error": None, "sources": [],
            "readings": False, "pid_refs": {}, "run_window": None, "assoc": [],
            "post_hoc": []}
    ej_path = d / "evidence.json"
    ej = None
    if ej_path.exists():
        try:
            ej = json.loads(_read_text(ej_path))
            info["evidence_present"] = isinstance(ej, dict)
        except ValueError as e:
            info["evidence_error"] = str(e)[:120]
    if not info["evidence_present"] and not opts["allow_no_evidence"]:
        rep.h("evidence_missing", "场景目录缺可解析的 evidence.json（--allow-no-evidence 可显式降级）")
    elif not info["evidence_present"]:
        rep.d("no_evidence")
        rep.w("evidence_missing_tolerated", "--allow-no-evidence 显式降级（血缘降级为 low）")

    # 真实过程证据
    sources = []
    for p, role in lineage_sources(scenario_dir):
        try:
            st = p.stat()
        except OSError:
            continue
        sources.append({"path": str(p.relative_to(d)) if str(p).startswith(str(d)) else str(p),
                        "role": role, "size": st.st_size, "mtime": round(st.st_mtime, 3)})
    info["sources"] = sources
    has_records = any(s["role"] == "records" and s["size"] > 0 for s in sources)
    log_ev = collect_log_evidence(d / "out" / "scrape.log")
    has_log_readings = log_ev["_lines_with_digits"] > 0
    info["readings"] = bool(has_records or has_log_readings)
    if not sources:
        rep.h("no_real_process_evidence", "场景目录内没有任何真实过程证据文件（records/scrape.log/logs/state…）")
    elif not info["readings"]:
        rep.h("no_process_readings",
              "真实过程证据无具体读数（scrape.log 无含数字行、records.jsonl 缺失或为空）"
              " ⇒ 全手写/空壳目录")

    # run 窗口
    win = None
    if isinstance(ej, dict):
        t0, t1 = _to_epoch(ej.get("started")), _to_epoch(ej.get("ended"))
        if ej.get("t0") is not None and t0 is None:
            t0 = _to_epoch(ej.get("t0"))
        if t0 is not None and t1 is not None and t1 >= t0:
            win = (t0, t1)
    if win is None:
        tss = [r["ts"] for r in records if r.get("ts") and r["file_authoritative"]]
        if tss:
            win = (min(tss) - 60.0, max(tss) + 60.0)
    info["run_window"] = [round(win[0], 3), round(win[1], 3)] if win else None

    # pid 关联：审计 pid 必须能在真实产物里找到
    text_by_role = {}
    for p, role in lineage_sources(scenario_dir):
        if p.suffix.lower() in (".pyc",):
            continue
        try:
            if p.stat().st_size > 4 * 1024 * 1024:
                continue
        except OSError:
            continue
        text_by_role.setdefault(role, []).append(_read_text(p))
    blob = "\n".join("\n".join(v) for v in text_by_role.values())
    pidfile_blob = "\n".join("\n".join(text_by_role.get(r, [])) for r in ("state",))
    STRONG_ROLES = ("run_log", "logs", "startup_audit", "state", "heartbeat", "records")

    def pid_strength(pid):
        if re.search(r"\bpid[=\":\s]*%d\b" % pid, blob) or \
                re.search(r"(?m)^%d$" % pid, pidfile_blob):
            return "strong"
        if re.search(r"\b%d\b" % pid, blob):
            return "weak"
        return None

    ai_groups = {}
    for r in records:
        if r["file_authoritative"] and not r.get("aux") and isinstance(r.get("pid"), int):
            ai_groups.setdefault(r["pid"], []).append(r)
    any_assoc = False
    for pid, rs in sorted(ai_groups.items()):
        cands = {pid}
        for r in rs:
            for k in ("from_pid", "to_pid", "pid"):
                v = (r.get("detail") or {}).get(k)
                if isinstance(v, int):
                    cands.add(v)
        best, src = None, None
        for c in sorted(cands):
            s = pid_strength(c)
            if s == "strong":
                best, src = "strong", c
                break
            if s == "weak" and best is None:
                best, src = "weak", c
        info["assoc"].append({"pid": pid, "strength": best, "via": src,
                              "records": len(rs), "file": Path(rs[0]["file"]).name})
        if best:
            any_assoc = True
        else:
            rep.w("pid_group_unassociated",
                  "pid=%s（%s，%d 条）在真实产物中找不到该 pid 的痕迹"
                  % (pid, Path(rs[0]["file"]).name, len(rs)))
    if ai_groups and not any_assoc:
        rep.h("pid_not_associated",
              "全部 %d 个审计 pid 组都无法与真实过程证据关联 ⇒ 判定为合成/伪造现场"
              % len(ai_groups))
    elif not ai_groups:
        rep.h("no_authoritative_records", "没有任何权威审计记录（v2 布局）可供判定")
    if any(a["strength"] == "weak" for a in info["assoc"]) and any_assoc:
        rep.w("pid_assoc_weak", "部分 pid 只能弱关联（仅数字 token 命中）")

    # 审计 sink 不得在 run 结束后被追加（堵 e1「事后追加一行伪造」）
    if win:
        for f in {r["file"] for r in records if r["file_authoritative"]}:
            try:
                mt = Path(f).stat().st_mtime
            except OSError:
                continue
            if mt > win[1] + POST_SLACK_S:
                rep.h("sink_modified_after_run",
                      "%s mtime=%s 晚于 run 结束 %s 超过 %.0fs ⇒ 存在事后追加/篡改"
                      % (Path(f).name, _iso(mt), _iso(win[1]), POST_SLACK_S))
                info["post_hoc"].append(Path(f).name)
            elif mt < win[0] - POST_SLACK_S:
                rep.w("sink_mtime_before_window",
                      "%s mtime=%s 早于 run 开始（可能是归档副本，非篡改）"
                      % (Path(f).name, _iso(mt)))
    return info, ej, win, log_ev


# ===========================================================================
# 时间窗 + 链路断言（契约 §5.2）
# ===========================================================================
def window_bounds(ej, win):
    """[first_inj - pre, last_inj + deadline]；返回 (lo, hi, note, enforced)。"""
    injs = []
    if isinstance(ej, dict):
        for i in (ej.get("injections") or []):
            if isinstance(i, dict):
                t = i.get("ts")
                if t is None:
                    t = i.get("t")
                e = _to_epoch(t)
                if e is not None:
                    injs.append(e)
    deadline = WINDOW_DEFAULT_S
    if isinstance(ej, dict):
        for k in ("audit_window_s", "deadline"):
            v = ej.get(k)
            if isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0:
                deadline = float(v)
                break
    if not injs:
        return None, None, "no_window", False
    lo = min(injs) - PRE_SLACK_S
    hi = max(injs) + deadline
    return lo, hi, "窗口 [%s, %s]（first_inj=%s deadline=%.0fs）" % (
        _iso(lo), _iso(hi), _iso(min(injs)), deadline), True


def _derived_ok(spec, ctx):
    """derived 通道（契约 §5.5 收紧）：来源在场景目录内 + mtime 在 run 窗口 + 含具体读数。"""
    hits, rejects = [], []
    src_ok, src_note = ctx["derived_source_ok"], ctx["derived_source_note"]
    log = ctx["log_ev"]
    prog = ctx["progress"]
    for item in spec.get("derived") or []:
        kind = item[0]
        if kind == "log":
            _k, pat, n = item
            c = (log.get(pat) or {}).get("numeric", 0)
            if c >= n:
                if src_ok:
                    hits.append({"channel": "log", "key": pat, "count": c,
                                 "source": "out/scrape.log"})
                else:
                    rejects.append({"channel": "log", "key": pat, "count": c,
                                    "reason": src_note})
        elif kind == "log_any":
            _k, opts_, n = item
            got, need = [], 0
            for pat, cnt in opts_:
                c = (log.get(pat) or {}).get("numeric", 0)
                if c >= cnt:
                    need += 1
                    got.append("%s=%d" % (pat, c))
            if need >= n:
                if src_ok:
                    hits.append({"channel": "log_any", "key": ",".join(got), "count": need,
                                 "source": "out/scrape.log"})
                else:
                    rejects.append({"channel": "log_any", "key": ",".join(got),
                                    "count": need, "reason": src_note})
        elif kind == "progress":
            _k, action, n = item
            c = ((prog or {}).get("recovery_actions") or {}).get(action, 0)
            if c >= n:
                if src_ok:
                    hits.append({"channel": "progress", "key": action, "count": c,
                                 "source": "out/progress.json"})
                else:
                    rejects.append({"channel": "progress", "key": action, "count": c,
                                    "reason": src_note})
    return hits, rejects


def check_links(scenario, records, ctx, rep, opts):
    """契约 §5.2 + §5.5：链路断言（audit 通道优先，derived 降级标注）。"""
    specs = LINK.get(scenario, [])
    any_windowed = any(s.get("windowed") for s in specs)
    lo, hi, note, enforced = ctx["window_lo"], ctx["window_hi"], ctx["window_note"], \
        ctx["window_enforced"]
    if any_windowed and not enforced:
        if opts["allow_no_window"]:
            rep.d("no_window")
            rep.w("no_window", "拿不到注入时间线 ⇒ 时间窗未生效（%s）；--allow-no-window 显式降级" % note)
        else:
            rep.h("no_window",
                  "窗口化断言拿不到注入时间线（%s）⇒ 无法验证；缺失本身即异常"
                  "（--allow-no-window 可显式降级为 soft）" % note)
    checks = []
    for spec in specs:
        names = set(spec["events"])
        want, want_src = spec["min_count"], "spec"
        ej = ctx["ej"] or {}
        if spec.get("per_injection"):
            key = spec["per_injection"]
            got_inj = len([i for i in (ej.get("injections") or [])
                           if isinstance(i, dict) and i.get(key)])
            if got_inj:
                want, want_src = got_inj, "injections.%s" % key
        sel = [r for r in records
               if r["file_authoritative"] and not r.get("aux")
               and r["canonical_event"] in names
               and (not r.get("level") or r["level"] == spec["level"])]
        in_window, window_ok = sel, True
        if spec.get("windowed") and enforced:
            in_window = [r for r in sel if r.get("ts") and lo <= r["ts"] <= hi]
            window_ok = len(in_window) >= want
        elif spec.get("windowed"):
            window_ok = "no_window"
        audit_ok = len(in_window) >= want
        hits, rejects = _derived_ok(spec, ctx)
        derived_ok = bool(hits)
        ok = bool(audit_ok or derived_ok)
        if ok and not audit_ok:
            rep.d("derived_only:%s" % spec["id"])
        checks.append({
            "id": spec["id"], "why": spec["why"], "level": spec["level"],
            "events": spec["events"], "min_count": want, "min_count_source": want_src,
            "audit_count": len(in_window), "audit_count_all": len(sel), "audit_ok": audit_ok,
            "window_ok": window_ok if spec.get("windowed") else True,
            "window_enforced": bool(enforced) if spec.get("windowed") else None,
            "window_note": note if spec.get("windowed") else "n/a",
            "derived_ok": derived_ok, "derived_hits": hits, "derived_rejects": rejects,
            "channel": "audit" if audit_ok else ("derived" if derived_ok else "none"),
            "evidence_level": "high" if audit_ok and (not spec.get("windowed") or enforced)
                              else "low",
            "ok": ok,
            "examples": [{"origin": r["origin"], "ts": r.get("ts"), "event": r.get("event"),
                          "layouts": r["layout"]} for r in in_window[:3]],
        })
    return checks


# ===========================================================================
# 主判定
# ===========================================================================
def verify(scenario, files, roles, scenario_dir, opts):
    rep = Report()
    records, stats = load_records(files, roles, opts)
    facts = check_facts(records, stats, rep, opts)

    ctx = {"ej": None, "log_ev": collect_log_evidence(Path(scenario_dir) / "out" / "scrape.log")
           if scenario_dir else {}, "progress": None, "window_lo": None, "window_hi": None,
           "window_note": "no_window", "window_enforced": False,
           "derived_source_ok": False, "derived_source_note": "无场景目录"}
    lineage = None
    win = None
    if scenario_dir:
        lineage, ej, win, log_ev = check_lineage(scenario_dir, records, rep, opts)
        ctx.update({"ej": ej, "log_ev": log_ev})
        ctx["progress"] = progress_view(Path(scenario_dir) / "out" / "progress.json")
        lo, hi, note, enforced = window_bounds(ej, win)
        ctx.update({"window_lo": lo, "window_hi": hi, "window_note": note,
                    "window_enforced": enforced})
        # derived 来源资格：在场景目录内 + mtime 在 run 窗口内
        src = Path(scenario_dir) / "out" / "scrape.log"
        ok, why = False, "scrape.log 不在场景目录内"
        try:
            rp = src.resolve()
            if rp.is_file() and str(rp).startswith(str(Path(scenario_dir).resolve())):
                mt = rp.stat().st_mtime
                if win and (win[0] - DERIVED_MTIME_SLACK_S) <= mt <= (win[1] + DERIVED_MTIME_SLACK_S):
                    if log_ev.get("_lines_with_digits"):
                        ok, why = True, "ok"
                    else:
                        ok, why = False, "scrape.log 不含具体读数（纯模板句不算）"
                else:
                    ok, why = False, "scrape.log mtime 不在 run 窗口内（mtime=%s）" % _iso(mt)
            else:
                ok, why = False, "scrape.log 不在场景目录内"
        except OSError:
            ok, why = False, "scrape.log 不可读"
        ctx.update({"derived_source_ok": ok, "derived_source_note": why})
        if not ok:
            rep.w("derived_source_rejected", "derived 通道来源不合格：%s" % why)

    checks = check_links(scenario, records, ctx, rep, opts) if scenario else None
    fails = [c for c in (checks or []) if not c["ok"]]
    derived_only = bool(checks) and all(c["channel"] == "derived" for c in checks)
    if checks and not fails and all(c["channel"] == "audit" for c in checks):
        channel = "audit"
    elif checks and any(c["channel"] == "audit" for c in checks):
        channel = "mixed"
    elif checks and derived_only:
        channel = "derived"
    else:
        channel = "none"
    window_enforced = bool(ctx["window_enforced"]) or \
        not any(s.get("windowed") for s in LINK.get(scenario or "", []))
    all_audit = bool(checks) and all(c["channel"] == "audit" for c in checks)
    canonical_pass = (not rep.hard) and (not fails) and all_audit and window_enforced \
        and facts["legacy_records"] == 0 and facts["v2_records"] > 0 \
        and "compat_v1_seq_grouping_by_pid" not in rep.degraded
    # 说明（v2.1）：compat 回退按 (pid,"") 分组的对象**不是** v2.1 canonical 产物
    # （缺 writer 行），故与 derived_only / no_window 同规格：降级可见 ⇒ canonical_pass=False。
    if facts["legacy_records"] and opts["compat_v1"]:
        rep.d("compat_v1_layout")
    if derived_only:
        rep.d("derived_only")
    if not window_enforced:
        rep.d("window_not_enforced")

    pass_ = (not rep.hard) and (not fails) and (checks is not None or opts["schema_only"])
    exit_code = 0 if pass_ else 1
    schema = {
        "files": stats["files"], "lines": stats["lines"], "parsed": stats["parsed"],
        "bad_lines": stats["bad_lines"], "bad_examples": stats["bad_examples"],
        "adapters": stats["adapters"], "by_level": facts["by_level"],
        "by_event": facts["by_event"], "by_layout": facts["by_layout"],
        "hard_deviations": len(rep.hard), "soft_deviations": len(rep.warn),
        "deviation_count": len(rep.hard) + len(rep.warn),
        "deviations": _deviations(rep),
        "canonical_records": facts["v2_records"], "r3_sink_present": facts["v2_records"] > 0,
        "v2_records": facts["v2_records"], "legacy_records": facts["legacy_records"],
        "authoritative_records": facts["authoritative_records"],
        "seq_groups": facts["seq_groups"],
        "writer_counts": facts["writer_counts"],
        "writer_groups": facts["writer_groups"],
        "writerless_groups": facts["writerless_groups"],
        "strict_pass": bool(facts["v2_records"]) and not rep.hard,
        "canonical_pass": canonical_pass,
        "r3_fields_pass": canonical_pass,
    }
    out = {
        "verifier": "audit-verify-v2", "contract": "r3-audit-contract-v2",
        "verifier_version": VERIFIER_VERSION, "contract_rev": CONTRACT_REV,
        "mode": "compat-v1" if opts["compat_v1"] else "strict-v2",
        "scenario": scenario, "scenario_dir": str(scenario_dir) if scenario_dir else None,
        "files": files,
        "sinks": [{"path": f, "role": (roles or {}).get(f, {}).get("role"),
                   "authoritative": (roles or {}).get(f, {}).get("authoritative", True)}
                  for f in files],
        "schema": schema,
        "checks": checks, "pass_count": len([c for c in (checks or []) if c["ok"]]),
        "check_count": len(checks or []),
        "missing_events": [{"id": c["id"], "events": c["events"], "level": c["level"]}
                           for c in fails],
        "fail_reasons": ["%s：audit 通道缺 %s（≥%d）且无合格 derived 证据"
                         % (c["id"], "/".join(c["events"]), c["min_count"]) for c in fails],
        "derived_only": derived_only, "audit_only": all_audit,
        "channel": channel, "evidence_level": "high" if canonical_pass else "low",
        "window": {"enforced": window_enforced, "lo": ctx["window_lo"], "hi": ctx["window_hi"],
                   "note": ctx["window_note"]},
        "lineage": lineage,
        "hard_violations": rep.hard, "warnings": rep.warn,
        "degraded_reasons": rep.degraded,
        "pass": bool(pass_), "canonical_pass": canonical_pass, "exit_code": exit_code,
        "ts": round(time.time(), 3),
    }
    return out


def _deviations(rep):
    devs = {}
    for bucket, tag in ((rep.hard, True), (rep.warn, False)):
        for v in bucket:
            e = devs.setdefault(v["kind"], {"kind": v["kind"], "count": 0, "hard": tag,
                                            "examples": []})
            e["count"] += 1
            if len(e["examples"]) < 4:
                e["examples"].append("%s %s" % (v.get("origin") or "", v["detail"]))
    return sorted(devs.values(), key=lambda d: (-d["count"]))


def _group(vios, limit=10):
    """按 kind 聚合违规，返回 [(kind, count, 首个例子)]。"""
    g = {}
    for v in vios:
        e = g.setdefault(v["kind"], {"count": 0, "ex": v["detail"]})
        e["count"] += 1
    return [(k, v["count"], v["ex"]) for k, v in
            sorted(g.items(), key=lambda kv: -kv[1]["count"])][:limit]


def render(out, quiet=False):
    L = []
    s = out["schema"]
    L.append("== audit-verify-v2（契约 r3-audit-contract-v2 rev %s，实现 v%s，模式 %s）：场景 %s =="
             % (out.get("contract_rev"), out.get("verifier_version"),
                out["mode"], out["scenario"] or "(未指定)"))
    L.append("审计输入 %d 个：%s" % (len(out["files"]),
                                    ", ".join(Path(f).name for f in out["files"]) or "无"))
    L.append("解析：行 %s / 成功 %s / 坏行 %s；布局 %s"
             % (s["lines"], s["parsed"], s["bad_lines"], s["by_layout"]))
    L.append("写者（契约 §8）：%s ⇒ writer 分组 %d 个 / 无 writer 分组 %d 个"
             % (s.get("writer_counts") or {}, s.get("writer_groups"),
                s.get("writerless_groups")))
    L.append("字段/枚举/单调：硬违规 %d，软违规 %d；v2 记录 %d；legacy 记录 %d；"
             "canonical_pass=%s；strict_pass=%s"
             % (s["hard_deviations"], s["soft_deviations"], s["v2_records"],
                s["legacy_records"], s["canonical_pass"], s["strict_pass"]))
    for d in s["deviations"][:8]:
        L.append("    - [%s] %s × %d  例：%s" % ("HARD" if d["hard"] else "soft", d["kind"],
                                                 d["count"], d["examples"][:1]))
    for kind, cnt, ex in _group(out["hard_violations"])[:10]:
        L.append("    ✗ HARD %-28s ×%d  %s" % (kind, cnt, ex[:140]))
    for kind, cnt, ex in _group(out["warnings"])[:10]:
        L.append("    · warn %-28s ×%d  %s" % (kind, cnt, ex[:140]))
    gs = s["seq_groups"]
    if gs:
        L.append("seq 分组（同文件内按 (pid, writer)，要求 1..N 连续）：%s"
                 % "; ".join("%s pid=%s writer=%s %d 条 %s"
                             % (g["file"], g["pid"], g.get("writer") or "<missing>",
                                g["count"], "OK" if g["ok"] else "违规")
                             for g in gs[:6]))
    if out["lineage"]:
        L.append("血缘：evidence=%s；真实证据 %d 件；读数=%s；run 窗口=%s；pid 关联=%s"
                 % (out["lineage"]["evidence_present"], len(out["lineage"]["sources"]),
                    out["lineage"]["readings"], out["lineage"]["run_window"],
                    [(a["pid"], a["strength"]) for a in out["lineage"]["assoc"]][:6]))
    L.append("时间窗：enforced=%s %s" % (out["window"]["enforced"], out["window"]["note"]))
    if out["checks"] is None:
        L.append("链路断言：未指定场景 ⇒ 跳过（--schema-only 语义）")
    else:
        L.append("链路断言：%d/%d 通过（channel=%s，evidence_level=%s）"
                 % (out["pass_count"], out["check_count"], out["channel"],
                    out["evidence_level"]))
        for c in out["checks"]:
            L.append("    [%s] %-24s level=%-7s 事件=%s 需≥%d；audit=%d%s；channel=%s；"
                     "evidence_level=%s%s"
                     % ("PASS" if c["ok"] else "FAIL", c["id"], c["level"],
                        "/".join(c["events"]), c["min_count"], c["audit_count"],
                        "" if c["window_ok"] is True else "(window=%s)" % c["window_ok"],
                        c["channel"], c["evidence_level"],
                        ("；derived=%s" % c["derived_hits"]) if c["derived_hits"] else ""))
            if c["derived_rejects"]:
                L.append("        ⚠ derived 被拒：%s" % c["derived_rejects"])
            if c["window_ok"] is not True:
                L.append("        ⚠ 时间窗：%s" % c["window_note"])
    if out["degraded_reasons"]:
        L.append("降级标注：%s" % ", ".join(out["degraded_reasons"]))
    L.append("判定：%s（exit %d；canonical_pass=%s）"
             % ("PASS" if out["pass"] else "FAIL", out["exit_code"], out["canonical_pass"]))
    text = "\n".join(L)
    if not quiet:
        print(text)
    return text


def _env_error(msg, scenario=None, json_out=None):
    out = {"verifier": "audit-verify-v2", "scenario": scenario, "pass": False,
           "canonical_pass": False, "exit_code": 2,
           "fail_reasons": [msg], "hard_violations": [{"kind": "usage_or_env", "detail": msg}],
           "warnings": [], "degraded_reasons": ["usage_or_env"], "checks": None,
           "files": [], "sinks": [], "lineage": None,
           "window": {"enforced": False, "lo": None, "hi": None, "note": msg},
           "schema": {"lines": 0, "parsed": 0, "bad_lines": 0, "hard_deviations": 0,
                      "soft_deviations": 0, "canonical_records": 0, "v2_records": 0,
                      "legacy_records": 0, "r3_sink_present": False, "strict_pass": False,
                      "canonical_pass": False, "deviations": [], "adapters": {},
                      "by_event": {}, "by_level": {}, "by_layout": {}, "files": [],
                      "seq_groups": [], "authoritative_records": 0}}
    if json_out:
        Path(json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(json_out).write_text(json.dumps(out, ensure_ascii=False, indent=1),
                                  encoding="utf-8")
    print("判定：FAIL(exit 2) —— %s" % msg)
    return 2


def _fail_closed(kind, msg, scenario, json_out, quiet=False):
    out = {"verifier": "audit-verify-v2", "scenario": scenario, "pass": False,
           "canonical_pass": False, "exit_code": 1, "fail_reasons": [msg],
           "hard_violations": [{"kind": kind, "detail": msg, "origin": None}],
           "warnings": [], "degraded_reasons": ["fail_closed"], "checks": None,
           "files": [], "sinks": [], "lineage": None,
           "window": {"enforced": False, "lo": None, "hi": None, "note": msg},
           "schema": {"lines": 0, "parsed": 0, "bad_lines": 0, "hard_deviations": 1,
                      "soft_deviations": 0, "canonical_records": 0, "v2_records": 0,
                      "legacy_records": 0, "r3_sink_present": False, "strict_pass": False,
                      "canonical_pass": False, "deviations": [{"kind": kind, "count": 1,
                                                               "hard": True, "examples": [msg]}],
                      "adapters": {}, "by_event": {}, "by_level": {}, "by_layout": {},
                      "files": [], "seq_groups": [], "authoritative_records": 0}}
    if json_out:
        Path(json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(json_out).write_text(json.dumps(out, ensure_ascii=False, indent=1),
                                  encoding="utf-8")
    if not quiet:
        print("== audit-verify-v2：fail-closed ==")
        print("    ✗ HARD %-24s %s" % (kind, msg))
        print("判定：FAIL（exit 1；canonical_pass=False）")
    return 1


# ===========================================================================
# CLI 流程（main 与 self_test 共用，保证 fail-closed 分支被真正覆盖）
# ===========================================================================
def run_cli(argv, quiet=True):
    """跑完整 CLI 流程，返回 (exit_code, out_dict_or_None)。"""
    import contextlib
    import io
    fd, path = tempfile.mkstemp(suffix=".json", prefix="av2-cli-")
    os.close(fd)
    os.unlink(path)
    args = list(argv)
    if "--json" not in args:
        args += ["--json", path]
    if quiet and "--quiet" not in args:
        args += ["--quiet"]
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = main(args)
    out = None
    if os.path.exists(path):
        try:
            out = json.loads(Path(path).read_text(encoding="utf-8"))
        except ValueError:
            out = None
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass
    return rc, out


# ===========================================================================
# 自检
# ===========================================================================
def self_test(tmp_root=None):
    """契约 §7 验收矩阵的自检。

    正例由**真实子进程**写出（真 pid / 真 seq / 真 ts），不是手写 JSON——
    这与契约 §5.3「全手写文件 ⇒ FAIL」的血缘要求一致。
    """
    import shutil
    import subprocess
    import tempfile

    root = Path(tmp_root or tempfile.mkdtemp(prefix="av2-selftest-"))
    if root.exists() and tmp_root:
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    checks = []

    def chk(name, cond, extra=""):
        checks.append({"name": name, "ok": bool(cond), "extra": str(extra)[:200]})
        print("  [%s] %s %s" % ("PASS" if cond else "FAIL", name, str(extra)[:160]))

    def run(args):
        """走完整 CLI 流程（含 fail-closed / usage 分支），返回 (exit_code, out)。"""
        return run_cli(args, quiet=True)

    # ---------- 正例：真实子进程写 v2 sink + 真实 log/records ----------
    d = root / "S4-real"
    (d / "out").mkdir(parents=True)
    gen = r'''
import json, os, sys, time
out = sys.argv[1]; pid = os.getpid()
ts = time.time()
recs = []
def emit(seq, level, event, detail):
    r = {"schema": "r3-audit-v2", "ts_epoch": round(time.time(), 6),
         "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()) + ".000",
         "pid": pid, "writer": "retry", "seq": seq, "level": level, "event": event,
         "detail": detail, "run": "selftest-real"}
    recs.append(r)
emit(1, "request", "error_classified", {"class": "conn_reset", "action": "retry"})
emit(2, "request", "retry_scheduled", {"attempt": 1, "wait_s": 2.0})
emit(3, "request", "retry_scheduled", {"attempt": 2, "wait_s": 4.0})
with open(os.path.join(out, "audit.jsonl"), "w", encoding="utf-8") as f:
    for r in recs:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
with open(os.path.join(out, "scrape.log"), "w", encoding="utf-8") as f:
    f.write("[t] EXC ConnResetError: connection reset\n")
    f.write("[t] HTTP 503 q=holding_call_number,begins_with,A\n")
    f.write("[t] [throttle] backoff | interval=0.20->0.40s\n")
with open(os.path.join(out, "records.jsonl"), "w", encoding="utf-8") as f:
    f.write(json.dumps({"mms": "99000000001", "prefix": "A"}) + "\n")
with open(os.path.join(out, "run.log"), "w", encoding="utf-8") as f:
    f.write("SHIM-READY pid=%d scrape=/tmp/x\n" % pid)
print(json.dumps({"pid": pid}))
'''
    p = subprocess.run([sys.executable, "-c", gen, str(d / "out")],
                       capture_output=True, text=True, timeout=60)
    pid = json.loads(p.stdout.strip().splitlines()[-1])["pid"]
    inj = time.time()
    (d / "evidence.json").write_text(json.dumps(
        {"scenario": "S4", "started": _iso(inj - 2), "ended": _iso(inj + 6),
         "deadline": 180, "injections": [{"ts": inj, "ts_iso": _iso(inj)}]},
        ensure_ascii=False), encoding="utf-8")
    rc_good, r_good = run(["--scenario", "S4", "--scenario-dir", str(d)])
    chk("good.exit0", rc_good == 0 and r_good["exit_code"] == 0, r_good["fail_reasons"])
    chk("good.canonical_pass", r_good["canonical_pass"] is True, r_good.get("degraded_reasons"))
    chk("good.window_enforced", r_good["window"]["enforced"] is True)
    chk("good.pid_associated",
        any(a["strength"] for a in (r_good["lineage"] or {}).get("assoc", [])))
    chk("good.seq_continuous",
        all(g["ok"] for g in r_good["schema"]["seq_groups"]))

    # ---------- 反例 1：未知 scenario ----------
    rc, r = run(["--scenario", "S99", "--scenario-dir", str(d)])
    chk("unknown_scenario.exit1", rc == 1 and r["exit_code"] == 1
        and any(h["kind"] == "scenario_unknown" for h in r["hard_violations"]))

    # ---------- 反例 2：缺 scenario ----------
    rc, r = run(["--scenario-dir", str(root)])   # 目录名不可推断场景 ⇒ fail-closed
    chk("missing_scenario.exit1", rc == 1 and r["exit_code"] == 1
        and any(h["kind"] == "scenario_missing" for h in r["hard_violations"]))

    # ---------- 反例 3：缺目录 ----------
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(root / "nope")])
    chk("missing_dir.exit1", rc == 1 and r["exit_code"] == 1
        and any(h["kind"] == "scenario_dir_missing" for h in r["hard_violations"]))

    # ---------- 反例 4：手写 9 行（无 evidence/无 log/无 records） ----------
    d4 = root / "forged9"
    (d4 / "out").mkdir(parents=True)
    now = time.time()
    lines = []
    for i in range(3):
        lines.append(json.dumps({"ts": now - 90000 + i, "pid": 101 + i, "seq": 1,
                                 "level": "process", "event": "keeper_restart",
                                 "detail": {}, "run": "made-up"}))
    for i in range(6):
        lines.append(json.dumps({"ts": now - 80000 + i, "pid": 101, "seq": 2 + i,
                                 "level": "process", "event": "heartbeat", "detail": {}}))
    (d4 / "out" / "audit.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    rc, r = run(["--scenario", "S1", "--scenario-dir", str(d4)])
    chk("forged9.exit1", rc == 1 and r["exit_code"] == 1,
        [h["kind"] for h in r["hard_violations"]][:4])
    chk("forged9.evidence_missing",
        any(h["kind"] == "evidence_missing" for h in r["hard_violations"]))

    # ---------- 反例 5：清空 injections（e4） ----------
    d5 = root / "S4-nowindow"
    shutil.copytree(d, d5)
    ej = json.loads((d5 / "evidence.json").read_text(encoding="utf-8"))
    ej["injections"] = []
    (d5 / "evidence.json").write_text(json.dumps(ej, ensure_ascii=False), encoding="utf-8")
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(d5)])
    chk("nowindow.exit1", rc == 1 and r["exit_code"] == 1
        and any(h["kind"] == "no_window" for h in r["hard_violations"]))
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(d5), "--allow-no-window"])
    chk("nowindow.degraded_visible",
        r["canonical_pass"] is False and "no_window" in r["degraded_reasons"]
        and r["window"]["enforced"] is False)

    # ---------- 反例 6：换 pid 插一行（e5） ----------
    d6 = root / "S4-newpid"
    shutil.copytree(d, d6)
    with open(d6 / "out" / "audit.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps({"schema": "r3-audit-v2", "ts_epoch": time.time(),
                            "ts_iso": _iso(time.time()), "pid": 515151, "writer": "retry",
                            "seq": 7, "level": "request", "event": "retry_scheduled",
                            "detail": {}, "run": "x"}) + "\n")
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(d6)])
    chk("newpid.exit1", rc == 1 and r["exit_code"] == 1
        and any(h["kind"].startswith("seq_group_not_from_1") for h in r["hard_violations"]))

    # ---------- 反例 7：seq 缺号 ----------
    d7 = root / "S4-seqgap"
    shutil.copytree(d, d7)
    p7 = d7 / "out" / "audit.jsonl"
    raw = [json.loads(x) for x in p7.read_text(encoding="utf-8").splitlines() if x.strip()]
    del raw[1]
    p7.write_text("".join(json.dumps(x, ensure_ascii=False) + "\n" for x in raw),
                  encoding="utf-8")
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(d7)])
    chk("seqgap.exit1", rc == 1 and r["exit_code"] == 1
        and any(h["kind"] == "seq_gap" for h in r["hard_violations"]))

    # ---------- 反例 8：旧布局（严格模式 FAIL / compat 不判死） ----------
    d8 = root / "S4-legacy"
    shutil.copytree(d, d8)
    p8 = d8 / "out" / "audit.jsonl"
    rws = [json.loads(x) for x in p8.read_text(encoding="utf-8").splitlines() if x.strip()]
    for x in rws:
        x["ts"] = x.pop("ts_epoch")
        x["schema"] = "r3-audit-v1"
    p8.write_text("".join(json.dumps(x, ensure_ascii=False) + "\n" for x in rws),
                  encoding="utf-8")
    rc_s, r_strict = run(["--scenario", "S4", "--scenario-dir", str(d8)])
    rc_c, r_compat = run(["--scenario", "S4", "--scenario-dir", str(d8), "--compat-v1"])
    chk("legacy.strict_fail", rc_s == 1 and r_strict["exit_code"] == 1
        and any(h["kind"] == "legacy_layout" for h in r_strict["hard_violations"]))
    chk("legacy.compat_readable", rc_c == 0 and r_compat["exit_code"] == 0
        and r_compat["canonical_pass"] is False
        and "compat_v1_layout" in r_compat["degraded_reasons"])

    # ---------- 反例 9：末行撕裂（容忍）与中间坏行（硬违规） ----------
    d9 = root / "S4-torn"
    shutil.copytree(d, d9)
    p9 = d9 / "out" / "audit.jsonl"
    p9.write_bytes(p9.read_bytes() + b'{"schema":"r3-audit-v2","ts_epoch":1')
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(d9)])
    chk("torn_tail.exit0", rc == 0 and r["exit_code"] == 0
        and any(w["kind"] == "torn_tail" for w in r["warnings"]), r["fail_reasons"])
    d10 = root / "S4-midbad"
    shutil.copytree(d, d10)
    p10 = d10 / "out" / "audit.jsonl"
    body = p10.read_text(encoding="utf-8").splitlines()
    body.insert(1, "{{{ not json")
    p10.write_text("\n".join(body) + "\n", encoding="utf-8")
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(d10)])
    chk("mid_bad_line.exit1", rc == 1 and r["exit_code"] == 1
        and any(h["kind"] == "bad_lines" for h in r["hard_violations"]))

    # ---------- 反例 10：derived-only 不再顶替 canonical ----------
    d11 = root / "S4-derivedonly"
    shutil.copytree(d, d11)
    (d11 / "out" / "audit.jsonl").write_text(
        json.dumps({"schema": "r3-audit-v2", "ts_epoch": inj, "ts_iso": _iso(inj),
                    "pid": pid, "writer": "retry", "seq": 1, "level": "process",
                    "event": "run_start",
                    "detail": {}, "run": "selftest-real"}) + "\n", encoding="utf-8")
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(d11)])
    chk("derived_only.not_canonical", r["canonical_pass"] is False
        and r["checks"] and r["checks"][0]["channel"] == "derived"
        and r["evidence_level"] == "low")

    # ---------- 反例 11：事后追加（sink mtime 晚于 run 窗口） ----------
    d12 = root / "S4-posthoc"
    shutil.copytree(d, d12)
    ej = json.loads((d12 / "evidence.json").read_text(encoding="utf-8"))
    ej["started"] = _iso(inj - 100000)
    ej["ended"] = _iso(inj - 90000)
    ej["injections"] = [{"ts": inj - 99000}]
    (d12 / "evidence.json").write_text(json.dumps(ej, ensure_ascii=False), encoding="utf-8")
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(d12)])
    chk("posthoc.exit1", any(h["kind"] == "sink_modified_after_run" for h in r["hard_violations"]),
        [h["kind"] for h in r["hard_violations"]][:4])

    # ---------- 反例 12：解析失败行占比过高 / 空目录 ----------
    d13 = root / "S4-garbage"
    (d13 / "out").mkdir(parents=True)
    (d13 / "out" / "audit.jsonl").write_text("###garbage###\n" * 50, encoding="utf-8")
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(d13)])
    chk("garbage.exit1", rc == 1 and r["exit_code"] == 1)
    d14 = root / "S4-empty"
    (d14 / "out").mkdir(parents=True)
    (d14 / "out" / "audit.jsonl").write_bytes(b"")
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(d14)])
    chk("empty_input.exit2", rc == 2 and r["exit_code"] == 2)

    # ================= 契约 §8（v2.1）：writer 分组键 / KI-1 回归 =================
    # 夹具①：**同 pid 双写者合流**（模拟真实 S4：scrape.py 的 converge 写者 × audit.py 的
    # retry 写者共用 pid 与同一 OUT/audit.jsonl，各持独立计数器）⇒ 必须 **PASS**。
    merged_rows = []
    for i in (1, 2, 3):
        merged_rows.append({"schema": "r3-audit-v2", "ts_epoch": inj - 2 + i * 0.1,
                            "ts_iso": _iso(inj - 2 + i * 0.1), "pid": pid, "writer": "retry",
                            "seq": i, "level": "request",
                            "event": ["error_classified", "retry_scheduled",
                                      "retry_scheduled"][i - 1],
                            "detail": {"attempt": i, "wait_s": float(i)}, "run": "selftest-real"})
        merged_rows.append({"schema": "r3-audit-v2", "ts_epoch": inj - 2 + i * 0.1 + 0.05,
                            "ts_iso": _iso(inj - 2 + i * 0.1 + 0.05), "pid": pid,
                            "writer": "converge", "seq": i, "level": "block",
                            "event": ["run_start", "probe_all_dead",
                                      "convergence_decision"][i - 1],
                            "detail": {"round": i}, "run": "selftest-real"})
    d_dual = root / "S4-dualwriter"
    shutil.copytree(d, d_dual)
    p_dual = d_dual / "out" / "audit.jsonl"
    p_dual.write_text("".join(json.dumps(x, ensure_ascii=False) + "\n" for x in merged_rows),
                      encoding="utf-8")
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(d_dual)])
    groups = (r.get("schema") or {}).get("seq_groups") or []
    chk("dualwriter.same_pid_pass", rc == 0 and r["exit_code"] == 0
        and not [h for h in r["hard_violations"] if h["kind"].startswith("seq_")],
        "rc=%s hard=%s groups=%s" % (rc, [h["kind"] for h in r["hard_violations"]][:4],
                                     [(g["pid"], g["writer"], g["count"], g["ok"]) for g in groups]))
    chk("dualwriter.two_groups_both_continuous",
        len(groups) == 2 and all(g["ok"] for g in groups)
        and sorted(g["writer"] for g in groups) == ["converge", "retry"]
        and {g["pid"] for g in groups} == {pid},
        [(g["pid"], g["writer"], g["seq_first"], g["seq_last"], g["ok"]) for g in groups])
    chk("dualwriter.writer_stats",
        (r["schema"].get("writer_groups") == 2 and r["schema"].get("writerless_groups") == 0
         and (r["schema"].get("writer_counts") or {}) == {"retry": 3, "converge": 3}),
        json.dumps({"counts": r["schema"].get("writer_counts"),
                    "groups": r["schema"].get("writer_groups")}, ensure_ascii=False))
    # 同字节、仅去掉 writer ⇒ compat 回退按 (pid,"") 分组 ⇒ KI-1 签名（seq_duplicate）复现
    d_dual_nw = root / "S4-dualwriter-nowriter"
    shutil.copytree(d, d_dual_nw)
    (d_dual_nw / "out" / "audit.jsonl").write_text(
        "".join(json.dumps({k: v for k, v in x.items() if k != "writer"}, ensure_ascii=False)
                + "\n" for x in merged_rows), encoding="utf-8")
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(d_dual_nw), "--compat-v1"])
    dups = [w for w in r["warnings"] if w["kind"] == "seq_duplicate"]
    chk("dualwriter.pid_only_key_would_dup",
        len(dups) >= 3 and (r["schema"].get("writerless_groups") == 1),
        "同一份字节去掉 writer ⇒ 按 pid 单键分组复现 %d 条 seq_duplicate（= KI-1 签名）"
        % len(dups))

    # 夹具②：**跨组 seq 交错**（1,1,2,2,3,3 交替），分组必须正确、不得误判
    d_il = root / "S4-interleave"
    shutil.copytree(d, d_il)
    inter = []
    for i in range(6):
        w = "retry" if i % 2 == 0 else "converge"
        n = i // 2 + 1
        inter.append({"schema": "r3-audit-v2", "ts_epoch": inj - 1 + i * 0.01,
                      "ts_iso": _iso(inj - 1 + i * 0.01), "pid": pid, "writer": w, "seq": n,
                      "level": "request" if w == "retry" else "block",
                      "event": ("retry_scheduled" if w == "retry" else "probe_retry"),
                      "detail": {"i": i}, "run": "selftest-real"})
    (d_il / "out" / "audit.jsonl").write_text(
        "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in inter), encoding="utf-8")
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(d_il)])
    groups = (r.get("schema") or {}).get("seq_groups") or []
    chk("interleave.no_false_positive",
        rc == 0 and r["exit_code"] == 0 and len(groups) == 2 and all(g["ok"] for g in groups)
        and not [h for h in r["hard_violations"] if h["kind"].startswith("seq_")],
        "交错行序 %s ⇒ 分组 %s"
        % ([x["writer"][0] + str(x["seq"]) for x in inter],
           [(g["writer"], g["seq_first"], g["seq_last"], g["ok"]) for g in groups]))

    # ---------- 反例 13：严格模式缺 writer ⇒ 硬违规 writer_missing ----------
    d_wm = root / "S4-writermissing"
    shutil.copytree(d, d_wm)
    p_wm = d_wm / "out" / "audit.jsonl"
    rows_wm = [json.loads(x) for x in p_wm.read_text(encoding="utf-8").splitlines() if x.strip()]
    p_wm.write_text("".join(json.dumps({k: v for k, v in x.items() if k != "writer"},
                                       ensure_ascii=False) + "\n" for x in rows_wm),
                    encoding="utf-8")
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(d_wm)])
    chk("writer_missing.strict_fail", rc == 1 and r["exit_code"] == 1
        and any(h["kind"] == "writer_missing" for h in r["hard_violations"]),
        [h["kind"] for h in r["hard_violations"]][:4])
    chk("writer_missing.strict_pass_false", r["schema"]["strict_pass"] is False
        and r["schema"].get("writerless_groups") == 1,
        "writerless_groups=%s" % r["schema"].get("writerless_groups"))
    # 契约 §8.2：compat 模式回退 (pid,"") 分组且**不判死**
    rc, r = run(["--scenario", "S4", "--scenario-dir", str(d_wm), "--compat-v1"])
    chk("writer_missing.compat_downgrade", rc == 0 and r["exit_code"] == 0
        and any(w["kind"] == "writer_missing" for w in r["warnings"])
        and "compat_v1_seq_grouping_by_pid" in r["degraded_reasons"]
        and r["canonical_pass"] is False,
        "rc=%s hard=%s warnings=%s degraded=%s" % (rc, [h["kind"] for h in r["hard_violations"]][:4],
                                                   sorted({w["kind"] for w in r["warnings"]}),
                                                   r["degraded_reasons"]))
    chk("writer_missing.compat_group_key", (r["schema"].get("seq_groups") or [{}])[0].get("writer")
        == "" and r["schema"].get("writerless_groups") == 1,
        json.dumps(r["schema"].get("seq_groups"), ensure_ascii=False)[:160])

    # ---------- R-9：keeper 写入名与校验器事件表对齐（两侧一张表） ----------
    keeper = HERE / "keeper-v3.sh"
    if keeper.exists():
        ksrc = keeper.read_text(encoding="utf-8", errors="replace")
        m_alias = re.search(r'R3_ALIAS_EVENTS="([^"]*)"', ksrc)
        alias_canon = [p.split(":")[0] for p in (m_alias.group(1).split() if m_alias else [])]
        calls = sorted(set(re.findall(r"(?:^|[\s;(&|])audit\s+([a-z_]+)", ksrc, re.M)))
        # 写入名 = r3_event_of(调用名)：别名表右端命中 ⇒ 取左端 canonical，否则原样
        alias_map = {}
        for p in (m_alias.group(1).split() if m_alias else []):
            left, _, right = p.partition(":")
            alias_map[right] = left
        written = sorted({alias_map.get(c, c) for c in calls} | set(alias_canon))
        missing = [e for e in written
                   if e not in EVENT_LEVELS and e not in ("restart", "adopt")]
        chk("keeper.event_table_aligned", not missing,
            "keeper 写入名 %d 个，表外 %d 个：%s" % (len(written), len(missing), missing))
    else:
        print("  [SKIP] keeper.event_table_aligned：%s 不在本目录（未校验两侧事件表对齐）"
              % keeper.name)

    # ---------- derived 模式表与 drilllib 一致 ----------
    try:
        sys.path.insert(0, str(HERE))
        from drilllib import evidence as _evd
        same = _evd.LOG_PATTERNS == LOG_PATTERNS
    except Exception as e:                                    # noqa: BLE001
        same = None
        print("    （drilllib 不可用，跳过表一致性：%s）" % e)
    if same is not None:
        chk("derived_table_parity_drilllib", same)

    ok = all(c["ok"] for c in checks)
    print("== audit-verify-v2 自检：%s（%d/%d）=="
          % ("PASS" if ok else "FAIL", sum(1 for c in checks if c["ok"]), len(checks)))
    return 0 if ok else 1


def _parse_verify_args(args):
    """把 CLI 参数解析成 verify() 的入参（供 self_test 直接复用主流程）。"""
    ap = _build_argparser()
    ns = ap.parse_args(args)
    opts = _opts_from(ns)
    files, roles = _resolve_inputs(ns)
    return ns.scenario, files, roles, ns.scenario_dir, opts


def _build_argparser():
    ap = argparse.ArgumentParser(description="R3-P4 统一审计校验器 v2（契约 v2）")
    ap.add_argument("--scenario", default=None, help="S1/S2/S3a/S3b/S4/S5/S6/S6b/S7a/S7b/REF")
    ap.add_argument("--scenario-dir", default=None)
    ap.add_argument("--files", nargs="*", default=None, help="显式指定审计 jsonl")
    ap.add_argument("--json", dest="json_out", default=None, help="把机读结果写到该路径")
    ap.add_argument("--json-stdout", action="store_true", help="把机读结果打到 stdout")
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--compat-v1", action="store_true",
                    help="读旧布局（r3-audit-v1 / keeper-v2）对照：只 warning，不判死")
    ap.add_argument("--allow-no-evidence", action="store_true",
                    help="显式允许缺 evidence.json（血缘降级为 low）")
    ap.add_argument("--allow-no-window", action="store_true",
                    help="显式允许拿不到注入时间线（window 降级为 soft，绝不静默）")
    ap.add_argument("--allow-legacy-ts", action="store_true",
                    help="容忍 v2 行里的遗留键 ts（默认硬违规）")
    ap.add_argument("--strict-unknown", action="store_true", help="表外事件升级为硬违规")
    ap.add_argument("--strict-aux", action="store_true", help="辅助 sink（memguard）一并硬判")
    ap.add_argument("--schema-only", action="store_true",
                    help="只做字段/seq/血缘校验，不要求 --scenario（链路断言跳过）")
    ap.add_argument("--self-test", action="store_true")
    return ap


def _opts_from(ns):
    return {"compat_v1": ns.compat_v1, "allow_no_evidence": ns.allow_no_evidence,
            "allow_no_window": ns.allow_no_window, "allow_legacy_ts": ns.allow_legacy_ts,
            "strict_unknown": ns.strict_unknown, "strict_aux": ns.strict_aux,
            "schema_only": ns.schema_only}


def _resolve_inputs(ns):
    files, roles = list(ns.files or []), {}
    if ns.scenario_dir:
        d = Path(ns.scenario_dir)
        if d.exists():
            auto, roles = discover_inputs(d)
            for f in auto:
                if f not in files:
                    files.append(f)
                roles.setdefault(f, {"role": "explicit", "authoritative": True})
    for f in files:
        roles.setdefault(f, {"role": "explicit", "authoritative": True})
    return files, roles


def main(argv=None):
    ap = _build_argparser()
    args = ap.parse_args(argv)
    if args.self_test:
        return self_test()
    opts = _opts_from(args)
    json_out = args.json_out

    scenario = args.scenario
    if args.scenario_dir:
        d = Path(args.scenario_dir)
        if not d.exists() or not d.is_dir():
            return _fail_closed("scenario_dir_missing",
                                "场景目录不存在：%s（fail-closed，契约 §5.1）" % d,
                                scenario, json_out, args.quiet)
        if not scenario:
            scenario = infer_scenario(d)
    if not scenario:
        if opts["schema_only"]:
            pass
        else:
            return _fail_closed("scenario_missing",
                                "未指定 --scenario 且无法从目录名推断；"
                                "契约 §5.1 要求 fail-closed（只做 schema 校验请显式 --schema-only）",
                                scenario, json_out, args.quiet)
    elif scenario not in LINK:
        return _fail_closed("scenario_unknown",
                            "未知 scenario=%r（已知：%s）" % (scenario, ", ".join(sorted(LINK))),
                            scenario, json_out, args.quiet)

    files, roles = _resolve_inputs(args)
    if not files:
        return _env_error("没有可校验的审计文件（既无 v2 audit.jsonl，也无 keeper/memguard jsonl）",
                          scenario, json_out)

    out = verify(scenario, files, roles, args.scenario_dir, opts)
    render(out, quiet=args.quiet)
    if json_out:
        Path(json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(json_out).write_text(json.dumps(out, ensure_ascii=False, indent=1),
                                  encoding="utf-8")
    if args.json_stdout:
        print(json.dumps(out, ensure_ascii=False))
    return out["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
