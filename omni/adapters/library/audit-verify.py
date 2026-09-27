#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""audit-verify.py —— R3 审计校验器（schema 校验 + 场景→事件链路断言）

判据来源：`refs/r3-audit-schema.md`
  · 公共字段：ts(epoch float) / ts_iso(可选) / pid(int) / seq(进程内单调 int) /
    level(request|block|process) / event / detail(对象) / run(可选)
  · 三级事件语义：request=C1 请求级自愈、block=C2 块级兜底、process=C3 进程级自愈
  · 兼容性要求：keeper-v2 既有 `keeper-audit/1`、memguard 的 `memguard.jsonl` 必须能被
    「子集兼容」地解析（可加字段，不破坏既有解析）

两条证据通道（这是本工具的核心设计，也是「修复前 vs 修复后」的读数来源）
  · **audit 通道**：符合 R3 schema 的 audit.jsonl（严格）；或 keeper/memguard 既有 jsonl
    （适配器归一化后参与同一套断言）。
  · **derived 通道**：scrape.log 的机器可解析行 + progress.json 的 recovery[] 账本 +
    mock 服务端请求日志。R3 的 audit.jsonl 尚未落地时，链路并非"不可验证"，
    但会被显式标注 `derived`（并且 strict 判定仍然不通过）。
  ⇒ 退出码 0 = 全部链路断言通过（任一通道）+ 无解析失败 + 无 schema 硬违规。
     `strict_pass`（只认 R3 schema 的 audit 通道）单独回报，供 R3 各槽对齐用。

用法
----
  python3 audit-verify.py --scenario S1 --scenario-dir runs/<run_id>/S1
  python3 audit-verify.py --scenario-dir runs/<run_id>/S1              # 场景自动推断
  python3 audit-verify.py --files a/audit.jsonl b/audit.jsonl --no-link # 只做 schema 校验
  python3 audit-verify.py --self-test                                  # 自检（正/反例夹具）
"""
import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from drilllib import evidence as evd                                    # noqa: E402

LEVELS = ("request", "block", "process")

# 规范事件表：level → {event: 建议 detail 字段}
EVENT_TABLE = {
    "request": {
        "retry_scheduled": ("attempt", "wait_s"),
        "retry_exhausted": ("attempts", "last_error"),
        "error_classified": ("class", "action"),
        "rate_limited": ("status",),
        "cooldown_enter": ("seconds",),
        "circuit_open": ("reason",),
        "circuit_close": (),
        "request_failed": (),
    },
    "block": {
        "probe_all_dead": ("prefix",),
        "probe_partial_dead": ("prefix",),
        "probe_retry": ("prefix",),          # 实现可细化命名：探测未完整枚举 → 重试
        "probe_enumeration_gap": (),
        "repair_round_enter": ("reason",),
        "repair_round_exit": (),
        "convergence_decision": ("decided",),
        "stall_detected": ("kind",),
        "gap_recorded": ("prefix",),
        "children_discovered": (),
    },
    "process": {
        "keeper_start": (), "keeper_stop": (), "keeper_restart": (), "keeper_abort": (),
        "run_start": (), "run_end": (), "singleton_acquired": (), "singleton_released": (),
        "heartbeat_class": (), "tolerance_done": (),
        "process_dead": (), "restart_failed": (), "restart_backoff": (), "restart_flapping": (),
        "guard_suppressed": (), "adopt": (), "keeper_adopt_refused": (), "detector_ambiguous": (),
        "singleton_conflict": (), "heartbeat": (), "heartbeat_stall": (), "graceful_stop": (),
        "env_probe": (), "env_probe_fail": (), "env_changed": (), "pidfile_stale": (),
        "deadline_breached": (), "progress_recovered": (), "checkpoint_restore": (),
        "short_write_detected": (), "short_write_repaired": (), "dir_recreated": (),
        "records_tail_repaired": (), "progress_write_failed": (), "mem_level_change": (),
        "gate_close": (), "gate_open": (), "mem_abort": (),
    },
}

# 既有实现（keeper-v2 / memguard / 早期版本）事件名 → 规范语义（子集兼容，不改原记录）
ALIASES = {
    # 实现细化命名 → 规范语义（schema 文档：建议事件，实现可细化命名，但需覆盖语义）
    "probe_incomplete": "probe_partial_dead",
    "restart": "keeper_restart",
    "restart_dry_run": "keeper_restart",
    "deadline_breach": "deadline_breached",
    "adopt": "adopt",
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

TS_MIN = 1735689600.0          # 2026-01-01T00:00:00Z —— 早于此的时间戳判为不合理
TS_FUTURE_TOL = 600.0          # 允许的时钟超前（s）


# ---------------------------------------------------------------------------
# 适配器：把不同来源的记录归一化成同一结构
# ---------------------------------------------------------------------------
def _to_epoch(v):
    """ts → epoch float。支持 epoch 数字与 ISO 字符串（keeper/memguard 用 ISO）。

    时区语义陷阱（实测）：keeper-v2 写的是 `...Z`（UTC，aware），而 memguard/heartbeat 写的是
    `datetime.now().isoformat()`（**本地时间、无偏移**）。若把后者按 UTC 解析，事件时间会整体
    偏移一个时区（东八区 ⇒ 8 小时"未来"）。这里按「无偏移 = 本地时间」处理，并把这类记录标注
    `ts_naive_local` 偏差 —— R3 schema 要求 `ts` 为 epoch float，正是为了消除这个歧义。
    """
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        aware = s.endswith("Z") or "+" in s[10:] or "-" in s[10:]
        s2 = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s2)
        except ValueError:
            for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                        "%Y-%m-%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(s, fmt)
                    break
                except ValueError:
                    dt = None
            if dt is None:
                return None
        if dt.tzinfo is None:
            # 无偏移 ⇒ 本地时间（不是 UTC）
            return dt.astimezone().timestamp()
        return dt.timestamp()
    return None


def _norm_common(d, ctx, issues):
    ts = _to_epoch(d.get("ts"))
    if ts is None:
        issues.append(("ts_missing_or_unparsable", "ts=%r" % (d.get("ts"),)))
    elif ts < TS_MIN:
        issues.append(("ts_implausible", "ts=%s" % ts))
    elif ts > time.time() + TS_FUTURE_TOL:
        issues.append(("ts_in_future", "ts=%s" % ts))
    raw = d.get("ts")
    if isinstance(raw, str) and not (raw.strip().endswith("Z") or "+" in raw.strip()[10:]
                                     or "-" in raw.strip()[10:]):
        issues.append(("ts_naive_local", "ts=%r 无时区偏移（按本地时间解析）" % raw))
    return ts


def adapt_canonical(d, ctx, issues):
    """R3 schema 记录。"""
    rec = {"adapter": "canonical"}
    rec["ts"] = _norm_common(d, ctx, issues)
    rec["ts_iso"] = d.get("ts_iso")
    pid = d.get("pid")
    rec["pid"] = pid if isinstance(pid, int) else None
    if rec["pid"] is None:
        issues.append(("pid_missing_or_not_int", "pid=%r" % (pid,)))
    seq = d.get("seq")
    rec["seq"] = seq if isinstance(seq, int) else None
    if rec["seq"] is None:
        issues.append(("seq_missing_or_not_int", "seq=%r" % (seq,)))
    lvl = d.get("level")
    rec["level"] = lvl if lvl in LEVELS else None
    if rec["level"] is None:
        issues.append(("level_not_in_enum", "level=%r" % (lvl,)))
    rec["event"] = d.get("event") if isinstance(d.get("event"), str) else None
    if not rec["event"]:
        issues.append(("event_missing", "event=%r" % (d.get("event"),)))
    det = d.get("detail")
    if det is None:
        issues.append(("detail_missing", ""))
        rec["detail"] = {}
    elif not isinstance(det, dict):
        issues.append(("detail_not_object", "type=%s" % type(det).__name__))
        rec["detail"] = {"_value": det}
    else:
        rec["detail"] = det
    rec["run"] = d.get("run")
    return rec


def adapt_keeper(d, ctx, issues):
    """keeper-v2/v3 的 `keeper-audit/1`。

    v3 的每行**同时携带 R3 字段**（`ts_epoch` / `level` / `seq` / `detail` / `r3_event`，
    见 keeper-v3.sh 的 audit()）⇒ 存在时优先采用这些字段（并把 `r3_fields` 标为真，
    供 `r3_fields_pass` 判定）；否则退回 v2 的子集适配（level=process、seq 由文件行序派生）。
    """
    r3 = bool(d.get("r3_event") or d.get("r3_schema") or d.get("ts_epoch") is not None)
    rec = {"adapter": "keeper", "r3_fields": r3}
    rec["ts"] = _norm_common(
        {"ts": d.get("ts_epoch") if d.get("ts_epoch") is not None else d.get("ts")}, ctx, issues)
    rec["ts_iso"] = d.get("ts")
    pid = d.get("keeper_pid") or d.get("pid")
    rec["pid"] = pid if isinstance(pid, int) else None
    rec["seq"] = d.get("seq") if isinstance(d.get("seq"), int) else None
    lvl = d.get("level")
    rec["level"] = lvl if lvl in LEVELS else "process"
    if r3 and lvl not in LEVELS:
        issues.append(("level_not_in_enum", "level=%r" % (lvl,)))
    ev = d.get("r3_event") if r3 else d.get("event")
    rec["event"] = ev if isinstance(ev, str) and ev else (
        d.get("event") if isinstance(d.get("event"), str) else None)
    det = d.get("detail") if isinstance(d.get("detail"), dict) else None
    if det is None:
        det = d.get("evidence") if isinstance(d.get("evidence"), dict) else {}
    rec["detail"] = dict(det)
    rec["detail"].setdefault("_from_pid", d.get("from_pid"))
    rec["detail"].setdefault("_to_pid", d.get("to_pid"))
    rec["run"] = d.get("run_id")
    if r3:
        for f, bad in (("pid", rec["pid"] is None), ("seq", rec["seq"] is None),
                       ("event", not rec["event"]),
                       ("detail", not isinstance(d.get("detail"), dict))):
            if bad:
                issues.append(("r3_field_missing:%s" % f, "r3_schema 行缺 %s" % f))
    else:
        for f in ("pid", "seq", "detail"):
            if rec[f] is None or (f == "detail" and not d.get("evidence")):
                issues.append(("adapter_field_missing:%s" % f, "keeper 记录无 %s 字段" % f))
    return rec


def adapt_memguard(d, ctx, issues):
    """memguard.jsonl：有 seq/pid/event，但 level 是**整数档位**（与 R3 schema 的字符串枚举冲突）。"""
    rec = {"adapter": "memguard"}
    rec["ts"] = _norm_common(d, ctx, issues)
    rec["ts_iso"] = d.get("ts")
    rec["pid"] = d.get("pid") if isinstance(d.get("pid"), int) else None
    rec["seq"] = d.get("seq") if isinstance(d.get("seq"), int) else None
    rec["level"] = "process"               # 语义上是进程级资源自愈
    if "level" in d:
        issues.append(("level_type_conflict", "memguard level=%r（整数档位，非 R3 枚举）"
                       % (d.get("level"),)))
    rec["event"] = d.get("event") if isinstance(d.get("event"), str) else None
    detail = {k: v for k, v in d.items()
              if k not in ("ts", "seq", "pid", "event", "level", "run_id", "tag")}
    rec["detail"] = detail
    rec["run"] = d.get("run_id")
    return rec


def _detect_adapter(d):
    sch = str(d.get("schema") or "")
    if sch.startswith("keeper-audit"):
        return "keeper"
    if "keeper_pid" in d or "from_pid" in d or "to_pid" in d:
        return "keeper"
    if "rss_mb" in d or "level_name" in d or "gate" in d:
        return "memguard"
    return "canonical"


def load_records(files):
    records, stats = [], {"files": [], "lines": 0, "parsed": 0, "bad_lines": 0,
                          "bad_examples": [], "adapters": {}}
    for f in files:
        p = Path(f)
        stats["files"].append(str(p))
        n = 0
        for i, line in enumerate(evd.read_text(p).splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            stats["lines"] += 1
            n += 1
            try:
                d = json.loads(line)
            except ValueError as e:
                stats["bad_lines"] += 1
                if len(stats["bad_examples"]) < 5:
                    stats["bad_examples"].append("%s:%d %s" % (p.name, i, str(e)[:80]))
                continue
            if not isinstance(d, dict):
                stats["bad_lines"] += 1
                continue
            issues = []
            kind = _detect_adapter(d)
            if kind == "keeper":
                rec = adapt_keeper(d, (p, i), issues)
            elif kind == "memguard":
                rec = adapt_memguard(d, (p, i), issues)
            else:
                rec = adapt_canonical(d, (p, i), issues)
            rec["origin"] = "%s:%d" % (p.name, i)
            rec["file"] = str(p)
            rec["line"] = i
            rec["issues"] = issues
            rec["raw_event"] = d.get("event")
            ev = rec.get("event")
            rec["canonical_event"] = ALIASES.get(ev, ev)
            rec["seq_order"] = i               # 文件行序（无 seq 时的派生序号）
            records.append(rec)
            stats["parsed"] += 1
            stats["adapters"][rec["adapter"]] = stats["adapters"].get(rec["adapter"], 0) + 1
        if n == 0:
            stats["bad_examples"].append("%s 为空" % p.name)
    return records, stats


def check_schema(records, stats):
    """字段/枚举/seq 单调/ts 合理性。硬违规 → strict_pass=False；软违规只计数。

    两档合规判定：
      · `strict_pass`      —— 必须存在**规范 R3 记录**（audit.jsonl），且无硬违规；
      · `r3_fields_pass`   —— keeper-v3 这类「自带 R3 字段（ts_epoch/level/seq/detail/r3_event）
        的历史格式行」全部字段齐备、类型与枚举合法（不要求另开 audit.jsonl）。
    """
    devs = {}
    hard = soft = hard_r3 = 0

    def bump(kind, example, is_hard, is_r3=False):
        nonlocal hard, soft, hard_r3
        e = devs.setdefault(kind, {"kind": kind, "count": 0, "hard": is_hard, "examples": []})
        e["count"] += 1
        if len(e["examples"]) < 4:
            e["examples"].append(example)
        if is_hard:
            hard += 1
            if is_r3:
                hard_r3 += 1
        else:
            soft += 1

    HARD_SOFT_PREFIX = {"ts_missing_or_unparsable": True, "ts_implausible": True,
                        "ts_in_future": True, "pid_missing_or_not_int": True,
                        "seq_missing_or_not_int": False, "level_not_in_enum": True,
                        "event_missing": True, "detail_missing": False,
                        "detail_not_object": True, "r3_field_missing": True,
                        "ts_naive_local": False}
    for r in records:
        is_r3 = bool(r.get("r3_fields"))
        strict_rec = r["adapter"] == "canonical" or is_r3
        for kind, ex in r["issues"]:
            base = kind.split(":")[0]
            is_hard = HARD_SOFT_PREFIX.get(base, False) and strict_rec
            bump(kind if r["adapter"] == "canonical" else "%s[%s]" % (kind, r["adapter"]),
                 "%s %s" % (r["origin"], ex), is_hard, is_r3)
        ev = r.get("canonical_event")
        lvl = r.get("level")
        known = None
        if ev:
            for L, table in EVENT_TABLE.items():
                if ev in table:
                    known = L
                    break
        strict_rec = r["adapter"] == "canonical" or r.get("r3_fields")
        if ev is None:
            pass
        elif known is None:
            # 事件名不在规范表里 ⇒ **软**偏差（schema 文档明确「建议事件，实现可细化命名，
            # 但需覆盖语义」）：只提示命名漂移，不判不合规。实测 keeper-v3 的
            # singleton_acquired / heartbeat_class / singleton_released 即属此类。
            bump("event_unknown", "%s event=%r" % (r["origin"], ev), False)
        elif lvl and lvl != known:
            bump("event_level_mismatch",
                 "%s event=%s 期望 level=%s 实际=%s" % (r["origin"], ev, known, lvl), strict_rec)
        elif known and ev in EVENT_TABLE[known] and EVENT_TABLE[known][ev]:
            missing = [f for f in EVENT_TABLE[known][ev] if f not in (r.get("detail") or {})]
            if missing:
                bump("detail_fields_missing",
                     "%s event=%s 缺 %s" % (r["origin"], ev, missing), False)

    # seq 单调：**按 (文件, pid, level) 分组** —— seq 是「某个审计流内」的单调序号，
    # 不同 sink（audit.jsonl / memguard.jsonl）各有自己的 seq 序列，跨文件比较必然误报
    # （实测：同一进程的 audit.jsonl 与 memguard.jsonl 各有 seq=1 ⇒ 假硬违规）。
    groups = {}
    for r in records:
        if isinstance(r.get("seq"), int):
            groups.setdefault((r.get("file"), r.get("pid"),
                               r.get("level") or r["adapter"]), []).append(r)
    for key, rs in groups.items():
        last = None
        for r in rs:
            if last is not None and r["seq"] <= last:
                bump("seq_not_monotonic", "%s seq=%s ≤ 前一条 %s (pid=%s)"
                     % (r["origin"], r["seq"], last, key[0]), True)
            last = max(last, r["seq"]) if last is not None else r["seq"]

    # ts 非降（按 pid 分组；跨进程合并会因时钟粒度抖动 ⇒ 记为软违规）
    tgroups = {}
    for r in records:
        if r.get("ts"):
            tgroups.setdefault((r.get("file"), r.get("pid")), []).append(r)
    for pid, rs in tgroups.items():
        last = None
        for r in sorted(rs, key=lambda x: x["line"]):
            if last is not None and r["ts"] < last - 1.0:
                bump("ts_not_monotonic", "%s ts=%.3f < 前一条 %.3f (pid=%s)"
                     % (r["origin"], r["ts"], last, pid), False)
            last = r["ts"] if last is None else max(last, r["ts"])

    by_level, by_event = {}, {}
    for r in records:
        by_level[r.get("level") or r["adapter"]] = by_level.get(r.get("level") or r["adapter"], 0) + 1
        by_event[r.get("canonical_event") or "?"] = by_event.get(r.get("canonical_event") or "?", 0) + 1
    canon = [r for r in records if r["adapter"] == "canonical"]
    r3f = [r for r in records if r.get("r3_fields")]
    strict_pass = bool(canon) and hard == 0 and stats["bad_lines"] == 0
    r3_fields_pass = bool(r3f) and hard_r3 == 0 and stats["bad_lines"] == 0
    return {
        "files": stats["files"], "lines": stats["lines"], "parsed": stats["parsed"],
        "bad_lines": stats["bad_lines"], "bad_examples": stats["bad_examples"],
        "adapters": stats["adapters"], "by_level": by_level, "by_event": by_event,
        "hard_deviations": hard, "soft_deviations": soft, "deviation_count": hard + soft,
        "deviations": sorted(devs.values(), key=lambda d: (-d["count"])),
        "canonical_records": len(canon), "r3_sink_present": bool(canon),
        "r3_field_records": len(r3f), "r3_fields_pass": r3_fields_pass,
        "r3_field_hard_deviations": hard_r3,
        "strict_pass": strict_pass,
    }


# ---------------------------------------------------------------------------
# 场景 → 事件链路断言表
#   audit：规范事件（含别名）需出现的 level / 事件集 / 最少条数
#   derived：R3 audit.jsonl 缺席时的旁路证据（log / progress 账本 / mock 服务端）
# ---------------------------------------------------------------------------
LINK = {
    "S1": [{"id": "S1.keeper_restart_per_kill", "level": "process",
            "events": ["keeper_restart"],
            "min_count": 3, "windowed": True, "per_injection": "killed",
            "derived": [("log", "reconcile", 1)],
            "why": "每一次 kill -9 都必须留下进程级重启事件（C3/A2）"}],
    "S2": [{"id": "S2.dir_heal", "level": "process",
            "events": ["keeper_restart", "dir_recreated"], "min_count": 1, "windowed": True,
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
            "events": ["probe_all_dead", "probe_partial_dead", "probe_retry",
                       "probe_enumeration_gap"],
            "min_count": 1, "windowed": True,
            "derived": [("log_any", [("probe_retry", 1), ("probe_skip", 1)], 1)],
            "why": "探测全灭必须留痕（C2）"},
           {"id": "S6.repair_round", "level": "block", "events": ["repair_round_enter"],
            "min_count": 1, "windowed": True,
            "derived": [("log_any", [("final_repair_round", 1), ("big_gap", 1)], 1)],
            "why": "探测全灭必须进修复轮兜底（C2：不得假收敛）"}],
    "S6b": [{"id": "S6b.probe_all_dead", "level": "block",
             "events": ["probe_all_dead", "probe_partial_dead", "probe_retry"],
             "min_count": 1, "windowed": True,
             "derived": [("log_any", [("probe_retry", 1), ("probe_skip", 1)], 1)],
             "why": "瞬时探测全灭也必须留痕（C2）；就地重试路径不要求修复轮事件"}],
    "S7a": [{"id": "S7a.progress_recovered", "level": "process",
             "events": ["progress_recovered", "checkpoint_restore"], "min_count": 1,
             "windowed": True,
             "derived": [("log", "progress_fallback", 1)],
             "why": "progress 半写 ⇒ 备份槽回退事件（A3）"}],
    "S7b": [{"id": "S7b.rebuild_from_records", "level": "process",
             "events": ["progress_recovered", "checkpoint_restore"], "min_count": 1,
             "windowed": True,
             "derived": [("log", "records_rebuild", 1)],
             "why": "progress + bak 双损 ⇒ 从 records 全量反推重建（A3/E1）"}],
    "REF": [],
}


def _derived_ok(spec, ev):
    """derived 通道是否满足（逐条列证据，便于报告里说清"靠什么过的"）。"""
    log = (ev.get("log") or {})
    prog = ev.get("progress") or {}
    actions = prog.get("recovery_actions") or {}
    hits = []
    for item in spec.get("derived") or []:
        kind = item[0]
        if kind == "log":
            _k, pat, n = item
            c = (log.get(pat) or {}).get("count", 0)
            if c >= n:
                hits.append({"channel": "log", "key": pat, "count": c})
        elif kind == "log_any":
            _k, opts, n = item
            need = 0
            got = []
            for pat, cnt in opts:
                c = (log.get(pat) or {}).get("count", 0)
                if c >= cnt:
                    need += 1
                    got.append("%s=%d" % (pat, c))
            if need >= n:
                hits.append({"channel": "log_any", "key": ",".join(got), "count": need})
        elif kind == "progress":
            _k, action, n = item
            c = actions.get(action, 0)
            if c >= n:
                hits.append({"channel": "progress", "key": action, "count": c})
        elif kind == "mock_reason":
            _k, reason, n = item
            c = (ev.get("mock_reason") or {}).get(reason, 0)
            if c >= n:
                hits.append({"channel": "mock", "key": reason, "count": c})
    return hits


def check_links(scenario, records, ev):
    specs = LINK.get(scenario)
    if specs is None:
        return None
    injections = []
    ej = ev.get("evidence_json") or {}
    for i in (ej.get("injections") or []):
        if isinstance(i, dict) and i.get("ts"):
            injections.append(i["ts"])
    first_inj = min(injections) if injections else None
    last_inj = max(injections) if injections else None
    deadline = float((ej.get("deadline") or 180))

    checks = []
    for spec in specs:
        names = set(spec["events"])
        # per_injection：要求「每一次真的做过的注入」都留下事件（比固定条数更贴合语义 ——
        # 例如 S1 只成功 kill 了 2 次时，正确的判据是「2 次 kill ⇒ ≥2 条 keeper_restart」，
        # 而不是死守 3 条）。
        want = spec["min_count"]
        want_src = "spec"
        if spec.get("per_injection"):
            key = spec["per_injection"]
            got_inj = len([i for i in (ej.get("injections") or [])
                           if isinstance(i, dict) and i.get(key)])
            if got_inj:
                want = got_inj
                want_src = "injections.%s" % key
        sel = [r for r in records
               if r.get("canonical_event") in names and (not r.get("level")
                                                         or r["level"] == spec["level"])]
        window_ok, win_note = True, "no_window"
        if spec.get("windowed") and first_inj is not None:
            lo, hi = first_inj - 2.0, last_inj + deadline
            inw = [r for r in sel if r.get("ts") and lo <= r["ts"] <= hi]
            window_ok = len(inw) >= want
            win_note = "窗口 [%.0f, %.0f] 内 %d 条（全部 %d 条）" % (lo, hi, len(inw), len(sel))
            sel = inw
        audit_ok = len(sel) >= want
        hits = _derived_ok(spec, ev)
        derived_ok = bool(hits)
        ok = audit_ok or derived_ok
        checks.append({
            "id": spec["id"], "why": spec["why"], "level": spec["level"],
            "events": spec["events"], "min_count": want, "min_count_source": want_src,
            "audit_count": len(sel), "audit_ok": audit_ok, "window_ok": window_ok,
            "window_note": win_note, "derived_ok": derived_ok, "derived_hits": hits,
            "channel": "audit" if audit_ok else ("derived" if derived_ok else "none"),
            "ok": ok,
            "examples": [{"origin": r["origin"], "ts": r.get("ts"), "event": r.get("event"),
                          "raw_event": r.get("raw_event")} for r in sel[:3]],
        })
    return checks


# ---------------------------------------------------------------------------
# 输入发现 / 主流程
# ---------------------------------------------------------------------------
def discover_inputs(scenario_dir):
    """发现一个场景目录里的**审计**输入。

    只收「审计类」jsonl：`audit.jsonl` / `*audit*.jsonl` / `memguard.jsonl`。
    刻意**排除** `records.jsonl`（数据行，不是审计事件）、mock 请求日志、心跳文件等 ——
    把数据当事件解析会制造大量假偏差（实测：把 records.jsonl 收进来会产出 1.6 万条伪字段偏差）。
    """
    d = Path(scenario_dir)
    DENY = {"records.jsonl", "mock-requests.jsonl", "heartbeat.json", "run.log"}
    files, roles = [], {}

    def consider(role, p):
        if not p.exists() or p.stat().st_size == 0:
            return
        if p.name in DENY:
            return
        if str(p) in files:
            return
        files.append(str(p))
        roles[str(p)] = role

    for role, p in (("r3_audit", d / "out" / "audit.jsonl"),
                    ("keeper_audit", d / "audit.jsonl"),
                    ("keeper_audit", d / "logs" / "audit.jsonl"),
                    ("r3_audit", d / "logs" / "audit.jsonl"),
                    ("memguard", d / "out" / "memguard.jsonl")):
        consider(role, p)
    for p in sorted(d.glob("*.jsonl")) + sorted((d / "out").glob("*.jsonl")):
        if "audit" in p.name.lower() or p.name == "memguard.jsonl":
            consider("other", p)
    return files, roles


def infer_scenario(scenario_dir):
    name = Path(scenario_dir).name
    m = re.match(r"^(S\d[ab]?|REF)", name)
    return m.group(1) if m else None


def verify(scenario, files, scenario_dir=None):
    records, stats = load_records(files)
    schema = check_schema(records, stats)
    ev = evd.collect(scenario_dir) if scenario_dir else {"log": {}, "progress": None}
    if scenario_dir:
        ms = evd.mock_req_stats(Path(scenario_dir) / "logs" / "mock-requests.jsonl")
        ev["mock_reason"] = ms.get("reason") or {}
    checks = check_links(scenario, records, ev) if scenario else None
    pass_count = len([c for c in checks or [] if c["ok"]])
    check_count = len(checks or [])
    fails = [c for c in (checks or []) if not c["ok"]]
    out = {
        "scenario": scenario, "scenario_dir": str(scenario_dir) if scenario_dir else None,
        "files": files, "schema": schema,
        "checks": checks, "pass_count": pass_count, "check_count": check_count,
        "missing_events": [{"id": c["id"], "events": c["events"], "level": c["level"]}
                           for c in fails],
        "fail_reasons": ["%s：audit 通道缺 %s（≥%d）且无 derived 证据"
                         % (c["id"], "/".join(c["events"]), c["min_count"]) for c in fails],
        "derived_only": bool(checks) and all(c["channel"] == "derived" for c in checks),
        "audit_only": bool(checks) and all(c["channel"] == "audit" for c in checks),
        "ts": time.time(),
    }
    hard = schema["hard_deviations"]
    parse_fail = schema["bad_lines"] > 0 and schema["parsed"] == 0
    out["pass"] = (not fails) and hard == 0 and not parse_fail
    out["exit_code"] = 0 if out["pass"] else 1
    return out


def render(out, quiet=False):
    L = []
    s = out["schema"]
    L.append("== audit-verify：场景 %s ==" % (out["scenario"] or "(未指定)"))
    L.append("审计文件 %d 个：%s" % (len(out["files"]), ", ".join(Path(f).name for f in out["files"])
                                    or "无"))
    L.append("解析：行 %s / 成功 %s / 坏行 %s；适配器 %s"
             % (s["lines"], s["parsed"], s["bad_lines"], s["adapters"]))
    L.append("字段/枚举/单调：硬违规 %d，软违规 %d；canonical 记录 %d；"
             "R3 sink 在场=%s；strict_pass=%s；带 R3 字段的记录=%s（r3_fields_pass=%s，硬违规=%s）"
             % (s["hard_deviations"], s["soft_deviations"], s["canonical_records"],
                s["r3_sink_present"], s["strict_pass"], s.get("r3_field_records"),
                s.get("r3_fields_pass"), s.get("r3_field_hard_deviations")))
    for d in s["deviations"][:8]:
        L.append("    - [%s] %s × %d  例：%s" % ("HARD" if d["hard"] else "soft", d["kind"],
                                               d["count"], d["examples"][:1]))
    if out["checks"] is None:
        L.append("链路断言：未指定场景 ⇒ 跳过")
    else:
        L.append("链路断言：%d/%d 通过" % (out["pass_count"], out["check_count"]))
        for c in out["checks"]:
            L.append("    [%s] %-24s level=%-7s 事件=%s 需≥%d；audit=%d%s；channel=%s%s"
                     % ("PASS" if c["ok"] else "FAIL", c["id"], c["level"],
                        "/".join(c["events"]), c["min_count"], c["audit_count"],
                        "" if c["window_ok"] else "(窗口外)", c["channel"],
                        ("；derived=%s" % c["derived_hits"]) if c["derived_hits"] else ""))
            L.append("        为什么：%s" % c["why"])
            if not c["window_ok"]:
                L.append("        ⚠ 时间窗：%s" % c["window_note"])
    L.append("判定：%s（exit %d）" % ("PASS" if out["pass"] else "FAIL", out["exit_code"]))
    text = "\n".join(L)
    if not quiet:
        print(text)
    return text


# ---------------------------------------------------------------------------
# 自检：正例（规范 schema + 全链路）与反例（缺字段/seq 回退/链路缺事件）
# ---------------------------------------------------------------------------
def self_test(tmp_dir=None):
    import tempfile
    d = Path(tmp_dir or tempfile.mkdtemp(prefix="auditverify-"))
    (d / "out").mkdir(parents=True, exist_ok=True)
    now = time.time()
    good = [
        {"ts": now, "pid": 101, "seq": 1, "level": "process", "event": "keeper_start",
         "detail": {}, "run": "t1"},
        {"ts": now + 1, "pid": 101, "seq": 2, "level": "process", "event": "keeper_restart",
         "detail": {"from_pid": 101, "to_pid": 202, "reason": "kill -9"}},
        {"ts": now + 6, "pid": 202, "seq": 1, "level": "request",
         "event": "retry_scheduled", "detail": {"attempt": 1, "wait_s": 5.0}},
        {"ts": now + 11, "pid": 202, "seq": 2, "level": "process", "event": "keeper_restart",
         "detail": {"from_pid": 202, "to_pid": 303, "reason": "kill -9"}},
        {"ts": now + 16, "pid": 303, "seq": 1, "level": "process", "event": "keeper_restart",
         "detail": {"from_pid": 303, "to_pid": 404, "reason": "kill -9"}},
        {"ts": now + 17, "pid": 404, "seq": 1, "level": "block", "event": "probe_all_dead",
         "detail": {"prefix": "A", "missing_chars": 36}},
        {"ts": now + 18, "pid": 404, "seq": 2, "level": "block",
         "event": "repair_round_enter", "detail": {"reason": "probe_all_dead"}},
    ]
    bad = [
        {"pid": 7, "level": "banana", "event": "no_such_event", "detail": "not-an-object"},
        {"ts": now + 9, "pid": 7, "seq": 5, "level": "process", "event": "keeper_restart",
         "detail": {}},
        {"ts": now + 8, "pid": 7, "seq": 4, "level": "process", "event": "keeper_restart",
         "detail": {}},
    ]
    (d / "out" / "audit.jsonl").write_text(
        "\n".join(json.dumps(x, ensure_ascii=False) for x in good) + "\n", encoding="utf-8")
    (d / "evidence.json").write_text(json.dumps({
        "scenario": "S1", "deadline": 180,
        "injections": [{"ts": now + 0.5}, {"ts": now + 1.5}, {"ts": now + 2.5}]},
        ensure_ascii=False), encoding="utf-8")
    (d / "out" / "scrape.log").write_text("", encoding="utf-8")
    r_good = verify("S1", [str(d / "out" / "audit.jsonl")], str(d))
    (d / "bad.jsonl").write_text(
        "\n".join(json.dumps(x, ensure_ascii=False) for x in bad) + "\n", encoding="utf-8")
    r_bad = verify("S1", [str(d / "bad.jsonl")], str(d))
    checks = [
        ("good.schema.strict", r_good["schema"]["strict_pass"] is True),
        ("good.seq_monotonic", not any(x["kind"] == "seq_not_monotonic"
                                       for x in r_good["schema"]["deviations"])),
        ("good.links_pass", r_good["pass"] is True and r_good["check_count"] == 1),
        ("bad.schema.hard_gt0", r_bad["schema"]["hard_deviations"] > 0),
        ("bad.seq_detected", any(x["kind"] == "seq_not_monotonic"
                                 for x in r_bad["schema"]["deviations"])),
        ("bad.level_detected", any(x["kind"] == "level_not_in_enum"
                                   for x in r_bad["schema"]["deviations"])),
        ("bad.event_detected", any(x["kind"] == "event_unknown"
                                   for x in r_bad["schema"]["deviations"])),
        ("bad.link_fails", r_bad["pass"] is False and r_bad["exit_code"] == 1),
    ]
    print("== audit-verify 自检 ==")
    ok = True
    for name, good_ in checks:
        print("  [%s] %s" % ("PASS" if good_ else "FAIL", name))
        ok = ok and good_
    print("自检：%s（%d/%d）" % ("PASS" if ok else "FAIL", sum(1 for _, g in checks if g),
                              len(checks)))
    return 0 if ok else 1


def main(argv=None):
    ap = argparse.ArgumentParser(description="R3 审计校验器（schema + 场景链路）")
    ap.add_argument("--scenario", default=None, help="S1/S2/S3a/S3b/S4/S5/S6/S7a/S7b/REF")
    ap.add_argument("--scenario-dir", default=None)
    ap.add_argument("--files", nargs="*", default=None, help="显式指定审计 jsonl")
    ap.add_argument("--json", dest="json_out", default=None)
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args(argv)

    if args.self_test:
        return self_test()

    files = list(args.files or [])
    scenario = args.scenario
    if args.scenario_dir:
        d = Path(args.scenario_dir)
        if not d.exists():
            raise SystemExit("场景目录不存在：%s" % d)
        if not scenario:
            scenario = infer_scenario(d)
        auto, roles = discover_inputs(d)
        for f in auto:
            if f not in files:
                files.append(f)
    if not files:
        print("没有可校验的审计文件（既非 R3 audit.jsonl，也无 keeper/memguard jsonl）")
        out = {"scenario": scenario, "files": [], "pass": False, "exit_code": 2,
               "fail_reasons": ["没有审计输入"], "checks": None,
               "schema": {"lines": 0, "parsed": 0, "bad_lines": 0, "hard_deviations": 0,
                          "soft_deviations": 0, "canonical_records": 0,
                          "r3_sink_present": False, "strict_pass": False, "deviations": [],
                          "adapters": {}, "by_event": {}, "by_level": {}, "files": []}}
        if args.json_out:
            Path(args.json_out).write_text(json.dumps(out, ensure_ascii=False, indent=1),
                                           encoding="utf-8")
        print("判定：FAIL（exit 2）")
        return 2

    out = verify(scenario, files, args.scenario_dir)
    render(out, quiet=args.quiet)
    if args.json_out:
        Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_out).write_text(json.dumps(out, ensure_ascii=False, indent=1),
                                       encoding="utf-8")
    return out["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
