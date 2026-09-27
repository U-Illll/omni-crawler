#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""drilllib/evidence.py —— 演练台的**旁路证据通道**（drill.py 与 audit-verify.py 共用）。

为什么要有它
------------
被测系统当前只把自愈事件写进人读日志（scrape.log）与 progress.json 的 recovery[]，
R3 审计 schema（audit.jsonl）尚未落地。演练台不能因此"看不见"链路，所以把证据分两条通道：

  · **audit 通道**：audit.jsonl（R3 规范）或 keeper/memguard 的既有 jsonl（子集兼容适配）；
  · **derived 通道**：scrape.log 的机器可解析行 + progress.json 的 recovery[] 账本。

audit-verify 对同一条链路断言会同时报告两个通道的满足情况：`strict` 只认 audit 通道，
`compat` 认任一通道但显式标注 `derived`。这样「修复前 vs 修复后」的差别是可读的读数，
而不是含糊的"没有就是没过"。

本模块只读文件、只做正则与 JSON 解析，不碰任何进程。
"""
import json
import re
from pathlib import Path

# ---------------------------------------------------------------------------
# scrape.log 里可机器解析的自愈/工作证据（对应 C1/C2/C3 三级的日志形态）
# ---------------------------------------------------------------------------
LOG_PATTERNS = {
    # 工作推进（恢复判据）
    "branch": r"细分 '([^']*)' \(total=(\d+)\) -> (\d+)子块 覆盖(\d+) 缺口(\d+)",
    "leaf": r"叶子 '([^']*)' total=(\d+) 唯一=(\d+)",
    "pre_fetch": r"先抓 '([^']*)': (\d+) 条落盘",
    # 块级自愈（C2）
    "probe_retry": r"探测失败 (\d+) 字符，30s 后重试（轮 (\d+)）",
    "probe_skip": r"探测失败 '([^']*)'，跳过",
    "big_gap": r"缺口 (\d+)（大），冷却",
    "final_repair_round": r"=== 终局修复轮：(\d+) 个遗留缺口 ===",
    # 请求级自愈（C1）
    "throttle_snapshot": r"\[throttle\] mode=(\w+) interval=([\d.]+)s",
    "throttle_adjust": r"\[throttle\] (.+?) \| interval=([\d.]+)->([\d.]+)s",
    "http_retry": r"HTTP (\d{3}) q=",
    "exc": r"EXC (\w+):",
    # 持久化自愈（A3）
    "progress_fallback": r"progress.json 不可用（缺失/空/损坏）→ 回退 progress.json.bak",
    "records_repair": r"records 修复（最小丢弃）：损坏行 (\d+) 行",
    "records_rebuild": r"records 全量反查重建：(\d+) 行有效记录",
    "reconcile": r"一致性自愈：扫描 (\d+) 行有效记录",
    "tolerance": r"容差|tolerance",
    # 终态
    "complete": r"全部完成",
    "converged": r"收敛：无新任务且无修复进展",
    # 条目级异常（活锁证据：写入点不可用时的「异常 → 回队尾」无限循环）
    "conc_item_error": r"\[conc\] 条目异常 '([^']*)': (\w+)",
}


def read_text(p):
    try:
        return Path(p).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def log_evidence(scrape_log):
    """按上表抽取计数与样例（人读摘要 + 链路断言的 derived 通道）。"""
    ev = {}
    text = read_text(scrape_log)
    for name, pat in LOG_PATTERNS.items():
        ms = re.findall(pat, text)
        ev[name] = {"count": len(ms),
                    "samples": [(m if isinstance(m, str) else list(m)) for m in ms[:5]]}
    return ev


def mms_set(records_path):
    """records.jsonl → unique mms + 损坏行 + 重复计数（旁路校验，不用被测自报）。"""
    mms, bad, total = [], 0, 0
    for line in read_text(records_path).splitlines():
        if not line.strip():
            continue
        total += 1
        try:
            d = json.loads(line)
        except ValueError:
            bad += 1
            continue
        if isinstance(d, dict) and isinstance(d.get("mms"), str) and d["mms"]:
            mms.append(d["mms"])
        else:
            bad += 1
    return {"mms": mms, "unique": sorted(set(mms)), "lines": total,
            "invalid_lines": bad, "dup_mms": len(mms) - len(set(mms))}


def mock_req_stats(log_path):
    """mock 服务端请求日志 → 请求读数 + 「重复抓取叶子块」判据。

    dup_leaf_sort_pairs：以 (prefix, sort) 为键、limit>=500 的整页抓取出现 >1 次的对数。
    干净跑应为 0；「进程被杀在 append 之前」或「输出目录被删导致整轮重抓」会把它抬起来 ——
    这正是 A2「恢复后重复抓取 ≤1 个叶子块」的机器判据（服务端视角，不依赖被测自报）。
    """
    out = {"requests": 0, "probe": 0, "leaf_page": 0, "status": {}, "reason": {},
           "pairs": {}, "dup_leaf_sort_pairs": 0, "dup_examples": []}
    for line in read_text(log_path).splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except ValueError:
            continue
        out["requests"] += 1
        st = str(r.get("status"))
        out["status"][st] = out["status"].get(st, 0) + 1
        rs = str(r.get("reason") or "")
        out["reason"][rs] = out["reason"].get(rs, 0) + 1
        try:
            lim = int(r.get("limit") or 0)
        except (TypeError, ValueError):
            lim = 0
        if lim <= 1:
            out["probe"] += 1
        if lim >= 500:
            out["leaf_page"] += 1
            out["pairs"]["%s|%s" % (r.get("prefix"), r.get("sort"))] = \
                out["pairs"].get("%s|%s" % (r.get("prefix"), r.get("sort")), 0) + 1
    dups = {k: v for k, v in out["pairs"].items() if v > 1}
    out["dup_leaf_sort_pairs"] = len(dups)
    out["dup_examples"] = sorted(dups.items(), key=lambda kv: -kv[1])[:8]
    out["pairs"] = len(out["pairs"])
    return out


def progress_view(progress_path):
    """progress.json 的旁路视图：recovery[] 账本 / 队列规模 / 污点账本。"""
    try:
        d = json.loads(read_text(progress_path) or "null")
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
            "recovery_actions": actions,
            "torn_requeue": len(d.get("torn_requeue") or []),
            "schema": d.get("schema"), "saved": d.get("saved")}


def collect(scenario_dir):
    """把一个场景目录的全部旁路证据收成一个 dict（audit-verify / drill 报告共用）。"""
    d = Path(scenario_dir)
    out = d / "out"
    ev = {
        "scenario_dir": str(d),
        "log": log_evidence(out / "scrape.log"),
        "progress": progress_view(out / "progress.json"),
        "evidence_json": None,
    }
    ej = d / "evidence.json"
    if ej.exists():
        try:
            ev["evidence_json"] = json.loads(read_text(ej))
        except ValueError:
            ev["evidence_json"] = None
    return ev
