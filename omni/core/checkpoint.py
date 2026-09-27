#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.core.checkpoint — 断点事务基元（原子写 / JSONL 修复 / progress 校验）。

融合来源：图书馆 R3 断点事务族（save_progress 原子写 / repair_records_tail /
reconcile 思想）的通用化裁剪版。
"""
import json
import os
import tempfile


def atomic_write(path, data_str):
    """原子写：同目录 tmp + fsync + os.replace（继承 R3/progress 纪律）。"""
    d = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(data_str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        return True, None
    except Exception as e:  # noqa: BLE001
        try:
            os.unlink(tmp)
        except Exception:  # noqa: BLE001
            pass
        return False, f"{type(e).__name__}: {e}"


def atomic_write_json(path, obj):
    return atomic_write(path, json.dumps(obj, ensure_ascii=False, indent=1))


def repair_jsonl(path, quarantine_dir=None):
    """JSONL 撕裂修复：
    - 末尾半行（无换行 / 不完整 / 不可解析）→ 截断到最后一个完整行；
    - 中间坏行 → 移入隔离文件（保数据、不静默丢）。
    返回 {"repaired": bool, "truncated_bytes": int, "bad_lines": int, "actions": [...]}。
    """
    result = {"repaired": False, "truncated_bytes": 0, "bad_lines": 0, "actions": []}
    if not os.path.exists(path):
        return result
    with open(path, "rb") as f:
        raw = f.read()
    if not raw:
        return result

    # 按行分割（保留字节精度）
    lines = raw.split(b"\n")
    tail_incomplete = lines[-1] != b""   # 不以换行结尾 → 尾行可能撕裂
    good_chunks = []
    bad = []
    n = len(lines)
    for i, ln in enumerate(lines):
        if ln == b"" and i == n - 1:
            continue
        is_tail = (i == n - 1)
        try:
            json.loads(ln.decode("utf-8"))
            good_chunks.append(ln)
        except Exception:  # noqa: BLE001
            if is_tail and tail_incomplete:
                # 尾行撕裂（半写）→ 截断（不算"坏行"）
                result["truncated_bytes"] += len(ln)
                result["actions"].append({"action": "truncate_tail", "bytes": len(ln)})
            else:
                bad.append(ln)
                result["bad_lines"] += 1

    if result["truncated_bytes"] or bad:
        result["repaired"] = True
        # 写回：保留好行；坏行（中段）隔离
        with open(path, "wb") as f:
            for c in good_chunks:
                f.write(c + b"\n")
        if bad and quarantine_dir:
            os.makedirs(quarantine_dir, exist_ok=True)
            qp = os.path.join(quarantine_dir, os.path.basename(path) + ".bad")
            with open(qp, "ab") as qf:
                for b in bad:
                    qf.write(b + b"\n")
            result["actions"].append({"action": "quarantine_bad", "count": len(bad),
                                      "path": qp})
    return result


def validate_progress(prog):
    """progress schema 校验（宽松但显式）。返回 (ok, 问题列表)。"""
    problems = []
    if not isinstance(prog, dict):
        return False, ["progress 不是 dict"]
    for key, typ in (("tasks_done", list), ("pending", list), ("stats", dict)):
        if key in prog and not isinstance(prog[key], typ):
            problems.append(f"{key} 类型错误（应为 {typ.__name__}）")
    stats = prog.get("stats", {})
    for k, v in stats.items():
        if not isinstance(v, (int, float)) or v < 0:
            problems.append(f"stats.{k} 值非法: {v!r}")
    return (len(problems) == 0), problems
