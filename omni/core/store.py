#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.core.store — 幂等存储 + 断点状态机。

融合来源：sm-recon sm_store.py（幂等 upsert / 原子写 / 状态机）泛化：
- items.jsonl   处理单元（url 或任务键 → status）；seen 集合防重复抓取
- records.jsonl 产出记录（按 (kind, key) 幂等；重复出现只累积 sources）
- progress.json {tasks_done, pending, stats}（原子写）

一致性纪律：启动时以**文件为准**重建内存（防旧值漂移——sm_store 教训）；
progress 与 jsonl 相互校验（reconcile）由 reconcile() 提供。
"""
import json
import os
import threading

from .checkpoint import atomic_write_json, repair_jsonl, validate_progress
from .log import audit, now_ts, ts_iso


class Store:
    def __init__(self, run_dir):
        self.dir = run_dir
        os.makedirs(self.dir, exist_ok=True)
        self.items_path = os.path.join(self.dir, "items.jsonl")
        self.records_path = os.path.join(self.dir, "records.jsonl")
        self.progress_path = os.path.join(self.dir, "progress.json")
        self._lock = threading.Lock()
        self.seen = {}        # key -> 最近一条 item 记录
        self.records = {}     # (kind, key) -> record dict
        self.progress = {"tasks_done": [], "pending": [], "stats": {}}
        self._load()

    # ---------- 加载 / 一致性 ----------
    def _load(self):
        # 启动自愈：修复撕裂 jsonl（有则记审计）
        for p in (self.items_path, self.records_path):
            rep = repair_jsonl(p, quarantine_dir=os.path.join(self.dir, "quarantine"))
            if rep["repaired"]:
                audit({"kind": "jsonl_repair", "file": os.path.basename(p), **rep})
        if os.path.exists(self.progress_path):
            try:
                with open(self.progress_path, encoding="utf-8") as f:
                    p = json.load(f)
                ok, problems = validate_progress(p)
                if ok:
                    self.progress.update(p)
                else:
                    audit({"kind": "progress_invalid", "problems": problems})
            except Exception as e:  # noqa: BLE001
                audit({"kind": "progress_unreadable", "err": f"{type(e).__name__}: {e}"})
        # items → seen（文件为准）
        n = 0
        if os.path.exists(self.items_path):
            with open(self.items_path, encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        k = rec.get("key")
                        if k:
                            self.seen[k] = rec
                            n += 1
                    except Exception:  # noqa: BLE001
                        continue
        self.progress.setdefault("stats", {})["items"] = n
        # records
        m = 0
        if os.path.exists(self.records_path):
            with open(self.records_path, encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        kind, key = rec.get("kind"), rec.get("key")
                        if kind and key is not None:
                            cur = self.records.get((kind, key))
                            if cur is None:
                                self.records[(kind, key)] = rec
                                m += 1
                            else:
                                src = rec.get("src")
                                cur.setdefault("sources", [])
                                if src and src not in cur["sources"]:
                                    cur["sources"].append(src)
                    except Exception:  # noqa: BLE001
                        continue
        self.progress.setdefault("stats", {})["records"] = len(self.records)

    def reconcile(self):
        """progress 与文件对账：pending 去重、去掉已处理项；返回动作数。"""
        actions = 0
        with self._lock:
            pend = self.progress.get("pending", [])
            new_pend = []
            for u in pend:
                if u in self.seen or u in new_pend:
                    actions += 1
                    continue
                new_pend.append(u)
            if actions:
                self.progress["pending"] = new_pend
                self._save_progress()
        if actions:
            audit({"kind": "reconcile", "removed": actions,
                   "pending_left": len(self.progress.get("pending", []))})
        return actions

    def _save_progress(self):
        ok, err = atomic_write_json(self.progress_path, self.progress)
        if not ok:
            audit({"kind": "progress_save_fail", "err": err})

    # ---------- items ----------
    def has_item(self, key):
        return key in self.seen

    def add_item(self, key, status="ok", **fields):
        with self._lock:
            if key in self.seen:
                return False
            rec = {"key": key, "status": status, "ts": now_ts()}
            rec.update(fields)
            with open(self.items_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self.seen[key] = rec
            st = self.progress.setdefault("stats", {})
            st["items"] = st.get("items", 0) + 1
            return True

    # ---------- records ----------
    def add_record(self, kind, key, value=None, src=None, evidence=None, **extra):
        """幂等：同 (kind, key) 只记一条；重复出现累积 sources。返回是否新建。"""
        with self._lock:
            k = (kind, key)
            cur = self.records.get(k)
            if cur is not None:
                if src and src not in cur.get("sources", []):
                    cur.setdefault("sources", []).append(src)
                return False
            rec = {"kind": kind, "key": key, "value": value, "src": src,
                   "sources": [src] if src else [], "evidence": evidence,
                   "first_ts": now_ts(), "ts_iso": ts_iso()}
            rec.update(extra)
            with open(self.records_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self.records[k] = rec
            st = self.progress.setdefault("stats", {})
            st["records"] = st.get("records", 0) + 1
            return True

    # ---------- 任务/进度 ----------
    def mark_task_done(self, task):
        with self._lock:
            td = self.progress.setdefault("tasks_done", [])
            if task not in td:
                td.append(task)
                self._save_progress()

    def task_done(self, task):
        return task in self.progress.get("tasks_done", [])

    def add_pending(self, keys, limit=500):
        added = 0
        with self._lock:
            pend = self.progress.setdefault("pending", [])
            for u in keys:
                if u and u not in pend and u not in self.seen and added < limit:
                    pend.append(u)
                    added += 1
            self._save_progress()
        return added

    def pop_pending(self, n=50):
        with self._lock:
            pend = self.progress.setdefault("pending", [])
            take = pend[:n]
            self.progress["pending"] = pend[n:]
            self._save_progress()
            return take

    def save(self):
        self._save_progress()

    def summary(self):
        return {
            "items": len(self.seen),
            "records": len(self.records),
            "pending": len(self.progress.get("pending", [])),
            "tasks_done": len(self.progress.get("tasks_done", [])),
        }
