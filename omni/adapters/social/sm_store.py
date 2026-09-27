#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sm_store — 存储层（R1′-1 断点/幂等）：pages.jsonl + leads.jsonl + progress.json"""
import json
import os
import threading

from sm_common import RUNS, now_ts, ts_iso

_LOCK = threading.Lock()


class Store:
    def __init__(self, run_dir=None):
        self.dir = run_dir or RUNS
        os.makedirs(self.dir, exist_ok=True)
        self.pages_path = os.path.join(self.dir, "pages.jsonl")
        self.leads_path = os.path.join(self.dir, "leads.jsonl")
        self.progress_path = os.path.join(self.dir, "progress.json")
        self.seen_urls = set()
        self.leads = {}      # (kind, value) -> dict
        self.progress = {"queries_done": [], "pending_urls": [], "stats": {}}
        self._load()

    # ---------- 加载（幂等基线） ----------
    def _load(self):
        # ① 先读 progress.json（基线）；后续文件计数会覆盖其 stats（防旧值漂移）
        if os.path.exists(self.progress_path):
            try:
                with open(self.progress_path, encoding="utf-8") as f:
                    p = json.load(f)
                self.progress.update(p)
            except Exception:  # noqa: BLE001
                pass
        # ② pages.jsonl 计数（以文件为准）
        n_pages = 0
        if os.path.exists(self.pages_path):
            with open(self.pages_path, encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        if rec.get("url"):
                            self.seen_urls.add(rec["url"])
                            n_pages += 1
                    except Exception:  # noqa: BLE001
                        continue
        self.progress.setdefault("stats", {})["pages"] = n_pages
        # ③ leads.jsonl
        if os.path.exists(self.leads_path):
            with open(self.leads_path, encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                        key = (rec.get("kind"), rec.get("value"))
                        if key[0] and key[1]:
                            cur = self.leads.get(key)
                            if cur is None:
                                self.leads[key] = rec
                            else:
                                cur.setdefault("sources", [])
                                if rec.get("src") and rec["src"] not in cur["sources"]:
                                    cur["sources"].append(rec["src"])
                    except Exception:  # noqa: BLE001
                        continue

    def _save_progress(self):
        tmp = self.progress_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.progress, f, ensure_ascii=False, indent=1)
        os.replace(tmp, self.progress_path)  # 原子写

    # ---------- 页面 ----------
    def has_url(self, url):
        return url in self.seen_urls

    def add_page(self, url, source, title="", rel=0.0, n_leads=0, status="ok", err=None):
        with _LOCK:
            if url in self.seen_urls:
                return False
            rec = {"url": url, "source": source, "title": title[:200], "rel": rel,
                   "n_leads": n_leads, "status": status, "err": err, "ts": now_ts()}
            with open(self.pages_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self.seen_urls.add(url)
            st = self.progress.setdefault("stats", {})
            st["pages"] = st.get("pages", 0) + 1
            return True

    # ---------- 群线索 ----------
    def add_lead(self, kind, value, evidence, src, rel=0.0, conf=0.0, llm=None):
        key = (kind, value)
        with _LOCK:
            cur = self.leads.get(key)
            if cur is None:
                rec = {"kind": kind, "value": value, "evidence": (evidence or "")[:400],
                       "src": src, "sources": [src] if src else [], "rel": rel, "conf": conf,
                       "llm": llm, "first_ts": now_ts(), "ts_iso": ts_iso()}
                with open(self.leads_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                self.leads[key] = rec
                st = self.progress.setdefault("stats", {})
                st["leads"] = st.get("leads", 0) + 1
                return True
            else:
                if src and src not in cur["sources"]:
                    cur["sources"].append(src)
                return False

    # ---------- 进度 ----------
    def mark_query_done(self, query):
        q = self.progress.setdefault("queries_done", [])
        if query not in q:
            q.append(query)
            self._save_progress()

    def query_done(self, query):
        return query in self.progress.get("queries_done", [])

    def add_pending(self, urls, limit=500):
        pend = self.progress.setdefault("pending_urls", [])
        added = 0
        for u in urls:
            if u and u not in pend and not self.has_url(u) and added < limit:
                pend.append(u)
                added += 1
        self._save_progress()
        return added

    def pop_pending(self, n=50):
        pend = self.progress.setdefault("pending_urls", [])
        take = pend[:n]
        self.progress["pending_urls"] = pend[n:]
        self._save_progress()
        return take

    def save(self):
        self._save_progress()

    def summary(self):
        return {
            "pages": len(self.seen_urls),
            "leads": len(self.leads),
            "pending": len(self.progress.get("pending_urls", [])),
            "queries_done": len(self.progress.get("queries_done", [])),
        }
