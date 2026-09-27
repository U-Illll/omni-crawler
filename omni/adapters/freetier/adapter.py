#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.adapters.freetier.adapter — 免费机制基线采集与变化监控（适配器实现）。

任务（2026-09-27 用户下达）：
- 补充爬取谷歌/OpenAI/GitHub 等主流官方平台的免费机制（定价/免费层/活动页面）；
- 建立原文基线 + refresh 变化监控（服务「Token 渠道」情报的动态跟踪）；
- 不与既有采集域重复（见 README「不重复」清单）。

两种模式：
- baseline：为每个尚无快照的站点建立首份快照（幂等；失败也落 meta，重跑用 refresh）。
- refresh ：重抓所有站点并与快照对比；仅在有变化时存新版本 + 产出 change 记录。

已实测的特殊处理（2026-09-27）：
- ai.google.dev 直连 302（signin 跳转）→ 两步握手：首个请求 allow_redirects=False
  捕获 signin cookie → 第二次请求 200。per-host session 复用（同域只握手一次）。
- openai.com 需"完整浏览器指纹头"（sec-ch-ua + Sec-Fetch-* + 版本一致的 UA）；
  出口无关（普通/住宅均可）。对全体站点统一使用 BROWSER_HEADERS（最保守一致）。

数据位置（root = 本包目录）：
- state/snapshots/<site_id>/v<N>.txt   快照（仅首次/变化时新增版本）
- state/snapshots/<site_id>/meta.json  站点状态（原子写）
- state/changes.jsonl                  变化账本（追加）
- runs/ logs/                          框架 Store / 审计（Engine 绑定）
"""
import difflib
import hashlib
import json
import os
import re
import sys
import urllib.parse

import requests

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from ..base import Adapter  # noqa: E402
from .sources import SITES, WATCH_WORDS  # noqa: E402
from ...core.checkpoint import atomic_write, atomic_write_json  # noqa: E402
from ...core.log import audit, log, now_ts, ts_iso  # noqa: E402

# 完整浏览器指纹头（2026-09-27 实测：openai.com CF 校验头部一致性；
# UA 与 sec-ch-ua 版本必须同源——故在此固定一整套，覆盖 fetch 默认头）。
BROWSER_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"),
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,*/*;q=0.8"),
    "Accept-Language": "en-US,en;q=0.9",
    "sec-ch-ua": '"Chromium";v="140", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}

# 登录页信号（handshake 站点防误存：若正文是 Google 登录页则视为失败）
LOGIN_SIGNS = ["sign in - google accounts", "use your google account"]


def _looks_like_login(html):
    low = (html or "")[:6000].lower()
    return any(s in low for s in LOGIN_SIGNS)


def _extract_title(html):
    m = re.search(r"<title[^>]*>([\s\S]*?)</title>", html or "", flags=re.I)
    if not m:
        return ""
    t = re.sub(r"\s+", " ", m.group(1)).strip()
    return t[:200]


def _html_to_text(html):
    """HTML → 可读文本（去 script/style；块级标签转换行；压缩空白）。"""
    body = re.sub(r"<script[\s\S]*?</script>", " ", html or "", flags=re.I)
    body = re.sub(r"<style[\s\S]*?</style>", " ", body, flags=re.I)
    body = re.sub(r"<br\s*/?>", "\n", body, flags=re.I)
    body = re.sub(r"</(p|div|li|tr|h[1-6]|section|article|td|th)>", "\n", body, flags=re.I)
    body = re.sub(r"<[^>]+>", " ", body)
    for a, b in (("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"), ("&quot;", '"'),
                 ("&#39;", "'"), ("&nbsp;", " "), ("&#x27;", "'")):
        body = body.replace(a, b)
    body = re.sub(r"[ \t\r\f\v]+", " ", body)
    body = re.sub(r" *\n *", "\n", body)
    body = re.sub(r"\n{2,}", "\n", body)
    return body.strip()


def _strip_snapshot_header(raw):
    """剥离快照文件头部（# 元信息行与空行），返回正文。"""
    lines = raw.splitlines()
    i = 0
    while i < len(lines) and (lines[i].startswith("#") or not lines[i].strip()):
        i += 1
    return "\n".join(lines[i:])


def _diff_preview(old_path, new_text, max_lines=60):
    """生成统一 diff 预览（截断；供变化记录与人工复核）。旧快照头部先剥离。"""
    try:
        with open(old_path, encoding="utf-8") as f:
            old = _strip_snapshot_header(f.read())
    except Exception:  # noqa: BLE001
        return "(旧快照缺失)"
    out = []
    for i, d in enumerate(difflib.unified_diff(old.splitlines(), new_text.splitlines(),
                                               "old", "new", lineterm="", n=1)):
        out.append(d[:240])
        if i >= max_lines - 1:
            out.append("... (截断)")
            break
    return "\n".join(out)


class FreeTierAdapter(Adapter):
    name = "freetier"

    def __init__(self, mode=None, root=None, sites=None):
        self._root = root or _HERE
        self.sites = list(sites if sites is not None else SITES)
        self.mode = mode        # 显式模式优先（None = 由 ctx.args.mode 决定）
        self._visited = set()   # refresh 本轮已复核站点（内存；重跑全量复核幂等无害）
        self._sessions = {}     # host -> requests.Session（handshake 用）

    def root(self):
        return self._root

    # ---------- 路径 ----------
    def _state_dir(self):
        return os.path.join(self._root, "state")

    def _site_dir(self, site_id):
        return os.path.join(self._state_dir(), "snapshots", site_id)

    def _meta_path(self, site_id):
        return os.path.join(self._site_dir(site_id), "meta.json")

    def _snap_path(self, site_id, ver):
        return os.path.join(self._site_dir(site_id), f"v{ver}.txt")

    def _load_meta(self, site_id):
        p = self._meta_path(site_id)
        if not os.path.exists(p):
            return {}
        try:
            with open(p, encoding="utf-8") as f:
                return json.load(f)
        except Exception:  # noqa: BLE001
            return {}

    def _save_meta(self, site_id, meta):
        ok, err = atomic_write_json(self._meta_path(site_id), meta)
        if not ok:
            audit({"kind": "freetier_meta_save_fail", "site": site_id, "err": err})

    def _append_changes(self, change):
        p = os.path.join(self._state_dir(), "changes.jsonl")
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps(change, ensure_ascii=False) + "\n")

    def _mode_of(self, ctx):
        """模式解析：显式构造参数优先，其次 ctx.args.mode，缺省 baseline。"""
        if self.mode:
            return self.mode
        a = getattr(ctx, "args", None)
        m = getattr(a, "mode", None) if a is not None else None
        return m or "baseline"

    # ---------- fetch（含 handshake） ----------
    def _fetch_site(self, ctx, site):
        url = site["url"]
        if not site.get("handshake"):
            return ctx.fetcher.get(url, env="AUTO", headers=BROWSER_HEADERS)
        host = urllib.parse.urlparse(url).hostname or ""
        s = self._sessions.get(host)
        if s is None:
            s = requests.Session()
            s.trust_env = False
            self._sessions[host] = s
        if not any(c.name == "signin" for c in s.cookies):
            # 两步握手：首个请求显式捕获跳转，只为拿到 signin cookie
            r0 = ctx.fetcher.get(url, env="AUTO", headers=BROWSER_HEADERS,
                                 session=s, allow_redirects=False)
            audit({"kind": "freetier_handshake", "site": site["id"],
                   "status": r0.status, "cls": r0.cls,
                   "cookies": sorted({c.name for c in s.cookies})})
        return ctx.fetcher.get(url, env="AUTO", headers=BROWSER_HEADERS, session=s)

    def _do_fetch(self, ctx, site):
        """返回 (ok, res, err, cls)。"""
        try:
            res = self._fetch_site(ctx, site)
        except Exception as e:  # noqa: BLE001
            return False, None, f"{type(e).__name__}: {str(e)[:200]}", "exception"
        if res is None:
            return False, None, "no result", "unknown"
        if (res.cls == "ok" and res.status is not None
                and 200 <= res.status < 300 and res.text):
            if site.get("handshake") and _looks_like_login(res.text):
                return False, res, "LOGIN_PAGE（握手未生效）", "unverified"
            return True, res, None, "ok"
        return False, res, (res.err or f"HTTP {res.status}"), res.cls

    # ---------- 处理单元 ----------
    def _process(self, ctx, site):
        sid = site["id"]
        url = site["url"]
        ok, res, err, cls = self._do_fetch(ctx, site)
        now = now_ts()
        meta = self._load_meta(sid)
        meta["id"] = sid
        meta["url"] = url
        meta["last_checked_ts"] = now
        if ok:
            meta["status"] = "ok"
            meta["last_error"] = None
            meta["last_ok_ts"] = now
            text = _html_to_text(res.text)
            sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
            t = _extract_title(res.text)
            if t:
                meta["title"] = t   # 每次成功刷新为最新标题（空则保留旧）
            old_sha = meta.get("last_sha")
            if old_sha is None:
                ver = int(meta.get("versions", 0)) + 1
                self._write_snapshot(site, ver, sha, text)
                meta["versions"] = ver
                meta["last_sha"] = sha
                meta["sha_history"] = [sha]
                log(f"[freetier] 基线: {sid} v{ver}（{len(text)} 字符）")
            elif sha != old_sha:
                # sha 历史以快照文件为权威（写入前重建 = v1..v(N-1)）；meta 字段为缓存
                hist = self._rebuild_sha_history(sid, meta)
                if old_sha and old_sha not in hist:
                    hist.append(old_sha)
                flap = sha in hist   # 新内容 == 历史版本（A/B 往返类抖动，标注供人甄别）
                ver = int(meta.get("versions", 0)) + 1
                diff_preview = _diff_preview(self._snap_path(sid, ver - 1), text)
                self._write_snapshot(site, ver, sha, text)
                meta["versions"] = ver
                meta["last_sha"] = sha
                meta["last_change_ts"] = now
                if sha not in hist:
                    hist.append(sha)
                meta["sha_history"] = hist
                change = {"site": sid, "vendor": site["vendor"], "url": url,
                          "ver": ver, "old_sha": old_sha[:12], "new_sha": sha[:12],
                          "flap": flap,
                          "diff_preview": diff_preview, "ts": ts_iso(now)}
                ctx.store.add_record(kind="freetier_change",
                                     key=f"{sid}#v{ver}", value=change, src=url)
                self._append_changes(change)
                log(f"[freetier] 变化: {sid} v{ver}（diff 预览 "
                    f"{len(diff_preview.splitlines())} 行"
                    + ("，flap 往返" if flap else "") + "）")
            else:
                log(f"[freetier] 无变化: {sid}")
        else:
            meta["status"] = "failed"
            meta["last_error"] = f"[{cls}] {err}"[:300]
            log(f"[freetier] 失败: {sid} → {meta['last_error']}")
        ctx.store.add_item(sid, status=meta["status"], url=url, vendor=site["vendor"])
        self._save_meta(sid, meta)
        audit({"kind": "freetier_site", "site": sid, "status": meta["status"],
               "versions": meta.get("versions", 0), "err": meta.get("last_error")})

    def _write_snapshot(self, site, ver, sha, text):
        out = (f"# {site['title']}\n# url: {site['url']}\n"
               f"# captured: {ts_iso()}\n# sha256: {sha}\n\n{text}\n")
        ok, err = atomic_write(self._snap_path(site["id"], ver), out)
        if not ok:
            audit({"kind": "freetier_snap_save_fail", "site": site["id"], "err": err})

    def _rebuild_sha_history(self, sid, meta):
        """从快照文件头部重建 sha 历史（兼容 sha_history 字段引入前的旧数据）。"""
        hist = []
        for v in range(1, int(meta.get("versions", 0)) + 1):
            p = self._snap_path(sid, v)
            try:
                with open(p, encoding="utf-8") as f:
                    for _ in range(8):
                        ln = f.readline()
                        if ln.startswith("# sha256:"):
                            hist.append(ln.split(":", 1)[1].strip())
                            break
            except Exception:  # noqa: BLE001
                continue
        return hist

    # ---------- Adapter 协议 ----------
    def iter_once(self, ctx):
        mode = self._mode_of(ctx)
        if mode == "refresh":
            site = next((s for s in self.sites if s["id"] not in self._visited), None)
            if site is None:
                return False
            self._visited.add(site["id"])
            self._process(ctx, site)
            return True
        # baseline：逐个处理"尚无 meta"的站点（失败也落 meta，不阻塞收敛）
        site = next((s for s in self.sites
                     if not os.path.exists(self._meta_path(s["id"]))), None)
        if site is None:
            return False
        self._process(ctx, site)
        return True

    def converged(self, ctx):
        if self._mode_of(ctx) == "refresh":
            return len(self._visited) >= len(self.sites)
        return all(os.path.exists(self._meta_path(s["id"])) for s in self.sites)

    # ---------- 报告 ----------
    def report(self, ctx=None):
        ts = ts_iso()
        rows = []
        ok_n = fail_n = 0
        for s in self.sites:
            meta = self._load_meta(s["id"])
            st = meta.get("status", "missing")
            ok_n += (st == "ok")
            fail_n += (st == "failed")
            rows.append((s, meta, st))
        lines = ["# freetier 采集报告", "",
                 f"- 生成时间: {ts}",
                 f"- 站点数: {len(self.sites)} | ok={ok_n} failed={fail_n} "
                 f"missing={len(self.sites) - ok_n - fail_n}",
                 f"- 快照目录: state/snapshots/<site_id>/v<N>.txt",
                 f"- 变化账本: state/changes.jsonl", "",
                 "## 站点状态", "",
                 "| id | vendor | 状态 | 版本 | sha12 | 最近变化 | 最近检查 |",
                 "|---|---|---|---|---|---|---|"]
        for s, meta, st in rows:
            lines.append(
                f"| {s['id']} | {s['vendor']} | {st} "
                f"| {meta.get('versions', 0)} | {(meta.get('last_sha') or '')[:12]} "
                f"| {ts_iso(meta['last_change_ts']) if meta.get('last_change_ts') else '-'} "
                f"| {ts_iso(meta['last_checked_ts']) if meta.get('last_checked_ts') else '-'} |")
        # 变化记录（账本尾部）
        changes = []
        cp = os.path.join(self._state_dir(), "changes.jsonl")
        if os.path.exists(cp):
            with open(cp, encoding="utf-8") as f:
                for ln in f:
                    try:
                        changes.append(json.loads(ln))
                    except Exception:  # noqa: BLE001
                        continue
        lines += ["", f"## 变化记录（共 {len(changes)} 条；最近 20 条）", ""]
        if not changes:
            lines.append("(无)")
        for c in changes[-20:]:
            tag = " **[flap·回到历史版本]**" if c.get("flap") else ""
            lines.append(f"- [{c.get('ts', '?')}] {c.get('site')} → v{c.get('ver')} "
                         f"({c.get('old_sha', '?')} → {c.get('new_sha', '?')}){tag}")
        # 失败清单
        fails = [(s, meta) for s, meta, st in rows if st == "failed"]
        lines += ["", "## 失败清单", ""]
        if not fails:
            lines.append("(无)")
        for s, meta in fails:
            lines.append(f"- {s['id']}: {meta.get('last_error', '?')}")
        # 关键词命中（对最新 ok 快照）
        lines += ["", "## 关键词命中（最新快照；词表见 sources.WATCH_WORDS）", ""]
        hits_any = False
        for s, meta, st in rows:
            if st != "ok" or not meta.get("versions"):
                continue
            try:
                with open(self._snap_path(s["id"], meta["versions"]),
                          encoding="utf-8") as f:
                    text = f.read().lower()
            except Exception:  # noqa: BLE001
                continue
            row = {w: text.count(w.lower()) for w in WATCH_WORDS}
            row = {k: v for k, v in row.items() if v}
            if row:
                hits_any = True
                lines.append(f"- {s['id']}: " +
                             ", ".join(f"{k}×{v}" for k, v in sorted(row.items())))
        if not hits_any:
            lines.append("(无)")
        body = "\n".join(lines) + "\n"
        out_path = os.path.join(self._root, "runs", "report.md")
        ok, err = atomic_write(out_path, body)
        if not ok:
            audit({"kind": "freetier_report_save_fail", "err": err})
        log(f"[freetier] 报告已写: {out_path}（ok={ok_n} failed={fail_n}）")
        return out_path
