#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""freetier adapter 冒烟：协议/握手/变化检测/失败路径/幂等（全离线，无网络）。

跑法：python3 tests/smoke_freetier.py
"""
import json
import os
import sys
import tempfile
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


class FakeFetcher:
    """与 HttpFetcher.get 同签名；behavior(url, allow_redirects, session) -> FetchResult。"""

    def __init__(self, behavior):
        self.behavior = behavior
        self.calls = []

    def get(self, url, env="AUTO", headers=None, referer=None, legacy_tls=False,
            timeout=None, session=None, detect=True, allow_redirects=True):
        self.calls.append({"url": url, "allow_redirects": allow_redirects})
        return self.behavior(url, allow_redirects, session)


def read_jsonl(path):
    recs = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for ln in f:
                try:
                    recs.append(json.loads(ln))
                except Exception:  # noqa: BLE001
                    pass
    return recs


def main():
    global PASS, FAIL
    from omni.fetch.http import FetchResult
    from omni.core.store import Store
    from omni.core.log import init as log_init
    from omni.adapters.freetier.sources import SITES
    from omni.adapters.freetier.adapter import (
        FreeTierAdapter, _html_to_text, _looks_like_login)

    # ---------- 1) sources 清单 ----------
    ids = [s["id"] for s in SITES]
    check("sources 清单（≥28）", len(SITES) >= 28, f"got {len(SITES)}")
    check("id 唯一", len(ids) == len(set(ids)))
    check("字段齐全", all(s.get("id") and s.get("vendor") and s.get("title")
                          and s.get("url", "").startswith("https://") for s in SITES))
    check("handshake 站 = ai.google.dev 两页",
          {s["id"] for s in SITES if s.get("handshake")}
          == {"google-gemini-pricing", "google-gemini-rate-limits"})

    # ---------- 2) 工具函数 ----------
    t = _html_to_text("<html><head><style>a{}</style><script>var x=1;</script>"
                      "<title>T</title></head><body><p>free tier &amp; more</p>"
                      "<div>next</div></body></html>")
    check("_html_to_text 去 script/style + 实体解码",
          "free tier & more" in t and "var x=1" not in t and "a{}" not in t)
    check("_looks_like_login 命中", _looks_like_login(
        "<title>Sign in - Google Accounts</title>") is True)
    check("_looks_like_login 不误判", _looks_like_login(
        "<html><body>Gemini API pricing</body></html>") is False)

    # ---------- 3) 离线全流程（假站点） ----------
    tmp = tempfile.mkdtemp(prefix="freetier-test-")
    log_init(tmp)   # 防 audit 落 cwd

    sites = [
        {"id": "t-a", "vendor": "T", "area": "x", "title": "A",
         "url": "https://a.example/page"},
        {"id": "t-b", "vendor": "T", "area": "x", "title": "B",
         "url": "https://b.example/page"},
    ]
    content = {
        "https://a.example/page": "<html><title>A</title><body>free tier v1</body></html>",
        "https://b.example/page": "<html><title>B</title><body>hello</body></html>",
    }

    def beh(url, allow_redirects, session):
        if url in content:
            return FetchResult(200, content[url], None, "ok")
        return FetchResult(404, "", "HTTP 404", "fatal")

    fake = FakeFetcher(beh)
    store = Store(os.path.join(tmp, "runs"))
    ctx = SimpleNamespace(fetcher=fake, store=store,
                          args=SimpleNamespace(mode="baseline"))
    a = FreeTierAdapter(root=tmp, sites=sites, mode="baseline")

    its = 0
    while not a.converged(ctx) and its < 10:
        a.iter_once(ctx)
        its += 1
    check("baseline 收敛（每站一迭代）", a.converged(ctx) and its == 2, f"its={its}")
    check("meta 已写", all(os.path.exists(a._meta_path(s["id"])) for s in sites))
    check("v1 快照已写", all(os.path.exists(a._snap_path(s["id"], 1)) for s in sites))
    check("快照含内容", "free tier v1" in open(a._snap_path("t-a", 1),
                                              encoding="utf-8").read())
    check("baseline 幂等（重跑无进展）", a.iter_once(ctx) is False)
    a_re = FreeTierAdapter(root=tmp, sites=sites, mode="baseline")
    check("baseline 重跑（新实例）立即收敛", a_re.converged(ctx))

    # ---------- 4) refresh：无变化 ----------
    ctx.args.mode = "refresh"
    a2 = FreeTierAdapter(root=tmp, sites=sites, mode="refresh")
    got = 0
    while not a2.converged(ctx) and got < 10:
        a2.iter_once(ctx)
        got += 1
    check("refresh 全量复核收敛", a2.converged(ctx) and got == 2, f"got={got}")
    check("无变化不产新版本", not os.path.exists(a2._snap_path("t-a", 2)))
    check("无变化不产记录",
          not os.path.exists(os.path.join(tmp, "state", "changes.jsonl")))

    # ---------- 5) refresh：有变化 ----------
    content["https://a.example/page"] = (
        "<html><title>A</title><body>free tier v2 bonus</body></html>")
    a3 = FreeTierAdapter(root=tmp, sites=sites, mode="refresh")
    a3.iter_once(ctx)
    a3.iter_once(ctx)
    check("变化产 v2 快照", os.path.exists(a3._snap_path("t-a", 2)))
    changes = read_jsonl(os.path.join(tmp, "state", "changes.jsonl"))
    check("变化账本 +1", len(changes) == 1 and changes[0]["site"] == "t-a"
          and changes[0]["ver"] == 2)
    check("diff 预览非空", "+free tier v2 bonus" in changes[0]["diff_preview"]
          or "free tier v2 bonus" in changes[0]["diff_preview"])
    recs = read_jsonl(os.path.join(tmp, "runs", "records.jsonl"))
    check("store 产出 freetier_change",
          any(r.get("kind") == "freetier_change" and r.get("key") == "t-a#v2"
              for r in recs))
    # flap：改回历史内容 → v3 + flap=True
    content["https://a.example/page"] = (
        "<html><title>A</title><body>free tier v1</body></html>")
    a5 = FreeTierAdapter(root=tmp, sites=sites, mode="refresh")
    a5.iter_once(ctx)
    a5.iter_once(ctx)
    ch2 = read_jsonl(os.path.join(tmp, "state", "changes.jsonl"))
    check("回历史内容 → v3 且 flap=True",
          len(ch2) == 2 and ch2[-1]["ver"] == 3 and ch2[-1].get("flap") is True,
          f"{[(c['ver'], c.get('flap')) for c in ch2]}")
    # 同内容再 refresh → 不再产出（幂等）
    a4 = FreeTierAdapter(root=tmp, sites=sites, mode="refresh")
    a4.iter_once(ctx)
    a4.iter_once(ctx)
    check("同内容重跑不再产出",
          len(read_jsonl(os.path.join(tmp, "state", "changes.jsonl"))) == 2
          and not os.path.exists(a4._snap_path("t-a", 4)))

    # ---------- 6) 失败路径 ----------
    tmp2 = tempfile.mkdtemp(prefix="freetier-test2-")
    log_init(tmp2)
    sites2 = [{"id": "f-1", "vendor": "T", "area": "x", "title": "F",
               "url": "https://f.example/page"}]
    fail_fake = FakeFetcher(lambda url, ar, s: FetchResult(err="boom", cls="transient"))
    store2 = Store(os.path.join(tmp2, "runs"))
    ctx2 = SimpleNamespace(fetcher=fail_fake, store=store2,
                           args=SimpleNamespace(mode="baseline"))
    b = FreeTierAdapter(root=tmp2, sites=sites2, mode="baseline")
    b.iter_once(ctx2)
    check("失败站收敛（meta 已写）", b.converged(ctx2))
    meta = b._load_meta("f-1")
    check("meta.status=failed", meta.get("status") == "failed"
          and "transient" in (meta.get("last_error") or ""))
    # refresh 重试成功
    ok_fake = FakeFetcher(lambda url, ar, s: FetchResult(
        200, "<html><body>recovered free tier</body></html>", None, "ok"))
    ctx2.fetcher = ok_fake
    b2 = FreeTierAdapter(root=tmp2, sites=sites2, mode="refresh")
    b2.iter_once(ctx2)
    check("refresh 重试失败站 → ok", b2._load_meta("f-1").get("status") == "ok")

    # ---------- 7) handshake（两步 + 同域复用） ----------
    tmp3 = tempfile.mkdtemp(prefix="freetier-test3-")
    log_init(tmp3)

    def hbeh(url, allow_redirects, session):
        if allow_redirects is False:
            session.cookies.set("signin", "1", domain="ai.example")
            return FetchResult(302, "<html>302</html>", None, "ok")
        return FetchResult(200, f"<html><body>page {url}</body></html>", None, "ok")

    hfake = FakeFetcher(hbeh)
    sites_h = [
        {"id": "h-1", "vendor": "H", "area": "x", "title": "H1",
         "url": "https://ai.example/p1", "handshake": True},
        {"id": "h-2", "vendor": "H", "area": "x", "title": "H2",
         "url": "https://ai.example/p2", "handshake": True},
    ]
    c = FreeTierAdapter(root=tmp3, sites=sites_h, mode="baseline")
    ctx3 = SimpleNamespace(fetcher=hfake, store=Store(os.path.join(tmp3, "runs")),
                           args=SimpleNamespace(mode="baseline"))
    c.iter_once(ctx3)
    c.iter_once(ctx3)
    seq = [(x["url"].rsplit("/", 1)[-1], x["allow_redirects"]) for x in hfake.calls]
    check("握手序列（p1: False→True；p2: True 复用 cookie）",
          seq == [("p1", False), ("p1", True), ("p2", True)], f"{seq}")
    check("握手站快照 ok", c._load_meta("h-1").get("status") == "ok")

    # ---------- 8) 登录页防误存 ----------
    tmp4 = tempfile.mkdtemp(prefix="freetier-test4-")
    log_init(tmp4)

    def lbeh(url, allow_redirects, session):
        if allow_redirects is False:
            session.cookies.set("signin", "1", domain="lg.example")
            return FetchResult(302, "302", None, "ok")
        return FetchResult(200, "<html><title>Sign in - Google Accounts</title>"
                                "<body>Use your Google Account</body></html>", None, "ok")

    lfake = FakeFetcher(lbeh)
    sites_l = [{"id": "l-1", "vendor": "L", "area": "x", "title": "L",
                "url": "https://lg.example/p", "handshake": True}]
    d = FreeTierAdapter(root=tmp4, sites=sites_l, mode="baseline")
    ctx4 = SimpleNamespace(fetcher=lfake, store=Store(os.path.join(tmp4, "runs")),
                           args=SimpleNamespace(mode="baseline"))
    d.iter_once(ctx4)
    check("登录页 → failed 不误存", d._load_meta("l-1").get("status") == "failed"
          and not os.path.exists(d._snap_path("l-1", 1)))

    # ---------- 9) Engine 装配 + report ----------
    try:
        from omni.core.engine import Engine
        tmp5 = tempfile.mkdtemp(prefix="freetier-test5-")
        e = Engine(FreeTierAdapter(root=tmp5, sites=[]),
                   args=SimpleNamespace(mode="baseline"))
        check("Engine 装配", e.store is not None and e.fetcher is not None)
    except Exception as ex:  # noqa: BLE001
        check("Engine 装配", False, f"{type(ex).__name__}: {ex}")

    out = a3.report(ctx)
    rp = os.path.join(tmp, "runs", "report.md")
    check("report 生成", os.path.exists(rp) and os.path.abspath(out) == os.path.abspath(rp))
    body = open(rp, encoding="utf-8").read()
    check("report 含站点表与变化记录",
          "| t-a |" in body and "变化记录" in body and "t-a → v2" in body)

    print(f"\n[smoke_freetier] PASS={PASS} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
