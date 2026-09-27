#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sm_main — 主调度（v0.2：stage 拆分 + loop 长程模式）

用法：
  python3 sm_main.py run --rounds 1,2 [--max-pages 120] [--max-seed 60]
  python3 sm_main.py loop [--max-iter 50]     # 长程：搜索↔抓取迭代直到收敛
  python3 sm_main.py wechat [--max-wx-articles 30]
  python3 sm_main.py report | status
"""
import argparse
import csv
import json
import os
import re
import sys
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sm_common import RUNS, log, jitter_sleep, audit
from sm_fetch import Fetcher, RateLimiter, normalize_url
from sm_search import search_with_failover, uncloak
from sm_extract import extract_groups, relevance, confidence
from sm_store import Store
import sm_sources

ALL_ROUNDS = "1,2,3,4,5"


def make_fetcher():
    return Fetcher(limiter=RateLimiter(base=2.5, jitter=1.5))


# ---------------- Stage: 搜索 ----------------
def stage_search(store, fetcher, rounds):
    done_any = False
    for r in rounds:
        for q in sm_sources.queries_for(r):
            if store.query_done(q):
                continue
            log(f"[search-r{r}] {q}")
            res = search_with_failover(fetcher, q)
            urls = []
            for item in res["results"]:
                u = item.get("raw")
                if not u:
                    continue
                if u.startswith("http"):
                    urls.append(u)
                elif u.startswith("/"):
                    base = "https://www.so.com" if res["engine"] == "so360" else "https://www.sogou.com"
                    urls.append(base + u)
            for u in urls:
                store.add_pending([u])
            store.mark_query_done(q)
            done_any = True
            jitter_sleep(0.8, 1.2)
    return done_any


# ---------------- Stage: 抓取 ----------------
def stage_crawl(store, fetcher, max_pages):
    fetched = 0
    while fetched < max_pages:
        batch = store.pop_pending(n=min(30, max_pages - fetched))
        if not batch:
            break
        for u in batch:
            real = uncloak(fetcher, u)
            if any(k in real for k in ("so.com/link", "sogou.com/link", "baidu.com/link")):
                store.add_page(u, "crawl", "", 0, 0, "uncloak_fail", "could not uncloak")
                continue
            if store.has_url(real):
                continue
            try:
                status, text, err = fetcher.get(real, referer="https://www.so.com/")
            except Exception as e:  # noqa: BLE001  （BlockedError 等：失败隔离，放回待抓池，不崩溃）
                store.add_pending([u])
                audit({"kind": "crawl_skip", "url": u[:200], "err": f"{type(e).__name__}: {str(e)[:100]}"})
                continue
            title = ""
            n_leads = 0
            rel = 0.0
            if text:
                m = re.search(r"<title[^>]*>(.*?)</title>", text, re.S)
                title = re.sub(r"\s+", " ", m.group(1)).strip() if m else ""
                rel = relevance(text)
                leads = extract_groups(text, real)
                for ld in leads:
                    store.add_lead(ld["kind"], ld["value"], ld["evidence"], real, rel,
                                   confidence(ld["kind"], ld["evidence"], rel))
                    n_leads += 1
            ok = err is None
            store.add_page(real, "crawl", title, rel, n_leads, "ok" if ok else "err", err)
            fetched += 1
            if fetched % 10 == 0:
                store.save()
                log(f"[crawl] 进度: {fetched}/{max_pages} pages, leads={store.summary()['leads']}")
            if n_leads:
                log(f"[lead+] {real} → {n_leads} 条线索 (rel={rel})")
    return fetched


# ---------------- Stage: 种子站 ----------------
def stage_seed(store, fetcher, max_seed):
    seed_done = 0
    for url, src in sm_sources.SEED_URLS:
        if seed_done >= max_seed:
            break
        if store.has_url(url):
            continue
        # 种子站内部信号：需要抓取（失败隔离）
        try:
            status, text, err = fetcher.get(url)
        except Exception as e:  # noqa: BLE001
            audit({"kind": "seed_skip", "url": url, "err": f"{type(e).__name__}: {str(e)[:100]}"})
            continue
        if not text:
            store.add_page(url, src, "", 0, 0, "err", err)
            continue
        m = re.search(r"<title[^>]*>(.*?)</title>", text, re.S)
        title = re.sub(r"\s+", " ", m.group(1)).strip() if m else ""
        leads = extract_groups(text, url)
        rel = relevance(text)
        for ld in leads:
            store.add_lead(ld["kind"], ld["value"], ld["evidence"], url, rel,
                           confidence(ld["kind"], ld["evidence"], rel))
        store.add_page(url, src, title, rel, len(leads))
        seed_done += 1
        links = re.findall(r'href="([^"]+)"', text)
        picks = []
        for l in links:
            lu = normalize_url(l, url)
            if not lu or not lu.startswith("http"):
                continue
            dom = urllib.parse.urlparse(lu).netloc
            if any(dom.endswith(d) for d in sm_sources.SEED_DOMAINS):
                if not store.has_url(lu) and lu not in picks:
                    picks.append(lu)
        store.add_pending(picks[: sm_sources.SEED_MAX_PAGES_PER_DOMAIN])
    return seed_done


# ---------------- 命令 ----------------
def cmd_run(args):
    store = Store()
    fetcher = make_fetcher()
    rounds = [int(x) for x in args.rounds.split(",")]
    stage_search(store, fetcher, rounds)
    stage_crawl(store, fetcher, args.max_pages)
    if args.max_seed:
        stage_seed(store, fetcher, args.max_seed)
    store.save()
    log(f"[done] {store.summary()}")
    cmd_report(args)


def cmd_loop(args):
    """长程模式：迭代 搜索↔抓取↔种子↔微信 直到收敛（供 keeper 看护）"""
    store = Store()
    fetcher = make_fetcher()
    rounds = [int(x) for x in args.rounds.split(",")]
    wx_ch = None
    for it in range(1, args.max_iter + 1):
        s = store.summary()
        log(f"[loop] 迭代 {it}: {s}")
        audit({"kind": "loop_iter", "iter": it, "summary": s})
        progressed = False
        # 1) 先消化 pending
        if s["pending"] > 0:
            n = stage_crawl(store, fetcher, args.max_pages)
            progressed = progressed or n > 0
        else:
            # 2) pending 空 → 推进搜索
            progressed = stage_search(store, fetcher, rounds) or progressed
            # 2.5) 微信通道推进（每迭代处理 1 个未完成查询）
            if wx_ch is None:
                import sm_wechat
                wx_ch = sm_wechat.WechatChannel(interval=4.5)
            wx_next = [q for q in sm_sources.WECHAT_QUERIES if not store.query_done(f"wx:{q}")]
            if wx_next:
                n = run_wechat_one(store, wx_ch, wx_next[0], budget=8, with_qr=True)
                progressed = progressed or n > 0
            # 3) 种子站（每轮补一点点）
            seed_now = min(10, args.max_seed)
            if seed_now:
                n = stage_seed(store, fetcher, seed_now)
                progressed = progressed or n > 0
        store.save()
        # 终止判定：所有轮次完成 + pending 空 + 种子/微信全部完成
        s2 = store.summary()
        all_q = all(store.query_done(q) for r in rounds for q in sm_sources.queries_for(r))
        all_seed = all(store.has_url(u) for u, _ in sm_sources.SEED_URLS)
        all_wx = all(store.query_done(f"wx:{q}") for q in sm_sources.WECHAT_QUERIES)
        if all_q and all_seed and all_wx and s2["pending"] == 0:
            log(f"[loop] 收敛于迭代 {it}: {s2}（巡检休整 900s，之后重启新一轮判断）")
            audit({"kind": "loop_converged", "iter": it, "summary": s2})
            import time as _t
            _t.sleep(900)
            break
        if not progressed:
            log("[loop] 本轮无推进，休眠 30s")
            import time as _t
            _t.sleep(30)
    cmd_report(args)


def cmd_report(args):
    store = Store()
    leads = list(store.leads.values())
    leads.sort(key=lambda x: (x.get("conf") or 0), reverse=True)
    out_md = os.path.join(RUNS, "leads-report.md")
    out_csv = os.path.join(RUNS, "leads.csv")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("# 群线索报告（v0.2）\n\n")
        f.write(f"汇总: {json.dumps(store.summary(), ensure_ascii=False)}\n\n")
        f.write("| kind | value | conf | rel | src | evidence |\n|---|---|---|---|---|---|\n")
        for ld in leads[:800]:
            ev = (ld.get("evidence") or "").replace("|", "／")[:110]
            src = (ld.get("src") or "")[:80]
            f.write(f"| {ld['kind']} | {ld['value']} | {ld.get('conf')} | {ld.get('rel')} | {src} | {ev} |\n")
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["kind", "value", "conf", "rel", "src", "sources", "evidence"])
        for ld in leads:
            w.writerow([ld["kind"], ld["value"], ld.get("conf"), ld.get("rel"),
                        ld.get("src"), ";".join(ld.get("sources", [])[:5]),
                        (ld.get("evidence") or "")[:300]])
    log(f"[report] {out_md} / {out_csv}（{len(leads)} 条线索）")


def cmd_status(args):
    store = Store()
    print(json.dumps(store.summary(), ensure_ascii=False, indent=1))


WECHAT_PRIO = re.compile(r"群|加群|迎新|新生|报到|招新|活动|加入|扫码|报名|申请|交流")


def _wx_score(a):
    s = 0
    if WECHAT_PRIO.search(a["title"]):
        s += 2
    if a.get("digest") and WECHAT_PRIO.search(a["digest"]):
        s += 1
    return s


def run_wechat_one(store, ch, q, budget, with_qr=False):
    """处理单个微信查询（供 cmd_wechat / cmd_loop 共用）；返回深挖文章数"""
    key = f"wx:{q}"
    if store.query_done(key):
        return 0
    try:
        arts = ch.search(q)
    except Exception as e:  # noqa: BLE001
        log(f"[wechat] 搜索失败 {q}: {e}")
        return 0
    arts.sort(key=_wx_score, reverse=True)
    log(f"[wechat] {q} → {len(arts)} 篇（高分：{sum(1 for a in arts if _wx_score(a) > 0)}）")
    fetched = 0
    for a in arts:
        if fetched >= budget:
            break
        head = f"{a['title']} {a['digest']}"
        hrel = relevance(head)
        for ld in extract_groups(head, "wx-list:" + a["href"][:100]):
            store.add_lead(ld["kind"], ld["value"], ld["evidence"],
                           "wechat-list", hrel, confidence(ld["kind"], ld["evidence"], hrel))
        if store.has_url(a["href"]):
            continue
        if _wx_score(a) == 0 and fetched >= budget * 0.7:
            continue  # 低分文章只在预算宽裕时深挖
        try:
            mp = ch.resolve(a["href"])
            if not mp:
                store.add_page(a["href"], "wechat", a["title"], 0, 0, "resolve_fail", None)
                continue
            art = ch.article(mp)
            # 环境验证页 = 临时失败：本轮跳过，不记录（下次重试）
            if "环境异常" in art["html"] or "去验证" in art["html"]:
                log("[wechat] 环境验证页 → 本轮跳过")
                continue
            # 无 js_content = 确定性无正文（迁移/删除/限制）：记录 href 终止重试
            if 'id="js_content"' not in art["html"]:
                store.add_page(a["href"], "wechat", a["title"], 0, 0, "no_content", None)
                continue
            rel = relevance(art["text"])
            n = 0
            for ld in extract_groups(art["html"], mp):
                store.add_lead(ld["kind"], ld["value"], ld["evidence"], mp, rel,
                               confidence(ld["kind"], ld["evidence"], rel))
                n += 1
            # 二维码扫描（微信群/QQ 群入口的关键手段）
            if with_qr:
                try:
                    import sm_qr
                    for img_url, img_path, codes in sm_qr.scan_html_for_qr(art["html"], max_images=12):
                        for c in codes:
                            c = (c or "").strip()
                            # QQ 加群链接 → 直接解析出群号（qm.qq.com，纯 HTTP 可解）
                            if "qm.qq.com" in c:
                                try:
                                    import sm_qq
                                    r = sm_qq.resolve_qm(c)
                                    if r:
                                        store.add_lead("qq_num", r["group_uin"],
                                                       f"QR→加群链接解析: {c[:140]}", mp,
                                                       max(rel, 0.8), 0.9)
                                        n += 1
                                        continue
                                except Exception as e2:  # noqa: BLE001
                                    log(f"[wechat] qm 解析失败: {str(e2)[:60]}")
                            store.add_lead("qr_content", c[:150],
                                           f"QR 图片: {img_url[:100]}", mp, rel, 0.5)
                            n += 1
                except Exception as e:  # noqa: BLE001
                    log(f"[wechat] QR 扫描失败: {str(e)[:80]}")
            store.add_page(mp, "wechat", art["title"], rel, n)
            fetched += 1
            if n:
                log(f"[wechat-lead+] {art['title'][:50]} → {n} 条")
        except Exception as e:  # noqa: BLE001
            log(f"[wechat] 文章失败: {type(e).__name__} {str(e)[:80]}")
    store.mark_query_done(key)
    store.save()
    return fetched


def cmd_wechat(args):
    import sm_wechat
    store = Store()
    ch = sm_wechat.WechatChannel(interval=args.wx_interval)
    total = 0
    for q in sm_sources.WECHAT_QUERIES:
        total += run_wechat_one(store, ch, q, args.max_wx_articles, with_qr=args.with_qr)
    log(f"[wechat] 完成: 文章 {total} 篇, summary={store.summary()}")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")
    p_run = sub.add_parser("run")
    p_run.add_argument("--rounds", default="1")
    p_run.add_argument("--max-pages", type=int, default=120)
    p_run.add_argument("--max-seed", type=int, default=60)
    p_run.set_defaults(func=cmd_run)
    p_loop = sub.add_parser("loop")
    p_loop.add_argument("--rounds", default=ALL_ROUNDS)
    p_loop.add_argument("--max-iter", type=int, default=60)
    p_loop.add_argument("--max-pages", type=int, default=80)
    p_loop.add_argument("--max-seed", type=int, default=80)
    p_loop.set_defaults(func=cmd_loop)
    p_rep = sub.add_parser("report")
    p_rep.set_defaults(func=cmd_report)
    p_st = sub.add_parser("status")
    p_st.set_defaults(func=cmd_status)
    p_wx = sub.add_parser("wechat")
    p_wx.add_argument("--max-wx-articles", type=int, default=30)
    p_wx.add_argument("--wx-interval", type=float, default=4.5)
    p_wx.add_argument("--with-qr", action="store_true", help="对文章图片做二维码扫描")
    p_wx.set_defaults(func=cmd_wechat)
    args = ap.parse_args()
    if not args.cmd:
        ap.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
