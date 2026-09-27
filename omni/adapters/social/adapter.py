#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.adapters.social — 社媒群号收集适配器（sm-recon 迁移版）。

迁移说明（2026-09-27）：
- 全部业务模块（sm_search/sm_extract/sm_wechat/sm_qr/sm_qq/sm_sources/sm_main…）
  自 sm-recon src 原样迁入；sm_common/sm_fetch/sm_llm 已替换为 omni 框架适配层。
- 本文件把 sm_main 的 loop 循环体封装为 omni Adapter 协议：
    iter_once = 一个迭代（消化 pending / 推进搜索 / 微信通道 / 种子站）
    converged = 全部轮次 + 种子 + 微信完成 + pending 空
- 原 CLI 入口保持可用（main.py 转发 sm_main）。
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from ..base import Adapter  # noqa: E402


class SocialAdapter(Adapter):
    name = "social"

    def __init__(self):
        self._booted = False
        self.rounds = [1, 2, 3, 4, 5]
        self.max_pages = 80
        self.max_seed = 80
        self.wx_budget = 8
        self.store = None
        self.fetcher = None

    def root(self):
        return _HERE

    def _ensure(self, ctx):
        if self._booted:
            return
        import sm_main  # noqa: E402  （惰性导入：仅实际运行时加载）
        a = getattr(ctx, "args", None)
        if a is not None:
            if getattr(a, "rounds", None):
                self.rounds = [int(x) for x in str(a.rounds).split(",")]
            self.max_pages = int(getattr(a, "max_pages", self.max_pages))
            self.max_seed = int(getattr(a, "max_seed", self.max_seed))
        self.store = sm_main.Store()
        self.fetcher = sm_main.make_fetcher()
        self._booted = True

    def iter_once(self, ctx):
        self._ensure(ctx)
        import sm_main
        import sm_sources  # noqa: E402
        store, fetcher = self.store, self.fetcher
        progressed = False
        s = store.summary()
        if s["pending"] > 0:
            n = sm_main.stage_crawl(store, fetcher, self.max_pages)
            progressed = progressed or n > 0
        else:
            progressed = sm_main.stage_search(store, fetcher, self.rounds) or progressed
            wx_next = [q for q in sm_sources.WECHAT_QUERIES
                       if not store.query_done(f"wx:{q}")]
            if wx_next:
                try:
                    import sm_wechat  # noqa: E402
                    ch = sm_wechat.WechatChannel(interval=4.5)
                    n = sm_main.run_wechat_one(store, ch, wx_next[0],
                                               budget=self.wx_budget, with_qr=True)
                    progressed = progressed or n > 0
                except Exception as e:  # noqa: BLE001  （wechat 依赖缺失时隔离）
                    from sm_common import audit
                    audit({"kind": "wx_channel_skip",
                           "err": f"{type(e).__name__}: {str(e)[:120]}"})
            if self.max_seed:
                n = sm_main.stage_seed(store, fetcher, min(10, self.max_seed))
                progressed = progressed or n > 0
        store.save()
        return progressed

    def converged(self, ctx):
        self._ensure(ctx)
        import sm_sources  # noqa: E402
        store = self.store
        if store is None:
            return False
        all_q = all(store.query_done(q) for r in self.rounds
                    for q in sm_sources.queries_for(r))
        all_seed = all(store.has_url(u) for u, _ in sm_sources.SEED_URLS)
        all_wx = all(store.query_done(f"wx:{q}") for q in sm_sources.WECHAT_QUERIES)
        return all_q and all_seed and all_wx and store.summary()["pending"] == 0

    def report(self, ctx):
        self._ensure(ctx)
        import sm_main  # noqa: E402
        sm_main.cmd_report(getattr(ctx, "args", None))
