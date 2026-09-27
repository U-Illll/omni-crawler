#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.core.engine — 调度骨架（长程模式 + keeper 契约）。

融合来源：sm-recon sm_main.cmd_loop 的收敛/休整/自然退出语义
（keeper 重启 = 新一轮判断；STALE_LIMIT 必须大于 REST_BETWEEN_ROUNDS）。

纪律：
- 迭代/收敛/停滞事件全部写审计（loop_iter / loop_converged / loop_stalled）。
- 收敛后：report → 休整 REST_BETWEEN_ROUNDS → 自然退出。
- 连续 STALL_LIMIT 轮无进展且未收敛 → 记审计后自然退出（防死转；keeper 会再拉起）。
"""
import time

from .config import REST_BETWEEN_ROUNDS
from .circuit import CircuitBreaker
from .limiter import RateLimiter
from .log import audit, init as log_init, log, runs_dir
from .store import Store

STALL_LIMIT = 5


class EngineCtx:
    """传给 adapter 的上下文（全部核心组件）。"""

    def __init__(self, engine):
        self.engine = engine
        self.store = engine.store
        self.limiter = engine.limiter
        self.breaker = engine.breaker
        self.channel = engine.channel
        self.fetcher = engine.fetcher
        self.args = engine.args

    @property
    def adapter(self):
        return self.engine.adapter


class Engine:
    def __init__(self, adapter, root=None, writer=None, args=None):
        from ..channel.manager import ChannelManager
        from ..fetch.http import HttpFetcher

        self.adapter = adapter
        self.root = root or adapter.root()
        log_init(self.root, writer=writer)
        self.args = args
        self.store = Store(runs_dir())
        self.limiter = RateLimiter()
        self.breaker = CircuitBreaker()
        self.channel = ChannelManager()
        self.fetcher = HttpFetcher(limiter=self.limiter, breaker=self.breaker,
                                   channel=self.channel)
        self.ctx = EngineCtx(self)

    # ---------- 模式 ----------
    def run_once(self):
        progressed = self.adapter.iter_once(self.ctx)
        self.store.save()
        return progressed

    def loop(self, max_iter=100):
        stall = 0
        # 休整时长可经 ctx.args.rest 覆盖（测试/演示用；默认 REST_BETWEEN_ROUNDS）
        rest = getattr(self.args, "rest", None)
        rest = float(rest) if rest is not None else REST_BETWEEN_ROUNDS
        for it in range(1, int(max_iter) + 1):
            s = self.store.summary()
            log(f"[loop] 迭代 {it}: {s}")
            audit({"kind": "loop_iter", "iter": it, "summary": s})
            try:
                progressed = self.adapter.iter_once(self.ctx)
            except KeyboardInterrupt:
                raise
            except Exception as e:  # noqa: BLE001  失败隔离：单迭代异常不终结长跑
                audit({"kind": "loop_iter_error", "iter": it,
                       "err": f"{type(e).__name__}: {str(e)[:200]}"})
                log(f"[loop] 迭代 {it} 异常（隔离，继续）: {type(e).__name__}: {e}")
                progressed = False
            self.store.save()
            if self.adapter.converged(self.ctx):
                log(f"[loop] 收敛于迭代 {it}: {self.store.summary()}")
                audit({"kind": "loop_converged", "iter": it,
                       "summary": self.store.summary()})
                try:
                    self.adapter.report(self.ctx)
                except Exception as e:  # noqa: BLE001
                    audit({"kind": "report_error",
                           "err": f"{type(e).__name__}: {str(e)[:200]}"})
                log(f"[loop] 休整 {int(rest)}s 后退出（keeper 将重启新一轮）")
                time.sleep(rest)
                return "converged"
            if not progressed:
                stall += 1
                audit({"kind": "loop_stalled", "iter": it, "stall": stall})
                if stall >= STALL_LIMIT:
                    log(f"[loop] 连续 {stall} 轮无进展且未收敛 → 退出（交给 keeper/人工）")
                    return "stalled"
            else:
                stall = 0
        log(f"[loop] 达到 max_iter={max_iter} → 退出")
        return "max_iter"
