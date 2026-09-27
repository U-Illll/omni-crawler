#!/usr/bin/env python3
"""mock_primo.py — 本地 Primo pnxs API 传输层 mock（R2 / slot-bench）

设计目标
--------
在**零外网、仅 loopback**的前提下，为图书馆抓取管线提供一个行为可信的受控服务端：
可配置的响应延迟分布、可配置的错误注入（429 / 400 / 5xx）、可配置的合成数据规模，
以及一个服务端令牌桶（用于复现 2026-09-14 实测的「速率安全带 / 黄线」阶梯）。

接口契约（从 scrape-v10.1.py 的 fetch()/get_total()/extract_record() 反推）
--------------------------------------------------------------------------
请求：GET /primaws/rest/pub/pnxs
      ?vid=..&tab=default_tab&scope=MyInstitution&q=<query>&limit=<1..500>&offset=<n>
      &lang=zh_CN&mode=Basic&getMore=0&inst=..[&sort=title|date]
query 形式：`holding_call_number,begins_with,<PREFIX>`（PREFIX 可空 → 全量基线）
响应：{"info": {"totalResultsLocal": N, ...},
       "docs": [{"pnx": {"display": {"mms": [..], "title": [..], "creator": [..],
                                    "publisher": [..], "creationdate": [..],
                                    "language": [..], "type": [..]}},
                 "delivery": {"holding": [{"libraryCode":.., "mainLocation":..,
                                           "subLocation":.., "subLocationCode":..,
                                           "callNumber":.., "availabilityStatus":..}]}}]}

诊断接口（非 Primo 语义，供 bench 使用）
----------------------------------------
GET /__mock/health   → {"ok": true, ...}
GET /__mock/config   → 生效配置 + 语料统计
GET /__mock/stats    → 服务端计数器（请求数/状态码/原因/服务端延迟分位）
GET /__mock/reset    → 计数器归零
GET /__mock/shutdown → 优雅退出（bench 用它收尾，不发任何信号）

用法
----
  python3 mock_primo.py --port 0 --ready-file /tmp/ready.json      # 端口 0=自动分配
  python3 mock_primo.py --profile real-20260914 --records 40000
  python3 mock_primo.py --err-429 0.05 --lat-p50 0.12

安全边界：**只允许绑定 loopback**（127.0.0.1 / ::1 / localhost）；绑定其他地址直接拒绝启动。
"""
import argparse
import bisect
import json
import os
import random
import string
import sys
import threading
import time
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

CHARSET = list(string.digits) + list(string.ascii_uppercase)   # 36 个字符，与 scrape 的 CHARSET 一致
PATH_API = "/primaws/rest/pub/pnxs"

# 预置 profile：把「真实世界的可观测行为」固化下来，便于复现
#   ideal          : 只有延迟，无服务端限流（用于延迟测量，避免限流污染 p95）
#   real-20260914  : 复现 refs/rate-profile.md §1 的实测阶梯（5.5 req/s 零错 / 8.5 req/s 出 400）
#                    令牌桶 rate=8.1 burst=8 由两条实测共同标定：
#                      5.5 req/s × 30s → (5.5-8.1)·30 < 0 → 0 错（实测 165/165 零错 ✓）
#                      8.5 req/s × 30s → (8.5-8.1)·30 - 8 = 4 → 4 错（实测 4×400 ✓）
#   aggressive     : 更严的限流 + 429（用于「黄线」压测的上限侧）
PROFILES = {
    "ideal":         {"limit_rps": 0.0, "limit_burst": 0, "limit_status": 429},
    "real-20260914": {"limit_rps": 8.1, "limit_burst": 8, "limit_status": 400},
    "aggressive":    {"limit_rps": 5.0, "limit_burst": 5, "limit_status": 429},
    # sweep 档：与 real-20260914 同速率上限，但 burst 更小 → 秒级窗口就能看到越线错误，
    # 便于压测台在 --quick（<60s）内定位「零错误最高速率 / 黄线」。
    "sweep-20260914": {"limit_rps": 8.1, "limit_burst": 2, "limit_status": 400},
}

TITLES = ["图书馆藏要目", "中国近代史资料", "Advanced Algorithms", "材料科学导论",
          "机器学习实践", "海洋工程学报", "Regional Economics", "生物信息学基础",
          "量子力学导论", "Digital Signal Processing", "城市规划原理", "环境化学"]
CREATORS = ["李四光", "钱学森", "J. Smith", "王选", "A. Turing", "陈景润", "M. Curie", "张衡"]
PUBLISHERS = ["科学出版社", "高等教育出版社", "Springer", "Elsevier", "清华大学出版社", "Wiley"]
LANGS = ["chi", "eng", "jpn"]
TYPES = ["book", "journal", "article", "map"]
LIBS = ["SUSTC", "SUSTC_SZ", "SUSTC_ARCH"]
MAINS = ["Main", "Reference", "Reserve"]
SUBS = ["A区", "B区", "C区", "Closed"]
STATUSES = ["available", "checked_out", "in_transit"]


def nearest_rank(sorted_vals, pct):
    """最近秩法分位（与 bench 侧算法一致，便于交叉核对）。
    idx = ceil(pct/100*n) - 1，clamp 到 [0, n-1]；不做插值（保守，可复现）。"""
    n = len(sorted_vals)
    if n == 0:
        return None
    idx = max(0, min(n - 1, -(-n * pct // 100) - 1))
    return sorted_vals[idx]


class Corpus:
    """合成语料：N 条记录 → 索引为「按 callNumber 前缀可二分检索」的 token 表。

    真实性要点：
      * token = 索书号前缀（holdings 里的 callNumber 以此开头）→ 与 begins_with 语义一致；
      * 深层字符只从 fan 个字符里取 → 每次 probe 36 个字符中约 30 个为 0（真实管线大量空探测）；
      * dist=zipf 时前缀分布倾斜 → 深层出现 >LEAF_MAX 的重节点（真实全馆分布特征）；
      * len_tail：一部分记录的 token 更短（记到更上层节点）→ 叶子尺寸分布更接近真实。
    """

    def __init__(self, records, seed=20260915, depth=5, fan=6, dist="zipf",
                 alpha=1.2, len_tail=(0.60, 0.30, 0.10), title_partial_ratio=0.15,
                 corpus_file=None, seeds=None, scale=1.0):
        self.records = int(records)
        self.seed = int(seed)
        self.depth = int(depth)
        self.fan = max(1, min(36, int(fan)))
        self.dist = dist
        self.alpha = float(alpha)
        self.len_tail = len_tail
        self.title_partial_ratio = float(title_partial_ratio)
        self.corpus_file = corpus_file
        self.seed_subset = list(seeds) if seeds else None
        self.scale = float(scale)
        self.mode = "real" if corpus_file else "synth"
        if corpus_file:
            self._build_from_real()
            return
        rng = random.Random(self.seed)
        # 每层的字符池：第 0 层用全 36 字符（每个种子都有货），更深层只用 fan 个
        deep_pool = []
        for i in range(36):
            if len(deep_pool) >= self.fan:
                break
            c = CHARSET[(i * 7 + 3) % 36]
            if c not in deep_pool:
                deep_pool.append(c)
        assert len(deep_pool) == self.fan, (len(deep_pool), self.fan)
        self.deep_pool = sorted(deep_pool)
        if dist == "zipf":
            w = [1.0 / ((i + 1) ** self.alpha) for i in range(len(deep_pool))]
            tot = sum(w)
            self.weights = [x / tot for x in w]
        else:
            self.weights = [1.0 / len(deep_pool)] * len(deep_pool)

        pairs = []
        for rid in range(self.records):
            L = self.depth
            r = rng.random()
            acc = 0.0
            for k, p in enumerate(self.len_tail):
                acc += p
                if r < acc:
                    L = max(1, self.depth - k)
                    break
            tok = [rng.choice(CHARSET)]                     # 第 0 层：36 字符均匀 → 每个种子非空
            for _ in range(L - 1):
                tok.append(rng.choices(self.deep_pool, weights=self.weights, k=1)[0])
            pairs.append(("".join(tok), rid))
        pairs.sort(key=lambda x: (x[0], x[1]))
        self.tokens = [t for t, _ in pairs]
        self.rids = [r for _, r in pairs]
        # 语料统计（真实树形特征，报告里会用到）
        self.counts_cache = {}

    def total(self, prefix):
        c = self.counts_cache.get(prefix)
        if c is None:
            lo = bisect.bisect_left(self.tokens, prefix)
            hi = bisect.bisect_left(self.tokens, prefix + "\uffff")
            c = hi - lo
            self.counts_cache[prefix] = c
        return c

    def _suffix(self, rid):
        return ".%02d/%s%d" % (rid % 100, CHARSET[rid % 36], rid % 10)

    def doc(self, rid):
        tok_idx = None
        # 由 rid 找回 token：记录构建顺序即 rid，但排序后需反查；用 map 维护
        tok = self.rid_to_token[rid]
        mms = "99%09d" % rid
        nh = 1 + (rid % 3)
        holds = []
        for h in range(nh):
            holds.append({
                "libraryCode": LIBS[(rid + h) % len(LIBS)],
                "mainLocation": MAINS[(rid + h) % len(MAINS)],
                "subLocation": SUBS[(rid + h) % len(SUBS)],
                "subLocationCode": "L%d" % ((rid + h) % 5),
                "callNumber": tok + (self._suffix(rid) if h == 0 else ".%02d" % ((rid + h) % 100)),
                "availabilityStatus": STATUSES[(rid + h) % len(STATUSES)],
            })
        y = 1960 + (rid * 7) % 65
        return {
            "pnx": {"display": {
                "mms": [mms],
                "title": ["%s (%d)" % (TITLES[rid % len(TITLES)], y)],
                "creator": [CREATORS[rid % len(CREATORS)]],
                "publisher": [PUBLISHERS[rid % len(PUBLISHERS)]],
                "creationdate": [str(y)],
                "language": [LANGS[rid % len(LANGS)]],
                "type": [TYPES[rid % len(TYPES)]],
            }},
            "delivery": {"holding": holds},
            "context": "L",
        }

    def query(self, prefix, limit, offset, sort):
        """按 begins_with 语义取 docs。sort=title 时对 title_partial 命中的前缀只返回一部分
        （复现真实 API 在 title 排序下偶尔少返回、迫使管线走并行补齐路径的行为）。"""
        lo = bisect.bisect_left(self.tokens, prefix)
        hi = bisect.bisect_left(self.tokens, prefix + "\uffff")
        rids = self.rids[lo:hi]
        total = len(rids)
        if sort == "title":
            # 确定性选择（不能用 hash()：CPython 字符串哈希逐进程随机化 → 不可复现）
            if (zlib.crc32(("%s|%d" % (prefix, self.seed)).encode()) % 100) \
                    < int(self.title_partial_ratio * 100):
                rids = rids[: int(total * 0.6)]
        elif sort == "date":
            rids = sorted(rids, key=lambda r: (self.rid_to_year[r], r))
        else:
            rids = sorted(rids)          # 默认序：稳定、可复现
        page = rids[int(offset): int(offset) + int(limit)]
        return total, [self.doc(r) for r in page]

    def _build_from_real(self):
        """真实树模式：按真实**前缀树**逐节点生成语料（不是按 token 多重集展开）。

        为什么用树而不是词表：真实数据里同一个 mms 可能有多条索书号（多个前缀），
        按词表展开会让该 mms 被重复计成多条文档 → mock 的 total 比真实偏大
        （首版实测 +8%，见 receipts 的 bug#7）。按树生成则：
          * total(prefix) 与真实 totalResultsLocal **逐前缀相等**；
          * 每个节点「自留条数」own = 真实计数 - 子节点计数之和 → 天然复现真实数据的
            「不可再分前缀」现象（真实全量作业里 113 个缺口正是这个成因），
            于是 gap / 修复轮 / prefetch(limit=500) 等真实路径都会被走到。
        输入：calib_real_tree.py 产出的 prefix_nodes（{前缀: 去重 mms 计数}）。
        """
        with open(self.corpus_file, encoding="utf-8") as f:
            data = json.load(f)
        nodes = {k: int(v) for k, v in (data.get("prefix_nodes") or {}).items()}
        if not nodes:
            raise SystemExit("corpus_file 缺少 prefix_nodes: %s" % self.corpus_file)
        kids = {}
        for k in nodes:
            if len(k) > 1:
                kids.setdefault(k[:-1], []).append(k)
        for v in kids.values():
            v.sort()
        roots = sorted(k for k in nodes if len(k) == 1)
        if self.seed_subset:
            allow = set(self.seed_subset)
            roots = [r for r in roots if r in allow]
        sc = self.scale
        docs = []

        def scaled(n):
            return n if sc == 1.0 else max(1, int(round(n * sc)))

        def walk(p, budget):
            """预算式下潜：节点 p 的子树**恰好**容纳 budget 条文档（budget 初值 = 真实计数）。
            子节点按真实占比分配；若子节点之和超过预算（真实数据里同一文档可有多条索书号，
            会出现「子计数之和 > 父计数」），按比例压缩并补齐余量 → 每个前缀的 total
            与真实 totalResultsLocal 逐点相等（见 receipts 的 bug#7 修正记录）。"""
            ch = [k for k in kids.get(p, []) if nodes.get(k, 0) > 0]
            tot = sum(scaled(nodes[k]) for k in ch)
            if tot <= budget:
                alloc = {k: scaled(nodes[k]) for k in ch}
            else:
                alloc = {}
                for k in ch:
                    alloc[k] = int(scaled(nodes[k]) * budget / tot)
                rem = budget - sum(alloc.values())
                if rem > 0:
                    frac = sorted(ch, key=lambda k: (-((scaled(nodes[k]) * budget / tot)
                                                       - int(scaled(nodes[k]) * budget / tot)), k))
                    for k in frac[:rem]:
                        alloc[k] += 1
            own = budget - sum(alloc.values())
            if own > 0:
                docs.extend([p] * own)       # 自留：这些文档的索书号在本层就结束（不可再分 → 复现真实缺口）
            for k in sorted(alloc):
                if alloc[k] > 0:
                    walk(k, alloc[k])

        for r in roots:
            walk(r, scaled(nodes[r]))
        pairs = [(t, i) for i, t in enumerate(docs)]
        pairs.sort(key=lambda x: (x[0], x[1]))
        self.tokens = [t for t, _ in pairs]
        self.rids = [r for _, r in pairs]
        self.records = len(pairs)
        self.real_source = data.get("source")
        self.real_total_records = data.get("unique_mms")
        self.real_distinct_runs = data.get("distinct_call_runs")
        self.tree_nodes = len(nodes)
        self.tree_roots = roots
        self.counts_cache = {}

    def finalize(self):
        self.rid_to_token = {}
        for t, r in zip(self.tokens, self.rids):
            self.rid_to_token[r] = t
        self.rid_to_year = {r: 1960 + (r * 7) % 65 for r in range(self.records)}

    def stats(self):
        l1 = {}
        for c in CHARSET:
            l1[c] = self.total(c)
        out = {
            "mode": self.mode,
            "records": self.records,
            "seed": self.seed,
            "title_partial_ratio": self.title_partial_ratio,
            "seed_totals_min": min(l1.values()) if l1 else 0,
            "seed_totals_max": max(l1.values()) if l1 else 0,
            "distinct_tokens": len(set(self.tokens)),
            "seed_totals": {c: l1[c] for c in CHARSET if l1[c] > 0},
        }
        if self.mode == "real":
            out.update({
                "corpus_file": self.corpus_file,
                "tree_nodes": getattr(self, "tree_nodes", None),
                "tree_roots": getattr(self, "tree_roots", None),
                "generation": "prefix-tree（每前缀 own = 真实计数 - 子节点计数和）",
                "real_source": getattr(self, "real_source", None),
                "real_total_records": getattr(self, "real_total_records", None),
                "real_distinct_runs": getattr(self, "real_distinct_runs", None),
                "seed_subset": self.seed_subset,
                "scale": self.scale,
            })
        else:
            out.update({
                "depth": self.depth, "fan": self.fan, "dist": self.dist,
                "alpha": self.alpha, "len_tail": list(self.len_tail),
                "deep_pool": "".join(self.deep_pool),
            })
        return out


def simulate_expected(corpus, seeds, leaf_max=490, bulk_limit=500):
    """按 scrape-v10.1 的算法在同一语料上**预测**抓取成本：
      内部节点：1×先抓(limit=500) + 36×探测；叶子：1×批量抓取(limit=500)。
    另给出两条 unique 预测，便于与实测对齐：
      predicted_unique     = 只有叶子被抓全的部分（实测 unique 的理论下界；
                             缺口节点还能靠「先抓 limit=500」额外捞回一部分，故实测通常略高）
    bench 用它把「实测」与「预测」逐项对齐 → 证明压测台跑的是同一个工作负载。"""
    req = 0
    uniq = 0
    written = 0
    inner = 0
    leaves = 0
    stack = []
    for s in seeds:
        t = corpus.total(s)
        req += 1
        stack.append((s, t))
    while stack:
        p, tot = stack.pop()
        if tot == 0:
            continue
        if tot <= leaf_max:
            leaves += 1
            uniq += tot
            written += tot
            req += 1
        else:
            inner += 1
            req += 1 + len(CHARSET)
            covered = 0
            kids = []
            for c in CHARSET:
                ct = corpus.total(p + c)
                if ct > 0:
                    kids.append((p + c, ct))
                    covered += ct
            # 先抓（limit=500）：捞回 min(500, total) 条；缺口部分由子块覆盖
            for kp, ct in kids:
                stack.append((kp, ct))
    return {"predicted_requests": req, "predicted_unique": uniq,
            "internal_nodes": inner, "leaves": leaves,
            "unique_per_request": round(uniq / req, 3) if req else 0}


class TokenBucket:
    def __init__(self, rps, burst, status, reason="ratelimit"):
        self.rps = float(rps)
        self.burst = max(1.0, float(burst))
        self.status = int(status)
        self.reason = reason
        self.lock = threading.Lock()
        self.tokens = self.burst
        self.last = time.monotonic()
        self.rejected = 0

    def allow(self):
        if self.rps <= 0:
            return True
        now = time.monotonic()
        with self.lock:
            self.tokens = min(self.burst, self.tokens + (now - self.last) * self.rps)
            self.last = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            self.rejected += 1
            return False


class MockState:
    def __init__(self, args):
        self.args = args
        self.lock = threading.Lock()
        self.corpus = Corpus(records=args.records, seed=args.seed, depth=args.depth,
                             fan=args.fan, dist=args.dist, alpha=args.alpha,
                             title_partial_ratio=args.title_partial_ratio,
                             corpus_file=args.corpus_file, seeds=args.seed_subset,
                             scale=args.scale, len_tail=args.len_tail)
        self.corpus.finalize()
        self.bucket = TokenBucket(args.limit_rps, args.limit_burst, args.limit_status)
        self.rng = random.Random(args.seed + 977)
        self.started = time.time()
        self.n = 0
        self.by_status = {}
        self.by_reason = {}
        self.lat = []
        self.lat_ok = []
        self.seq = 0
        self.log_fh = open(args.log, "a", buffering=1) if args.log else None
        # --- R3/slot-drill 增补：运行期故障注入控制面（默认全部关闭 ⇒ 行为与 stock 逐字等价）
        self.ctrl = {
            "err429": 0.0,          # 追加 429 概率（0..1，覆盖在 args 之上）
            "err400": 0.0,
            "err5xx": 0.0,
            "probe_status": 0,      # 非 0 ⇒ 对 limit<=probe_limit 的“探测型”请求直接返回该状态码
            "probe_limit": 1,       # 探测型请求判据：limit <= 该值（scrape 的 probe 用 limit=1）
            "refuse": False,        # True ⇒ 不发响应直接断开（模拟网络中断/连接被拒）
            "note": "",
            "applied": [],          # 注入时间线（供证据归档）
        }

    def bump(self, status, reason, latency_ms, prefix, limit, sort):
        with self.lock:
            self.n += 1
            self.by_status[str(status)] = self.by_status.get(str(status), 0) + 1
            self.by_reason[reason] = self.by_reason.get(reason, 0) + 1
            if len(self.lat) < 200000:
                self.lat.append(latency_ms)
            if status == 200 and len(self.lat_ok) < 200000:
                self.lat_ok.append(latency_ms)
            if self.log_fh:
                self.log_fh.write(json.dumps({
                    "seq": self.n, "ts": time.time(), "status": status, "reason": reason,
                    "latency_ms": round(latency_ms, 3), "prefix": prefix,
                    "limit": limit, "sort": sort}, ensure_ascii=False) + "\n")

    def stats(self):
        with self.lock:
            lat = sorted(self.lat)
            lat_ok = sorted(self.lat_ok)
            return {
                "ok": True,
                "uptime_s": round(time.time() - self.started, 3),
                "requests": self.n,
                "by_status": dict(self.by_status),
                "by_reason": dict(self.by_reason),
                "ratelimit_rejected": self.bucket.rejected,
                "server_latency_all_ms": {
                    "n": len(lat),
                    "p50": nearest_rank(lat, 50),
                    "p95": nearest_rank(lat, 95),
                    "p99": nearest_rank(lat, 99),
                    "max": lat[-1] if lat else None,
                },
                "server_latency_ms": {          # 只统计 200 响应（延迟分布的真实口径）
                    "n": len(lat_ok),
                    "p50": nearest_rank(lat_ok, 50),
                    "p95": nearest_rank(lat_ok, 95),
                    "p99": nearest_rank(lat_ok, 99),
                    "max": lat_ok[-1] if lat_ok else None,
                },
                "config": {k: v for k, v in vars(self.args).items() if k not in ("log",)},
                "ctrl": dict(self.ctrl),
                "corpus": self.corpus.stats(),
            }

    def reset(self):
        with self.lock:
            self.n = 0
            self.by_status = {}
            self.by_reason = {}
            self.lat = []


def make_handler(state):
    args = state.args

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "mock-primo/1.0"

        def log_message(self, *a):        # 静音默认 stderr 访问日志
            if args.verbose:
                sys.stderr.write("mock: " + (a[0] % a[1:]) + "\n")

        # ---- helpers -------------------------------------------------
        def _send(self, status, payload, extra=None):
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            if extra:
                for k, v in extra.items():
                    self.send_header(k, v)
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def _sleep_latency(self):
            if args.lat_p50 <= 0 and args.lat_overhead <= 0:
                return 0.0
            if args.lat_sigma <= 0:
                d = args.lat_p50
            else:
                d = state.rng.lognormvariate(0.0, args.lat_sigma)
                # lognormal 中位数=1 → 乘以 p50 即得中位数为 p50 的分布
                d = d * args.lat_p50
            d += args.lat_overhead
            d = max(0.0, d)
            time.sleep(d)
            return d * 1000.0

        # ---- endpoints -----------------------------------------------
        def do_GET(self):
            u = urlparse(self.path)
            if u.path.startswith("/__mock/"):
                return self._diagnostic(u.path, parse_qs(u.query, keep_blank_values=True))
            if u.path != PATH_API:
                return self._send(404, {"errors": [{"code": "not-found", "path": u.path}]})
            return self._api(parse_qs(u.query, keep_blank_values=True))

        def _diagnostic(self, path, q=None):
            if path == "/__mock/health":
                return self._send(200, {"ok": True, "pid": os.getpid()})
            if path == "/__mock/fault":
                # R3/slot-drill：运行期故障注入控制面（仅 loopback，仅 drill 自建 mock 使用）
                #   /__mock/fault?err429=1.0&probe_status=404&refuse=1&note=...
                c = state.ctrl
                q = q or {}
                def one(name, cast):
                    v = (q.get(name) or [None])[0]
                    if v is None or v == "":
                        return None
                    return cast(v)
                upd = {}
                for name, cast in (("err429", float), ("err400", float), ("err5xx", float),
                                   ("probe_status", int), ("probe_limit", int)):
                    v = one(name, cast)
                    if v is not None:
                        upd[name] = v
                v = one("refuse", int)
                if v is not None:
                    upd["refuse"] = bool(v)
                v = (q.get("note") or [None])[0]
                if v is not None:
                    upd["note"] = v
                if "clear" in q:
                    upd = {"err429": 0.0, "err400": 0.0, "err5xx": 0.0,
                           "probe_status": 0, "refuse": False, "note": ""}
                c.update(upd)
                c["applied"].append({"ts": time.time(), **upd})
                return self._send(200, {"ok": True, "ctrl": {k: v for k, v in c.items()}})
            if path == "/__mock/config":
                cfg = {"ok": True, "config": vars(args), "corpus": state.corpus.stats()}
                if args.seed_subset:
                    cfg["expected_crawl"] = simulate_expected(state.corpus, args.seed_subset)
                return self._send(200, cfg)
            if path == "/__mock/stats":
                return self._send(200, state.stats())
            if path == "/__mock/reset":
                state.reset()
                state.bucket.rejected = 0
                return self._send(200, {"ok": True, "reset": True})
            if path == "/__mock/shutdown":
                self._send(200, {"ok": True, "shutdown": True})
                threading.Thread(target=self.server.shutdown, daemon=True).start()
                return
            return self._send(404, {"ok": False, "error": "unknown diagnostic endpoint"})

        def _api(self, q):
            t0 = time.perf_counter()
            limit = q.get("limit", ["1"])[0]
            offset = q.get("offset", ["0"])[0]
            sort = q.get("sort", [None])[0]
            query = q.get("q", [""])[0]
            vid = q.get("vid", [None])[0]
            prefix = ""

            def fail(status, reason):
                state.bump(status, reason, (time.perf_counter() - t0) * 1000.0, prefix,
                           limit, sort)
                return self._send(status, {"errors": [{"code": reason, "msg":
                                                       "mock-primo: %s" % reason}]})

            # (1) 语义/参数错误 → 确定性 400（复现真实 API 的「400 出现条件」）
            if not vid:
                return fail(400, "malformed-missing-vid")
            try:
                li = int(limit)
                off = int(offset)
            except (TypeError, ValueError):
                return fail(400, "malformed-non-integer-limit")
            if li < 1 or off < 0:
                return fail(400, "malformed-range")
            if li > 500:
                return fail(400, "limit-too-large")     # 真实 Primo 单页上限 500
            if query == "":
                prefix = ""
            elif query.startswith("holding_call_number,begins_with,"):
                prefix = query[len("holding_call_number,begins_with,"):]
                if any(c not in CHARSET for c in prefix):
                    return fail(400, "malformed-prefix-charset")
            else:
                return fail(400, "unsupported-query-syntax")

            # (1.5) R3/slot-drill 运行期故障注入（默认值全关 ⇒ 与 stock mock 逐字等价）
            c = state.ctrl
            if c["refuse"]:
                state.bump(-1, "inject-refuse", (time.perf_counter() - t0) * 1000.0,
                           prefix, limit, sort)
                self.close_connection = True
                try:
                    self.connection.close()
                except Exception:
                    pass
                return
            if c["probe_status"] and li <= c["probe_limit"]:
                return fail(c["probe_status"], "inject-probe-dead")

            # (2) 服务端令牌桶（限流）：同一把锁，先于随机注入
            if not state.bucket.allow():
                return fail(state.bucket.status, state.bucket.reason)

            # (3) 错误注入（ctl 通道与启动参数通道叠加，ctl 优先判定）
            with state.lock:
                state.seq += 1
                seq = state.seq
            if c["err429"] or c["err400"] or c["err5xx"]:
                rc = state.rng.random()
                if c["err429"] and rc < c["err429"]:
                    return fail(429, "inject-429-ctl")
                if c["err400"] and rc < c["err429"] + c["err400"]:
                    return fail(400, "inject-400-ctl")
                if c["err5xx"] and rc < c["err429"] + c["err400"] + c["err5xx"]:
                    return fail(503, "inject-503-ctl")
            if args.err_first_n and seq <= args.err_first_n:
                return fail(args.err_first_status, "inject-first-n")
            if args.err_every_n and seq % args.err_every_n == 0:
                return fail(args.err_every_status, "inject-every-n")
            r = state.rng.random()
            if args.err_429 and r < args.err_429:
                return fail(429, "inject-429")
            if args.err_400 and r < args.err_429 + args.err_400:
                return fail(400, "inject-400")
            if args.err_5xx and r < args.err_429 + args.err_400 + args.err_5xx:
                return fail(args.err_5xx_status, "inject-5xx")

            # (4) 正常响应
            self._sleep_latency()
            total, docs = state.corpus.query(prefix, li, off, sort)
            latency_ms = (time.perf_counter() - t0) * 1000.0
            page = 1 + (off // li if li else 0)
            state.bump(200, "ok", latency_ms, prefix, li, sort)
            return self._send(200, {
                "info": {
                    "totalResultsLocal": total,
                    "totalResultsPC": total,
                    "total": total,
                    "page": page,
                    "perPage": li,
                    "lastPage": (off + len(docs)) >= total,
                },
                "docs": docs,
                "timing": {"mock_ms": round(latency_ms, 3)},
            }, extra={"X-Mock-Latency-Ms": "%.2f" % latency_ms})

    return Handler


def build_parser():
    p = argparse.ArgumentParser(description="loopback-only Primo pnxs mock")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=0, help="0=自动分配")
    p.add_argument("--profile", default="ideal", choices=sorted(PROFILES))
    # 语料
    p.add_argument("--records", type=int, default=40000)
    p.add_argument("--seed", type=int, default=20260915)
    p.add_argument("--depth", type=int, default=5, help="token 最大长度（树深，synth 模式）")
    p.add_argument("--fan", type=int, default=6, help="深层每节点非空子字符数（synth 模式）")
    p.add_argument("--dist", default="zipf", choices=["uniform", "zipf"])
    p.add_argument("--alpha", type=float, default=1.2)
    p.add_argument("--corpus-file", default=None,
                   help="真实树语料（calib_real_tree.py 产出）→ 语料形状与生产一致")
    p.add_argument("--seed-subset", default=None,
                   help="只保留以这些字符开头的语料（逗号分隔，如 A,E,S）；配合 bench 的种子参数")
    p.add_argument("--scale", type=float, default=1.0, help="多重集缩放系数（真实树模式）")
    p.add_argument("--len-tail", default="0.60,0.30,0.10",
                   help="R3/slot-drill 增补：token 长度分布（depth, depth-1, depth-2 … 的占比）。"
                        "默认与 stock mock 一致；设为 1.0 即「满深树」（无短 token ⇒ 无"
                        "天然缺口，演练不会把时间耗在空转的复验轮上）")
    p.add_argument("--title-partial-ratio", type=float, default=0.15,
                   help="sort=title 时少返回的前缀比例（触发管线并行补齐路径）")
    # 延迟
    p.add_argument("--lat-p50", type=float, default=0.115, help="响应延迟中位数（秒）")
    p.add_argument("--lat-sigma", type=float, default=0.55, help="lognormal sigma（0=固定延迟）")
    p.add_argument("--lat-overhead", type=float, default=0.005, help="固定额外开销（秒）")
    # 错误注入
    p.add_argument("--err-429", type=float, default=0.0)
    p.add_argument("--err-400", type=float, default=0.0)
    p.add_argument("--err-5xx", type=float, default=0.0)
    p.add_argument("--err-5xx-status", type=int, default=503)
    p.add_argument("--err-every-n", type=int, default=0, help="确定性注入：每 n 个请求错 1 个")
    p.add_argument("--err-every-status", type=int, default=429)
    p.add_argument("--err-first-n", type=int, default=0)
    p.add_argument("--err-first-status", type=int, default=429)
    # 服务端限流
    p.add_argument("--limit-rps", type=float, default=None)
    p.add_argument("--limit-burst", type=float, default=None)
    p.add_argument("--limit-status", type=int, default=None)
    # 观测
    p.add_argument("--ready-file", default=None, help="启动完成后写 {port,pid} 的 JSON")
    p.add_argument("--stats-file", default=None, help="退出时写服务端统计 JSON")
    p.add_argument("--log", default=None, help="逐请求 jsonl 日志（可选）")
    p.add_argument("--verbose", action="store_true")
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    if isinstance(args.seed_subset, str):
        args.seed_subset = [c for c in args.seed_subset.replace(",", "") if c.strip()]
        if not args.seed_subset:
            args.seed_subset = None
    if isinstance(args.len_tail, str):
        parts = [x for x in args.len_tail.replace(" ", "").split(",") if x != ""]
        vals = [float(x) for x in parts]
        total = sum(vals)
        if total <= 0:
            raise SystemExit("--len-tail 之和必须 > 0")
        args.len_tail = tuple(v / total for v in vals)
    prof = PROFILES[args.profile]
    if args.limit_rps is None:
        args.limit_rps = prof["limit_rps"]
    if args.limit_burst is None:
        args.limit_burst = prof["limit_burst"]
    if args.limit_status is None:
        args.limit_status = prof["limit_status"]

    # 红线：只允许 loopback
    if args.host not in ("127.0.0.1", "::1", "localhost"):
        print("REFUSE: mock_primo 只允许绑定 loopback，收到 host=%r" % args.host, file=sys.stderr)
        return 2

    state = MockState(args)
    handler = make_handler(state)
    httpd = ThreadingHTTPServer((args.host, args.port), handler)
    httpd.daemon_threads = True
    port = httpd.server_address[1]
    info = {"port": port, "pid": os.getpid(), "host": args.host, "profile": args.profile}
    if args.ready_file:
        tmp = args.ready_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(info, f)
        os.replace(tmp, args.ready_file)
    print("MOCK_READY " + json.dumps(info), flush=True)
    try:
        httpd.serve_forever(poll_interval=0.1)
    except KeyboardInterrupt:
        pass
    finally:
        if args.stats_file:
            with open(args.stats_file, "w") as f:
                json.dump(state.stats(), f, ensure_ascii=False, indent=1)
        if state.log_fh:
            state.log_fh.close()
        httpd.server_close()
        print("MOCK_STOPPED " + json.dumps({"requests": state.n}), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
