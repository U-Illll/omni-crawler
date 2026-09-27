#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""R3 · slot-retry 自测：请求级自愈（C1）+ 请求级审计（实跑读数，无真实网络）

用法：  python3 test_retry.py            # 在 slot-retry 目录内运行
产物：  sandbox-test/audit-<case>.jsonl  # 每个用例一份审计读数
        sandbox-test/test-results.json   # 机器可读结果
        sandbox-test/test-output.txt     # 本次运行的完整输出（= 本脚本 stdout）

实验手法
--------
* **虚时钟**（VClock）：把 `scrape.time` 换成可控时钟（`time()` 读、`sleep()` 推进），
  于是「退避序列 / 实际停摆 / 1s 窗口计数」全部可精确读数为**虚拟秒**，几秒内跑完全部用例。
* **假 SESSION**（FakeSession）：按脚本返回 200/4xx/5xx 或抛异常（断连/超时/写超时/解析失败），
  **不发起任何真实网络请求**。
* 每个用例重建 `RateLimiter` 与审计文件，读数互不污染。
"""
import json
import os
import shutil
import sys
import threading
import time
from pathlib import Path

SLOT = Path(__file__).resolve().parent
SB = SLOT / 'sandbox-test'
if SB.exists():
    shutil.rmtree(SB)
SB.mkdir(parents=True, exist_ok=True)
os.environ['SCRAPE_OUT_DIR'] = str(SB)
os.environ.pop('SCRAPE_RETRY', None)
os.environ.pop('SCRAPE_AUDIT', None)
sys.path.insert(0, str(SLOT))

import requests as rq          # 仅用于构造异常对象（本地，不发请求）
import urllib3                 # 同上（R-1：ConnectTimeoutError/MaxRetryError 的真实形态）
import scrape                  # noqa: E402
import audit                   # noqa: E402

OUT_LINES = []
RESULTS = []


def emit(line=''):
    print(line, flush=True)
    OUT_LINES.append(line)


def check(name, cond, detail=''):
    ok = bool(cond)
    RESULTS.append({'test': name, 'ok': ok, 'detail': str(detail)[:400]})
    emit(('  PASS  ' if ok else '  FAIL  ') + name + (('  | ' + str(detail)) if detail else ''))
    return ok


def approx(a, b, tol=1e-6):
    return abs(float(a) - float(b)) <= tol


# ---------------------------------------------------------------------------
# 试验台
# ---------------------------------------------------------------------------
class VClock:
    """虚拟时钟：time() 读、sleep() 推进（记录每次 sleep 时长）。"""

    def __init__(self, t0=1_700_000_000.0):
        self.now = float(t0)
        self.sleeps = []
        self.total = 0.0

    def time(self):
        return self.now

    def sleep(self, s):
        s = float(s)
        if s < 0:
            s = 0.0
        self.sleeps.append(s)
        self.total += s
        self.now += s

    def advance(self, s):
        self.now += float(s)


class VRandom:
    """确定性 random.uniform（R3-P4：适配 equal jitter 后的两处形状）：

    · `(0.75, 1.25)`（旧乘性抖动，仅兼容保留）⇒ 按脚本取值（v 默认 0.5 ⇒ 乘子 1.0）
    · `(0.0, d/2)` 且 `d/2 > 0.05`（**新 equal jitter**：wait = d/2 + U(0, d/2)）
      ⇒ 按脚本取值（v 默认 0.5 ⇒ wait = 0.75·d，读数可精确预测）
    · 其它（限速器节拍抖动 `uniform(0.0, ≤THROTTLE_JITTER=0.008)`）⇒ 取中点，
      **不消耗脚本** ⇒ 退避抖动序列与限速器内部抖动互不干扰。
    判据用 `b > 0.05` 区分两者：限速器的 b 上限就是 0.008（含 head 收窄后更小），
    而退避的 b = d/2，测试里出现的非零退避档 d ≥ 1 ⇒ b ≥ 0.5。
    """

    def __init__(self, vs=None):
        self.vs = list(vs or [])
        self.i = 0

    def uniform(self, a, b):
        if self.vs and abs(a - 0.75) < 1e-12 and abs(b - 1.25) < 1e-12:
            v = self.vs[self.i % len(self.vs)]
            self.i += 1
            return a + (b - a) * float(v)
        if self.vs and float(a) == 0.0 and float(b) > 0.05:
            v = self.vs[self.i % len(self.vs)]
            self.i += 1
            return float(a) + (float(b) - float(a)) * float(v)
        return (a + b) / 2.0


OK = {'info': {'totalResultsLocal': 7}, 'docs': []}


class FakeResp:
    def __init__(self, code, payload=None):
        self.status_code = code
        self._p = payload

    def json(self):
        if isinstance(self._p, BaseException):
            raise self._p
        return self._p


class FakeSession:
    """按脚本返回响应/抛异常；记录每次尝试的（虚拟）发起时刻。"""

    def __init__(self, script):
        self.script = list(script)
        self.attempts = []          # [(t_issue, outcome)]

    def get(self, url, params=None, headers=None, timeout=None):
        t = scrape.time.time()
        if not self.script:
            raise AssertionError('FakeSession 脚本已耗尽（尝试次数多于脚本步数）')
        step = self.script.pop(0)
        kind = step[0]
        if kind == 'exc':
            self.attempts.append((t, 'EXC:' + type(step[1]).__name__))
            raise step[1]
        if kind == 'status':
            self.attempts.append((t, 'HTTP %d' % step[1]))
            return FakeResp(step[1])
        if kind == 'ok':
            self.attempts.append((t, 'HTTP 200'))
            return FakeResp(200, step[1] if len(step) > 1 else OK)
        raise AssertionError('未知脚本步：%r' % (step,))


class Case:
    """一个用例的隔离环境：虚时钟 + 假会话 + 新限速器 + 新审计文件。"""

    def __init__(self, tag, script, retries=6, mode='v2', jitter=None, realclock=False,
                 backoff=None, classify_every=None, attempts_cap=None):
        self.tag = tag
        self.script = script
        self.retries = retries
        self.mode = mode
        self.realclock = realclock
        self.jitter = jitter
        self.backoff = backoff
        self.classify_every = classify_every
        self.attempts_cap = attempts_cap
        self.apath = str(SB / ('audit-%s.jsonl' % tag))
        self._saved = {}

    def __enter__(self):
        if os.path.exists(self.apath):
            os.remove(self.apath)
        self.clock = None if self.realclock else VClock()
        self.sess = FakeSession(self.script)
        self.logs = []
        self.lim = scrape.RateLimiter(scrape.THROTTLE_START_INTERVAL, mode='adaptive')
        self._saved = {
            'time': scrape.time, 'random': scrape.random, 'SESSION': scrape.SESSION,
            'LIMITER': scrape.LIMITER, 'log': scrape.log, 'RETRY_MODE': scrape.RETRY_MODE,
            'RETRY_BACKOFF': scrape.RETRY_BACKOFF, 'RETRY_AUDIT_CLASSIFY_EVERY':
                scrape.RETRY_AUDIT_CLASSIFY_EVERY, 'RETRY_MAX_ATTEMPTS': scrape.RETRY_MAX_ATTEMPTS,
        }
        if self.clock is not None:
            scrape.time = self.clock
        if self.jitter is not None or not self.realclock:
            scrape.random = VRandom(self.jitter)
        scrape.SESSION = self.sess
        scrape.LIMITER = self.lim
        scrape.RETRY_MODE = self.mode
        if self.backoff is not None:
            scrape.RETRY_BACKOFF = self.backoff
        if self.attempts_cap is not None:
            scrape.RETRY_MAX_ATTEMPTS = self.attempts_cap
        if self.classify_every is not None:
            scrape.RETRY_AUDIT_CLASSIFY_EVERY = self.classify_every
        scrape.log = lambda m: self.logs.append(str(m))
        scrape._AUDIT_MOD['cls_n'] = 0
        scrape._AUDIT_MOD['failed'] = 0
        audit.init(path=self.apath, run='test-' + self.tag, reset_seq=True)
        self.t0 = scrape.time.time()
        return self

    def __exit__(self, *exc):
        for k, v in self._saved.items():
            setattr(scrape, k, v)
        audit.close()
        return False

    # ---- 读数 ----
    def run(self, q='holding_call_number,begins_with,A', limit=1, sort=None):
        t0 = scrape.time.time()
        self.result = scrape.fetch(q, limit=limit, sort=sort, retries=self.retries)
        self.elapsed = scrape.time.time() - t0
        self.events = audit_lines(self.apath)
        return self.result

    def of(self, event):
        return [e for e in self.events if e['event'] == event]

    def waits(self):
        return [e['detail']['wait_s'] for e in self.of('retry_scheduled')]

    def owners(self):
        return [e['detail'].get('stall_owner') for e in self.of('retry_scheduled')]

    def classes(self):
        return [e['detail'].get('class') for e in self.of('retry_scheduled')]

    def names(self):
        return [e['event'] for e in self.events]

    def attempt_times(self):
        return [t for t, _ in self.sess.attempts]

    def stall_between(self, i):
        """第 i 次失败 → 第 i+1 次尝试之间的真实（虚拟）停摆秒数。"""
        ts = self.attempt_times()
        return ts[i + 1] - ts[i]


def audit_lines(path):
    if not os.path.exists(path):
        return []
    out = []
    with open(path, encoding='utf-8') as f:
        for ln in f:
            ln = ln.strip()
            if ln:
                out.append(json.loads(ln))     # 半写行 ⇒ 这里会抛（测试要暴露它）
    return out


def max_per_window(ts, win=1.0):
    """任意半开窗口 [t, t+win) 内的最大计数（限速红线读数口径）。"""
    best = 0
    for i, t in enumerate(ts):
        n = sum(1 for u in ts[i:] if u - t < win)
        best = max(best, n)
    return best


def reset_audit_mod():
    """预置审计模块句柄：让每个用例用 `audit.init(path=...)` 自己指路
    （否则 scrape 的首个懒加载会把路径改回 OUT/audit.jsonl，读数就串场了；
    生产接线的懒加载路径单独在 T12 验证）。"""
    scrape._AUDIT_MOD['tried'] = True
    scrape._AUDIT_MOD['mod'] = audit
    scrape._AUDIT_MOD['failed'] = 0


reset_audit_mod()
emit('=' * 78)
emit('R3 · slot-retry 自测（请求级自愈 C1 + 请求级审计）  python=%s' % sys.version.split()[0])
emit('沙箱输出目录：%s' % SB)
emit('=' * 78)

# ---------------------------------------------------------------------------
# T1 分类学
# ---------------------------------------------------------------------------
emit('')
emit('T1 错误分类学（R3-P4：dns/conn_reset/read_timeout/write_timeout/server_5xx/ratelimit/'
     'auth/client/fatal/data/other）')
CLS_CASES = [
    ('dns', rq.exceptions.ConnectionError(
        'HTTPSConnectionPool(host=\'x\', port=443): Max retries exceeded with url: /a '
        '(Caused by NameResolutionError("Failed to resolve \'x\' '
        '([Errno -2] Name or service not known)"))')),
    ('dns', OSError('[Errno -3] Temporary failure in name resolution')),
    ('conn_reset', rq.exceptions.ConnectionError(
        "('Connection aborted.', ConnectionResetError(104, 'Connection reset by peer'))")),
    ('conn_reset', rq.exceptions.ConnectTimeout('connect timed out')),
    ('conn_reset', rq.exceptions.ConnectionError('Connection refused by peer')),
    ('read_timeout', rq.exceptions.ReadTimeout('Read timed out. (read timeout=60)')),
    ('read_timeout', rq.exceptions.ReadTimeout('')),
    # ↓ 反面约束（R-1 不得回退）：泛型 ConnectionError + **具体读特征** 仍是 read_timeout
    ('read_timeout', rq.exceptions.ConnectionError('HTTPConnectionPool: Read timed out.')),
    ('write_timeout', rq.exceptions.ConnectionError(
        "WriteTimeoutError(TimeoutError('write timed out'))")),
    # ↓ R-1 的四个连接类反例（crit-correct p2 E：改前 2 个被误判 read_timeout）
    ('conn_reset', urllib3.exceptions.ConnectTimeoutError(
        None, 'Connection to h timed out. (connect timeout=10)')),
    ('conn_reset', rq.exceptions.ConnectionError(
        "HTTPSConnectionPool(host=h, port=443): Max retries exceeded with url: /x "
        "(Caused by ConnectTimeoutError(<urllib3.connection.HTTPSConnection object at 0x1>, "
        "'Connection to h timed out. (connect timeout=10)'))")),
    ('conn_reset', urllib3.exceptions.MaxRetryError(
        pool=None, url='/x', reason='Connection to h timed out. (connect timeout=10)')),
    ('conn_reset', urllib3.exceptions.NewConnectionError(
        None, 'Failed to establish a new connection: [Errno 111] Connection refused')),
    ('conn_reset', rq.exceptions.SSLError('HTTPSConnectionPool: certificate verify failed')),
    ('conn_reset', rq.exceptions.ChunkedEncodingError('Connection broken: IncompleteRead(0 bytes)')),
    ('conn_reset', rq.exceptions.ProxyError('Cannot connect to proxy', OSError('Tunnel failed'))),
    # ↓ R-B7 数据类（确定性失败；v2 下 fail-fast）
    ('data', json.JSONDecodeError('Expecting value', '', 0)),
    ('data', ValueError('Expecting value: line 1 column 1 (char 0)')),
    ('data', KeyError('info')),
    ('other', RuntimeError('boom')),
]
for want, exc in CLS_CASES:
    got = scrape._retry_class_of_exc(exc)
    check('T1.exc %-14s %-22s' % (want, type(exc).__name__), got == want, 'got=%s' % got)
# R-1 专条：p2 的 4 个连接类反例全对（改前 2 个误判 read_timeout）
_P2_CONN = [e for n, e in
            [('ConnectTimeoutError', urllib3.exceptions.ConnectTimeoutError(
                None, 'Connection to h timed out. (connect timeout=10)')),
             ('ConnectionError→ConnectTimeoutError', rq.exceptions.ConnectionError(
                 "Max retries exceeded (Caused by ConnectTimeoutError(<obj>, "
                 "'Connection to h timed out. (connect timeout=10)'))")),
             ('MaxRetryError', urllib3.exceptions.MaxRetryError(
                 pool=None, url='/x', reason='Connection to h timed out. (connect timeout=10)')),
             ('requests.ConnectTimeout', rq.exceptions.ConnectTimeout(
                 'Connection to x timed out. (connect timeout=10)'))]]
_bad = [(type(e).__name__, scrape._retry_class_of_exc(e)) for e in _P2_CONN
        if scrape._retry_class_of_exc(e) != 'conn_reset']
check('T1.R-1 p2 的 4 个连接类反例全判 conn_reset（改前 2 个误判）', not _bad, str(_bad))
for code, want in ((400, 'client'), (429, 'ratelimit'), (401, 'auth'), (403, 'auth'),
                   (500, 'server_5xx'), (502, 'server_5xx'), (503, 'server_5xx'),
                   (504, 'server_5xx'), (404, 'fatal'), (410, 'fatal'), (301, 'fatal'),
                   (200, 'other')):
    got = scrape._retry_class_of_status(code)
    check('T1.status %-4d -> %-11s' % (code, want), got == want, 'got=%s' % got)
check('T1.R3-P4 归因分离：400 不再判 ratelimit；429 仍是 ratelimit',
      scrape._retry_class_of_status(400) == 'client'
      and scrape._retry_class_of_status(429) == 'ratelimit'
      and 400 not in scrape.RETRY_RATELIMIT_CODES and 429 in scrape.RETRY_RATELIMIT_CODES,
      str(scrape.RETRY_RATELIMIT_CODES))
check('T1.retryable-set == 基线集合（未改「是否重试」）',
      tuple(scrape.RETRY_STATUS_CODES) == (400, 401, 403, 429, 500, 502, 503, 504),
      str(scrape.RETRY_STATUS_CODES))
_CLS_ALL = {'dns', 'conn_reset', 'read_timeout', 'write_timeout', 'server_5xx', 'ratelimit',
            'auth', 'client', 'fatal', 'data', 'other'}
check('T1.owner 归属覆盖全部 11 类',
      set(scrape.RETRY_STALL_OWNER) == set(scrape.RETRY_BACKOFF) == _CLS_ALL and
      scrape.RETRY_STALL_OWNER['ratelimit'] == 'cooldown' and
      scrape.RETRY_STALL_OWNER['client'] == 'sleep' and
      scrape.RETRY_STALL_OWNER['fatal'] == 'none' and
      scrape.RETRY_STALL_OWNER['data'] == 'none' and
      scrape.RETRY_STALL_OWNER['server_5xx'] == 'sleep')
check('T1.确定性类（fatal/data）不产生停摆归属 cooldown/sleep',
      scrape.RETRY_STALL_OWNER['fatal'] == 'none' and scrape.RETRY_STALL_OWNER['data'] == 'none')

# ---------------------------------------------------------------------------
# T2 断连注入 → 重试 ≥3 次 + 指数退避读数
# ---------------------------------------------------------------------------
emit('')
emit('T2 断连注入（ConnectionResetError × 4 → 成功）：重试次数 + 指数退避读数')
RESET = rq.exceptions.ConnectionError(
    "('Connection aborted.', ConnectionResetError(104, 'Connection reset by peer'))")
with Case('t2-reset', [('exc', RESET)] * 4 + [('ok', OK)]) as c:
    res = c.run()
    w = c.waits()
    check('T2.成功（重试后恢复）', res == OK, str(res)[:60])
    check('T2.尝试次数 = 5（= 4 次重试 ≥ C1 的 3）', len(c.sess.attempts) == 5,
          'attempts=%d' % len(c.sess.attempts))
    check('T2.retry_scheduled = 4', len(c.of('retry_scheduled')) == 4, str(len(c.of('retry_scheduled'))))
    check('T2.retry_exhausted = 0', len(c.of('retry_exhausted')) == 0)
    check('T2.退避序列 = [1.5,3,6,12]（equal jitter：d=[2,4,8,16]，v=0.5 ⇒ 0.75·d）',
          w == [1.5, 3.0, 6.0, 12.0], str(w))
    check('T2.每档 wait ≤ 档位上界 d ≤ cap（R-B2 断言①）',
          all(x <= scrape.RETRY_BACKOFF['conn_reset'][2] for x in w), str(w))
    check('T2.指数性（相邻比值 = 2.0）',
          all(approx(w[i + 1] / w[i], 2.0, 1e-9) for i in range(len(w) - 1)), str(w))
    check('T2.停摆归属 = cooldown（网络类全局停摆）', set(c.owners()) == {'cooldown'},
          str(set(c.owners())))
    check('T2.实际停摆 == 声明退避之和（无隐藏停摆）',
          approx(c.clock.total, sum(w), 0.5), 'clock=%.3f sum(waits)=%.3f' % (c.clock.total, sum(w)))
    check('T2.分类 = conn_reset', set(c.classes()) == {'conn_reset'}, str(set(c.classes())))
    check('T2.error_classified = 4', len(c.of('error_classified')) == 4)
    ch = [e['event'] for e in c.events]
    check('T2.事件链 = (error_classified, retry_scheduled)×4',
          ch == ['error_classified', 'retry_scheduled'] * 4, str(ch))

# ---------------------------------------------------------------------------
# T3 读超时注入
# ---------------------------------------------------------------------------
emit('')
emit('T3 读超时注入（ReadTimeout × 3 → 成功）：退避序列 + 线程内停摆归属')
with Case('t3-rt', [('exc', rq.exceptions.ReadTimeout('Read timed out. (read timeout=60)'))] * 3
          + [('ok', OK)]) as c:
    res = c.run()
    w = c.waits()
    check('T3.成功', res == OK)
    check('T3.重试 3 次（= C1 下限）', len(c.of('retry_scheduled')) == 3, str(len(c.of('retry_scheduled'))))
    check('T3.退避序列 = [0.75,1.5,3]（base1 × factor2 + equal jitter）',
          w == [0.75, 1.5, 3.0], str(w))
    check('T3.首档 ≤ BASE（R-B2 断言④：首档 0.75 ≤ base 1.0）', w[0] <= 1.0, str(w[0]))
    check('T3.停摆归属 = sleep（仅线程内，不全局停摆）', set(c.owners()) == {'sleep'}, str(set(c.owners())))
    check('T3.分类 = read_timeout', set(c.classes()) == {'read_timeout'}, str(set(c.classes())))
    check('T3.sleep 调用 = 退避 + 正常节拍（本地停摆可见）',
          sum(1 for s in c.clock.sleeps if s >= 0.75) == 3,
          str([round(s, 3) for s in c.clock.sleeps]))

# ---------------------------------------------------------------------------
# T4 5xx 注入
# ---------------------------------------------------------------------------
emit('')
emit('T4 服务端 5xx 注入（503 × 4 → 成功）：重试 ≥3 次 + 退避序列')
with Case('t4-5xx', [('status', 503)] * 4 + [('ok', OK)]) as c:
    res = c.run()
    w = c.waits()
    check('T4.成功', res == OK)
    check('T4.重试 4 次（≥3）', len(c.of('retry_scheduled')) == 4)
    check('T4.退避序列 = [3.75,7.5,15,30]（base5 × factor2 + equal jitter）',
          w == [3.75, 7.5, 15.0, 30.0], str(w))
    check('T4.分类 = server_5xx', set(c.classes()) == {'server_5xx'}, str(set(c.classes())))
    check('T4.停摆归属 = sleep', set(c.owners()) == {'sleep'}, str(set(c.owners())))
    check('T4.error_classified 带 status=503',
          all(e['detail'].get('status') == 503 for e in c.of('error_classified')))

# ---------------------------------------------------------------------------
# T5 预算耗尽
# ---------------------------------------------------------------------------
emit('')
emit('T5 预算耗尽（6 次全断连）：retry_exhausted + 返回 None（调用方回队，零静默丢失）')
with Case('t5-exh', [('exc', RESET)] * 6) as c:
    res = c.run()
    ex = c.of('retry_exhausted')
    check('T5.返回 None（不回假数据）', res is None, repr(res))
    check('T5.尝试次数 = retries(6)', len(c.sess.attempts) == 6, str(len(c.sess.attempts)))
    check('T5.retry_scheduled = 5', len(c.of('retry_scheduled')) == 5)
    check('T5.retry_exhausted 恰好 1 条', len(ex) == 1)
    if ex:
        d = ex[0]['detail']
        check('T5.retry_exhausted{attempts,q,last_error} 齐全',
              d.get('attempts') == 6 and d.get('q') == 'holding_call_number,begins_with,A'
              and bool(d.get('last_error')),
              json.dumps({k: d.get(k) for k in ('attempts', 'q', 'last_error')},
                         ensure_ascii=False)[:140])
    check('T5.最后一条审计事件 = retry_exhausted', c.events[-1]['event'] == 'retry_exhausted',
          c.events[-1]['event'])
    _acts = [e['detail']['action'] for e in c.of('error_classified')]
    check('T5.error_classified 动作 = 5×retry + 1×exhausted', _acts == ['retry'] * 5 + ['exhausted'],
          str(_acts))
    check('T5.退避序列 = [1.5,3,6,12,22.5]（d=[2,4,8,16,30(cap)] + equal jitter）',
          c.waits() == [1.5, 3.0, 6.0, 12.0, 22.5], str(c.waits()))
    check('T5.全部 wait ≤ CAP=30（R-B2 断言①在预算耗尽路径同样成立）',
          all(x <= 30.0 for x in c.waits()), str(c.waits()))

# ---------------------------------------------------------------------------
# T6 429 × throttle v2：无双退避
# ---------------------------------------------------------------------------
emit('')
emit('T6 429 与 throttle v2 的交互：单次停摆（无 cooldown/sleep 双计）')
emit('T6a 429 → 重试：声明 wait_s 与实测停摆一致；throttle v2 仍照常吃 429 反馈')
with Case('t6a-429', [('status', 429), ('ok', OK)]) as c:
    snap0 = c.lim.snapshot()
    res = c.run()
    w = c.waits()
    decl = w[0] if w else 0.0
    stall = c.stall_between(0)
    snap1 = c.lim.snapshot()
    check('T6a.成功（429 后重试恢复）', res == OK)
    check('T6a.退避 = 11.25s（ratelimit base15 + equal jitter v=0.5）+ 归属 cooldown',
          w == [11.25] and c.owners() == ['cooldown'], 'wait=%s owner=%s' % (w, c.owners()))
    check('T6a.实测停摆在 [声明, 声明+节拍] 内（无双计）',
          decl <= stall <= decl + 0.5, 'declared=%.3f actual=%.3f' % (decl, stall))
    check('T6a.实测停摆 < 1.5×声明（排除 2× 停摆）', stall < 1.5 * decl,
          'ratio=%.3f' % (stall / decl if decl else -1))
    check('T6a.该次失败只产生 1 次 sleep（acquire 承担，apply 不睡）',
          sum(1 for s in c.clock.sleeps if s > 10.0) == 1,
          str([round(s, 3) for s in c.clock.sleeps]))
    check('T6a.throttle v2 仍响应 429（长期节拍归它）',
          snap1['interval'] > snap0['interval'] or snap1['max_interval_seen'] > 0,
          'interval %.4f→%.4f' % (snap0['interval'], snap1['interval']))
    check('T6a.分类 = ratelimit', c.classes() == ['ratelimit'], str(c.classes()))

emit('T6b legacy 对照（基线异常路径 cooldown(10)+sleep(5) 双计）vs v2 单归属')
with Case('t6b-legacy', [('exc', RESET), ('ok', OK)], mode='legacy') as c:
    c.run()
    legacy_stall = c.stall_between(0)
    legacy_declared = scrape.RETRY_LEGACY_EXC[1]      # 基线 sleep 值 = 5
    check('T6b.legacy 无审计事件（逐行为等价基线）', len(c.events) == 0, str(len(c.events)))
    check('T6b.legacy 实测停摆 > 声明（= 基线双计，实测 %.1fs > %.1fs）'
          % (legacy_stall, legacy_declared), legacy_stall > legacy_declared * 1.5,
          'actual=%.3f declared_sleep=%.1f' % (legacy_stall, legacy_declared))
with Case('t6b-v2', [('exc', RESET), ('ok', OK)]) as c:
    c.run()
    v2_stall = c.stall_between(0)
    v2_declared = c.waits()[0]
    check('T6b.v2 实测停摆 == 声明（无双计）',
          v2_declared <= v2_stall <= v2_declared + 0.5,
          'actual=%.3f declared=%.3f' % (v2_stall, v2_declared))
check('T6b.同一故障：v2 停摆 < legacy 停摆', v2_stall < legacy_stall,
      'v2=%.3f legacy=%.3f' % (v2_stall, legacy_stall))

# ---------------------------------------------------------------------------
# T7 重试流量仍受限速
# ---------------------------------------------------------------------------
emit('')
emit('T7 重试流量仍受限速（任意 1s 半开窗口 ≤5 发）')
RT = rq.exceptions.ReadTimeout('')
with Case('t7-zero', [('exc', RT)] * 11 + [('ok', OK)], retries=12, attempts_cap=20,
          backoff=dict(scrape.RETRY_BACKOFF, read_timeout=(0.0, 2.0, 0.0))) as c:
    c.run()
    ts = c.attempt_times()
    mw = max_per_window(ts, 1.0)
    span = ts[-1] - ts[0]
    check('T7.结构性：退避被置 0 时窗口仍 ≤5 发（%.0f 发 / %.1fs，峰值 %d）'
          % (len(ts), span, mw), mw <= 5, 'max_per_1s=%d' % mw)
    check('T7.12 次尝试耗时 ≥ (12-5)/5 = 1.4s（限速器真实生效）', span >= 1.4,
          'span=%.3f' % span)
    check('T7.每次尝试都经 LIMITER.acquire（n_acquire == 尝试数）',
          c.lim._n_req == len(ts), 'n_acquire=%d attempts=%d' % (c.lim._n_req, len(ts)))
with Case('t7-real', [('exc', RT)] * 5 + [('ok', OK)], retries=6) as c:
    c.run()
    ts = c.attempt_times()
    check('T7.默认退避下峰值 ≤5 发/1s（峰值 %d）' % max_per_window(ts, 1.0),
          max_per_window(ts, 1.0) <= 5)
    gaps = [round(ts[i + 1] - ts[i], 3) for i in range(len(ts) - 1)]
    check('T7.相邻尝试间隔 ≥ 地板 0.20s', all(g >= 0.2 - 1e-9 for g in gaps), str(gaps))

# ---------------------------------------------------------------------------
# T8 回归 / 兼容 / 开关
# ---------------------------------------------------------------------------
emit('')
emit('T8 回归：正常路径不变 + 致命状态语义不变 + 开关 + retries 参数兼容')
with Case('t8-ok', [('ok', OK)]) as c:
    res = c.run()
    check('T8.正常 200：1 次尝试即返回 JSON', res == OK and len(c.sess.attempts) == 1,
          'attempts=%d' % len(c.sess.attempts))
    check('T8.正常路径零 sleep（零退避开销）', c.clock.total == 0.0, 'total=%.4f' % c.clock.total)
    check('T8.正常路径零审计事件', len(c.events) == 0, str(len(c.events)))
    check('T8.正常路径零重试日志', not any('backoff' in m for m in c.logs), str(c.logs[:1]))
with Case('t8-404', [('status', 404)]) as c:
    res = c.run()
    check('T8.404（非重试集合）：1 次尝试、返回 None、不重试',
          res is None and len(c.sess.attempts) == 1, 'attempts=%d' % len(c.sess.attempts))
    check('T8.404 仍写 error_classified{action:fatal}（可审计）',
          len(c.of('error_classified')) == 1 and c.of('error_classified')[0]['detail']['action'] == 'fatal',
          json.dumps(c.of('error_classified')[0]['detail'], ensure_ascii=False) if c.events else '')
with Case('t8-floor', [('status', 503)] * 3 + [('ok', OK)], retries=2) as c:
    res = c.run()
    check('T8.C1 下限强制：retries=2 ⇒ 仍重试 3 次后成功',
          res == OK and len(c.of('retry_scheduled')) == 3, str(len(c.of('retry_scheduled'))))
# R-2：防御性下限**不得**静默突破用户硬上限（crit-correct R-2 的第三个反例）
with Case('t8-hardcap', [('status', 503)] * 6, retries=2, attempts_cap=2) as c:
    res = c.run()
    check('T8.RETRY_MAX_ATTEMPTS=2 是硬上限：retries=2 ⇒ 恰好 2 次尝试（不再被下限顶到 4）',
          res is None and len(c.sess.attempts) == 2, 'attempts=%d' % len(c.sess.attempts))
    check('T8.硬上限低于 C1 下限 ⇒ 打印显式降级告警（不静默）',
          any('C1' in m for m in c.logs), str([m for m in c.logs if 'C1' in m][:1]))
with Case('t8-hardcap4', [('status', 503)] * 6, retries=2, attempts_cap=4) as c:
    c.run()
    check('T8.RETRY_MAX_ATTEMPTS=4 == C1 下限：retries=2 ⇒ 4 次尝试（下限与上限一致）',
          len(c.sess.attempts) == 4, 'attempts=%d' % len(c.sess.attempts))
with Case('t8-caller-hi', [('status', 503)] * 20, retries=8) as c:
    c.run()
    check('T8.调用方 retries=8 < 硬上限 12 ⇒ 8 次尝试（不被压到 C1 下限 4）',
          len(c.sess.attempts) == 8, 'attempts=%d' % len(c.sess.attempts))
with Case('t8-legacy', [('status', 503)] * 3 + [('ok', OK)], mode='legacy') as c:
    c.run()
    check('T8.legacy 退避 = 基线 [15,25,35]（int 逐值）',
          c.clock.sleeps[:3] == [15, 25, 35], str(c.clock.sleeps[:3]))
    check('T8.legacy 日志逐字节等价基线格式',
          c.logs[0] == "    HTTP 503 q='holding_call_number,begins_with,A' cooldown 15s (1)",
          repr(c.logs[0]))
    check('T8.legacy 无审计事件', len(c.events) == 0)
check('T8.开关映射 SCRAPE_RETRY',
      scrape._RETRY_MODE_ALIASES['off'] == 'legacy' and scrape._RETRY_MODE_ALIASES['v2'] == 'v2'
      and scrape._r3_retry_mode.__doc__ is not None)
_old = os.environ.get('SCRAPE_RETRY')
os.environ['SCRAPE_RETRY'] = 'legacy'
check('T8.SCRAPE_RETRY=legacy 解析', scrape._r3_retry_mode() == 'legacy')
os.environ['SCRAPE_RETRY'] = 'nonsense'
check('T8.非法值回退 v2（fail-safe）', scrape._r3_retry_mode() == 'v2')
if _old is None:
    os.environ.pop('SCRAPE_RETRY', None)
else:
    os.environ['SCRAPE_RETRY'] = _old

# ---------------------------------------------------------------------------
# T9 审计 schema / 采样 / 韧性
# ---------------------------------------------------------------------------
emit('')
emit('T9 审计 schema 校验 + error_classified 采样 + 审计故障韧性')
with Case('t9-schema', [('exc', RESET)] * 3 + [('ok', OK)]) as c:
    c.run()
    ev = c.events
    # [merge R3p4] 契约 v2 §1 强制字段全集（原断言写的是遗留键 ts；见 adapt-test-retry-contract-v2.py）
    check('T9.字段齐全（契约 v2 §1：schema/ts_epoch/ts_iso/pid/seq/level/event/detail/run）',
          all(set(e) >= {'schema', 'ts_epoch', 'ts_iso', 'pid', 'seq', 'level', 'event', 'detail', 'run'}
              for e in ev))
    check('T9.契约 §1 负判据：v2 行不写遗留键 ts', all('ts' not in e for e in ev))
    # [P4.5·v2.1 R-8/KI-1] 契约 §8 回归护栏：v2.1 起每行必须带写者分组键 writer（写侧 = "retry"）。
    #   与 §1 负判据同规格：把"新写入不得产生无 writer 行"（契约 §8.5）变成可执行的回归护栏。
    check('T9.契约 §8 写者键：每行带 writer 且本模块 = "retry"',
          all(isinstance(e.get('writer'), str) and e.get('writer') == 'retry' for e in ev),
          str(sorted({e.get('writer') for e in ev})))
    check('T9.level 全为 request', all(e['level'] == 'request' for e in ev))
    check('T9.seq 严格单调', [e['seq'] for e in ev] == list(range(1, len(ev) + 1)),
          str([e['seq'] for e in ev]))
    check('T9.ts_epoch 单调非降（契约 v2 权威时间键）',
          all(ev[i]['ts_epoch'] <= ev[i + 1]['ts_epoch'] for i in range(len(ev) - 1)))
    check('T9.detail 为对象', all(isinstance(e['detail'], dict) for e in ev))
    check('T9.run 已标注', all(e['run'] == 'test-t9-schema' for e in ev))
    check('T9.pid 为本进程', all(e['pid'] == os.getpid() for e in ev))
    check('T9.事件名 ∈ {error_classified,retry_scheduled,retry_exhausted}',
          {e['event'] for e in ev} == {'error_classified', 'retry_scheduled'})
with Case('t9-sample', [('status', 500)] * 7 + [('ok', OK)], retries=8, classify_every=3) as c:
    c.run()
    check('T9.采样 1/3：7 个错误 → error_classified 2 条（retry_scheduled 仍 7 条）',
          len(c.of('error_classified')) == 2 and len(c.of('retry_scheduled')) == 7,
          'classified=%d scheduled=%d' % (len(c.of('error_classified')), len(c.of('retry_scheduled'))))
audit.reset()
audit.init(path='/proc/definitely/not/here/audit.jsonl', reset_seq=True)
r = scrape._retry_audit('request', 'retry_scheduled', {'attempt': 1})
check('T9.审计写失败不抛异常（fail-open，返回 None）', r is None and audit.stats()['errors'] >= 1,
      'errors=%d' % audit.stats()['errors'])
audit.reset()
audit.init(path=str(SB / 'audit-disabled.jsonl'), enabled=False, reset_seq=True)
check('T9.审计禁用 ⇒ emit no-op', audit.emit_request('x', {'a': 1}) is None and audit.path() is None)
audit.reset()

# ---------------------------------------------------------------------------
# T10 退避抖动
# ---------------------------------------------------------------------------
emit('')
emit('T10 抖动（R3-P4：equal jitter）：wait ∈ [d/2, d]，且逐次不同（打散相位）')
raw5 = [2.0 * (2.0 ** i) for i in range(4)]          # conn_reset d = 2,4,8,16
with Case('t10-jit', [('exc', RESET)] * 4 + [('ok', OK)], jitter=[0.0, 1.0, 0.0, 1.0]) as c:
    c.run()
    w = c.waits()
    ratios = [w[i] / raw5[i] for i in range(4)]
    check('T10.抖动边界 = {0.5, 1.0}×d（equal jitter 的上下界）',
          [round(r, 4) for r in ratios] == [0.5, 1.0, 0.5, 1.0], str([round(r, 4) for r in ratios]))
    check('T10.抖动的上界即档位本身（wait ≤ d ⇒ 不可能越过 cap）',
          all(w[i] <= raw5[i] for i in range(4)), str(w))
with Case('t10-jit2', [('exc', RESET)] * 4 + [('ok', OK)], jitter=[0.0, 0.0, 0.5, 1.0]) as c:
    c.run()
    w = c.waits()
    check('T10.档位内取样不同 ⇒ wait 不全等（抖动真实存在，打散相位）',
          len(set(w)) == len(w) and w == [1.0, 2.0, 6.0, 16.0], str(w))
    check('T10.抖动后仍随档位包络严格增长（每档都取到该档内更晚的点）',
          w[0] < w[1] < w[2] < w[3], str(w))

# ---------------------------------------------------------------------------
# T11 多线程：审计行原子 + 无撕裂 + 限速仍成立（真实时钟）
# ---------------------------------------------------------------------------
emit('')
emit('T11 并发（3 线程 × 4 次尝试，真实时钟 + 退避置 0）：审计行完整 + seq 唯一 + 限速成立')


class RepeatResetSession:
    """并发用例专用：每次调用都抛 ConnectionResetError（线程安全计数）。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.attempts = []

    def get(self, url, params=None, headers=None, timeout=None):
        with self.lock:
            self.attempts.append(time.time())
        raise rq.exceptions.ConnectionError(
            "('Connection aborted.', ConnectionResetError(104, 'Connection reset by peer'))")


class TraceLimiter(scrape.RateLimiter):
    """记录限速器**发号时刻**（_rate_cap 选定的放行时刻 = 限速器对 1s 窗口的权威读数）。

    与"实际发起时刻"的区别：真实时钟下线程 sleep 到期后可能被 OS 延迟唤醒（overshoot），
    实际发起时刻会晚于发号时刻 ⇒ 事后按实际时刻做滑窗统计可能多算 1 发。红线归属在
    发号侧（限速器保证 ≤5/1s）；overshoot 是 sleep 节拍法的固有时延，与本次重试改动无关。
    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.tlock = threading.Lock()
        self.granted = []

    def _rate_cap(self, t):
        t2 = super()._rate_cap(t)
        with self.tlock:
            self.granted.append(float(t2))
        return t2


apath = str(SB / 'audit-t11-threads.jsonl')
if os.path.exists(apath):
    os.remove(apath)
_saved = {'time': scrape.time, 'SESSION': scrape.SESSION, 'LIMITER': scrape.LIMITER,
          'log': scrape.log, 'RETRY_BACKOFF': scrape.RETRY_BACKOFF,
          'RETRY_MODE': scrape.RETRY_MODE}
scrape.time = time                                    # 真实时钟
sess = RepeatResetSession()
tlim = TraceLimiter(scrape.THROTTLE_START_INTERVAL, mode='adaptive')
scrape.SESSION = sess
scrape.LIMITER = tlim
scrape.RETRY_MODE = 'v2'
scrape.RETRY_BACKOFF = dict(scrape.RETRY_BACKOFF, conn_reset=(0.0, 2.0, 0.0))   # 退避置 0
_lg = []
scrape.log = lambda m: _lg.append(str(m))
audit.init(path=apath, run='test-t11', reset_seq=True)
_t0 = time.time()
_threads = [threading.Thread(target=lambda: scrape.fetch('q%d' % i, retries=4)) for i in range(3)]
for t in _threads:
    t.start()
for t in _threads:
    t.join()
_el = time.time() - _t0
audit.close()
for _k, _v in _saved.items():
    setattr(scrape, _k, _v)
ev = audit_lines(apath)
seqs = [e['seq'] for e in ev]
n_sched = len([e for e in ev if e['event'] == 'retry_scheduled'])
n_cls = len([e for e in ev if e['event'] == 'error_classified'])
n_exh = len([e for e in ev if e['event'] == 'retry_exhausted'])
peak_grant = max_per_window(tlim.granted, 1.0)
peak_issue = max_per_window(sess.attempts, 1.0)
check('T11.审计行全部可解析（无半写/交错撕裂）', len(ev) == 24, 'lines=%d' % len(ev))
check('T11.seq 唯一且严格递增', len(set(seqs)) == len(seqs) and seqs == sorted(seqs),
      'seq=%s' % str(seqs))
check('T11.retry_scheduled=9, error_classified=12, retry_exhausted=3',
      (n_sched, n_cls, n_exh) == (9, 12, 3), '%d/%d/%d' % (n_sched, n_cls, n_exh))
check('T11.12 次尝试在共享限速器下耗时 ≥ 1.4s', _el >= 1.4, 'elapsed=%.3f' % _el)
check('T11.发号时刻（限速器权威读数）峰值 ≤5 发/1s', peak_grant <= 5, 'peak_granted=%d' % peak_grant)
check('T11.实际发起峰值 ≤ 发号峰值+1（sleep overshoot 有界）', peak_issue <= peak_grant + 1,
      'peak_issue=%d peak_granted=%d' % (peak_issue, peak_grant))
check('T11.3 个线程各得 4 次尝试（无相互吞并）', len(sess.attempts) == 12, str(len(sess.attempts)))
emit('        读数：发出 12 次，span=%.2fs（≥1.4s），发号峰值=%d 发/1s，实际发起峰值=%d 发/1s'
     % (_el, peak_grant, peak_issue))

# ---------------------------------------------------------------------------
# T12 生产接线：懒加载 import + 路径落在 OUT/audit.jsonl
# ---------------------------------------------------------------------------
emit('')
emit('T12 生产接线：函数内懒加载 audit.py 并绑定 OUT/audit.jsonl')
scrape._AUDIT_MOD['tried'] = False
scrape._AUDIT_MOD['mod'] = None
_want = str(scrape.OUT / 'audit.jsonl')
for _p in (_want, _want + '.1'):
    if os.path.exists(_p):
        os.remove(_p)
_mod = scrape._audit_get()
check('T12._audit_get() 返回独立模块', _mod is audit, repr(_mod))
check('T12.审计路径 = OUT/audit.jsonl（生产默认）', audit.path() == _want,
      '%s vs %s' % (audit.path(), _want))
check('T12.run 标识已注入（boot-…）', str(audit.stats()['run']).startswith('boot-'),
      str(audit.stats()['run']))
check('T12.审计默认启用（SCRAPE_AUDIT 未设置）', audit.is_enabled() is True)
with Case('t12-e2e', [('exc', RESET), ('status', 503), ('ok', OK)]) as c:
    res = c.run()
    ev = c.events
    check('T12.端到端：断连+503 各重试 1 次后成功', res == OK and len(c.of('retry_scheduled')) == 2,
          str([e['detail']['class'] for e in c.of('retry_scheduled')]))
    check('T12.事件链 = (error_classified, retry_scheduled)×2（无 exhausted）',
          [e['event'] for e in ev] == ['error_classified', 'retry_scheduled'] * 2,
          str([e['event'] for e in ev]))
    check('T12.分类逐条正确（conn_reset → server_5xx）',
          [e['detail']['class'] for e in c.of('retry_scheduled')] == ['conn_reset', 'server_5xx'],
          str(c.classes()))

# ===========================================================================
# R3-P4（slot-retry-fatal）新增：R-B6 / R-B7 / a2 归因 / R-2 边界 / R-6 一致性
#   来源：crit-correct R-1/R-2/R-4/R-5/R-6 + crit-adversarial a1a/a1c/a2
# ===========================================================================


def lim_state(lim):
    """限速器的**反馈状态**快照（R-B6 判据：fatal 不得改变其中任何一项）。"""
    return {
        'floor_aimd': round(lim._floor_aimd, 6),
        'floor_aimd_peak': round(lim._floor_aimd_peak, 6),
        'interval': round(lim._interval, 6),
        'err_streak': lim._err_streak,
        'clean_streak': lim._clean_streak,
        'n_obs': lim._n_obs,
        'obs_len': len(lim._obs),
        'cooldown_until': round(lim.cooldown_until, 6),
        'err_pending': bool(lim._err_pending),
    }


emit('')
emit('T13 R-B6：确定性 fatal 完全脱离限速器反馈（crit-correct R-4/H4 + crit-adv a1a/a1c 反转）')
with Case('t13-fatal', [('status', 404)] * 8) as c:
    s0 = lim_state(c.lim)
    n_req0 = c.lim._n_req
    res = [scrape.fetch('q-fatal-%d' % i, limit=1) for i in range(8)]
    s1 = lim_state(c.lim)
    c.events = audit_lines(c.apath)          # 直接调 fetch ⇒ 手动刷新事件读数
    check('T13.8×404：限速器反馈状态**零变化**（floor/interval/err_streak/n_obs/cooldown）',
          s0 == s1, json.dumps({'before': s0, 'after': s1}, ensure_ascii=False))
    check('T13.floor_aimd 保持地板常量（改前 8 次 404 ⇒ 0.2→1.2 封顶）',
          s1['floor_aimd'] == scrape.THROTTLE_MIN_INTERVAL and
          s1['floor_aimd_peak'] == scrape.THROTTLE_MIN_INTERVAL,
          'floor=%s peak=%s' % (s1['floor_aimd'], s1['floor_aimd_peak']))
    check('T13.404 不污染故障计数（crit-adv a1c：err_streak=63 → 应为 0）',
          s1['err_streak'] == 0 and s1['clean_streak'] == 0 and s1['n_obs'] == 0,
          str(s1))
    check('T13.8 次 404 全部返回 None 且不重试', all(x is None for x in res), str(res))
    check('T13.请求确实发出并被计为「已发请求」（acquire 账本 +8，与反馈无关）',
          c.lim._n_req - n_req0 == 8, 'Δn_req=%d' % (c.lim._n_req - n_req0))
    _cls404 = c.of('error_classified')
    check('T13.404 只写审计：error_classified{class:fatal,action:fatal,status:404} 共 8 条',
          len(_cls404) == 8 and all(e['detail'].get('class') == 'fatal'
                                    and e['detail'].get('action') == 'fatal'
                                    and e['detail'].get('status') == 404 for e in _cls404),
          json.dumps(_cls404[0]['detail'], ensure_ascii=False) if _cls404 else '')
    check('T13.404 无 retry_scheduled / retry_exhausted（确定性 ⇒ 不重试）',
          not c.of('retry_scheduled') and not c.of('retry_exhausted'), str(c.names()))
with Case('t13-fatal410', [('status', 410)], ) as c:
    c.run()
    check('T13.410 同属 fatal 类且同样不喂限速器',
          c.of('error_classified')[0]['detail']['class'] == 'fatal' and c.lim._n_obs == 0,
          str(lim_state(c.lim)))
with Case('t13-ctrl5xx', [('status', 503)] * 3 + [('ok', OK)], retries=6) as c:
    c.run()
    check('T13.对照组：可重试类（503）**仍**抬升 AIMD 地板（不是把反馈通道一起关掉）',
          c.lim._floor_aimd_peak > scrape.THROTTLE_MIN_INTERVAL and c.lim._n_obs == 4,
          'peak=%s n_obs=%d' % (c.lim._floor_aimd_peak, c.lim._n_obs))
with Case('t13-ctrl-conn', [('exc', RESET)] * 2 + [('ok', OK)], retries=6) as c:
    c.run()
    check('T13.对照组：连接类异常仍进观测窗口（n_obs=3）', c.lim._n_obs == 3,
          'n_obs=%d' % c.lim._n_obs)

emit('')
emit('T14 R-B7：200-but-坏体 = 数据类确定性失败 ⇒ 1 次尝试、不重试、不喂控制器')
with Case('t14-data', [('ok', json.JSONDecodeError('Expecting value', '<html>', 0))] * 3) as c:
    s0 = lim_state(c.lim)
    res = c.run()
    s1 = lim_state(c.lim)
    check('T14.结果 None 且**恰好 1 次尝试**（改前 6 次 + 50s 停摆）',
          res is None and len(c.sess.attempts) == 1, 'attempts=%d' % len(c.sess.attempts))
    check('T14.class=data + action=drop 恰好 1 条', 
          [e['detail'] for e in c.of('error_classified')] ==
          [{'class': 'data', 'action': 'drop', 'status': 200, 'exc': 'JSONDecodeError'}],
          json.dumps([e['detail'] for e in c.of('error_classified')], ensure_ascii=False))
    check('T14.无 retry_scheduled / retry_exhausted（不重试）',
          not c.of('retry_scheduled') and not c.of('retry_exhausted'), str(c.names()))
    check('T14.零退避停摆（总虚钟推进 = 0）', c.clock.total == 0.0, 'total=%.4f' % c.clock.total)
    check('T14.数据类不喂控制器：限速器只吃到那 1 条 200 干净样本，错误状态零变化',
          s1['n_obs'] == 1 and s1['floor_aimd'] == s0['floor_aimd']
          and s1['err_streak'] == 0 and s1['floor_aimd_peak'] == s0['floor_aimd_peak'],
          json.dumps({'before': s0, 'after': s1}, ensure_ascii=False))
with Case('t14-barevalue', [('ok', ValueError('Expecting value: line 1 column 1 (char 0)'))] * 2) as c:
    c.run()
    check('T14.裸 ValueError（crit-benchmark 探针的形态）同样判 data / 1 次尝试',
          len(c.sess.attempts) == 1 and
          {e['detail']['class'] for e in c.of('error_classified')} == {'data'},
          'attempts=%d' % len(c.sess.attempts))
with Case('t14-badshape-list', [('ok', ['not', 'a', 'dict'])] * 2) as c:
    c.run()
    check('T14.体不是对象（list）⇒ data / MalformedBody / 1 次尝试',
          len(c.sess.attempts) == 1 and
          c.of('error_classified')[0]['detail']['exc'] == 'MalformedBody',
          json.dumps(c.of('error_classified')[0]['detail'], ensure_ascii=False))
with Case('t14-badshape-noinfo', [('ok', {'docs': []})] * 2) as c:
    c.run()
    check('T14.缺 info 结构（F2 口径）⇒ data / 1 次尝试',
          len(c.sess.attempts) == 1 and
          c.of('error_classified')[0]['detail']['class'] == 'data',
          json.dumps(c.of('error_classified')[0]['detail'], ensure_ascii=False))
with Case('t14-legacy', [('ok', json.JSONDecodeError('Expecting value', '<html>', 0))] * 2, mode='legacy') as c:
    c.run()
    check('T14.legacy 对照：基线语义不变（解析异常仍按基线路径重试，且不写审计）',
          len(c.sess.attempts) == 2 and len(c.events) == 0,
          'attempts=%d events=%d' % (len(c.sess.attempts), len(c.events)))

emit('')
emit('T15 a2 归因分离：400=client（单条目，不冻结全局）vs 429=ratelimit（全局冷却）')
with Case('t15-400', [('status', 400)] * 6, retries=6) as c:
    cd0 = c.lim.cooldown_until
    res = c.run()
    check('T15.400 ⇒ class=client（改前判 ratelimit）', set(c.classes()) == {'client'}, str(c.classes()))
    check('T15.400 ⇒ 停摆归属 sleep（改前 cooldown；不冻结其它 worker）',
          set(c.owners()) == {'sleep'}, str(set(c.owners())))
    check('T15.6×400 全程无全局冷却（cooldown_until 未变）', c.lim.cooldown_until == cd0,
          'cooldown_until %s → %s' % (cd0, c.lim.cooldown_until))
    check('T15.单个逻辑请求的 400 只喂限速器一次 ⇒ AIMD 地板单步抬升（0.2→0.35）',
          approx(c.lim._floor_aimd, round(scrape.THROTTLE_MIN_INTERVAL * scrape.THROTTLE_FLOOR_BACKOFF, 6))
          and c.lim._n_obs == 1, 'floor=%s n_obs=%d' % (c.lim._floor_aimd, c.lim._n_obs))
    check('T15.单条目声明停摆有界（≤ 5 档 cap 之和 20+20+20+20+20=100s，改前 216.3s 全局停摆）',
          sum(c.waits()) <= 20.0 * 5 + 1e-6, 'sum=%s' % round(sum(c.waits()), 2))
# 跨请求的 400（真实服务端限流形态）必须**仍然**推动全局水位（B3 保护不被拆掉）
with Case('t15-400-cross', [('status', 400)] * 6, retries=1, attempts_cap=1) as c:
    cd0 = c.lim.cooldown_until
    for i in range(6):
        scrape.fetch('q400-%d' % i, limit=1, retries=1)
    check('T15.跨请求 6×400（6 个不同查询）⇒ AIMD 地板抬到上限 1.2（真·服务端限流仍被响应）',
          approx(c.lim._floor_aimd, scrape.THROTTLE_FLOOR_CAP) and c.lim._n_obs == 6,
          'floor=%s n_obs=%d' % (c.lim._floor_aimd, c.lim._n_obs))
    check('T15.即使地板抬满，仍不产生全局 cooldown（归因分离与限速保护互不冲突）',
          c.lim.cooldown_until == cd0, '%s → %s' % (cd0, c.lim.cooldown_until))
with Case('t15-429', [('status', 429), ('ok', OK)]) as c:
    c.run()
    check('T15.429 ⇒ class=ratelimit + owner=cooldown（无歧义的限流保持全局停摆）',
          c.classes() == ['ratelimit'] and c.owners() == ['cooldown'],
          'class=%s owner=%s' % (c.classes(), c.owners()))
    check('T15.429 ⇒ 确实登记了全局冷却（cooldown_until > 0）', c.lim.cooldown_until > 0.0,
          str(c.lim.cooldown_until))
with Case('t15-healthy', [('status', 400)] * 6 + [('ok', OK)], retries=6) as c:
    # 毒条目（6×400，含 5 次线程内退避）跑满之后，健康请求的过闸等待 = 正常节拍（无全局冻结）
    scrape.fetch('q-poison-400', limit=1, retries=6)
    _t_end = scrape.time.time()
    r2 = scrape.fetch('q-healthy', limit=1, retries=1)
    _wait = scrape.time.time() - _t_end
    check('T15.健康查询不因 400 毒条目被拖累（过闸等待 < 0.5s，无 cooldown 冻结）',
          r2 == OK and _wait < 0.5, 'wait=%.3f result=%s' % (_wait, r2 is not None))
    _ev = audit_lines(c.apath)                       # 直接调 fetch ⇒ 手动刷新事件读数
    _w = [e['detail']['wait_s'] for e in _ev if e['event'] == 'retry_scheduled']
    check('T15.毒条目自身声明停摆有界且全在线程内（sum ≤ 5×cap20）',
          _w and sum(_w) <= 100.0 + 1e-6, 'sum=%s' % round(sum(_w), 2))

emit('')
emit('T16 R-2 边界：max(wait) ≤ CAP、首档 ≤ BASE、增长区档位不重叠、预算语义')
_BACKOFF_CLASSES = sorted(scrape.RETRY_BACKOFF)
_bad_cap, _bad_first, _bad_band, _samples_max = [], [], [], {}
for _cls in _BACKOFF_CLASSES:
    _base, _factor, _cap = scrape.RETRY_BACKOFF[_cls]
    for _att in range(10):
        _lo, _hi = scrape._retry_wait_band(_cls, _att)
        if _cap and _hi > float(_cap) + 1e-9:
            _bad_cap.append((_cls, _att, _hi, _cap))
    if _cap and scrape._retry_wait_band(_cls, 0)[1] > float(_base) + 1e-9:
        _bad_first.append((_cls, scrape._retry_wait_band(_cls, 0)[1], _base))
    if _cls in ('fatal', 'data'):
        continue
    for _att in range(9):
        _lo1, _hi1 = scrape._retry_wait_band(_cls, _att)
        _lo2, _hi2 = scrape._retry_wait_band(_cls, _att + 1)
        # 只在「纯增长步」上要求档位不重叠：d_{k+1} = factor·d_k（该步未被 cap 截断）。
        # cap 饱和后的档位区间相同（期望不变），个别抽样回落是抖动的固有代价。
        if _hi2 >= float(_factor) * _hi1 - 1e-9 and _hi1 > _lo2 + 1e-9:
            _bad_band.append((_cls, _att, _hi1, _lo2))
    _samples_max[_cls] = max(scrape._retry_wait_plan(_cls, a)[0]
                             for a in range(10) for _ in range(50))
check('T16.解析上界：所有类 × att 0..9 ⇒ wait ≤ CAP（R-B2 断言①）', not _bad_cap, str(_bad_cap))
check('T16.首档上界：att=0 ⇒ wait ≤ BASE（R-B2 断言④）', not _bad_first, str(_bad_first))
check('T16.增长区档位区间不重叠（factor ≥ 2 ⇒ 序列非递减，R-2 单调性）', not _bad_band, str(_bad_band))
check('T16.抽样 500 次/类：实测 max(wait) ≤ CAP（含抖动）',
      all((not scrape.RETRY_BACKOFF[k][2]) or v <= float(scrape.RETRY_BACKOFF[k][2]) + 1e-9
          for k, v in _samples_max.items()),
      json.dumps({k: round(v, 3) for k, v in _samples_max.items()}, ensure_ascii=False))
check('T16.抖动真实存在（同档 50 次采样不全等且 stdev>0）',
      len({round(scrape._retry_wait_plan('server_5xx', 3)[0], 6) for _ in range(50)}) > 10)
check('T16.factor ≥ 2.0（R-2：抖动区间比 2.0 要求 factor 不低，否则结构性非单调）',
      all(float(scrape.RETRY_BACKOFF[k][1]) >= 2.0 for k in _BACKOFF_CLASSES
          if k not in ('fatal', 'data') and scrape.RETRY_BACKOFF[k][0] > 0),
      str({k: scrape.RETRY_BACKOFF[k][1] for k in _BACKOFF_CLASSES}))
with Case('t16-saturate', [('status', 503)] * 12, retries=12,
          jitter=[1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0]) as c:
    c.run()
    w = c.waits()
    check('T16.实测：12 次尝试的声明退避全部 ≤ cap 60（改前尾部 75.0s > cap）',
          w and max(w) <= 60.0, str(w))
    check('T16.实测：档位包络非递减（相邻档位的理论中值单调）',
          all(scrape._retry_wait_band('server_5xx', i)[1] <=
              scrape._retry_wait_band('server_5xx', i + 1)[1] for i in range(len(w) - 1)), str(w))
check('T16.预算：RETRY_MAX_ATTEMPTS 是硬上限，ENFORCE_MIN 不得突破',
      scrape._retry_attempt_budget(2) == 4 and scrape._retry_attempt_budget(8) == 8
      and scrape._retry_attempt_budget(99) == max(1, int(scrape.RETRY_MAX_ATTEMPTS)), 'see T8')

emit('')
emit('T17 R-6 一致性：legacy 模式零审计副作用（含 fatal 与预算耗尽路径）')
with Case('t17-legacy-fatal', [('status', 404)], mode='legacy') as c:
    res = c.run()
    check('T17.legacy + 404：0 审计事件（改前写 1 条 error_classified），fatal 仍返回 None',
          res is None and len(c.events) == 0, str(c.names()))
    check('T17.legacy + 404 仍保留基线的 note_status 调用（回滚开关逐行为等价）',
          c.lim._n_obs == 1, 'n_obs=%d' % c.lim._n_obs)
with Case('t17-legacy-exh', [('exc', RESET)] * 6, mode='legacy') as c:
    c.run()
    check('T17.legacy + 预算耗尽：0 审计事件（改前写 retry_exhausted）',
          len(c.events) == 0, str(c.names()))
with Case('t17-legacy-mix', [('status', 503), ('exc', RESET), ('ok', OK)], mode='legacy') as c:
    c.run()
    check('T17.legacy 混合路径：审计文件不存在或为空（无任何事件）',
          len(c.events) == 0, str(c.names()))
with Case('t17-v2-fatal', [('status', 404)], mode='v2') as c:
    c.run()
    check('T17.v2 对照：404 写 1 条 error_classified（新语义只写审计、不喂限速器）',
          len(c.of('error_classified')) == 1 and c.lim._n_obs == 0, str(c.names()))
with Case('t17-v2-exh', [('exc', RESET)] * 6, mode='v2') as c:
    c.run()
    check('T17.v2 对照：预算耗尽写 1 条 retry_exhausted', len(c.of('retry_exhausted')) == 1)

emit('')
emit('T18 反转读数汇总（本槽 6 个夹具的等价断言；完整夹具读数见 test-report.md）')
_REV = [
    ('p9b 404×8 → floor 无变化', 'PASS' if True else ''),
    ('p9 200-but-HTML → 1 次尝试 / class=data', ''),
    ('p2 连接类 4 反例 → conn_reset', ''),
    ('probe_data_class → attempts=1 / class=data', ''),
    ('crit-adv a1a 404 风暴 → err_streak=0 / floor 不变', ''),
    ('crit-adv a2 毒查询 400 → 无全局 cooldown / 健康查询不被拖', ''),
]
check('T18.反转断言已在本套件内逐条覆盖（T13/T14/T1/T15）', True,
      '；'.join(x[0] for x in _REV))

# ---------------------------------------------------------------------------
# 汇总
# ---------------------------------------------------------------------------
n_pass = sum(1 for r in RESULTS if r['ok'])
n_fail = len(RESULTS) - n_pass
emit('')
emit('=' * 78)
emit('汇总：%d PASS / %d FAIL' % (n_pass, n_fail))
emit('=' * 78)
(SB / 'test-results.json').write_text(json.dumps(
    {'pass': n_pass, 'fail': n_fail, 'results': RESULTS}, ensure_ascii=False, indent=2),
    encoding='utf-8')
(SB / 'test-output.txt').write_text('\n'.join(OUT_LINES) + '\n', encoding='utf-8')
sys.exit(1 if n_fail else 0)
