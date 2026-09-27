#!/usr/bin/env python3
"""R3·slot-converge 测试台 —— 块级兜底（C2）/ 收敛判据（F1·F6）/ 停滞判据（F5）/ 边界修复（F2·F3）

设计约束（与 R2 slot-bench / p4 测试台同款）：
  · **零网络**：全部请求走内存 mock（patch `fetch` / `get_total`），不连任何 socket，
    更不碰图书馆域名与生产路径；
  · 被测源码先**复制**到 sandbox/<scenario>/<variant>/mod/ 再按路径 import，
    绝不 import 原文件、绝不修改仓库文件；"只拷 scrape.py" 的布局与 bench 沙箱一致
    （memguard/heartbeat 缺席 ⇒ 走 fail-open 分支，本身就是一条要回归的路径）；
  · 快/慢两条路都给读数：每条断言都打印真实读数（rc / unique / 审计事件计数 / 报告字段）。

用法：
    python3 test_converge.py            # 全量跑（父进程编排）
    python3 test_converge.py --child …  # 子进程入口（父进程内部使用）
"""
import argparse
import importlib.util
import json
import os
import random
import re
import shutil
import string
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
BASELINE = Path('/tmp/lib-opt-work/R3/refs/scrape-r2cand.py')     # 只读基线（R2 收口候选版）
MODIFIED = HERE / 'scrape.py'                                     # 本 slot 产物
SANDBOX = HERE / 'sandbox'                  # R3p4/slot-converge-f1 路径适配（不再写 R3/）
READINGS = HERE / 'test-results'

# ===========================================================================
# 靶模式（R3-P4·P4.5 · R-4 修复）
# ---------------------------------------------------------------------------
# 病灶（KI-3/R-4）：本套件有 3 条断言是**单槽不变量**，在任何「合流靶」上必然失败：
#   · scope-discipline：scope-check.py 判"改动是否全落在 **converge 槽登记区域**内"；
#   · fetch/get_total 逐字节未变：converge 槽承诺不碰 fetch，而 retry 槽的重试逻辑就写在 fetch 里；
#   · patch-roundtrip：断言"converge-f1.patch 施加回 converge 产物 == 本目录 scrape.py"
#     （自指血缘不变量；合流版还含 retry 段 ⇒ 必然不等）。
# 处置（与 KI-4 同法：只改候选副本、fail-closed、lineage/ 留档、原件只读）：
#   **合流模式（merge）**把这三条换成**合流作用域下的等价不变量**（强于"整条豁免"）：
#   ① 分支重建：把 r2cand 按补丁链重放成分支件（converge 分支 / retry 分支 / 合流件），
#      每一步 --fuzz=0，且可核对时**必须**与只读槽产物逐字节一致；
#   ② scope-discipline(merge)：converge 分支件本身仍过单槽纪律（同一算法、换靶）+ MOD 的
#      **变更区域集合**必须恰好等于两分支区域之并（无外来改动）+ new_defs 并集闭合；
#   ③ fetch/get_total(merge)：get_total 逐字节未变（原判据保留）+ MOD 的 fetch 体与 **retry
#      分支件**逐字节相同（归因：converge 对 fetch 零贡献，且 retry 的贡献真实存在）；
#   ④ patch-roundtrip(merge)：**全链重放**（retry→retry-fatal→converge→converge-f1→
#      align-converge[→v2.1 增量]）逐字节还原本目录 scrape.py —— 比单槽自指往返更强的
#      "交付物自洽性"证据。
# 模式解析：`--target-mode auto|single|merge`；auto = 同目录同时有 converge.patch 与
#   retry.patch（即合流/生产者树）⇒ merge，否则 single。解析结果**写进读数**，绝不静默换靶。
# ===========================================================================
TARGET_MODE = 'single'          # 由 main() 按 --target-mode 解析后设定

# 合流链（顺序 = 施加顺序；每条都必须 --fuzz=0 通过）：
#   分支 C（converge 单槽）: r2cand +converge +converge-f1
#   分支 R（retry   单槽）: r2cand +retry +retry-fatal
#   合流 V2.0            : 分支R +converge +converge-f1 +audit-align-converge
#   合流 V2.1（= 本目录）: 合流V2.0 +lineage/scrape-v2.0-to-v2.1.diff（存在时）
MERGE_CHAIN_CONV = ['converge.patch', 'converge-f1.patch']
MERGE_CHAIN_RETRY = ['retry.patch', 'retry-fatal-scrape.patch']
MERGE_CHAIN_MERGED = ['retry.patch', 'retry-fatal-scrape.patch', 'converge.patch',
                      'converge-f1.patch', 'audit-align-converge.patch']
MERGE_DELTA = 'lineage/scrape-v2.0-to-v2.1.diff'    # v2.1 增量（候选副本的 lineage 留档）
# 只读外部参照（可核对时逐字节比对；缺席 ⇒ 读数显式记 skipped，但合流链本身的判据不放宽）
MERGE_REF_SLOTS = {
    'conv_slot': '/tmp/lib-opt-work/R3/impl/slot-converge/scrape.py',
    'conv_f1_slot': '/tmp/lib-opt-work/R3p4/slot-converge-f1/scrape.py',
    'retry_slot': '/tmp/lib-opt-work/R3/impl/slot-retry/scrape.py',
    'retry_fatal_slot': '/tmp/lib-opt-work/R3p4/slot-retry-fatal/scrape.py',
}

CATALOG_SEED = 20260915
CATALOG_N = 1200            # 种子 'A' 的库藏量（> LEAF_MAX=490 ⇒ 必须走分支细分）
SLEEP_FAST_GE = 5.0         # ≥ 该值的 sleep 在测试进程内被压缩（30/45/20s 的冷却）
SLEEP_FAST_TO = 0.02


# ===========================================================================
# 语料：确定性合成目录树（call number → mms）
# ===========================================================================
def build_calls(n=CATALOG_N, seed=CATALOG_SEED):
    rnd = random.Random(seed)
    letters = string.digits + string.ascii_uppercase
    seen, out = set(), []
    while len(out) < n:
        c = 'A' + ''.join(rnd.choice(letters) for _ in range(6))
        if c in seen:
            continue
        seen.add(c)
        out.append(c)
    return sorted(out)


CALLS = build_calls()


def total_of(prefix):
    return sum(1 for c in CALLS if c.startswith(prefix))


def _order(calls, sort, prefix):
    """不同排序给不同顺序（复刻真实 API 的"排序视角"，让叶子补抓的并集有意义）。"""
    if sort == 'title':
        return sorted(calls, key=lambda c: c[::-1])
    if sort == 'date':
        return sorted(calls, key=lambda c: (hash((prefix, c, 'd')) % 9973, c))
    return sorted(calls)


def _doc(call):
    return {'pnx': {'display': {'mms': [call], 'title': ['T-' + call],
                                'creator': ['C'], 'publisher': ['P'],
                                'creationdate': ['2020'], 'language': ['chi'],
                                'type': ['book']}},
            'delivery': {'holding': [{'libraryCode': 'LIB', 'mainLocation': 'M',
                                      'subLocation': 'S', 'subLocationCode': 'SC',
                                      'callNumber': call, 'availabilityStatus': 'available'}]}}


def _prefix_of(q):
    parts = str(q).split(',')
    return parts[2] if len(parts) >= 3 else ''


# ===========================================================================
# 子进程：加载被测源码 + 装 mock + 跑 main()
# ===========================================================================
def install_mock(sc, scenario, outdir):
    """把 fetch / get_total 换成内存 mock；按 scenario 注入故障。"""
    st = {'fetch': 0, 'total': 0, 'injected': False}
    outdir = Path(outdir)
    hang_s = float(os.environ.get('MOCK_HANG_S', '0') or 0)
    rm_after = int(os.environ.get('MOCK_RMTREE_AFTER_FETCH', '0') or 0)
    _real_sleep = time.sleep

    def mock_fetch(q, limit=1, offset=0, sort=None, retries=6):
        st['fetch'] += 1
        prefix = _prefix_of(q)
        # ---- F3 注入：第 N 次 fetch 时把整个 output 目录删掉 ----
        if rm_after and st['fetch'] == rm_after and not st['injected']:
            st['injected'] = True
            shutil.rmtree(str(outdir), ignore_errors=True)
            print(f"__INJECT__ rmtree(out) at fetch#{st['fetch']} exists={outdir.exists()}",
                  flush=True)
        # ---- F2 注入：探测类请求（limit=1）且 prefix 非根 → 200 但缺 info ----
        if scenario == 'f2_missing_info' and limit == 1 and len(prefix) > 1:
            st['malformed'] = st.get('malformed', 0) + 1
            return {'docs': [], 'vid': 'MOCK'}          # 无 'info' ⇒ 真 get_total 抛 KeyError
        # ---- R3p4·P4-A 注入：**条目级** total 探测（含种子块）也 200-缺-info ----
        #   crit-correct C-1/C-2/C-3 的复现谓词：`len(prefix) > 1` 天然打不到条目 total
        #   （条目级 total 解析只发生在 total=None 的种子上）⇒ 这里覆盖种子块那扇门。
        if scenario == 'entry_missing_info' and limit == 1:
            st['malformed'] = st.get('malformed', 0) + 1
            return {'docs': [], 'vid': 'MOCK'}
        # ---- 停滞注入：分支大页面请求挂住（模拟服务端不响应；叶子请求不受影响） ----
        if hang_s and limit >= 50 and prefix == 'A':
            _real_sleep(hang_s)
        calls = [c for c in CALLS if c.startswith(prefix)]
        page = _order(calls, sort, prefix)[offset:offset + limit]
        return {'info': {'totalResultsLocal': len(calls)},
                'docs': [_doc(c) for c in page]}

    def mock_get_total(prefix, sort=None):
        st['total'] += 1
        if scenario == 'probe_all_dead' and len(prefix) > 1:
            return None                                  # 子块探测全灭
        if scenario == 'total_none' and prefix == 'A':
            return None                                  # R3p4·P4-A：条目 total 解析失败（None 语义）
        return total_of(prefix)

    sc.fetch = mock_fetch
    if scenario in ('probe_all_dead', 'total_none'):
        sc.get_total = mock_get_total
    # ---- R3p4·P4-A 注入：条目体内抛异常（非 get_total 路径）→ 走条目异常预算 ----
    if scenario == 'item_exc':
        _real_leaf = sc.scrape_leaf

        def boom_leaf(prefix, total, deep_retry=True):
            if prefix == 'A1':
                raise RuntimeError('INJECTED item-level failure for A1')
            return _real_leaf(prefix, total, deep_retry=deep_retry)

        sc.scrape_leaf = boom_leaf
    return st


def load_under_test(path, name='scrape_ut'):
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def patch_limiter(sc):
    """收敛逻辑与限速无关：把 LIMITER 的等待去掉，只保留语义字段（cooldown_until 只读观测）。"""
    try:
        sc.LIMITER.acquire = lambda *a, **k: None
        sc.LIMITER.cooldown = lambda sec: None
    except Exception as e:
        print(f"__WARN__ limiter patch failed: {e}", flush=True)


def patch_sleep():
    """把冷却用的长 sleep（20/30/45s）压成 0.02s；短 sleep 原样保留（停滞注入要真时间）。"""
    real = time.sleep

    def fast(s):
        return real(SLEEP_FAST_TO) if (s and s >= SLEEP_FAST_GE) else real(s)

    time.sleep = fast


def child_main(args):
    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)
    os.environ['SCRAPE_OUT_DIR'] = str(outdir)
    patch_sleep()
    sc = load_under_test(args.module)
    patch_limiter(sc)
    install_mock(sc, args.child, outdir)
    sys.argv = ['scrape_under_test.py', 'A']             # 种子只有 'A'（确定性）
    rc = sc.main()
    counts = sc.audit_counts() if hasattr(sc, 'audit_counts') else {}
    print(f"__RC__={rc}", flush=True)
    print(f"__COUNTS__={json.dumps(counts, ensure_ascii=False)}", flush=True)
    return rc if isinstance(rc, int) else 0       # 退出码 = 收敛判据（F6）


# ===========================================================================
# 父进程：编排 / 读数 / 断言
# ===========================================================================
def scenario_env(scenario):
    """每个 scenario 的公共环境（阈值全部缩小，使测试秒级完成）。"""
    env = {
        'PYTHONUNBUFFERED': '1',
        'SCRAPE_LEAF_RETRY_S': '0.05',
        'SCRAPE_BRANCH_RECHECK_S': '0.05',
        'SCRAPE_PROBE_DEAD_DELAY_S': '0.05',
        'SCRAPE_ITEM_EXC_DELAY_S': '0.05',          # R3p4·P4-A：条目异常回队尾延迟（测试压缩）
        'SCRAPE_STALL_MONITOR': '0',
        'SCRAPE_STALL_AGE_S': '900',
        'SCRAPE_STALL_STOP_AGE_S': '1800',
        'PYTHONDONTWRITEBYTECODE': '1',
    }
    if scenario == 'stall_audit' or scenario == 'stall_stop':
        env.update({
            'SCRAPE_STALL_MONITOR': '1',
            'SCRAPE_STALL_AGE_S': '0.30',
            'SCRAPE_STALL_STOP_AGE_S': '1.00',
            'SCRAPE_STALL_CHECK_S': '0.05',
            'SCRAPE_STALL_ACTION': 'stop' if scenario == 'stall_stop' else 'audit',
            'SCRAPE_STALL_EXIT': '4',
            'MOCK_HANG_S': '1.60',
        })
    if scenario == 'outdir_gone':
        env['MOCK_RMTREE_AFTER_FETCH'] = '4'
    return env


def run_variant(scenario, variant, timeout=240, extra_env=None, module=None, args=None):
    d = SANDBOX / scenario / variant
    if d.exists():
        shutil.rmtree(d)
    moddir = d / 'mod'
    outdir = d / 'out'
    moddir.mkdir(parents=True)
    outdir.mkdir(parents=True)
    src = Path(module) if module else (BASELINE if variant == 'baseline' else MODIFIED)
    dst = moddir / 'scrape_under_test.py'
    shutil.copy2(src, dst)
    env = dict(os.environ)
    env.update(scenario_env(scenario))
    env['SCRAPE_OUT_DIR'] = str(outdir)
    if extra_env:
        env.update({k: str(v) for k, v in extra_env.items()})
    argv = [sys.executable, str(HERE / 'test_converge.py'), '--child', scenario,
            '--module', str(dst), '--out', str(outdir)]
    if args:
        argv += list(args)
    t0 = time.time()
    kill = False
    try:
        p = subprocess.run(argv, cwd=str(moddir), env=env, capture_output=True,
                           text=True, timeout=timeout)
        rc, outp, errp = p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired as e:
        kill = True
        rc = None
        outp = (e.stdout or b'').decode('utf-8', 'replace') if isinstance(e.stdout, bytes) else (e.stdout or '')
        errp = (e.stderr or b'').decode('utf-8', 'replace') if isinstance(e.stderr, bytes) else (e.stderr or '')
    wall = time.time() - t0
    m = re.search(r'__RC__=(-?\d+)', outp or '')   # 进程内收敛判据（F6）；os._exit 路径无此行
    verdict = int(m.group(1)) if m else None
    return {'scenario': scenario, 'variant': variant, 'src': str(src), 'dir': str(d),
            'out': str(outdir), 'rc': verdict if verdict is not None else rc,
            'proc_rc': rc, 'verdict_rc': verdict,
            'wall_s': round(wall, 3), 'timeout': kill,
            'stdout': outp, 'stderr': errp, **read_out(outdir)}


def read_out(outdir):
    """读回一次运行的全部机器可读读数。"""
    out = Path(outdir)
    res = {'records_lines': 0, 'records_unique': 0, 'audit': [], 'audit_counts': {},
           'conv': None, 'progress': None, 'log_has_all_done': False,
           'log_has_unconverged': False, 'log_has_dir_recreated': False,
           'log_r3_lines': [], 'out_files': []}
    rec = out / 'records.jsonl'
    if rec.exists():
        mms = set()
        with open(rec, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                res['records_lines'] += 1
                try:
                    mms.add(json.loads(line).get('mms'))
                except ValueError:
                    pass
        res['records_unique'] = len(mms)
    ap = out / 'audit.jsonl'
    if ap.exists():
        for line in ap.read_text(encoding='utf-8', errors='replace').splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            res['audit'].append(ev)
            res['audit_counts'][ev.get('event')] = res['audit_counts'].get(ev.get('event'), 0) + 1
    cp = out / 'convergence-report.json'
    if cp.exists():
        try:
            res['conv'] = json.loads(cp.read_text())
        except ValueError:
            res['conv'] = {'_parse': 'failed'}
    pp = out / 'progress.json'
    if pp.exists():
        try:
            prog = json.loads(pp.read_text())
            res['progress'] = {'todo': len(prog.get('todo') or []),
                               'done': len(prog.get('done') or []),
                               'gaps': len(prog.get('gaps') or []),
                               'stats': prog.get('stats'),
                               'probe_dead': len(prog.get('probe_dead') or [])}
        except ValueError:
            res['progress'] = {'_parse': 'failed'}
    lp = out / 'scrape.log'
    if lp.exists():
        txt = lp.read_text(encoding='utf-8', errors='replace')
        res['log_has_all_done'] = '全部完成' in txt
        res['log_has_unconverged'] = bool(re.search(r'未收敛（exit \d+）', txt))   # 只认最终裁决行
        res['log_has_dir_recreated'] = 'output 目录消失' in txt
        res['log_r3_lines'] = [l for l in txt.splitlines() if '[r3]' in l][:12]
        res['log_tail'] = txt[-1200:]
    res['out_files'] = sorted(p.name for p in out.glob('*'))
    return res


class Checks:
    def __init__(self, name):
        self.name = name
        self.items = []

    def add(self, ok, label, reading=''):
        self.items.append({'ok': bool(ok), 'label': label, 'reading': str(reading)[:400]})
        return bool(ok)

    @property
    def ok(self):
        return all(i['ok'] for i in self.items)


def scenario_normal(report):
    """e) 回归：正常路径 基线 vs 改后 —— 行为必须逐读一致（都能收敛 exit 0）。"""
    c = Checks('normal-converge')
    b = run_variant('normal', 'baseline')
    m = run_variant('normal', 'modified')
    exp = total_of('A')
    c.add(b['rc'] == 0, 'baseline rc=0（收敛）', f"rc={b['rc']}")
    c.add(m['rc'] == 0, 'modified rc=0（收敛）', f"rc={m['rc']}")
    c.add(m['records_unique'] == exp, f'modified 唯一 mms == 期望 {exp}',
          f"unique={m['records_unique']} lines={m['records_lines']}")
    c.add(m['records_unique'] == b['records_unique'],
          'baseline/modified 唯一 mms 一致（回归等价）',
          f"base={b['records_unique']} mod={m['records_unique']}")
    c.add((m['progress'] or {}).get('gaps', -1) == 0, '无遗留缺口（gaps=0）',
          f"progress={m['progress']}")
    conv = m['conv'] or {}
    c.add(conv.get('decided') == 'done' and conv.get('exit_code') == 0 and conv.get('terminal'),
          'convergence-report: decided=done/terminal/exit_code=0', json.dumps(
              {k: conv.get(k) for k in ('decided', 'terminal', 'exit_code', 'reason')},
              ensure_ascii=False))
    c.add(m['audit_counts'].get('convergence_decision', 0) >= 1,
          'convergence_decision 审计事件存在', f"counts={m['audit_counts']}")
    c.add(m['log_has_all_done'] and not m['log_has_unconverged'],
          '收敛运行时保留「全部完成」字样', f"all_done={m['log_has_all_done']}")
    c.add(b['audit_counts'] == {}, 'baseline 不产生 R3 审计（对照干净）',
          f"counts={b['audit_counts']}")
    report['normal'] = {'baseline': b, 'modified': m}
    return c


def scenario_probe_dead(report):
    """a) 探测全灭注入 → 块级兜底（重试轮→降级→修复轮）+ 审计；F1 假收敛必须被消灭。"""
    c = Checks('probe-all-dead')
    b = run_variant('probe_all_dead', 'baseline')
    m = run_variant('probe_all_dead', 'modified')
    ac = m['audit_counts']
    conv = m['conv'] or {}
    c.add(b['rc'] == 0 and b['log_has_all_done'] and b['records_unique'] < total_of('A'),
          'baseline：探测全灭 → 丢子树仍 exit 0 + 打印「全部完成」（F1 假收敛复现）',
          f"rc={b['rc']} unique={b['records_unique']}/{total_of('A')} "
          f"丢失={total_of('A') - b['records_unique']} gaps={(b['progress'] or {}).get('gaps')} "
          f"log_all_done={b['log_has_all_done']}")
    c.add(m['rc'] == 3, 'modified：未收敛 → exit 3（不再是 0）', f"rc={m['rc']}")
    c.add(m['records_unique'] > 0, '数据保险仍落盘（父块自身页已抓）',
          f"unique={m['records_unique']} lines={m['records_lines']}")
    c.add(ac.get('probe_all_dead', 0) >= 1, 'probe_all_dead 审计事件',
          f"probe_all_dead={ac.get('probe_all_dead')}")
    c.add(ac.get('probe_retry', 0) == 2, 'probe_retry：重试轮 2 次（= SCRAPE_PROBE_DEAD_RETRY）',
          f"probe_retry={ac.get('probe_retry')}")
    c.add(ac.get('repair_round_enter', 0) >= 2, 'repair_round_enter：进入修复轮',
          f"repair_round_enter={ac.get('repair_round_enter')}")
    c.add(ac.get('repair_exhausted', 0) >= 1, 'repair_exhausted：预算用尽标 terminal',
          f"repair_exhausted={ac.get('repair_exhausted')}")
    c.add(ac.get('convergence_decision', 0) >= 1 and
          any(e.get('event') == 'convergence_decision' and e.get('detail', {}).get('terminal')
              and e.get('detail', {}).get('exit_code') == 3 for e in m['audit']),
          'convergence_decision: terminal=True exit_code=3',
          json.dumps([{k: e['detail'].get(k) for k in ('decided', 'reason', 'exit_code',
                                                       'terminal', 'gaps')}
                      for e in m['audit'] if e.get('event') == 'convergence_decision'][-2:],
                     ensure_ascii=False))
    c.add(conv.get('exit_code') == 3 and conv.get('terminal') is True
          and (conv.get('gaps') or [{}])[0].get('degraded') is True,
          'convergence-report: exit_code=3 + 缺口标 degraded',
          json.dumps({'exit_code': conv.get('exit_code'), 'reason': conv.get('reason'),
                      'gaps': conv.get('gaps'), 'probe_dead': conv.get('probe_dead')},
                     ensure_ascii=False)[:380])
    c.add(m['log_has_unconverged'] and not m['log_has_all_done'],
          '未收敛时不打印「全部完成」（去 grep 化）',
          f"unconverged={m['log_has_unconverged']} all_done={m['log_has_all_done']}")
    c.add((m['progress'] or {}).get('probe_dead', 0) >= 1, 'progress.probe_dead 账本有记录',
          f"progress={m['progress']}")
    report['probe_all_dead'] = {'baseline': b, 'modified': m}
    return c


def scenario_f2(report):
    """边界 F2：HTTP 200 但缺 info → KeyError 从复验路径冲出（基线崩溃）；
    改后在调用侧分类审计 + 记为未定论 + 走块级兜底（未收敛而非崩溃）。"""
    c = Checks('f2-malformed-200')
    b = run_variant('f2_missing_info', 'baseline')
    m = run_variant('f2_missing_info', 'modified')
    ac = m['audit_counts']
    classes = [e.get('detail', {}).get('class') for e in m['audit']
               if e.get('event') == 'error_classified']
    c.add(b['rc'] not in (0, None) and 'KeyError' in (b['stderr'] + b['stdout']),
          'baseline：200-缺-info → KeyError 打死进程（缺陷复现：final_recheck 复验路径）',
          f"rc={b['rc']} keyerr={'KeyError' in (b['stderr'] + b['stdout'])} "
          f"unique={b['records_unique']}")
    c.add(m['rc'] == 3, 'modified：未收敛 → exit 3（进程存活）',
          f"rc={m['rc']} proc_rc={m.get('proc_rc')}")
    c.add('malformed_response' in classes, 'error_classified: class=malformed_response',
          f"classes={sorted(set(classes))} n={len(classes)}")
    c.add(ac.get('probe_all_dead', 0) >= 1, 'probe_all_dead 审计事件（异常也算未定论）',
          f"probe_all_dead={ac.get('probe_all_dead')}")
    c.add(m['records_unique'] > 0, '父块自身页仍落盘（不丢数据面）',
          f"unique={m['records_unique']}")
    report['f2'] = {'baseline': b, 'modified': m}
    return c


def scenario_entry_total_undetermined(report):
    """R3p4·P4-A（crit-correct C-1/C-2/C-3）：**条目级** total 解析失败的收敛残留门。

    注入两族（零网络 mock）：
      · total_none      ：种子块 get_total → None（探测失败语义）
      · entry_missing_info：种子块的 total 探测请求 200-缺-info ⇒ 真 get_total 抛 KeyError
    两条主循环（并发默认 SCRAPE_CONC=1 / 串行 SCRAPE_CONC=0）各跑一遍。

    期望（改后）：rc=3 未收敛 + gaps 出现 undetermined 缺口 + 审计有记账事件 +
    进程**有界退出**（不 hang）；对照（基线）：rc=0 + 「全部完成」= 缺陷复现。"""
    c = Checks('entry-total-undetermined')
    serial = {'SCRAPE_CONC': '0', 'SCRAPE_W_MAX': '1'}
    runs = {
        'total_none_conc': run_variant('total_none', 'modified', timeout=120),
        'total_none_serial': run_variant('total_none', 'modified', timeout=120, extra_env=serial),
        'entry_missing_conc': run_variant('entry_missing_info', 'modified', timeout=120),
        'entry_missing_serial': run_variant('entry_missing_info', 'modified', timeout=120,
                                            extra_env=serial),
        'base_total_none_conc': run_variant('total_none', 'baseline', timeout=120),
        'base_entry_missing_conc': run_variant('entry_missing_info', 'baseline', timeout=120),
    }
    for tag, r in runs.items():
        print(f"    [{tag}] rc={r['rc']} hang={r['timeout']} wall={r['wall_s']}s "
              f"unique={r['records_unique']} all_done={r['log_has_all_done']} "
              f"unc={r['log_has_unconverged']} counts={r['audit_counts']}", flush=True)

    # 1) 有界退出：四个改后变体都不得 hang
    c.add(all(not runs[k]['timeout'] for k in runs if k.startswith(('total_none', 'entry_missing'))),
          '改后 4 变体（total_none/entry_missing × conc/serial）均**有界退出**（无 hang）',
          json.dumps({k: {'hang': runs[k]['timeout'], 'wall_s': runs[k]['wall_s']}
                      for k in runs if k.startswith(('total_none', 'entry_missing'))},
                     ensure_ascii=False))
    # 2) 不再假收敛：rc=3（未收敛）
    for tag in ('total_none_conc', 'total_none_serial', 'entry_missing_conc',
                'entry_missing_serial'):
        r = runs[tag]
        conv = r['conv'] or {}
        c.add(r['rc'] == 3 and conv.get('exit_code') == 3,
              f'{tag}：不再 rc=0 假收敛 → exit 3 + 收敛报告 exit_code=3',
              f"rc={r['rc']} conv={{exit_code:{conv.get('exit_code')}, "
              f"reason:{conv.get('reason')}, decided:{conv.get('decided')}}} "
              f"log_all_done={r['log_has_all_done']}")
    # 3) 降级记账可见：gaps>0 且标 undetermined/degraded
    for tag in ('total_none_conc', 'total_none_serial', 'entry_missing_conc',
                'entry_missing_serial'):
        r = runs[tag]
        gaps = (r['conv'] or {}).get('gaps') or []
        c.add(len(gaps) >= 1 and gaps[0].get('type') == 'undetermined'
              and gaps[0].get('degraded') is True,
              f'{tag}：子树登记为未决缺口（type=undetermined, degraded=true，不再静默丢）',
              json.dumps(gaps, ensure_ascii=False)[:300])
    # 4) 审计记账：probe_retry（重试轮）+ probe_incomplete（降级）+ repair_exhausted（判未收敛）
    for tag in ('total_none_conc', 'total_none_serial', 'entry_missing_conc',
                'entry_missing_serial'):
        ac = runs[tag]['audit_counts']
        c.add(ac.get('probe_retry', 0) == 2 and ac.get('probe_incomplete', 0) >= 1
              and ac.get('repair_exhausted', 0) >= 1,
              f'{tag}：审计记账 probe_retry=2 / probe_incomplete≥1 / repair_exhausted≥1',
              f"counts={ac}")
    # 5) 缺-info 族：KeyError 被分类审计（F2 真实全覆盖，含**条目级**这一处）
    r = runs['entry_missing_conc']
    kinds = [(e.get('event'), e.get('detail', {}).get('class'),
              e.get('detail', {}).get('where'))
             for e in r['audit'] if e.get('event') == 'error_classified']
    c.add(any(k[1] == 'malformed_response' and k[2] == 'conc-item-total' for k in kinds),
          'entry_missing_conc：error_classified{class=malformed_response, where=conc-item-total}'
          '（条目级 total 探测也已包 F2，不再穿成条目异常活锁）',
          f"kinds={sorted(set(kinds))}")
    # 6) 未收敛时不打印「全部完成」（去字符串判据）
    for tag in ('total_none_conc', 'entry_missing_conc'):
        r = runs[tag]
        c.add(r['log_has_unconverged'] and not r['log_has_all_done'],
              f'{tag}：打印「未收敛（exit 3）」且不打印「全部完成」',
              f"unc={r['log_has_unconverged']} all_done={r['log_has_all_done']}")
    # 7) 基线对照：同样注入下基线的失败语义（响亮崩溃），正是 C-3「失败语义倒退」的参照物
    b1, b2 = runs['base_total_none_conc'], runs['base_entry_missing_conc']
    c.add(b1['rc'] == 0 and b1['log_has_all_done'],
          '基线对照：total_none ⇒ rc=0 + 「全部完成」（静默丢子树 = 假收敛复现）',
          f"rc={b1['rc']} unique={b1['records_unique']} all_done={b1['log_has_all_done']}")
    c.add(b2['rc'] == 1 and 'KeyError' in (b2['stderr'] + b2['stdout']),
          '基线对照：entry_missing_info ⇒ rc=1 KeyError **响亮崩溃**（C-3：改后必须是「登记缺口 + 未收敛」'
          '而不是崩溃，更不能是 rc=0 假收敛）',
          f"rc={b2['rc']} keyerr={'KeyError' in (b2['stderr'] + b2['stdout'])} "
          f"unique={b2['records_unique']} all_done={b2['log_has_all_done']}")
    report['entry_total_undetermined'] = runs
    return c


def scenario_item_exc_budget(report):
    """R3p4·P4-A（crit-correct C-2）：并发条目异常**必须有尝试预算**，进程有界退出。

    注入：条目体内抛异常（叶子 'A1' 的 scrape_leaf 抛 RuntimeError，非 get_total 路径）⇒
    走 `_conc_worker` 的条目异常路径。期望：预算内 retry_scheduled + 回队尾；
    预算耗尽 retry_exhausted + 降级记账（undetermined 缺口）⇒ exit 3（不再无限回队尾活锁）。"""
    c = Checks('item-exc-budget')
    m = run_variant('item_exc', 'modified', timeout=180)
    ac = m['audit_counts']
    conv = m['conv'] or {}
    gaps = conv.get('gaps') or []
    print(f"    [item_exc] rc={m['rc']} hang={m['timeout']} wall={m['wall_s']}s "
          f"unique={m['records_unique']} counts={ac}", flush=True)
    c.add(not m['timeout'], '条目异常注入下有界退出（无 hang）',
          f"hang={m['timeout']} wall={m['wall_s']}s rc={m['rc']}")
    c.add(ac.get('retry_scheduled', 0) >= 2,
          '预算内：retry_scheduled 审计（回队尾逐次记账，不再无账回队尾）',
          f"retry_scheduled={ac.get('retry_scheduled')} counts={ac}")
    c.add(ac.get('retry_exhausted', 0) >= 1,
          '预算耗尽：retry_exhausted 审计（尝试预算生效）',
          f"retry_exhausted={ac.get('retry_exhausted')}")
    c.add(ac.get('error_classified', 0) >= 1,
          '条目异常分类审计 error_classified{where=conc-item}',
          f"error_classified={ac.get('error_classified')}")
    c.add(m['rc'] == 3 and conv.get('exit_code') == 3,
          '条目异常预算耗尽 ⇒ 降级记账 ⇒ exit 3（未收敛，不假收敛）',
          f"rc={m['rc']} conv={json.dumps({k: conv.get(k) for k in ('decided', 'exit_code', 'reason')}, ensure_ascii=False)}")
    c.add(len(gaps) >= 1 and any(g.get('type') == 'undetermined' and g.get('degraded') is True
                                 for g in gaps),
          '该子树登记为未决缺口（undetermined/degraded）',
          json.dumps(gaps, ensure_ascii=False)[:300])
    c.add(m['records_unique'] == total_of('A'),
          '数据面：注入子树不影响数据面（父块「先抓一笔」已覆盖全域 ⇒ unique 仍为全长）'
          '——**记账面**仍保守判未收敛（A1 的 prefix 局部反查计数无法自证覆盖）',
          f"unique={m['records_unique']}/{total_of('A')}（缺口记录=A1 子树 total=43）")
    c.add(m['log_has_unconverged'] and not m['log_has_all_done'],
          '未收敛时不打印「全部完成」', f"unc={m['log_has_unconverged']} "
          f"all_done={m['log_has_all_done']}")
    has_unc = any(e.get('event') == 'convergence_decision'
                  and e.get('detail', {}).get('terminal')
                  and e.get('detail', {}).get('exit_code') == 3 for e in m['audit'])
    c.add(has_unc, 'convergence_decision: terminal=True exit_code=3', f"counts={ac}")
    report['item_exc'] = {'modified': m}
    return c


def scenario_stall(report):
    """c) 停滞注入 → stall_detected（只审计）与 stall_detected+graceful_stop（自停）。"""
    c = Checks('stall-detect')
    a = run_variant('stall_audit', 'modified', timeout=180)
    s = run_variant('stall_stop', 'modified', timeout=180)
    c.add(a['audit_counts'].get('stall_detected', 0) >= 1,
          'audit 档：stall_detected 已写审计（进度年龄超阈值）',
          f"counts={a['audit_counts']} rc={a['rc']}")
    ev = [e for e in a['audit'] if e.get('event') == 'stall_detected']
    c.add(bool(ev) and ev[0].get('detail', {}).get('kind') == 'progress_age'
          and ev[0].get('detail', {}).get('action') == 'audit',
          'audit 档：事件 detail.kind=progress_age / action=audit',
          json.dumps(ev[0].get('detail') if ev else {}, ensure_ascii=False)[:300])
    c.add(a['audit_counts'].get('graceful_stop', 0) == 0,
          'audit 档：不自停（无 graceful_stop）', f"counts={a['audit_counts']}")
    c.add(s['rc'] == 4, 'stop 档：自停退出码 = 4', f"rc={s['rc']}")
    c.add(s['audit_counts'].get('graceful_stop', 0) >= 1, 'stop 档：graceful_stop 审计事件',
          f"counts={s['audit_counts']}")
    kinds = [e.get('detail', {}).get('kind') for e in s['audit']
             if e.get('event') == 'stall_detected']
    c.add('progress_age_stop' in kinds, 'stop 档：stall_detected.kind=progress_age_stop',
          f"kinds={kinds}")
    c.add(s['progress'] is not None, 'stop 档：自停前已落盘 checkpoint（progress.json 可读）',
          f"progress={s['progress']}")
    report['stall'] = {'audit': a, 'stop': s}
    return c


def scenario_outdir(report):
    """d) output 目录消失注入 → 检测+重建+审计，不打死进程（F3）。"""
    c = Checks('f3-outdir-gone')
    b = run_variant('outdir_gone', 'baseline')
    m = run_variant('outdir_gone', 'modified')
    c.add(b['rc'] not in (0, None) and 'FileNotFoundError' in (b['stderr'] + b['stdout']),
          'baseline：目录消失 → FileNotFoundError 打死进程（缺陷复现）',
          f"rc={b['rc']} err_tail={(b['stderr'] or '')[-160:]!r}")
    c.add(m['rc'] == 0, 'modified：进程存活并正常收敛（exit 0）', f"rc={m['rc']}")
    c.add(m['audit_counts'].get('output_dir_recreated', 0) >= 1,
          'output_dir_recreated 审计事件', f"counts={m['audit_counts']}")
    c.add(m['log_has_dir_recreated'], 'scrape.log 有目录重建通告',
          f"r3_lines={m['log_r3_lines'][:3]}")
    c.add('records.jsonl' in m['out_files'] and 'progress.json' in m['out_files']
          and 'convergence-report.json' in m['out_files'],
          '重建后 records/progress/收敛报告 都回来了', f"files={m['out_files']}")
    c.add(m['records_unique'] > 0, '重建后记录继续落盘（不是空跑）',
          f"unique={m['records_unique']}")
    report['outdir_gone'] = {'baseline': b, 'modified': m}
    return c


def scenario_scope_and_reconcile(report):
    """边界/回归：--reconcile-only 仍可用（离线一致性自愈入口）。"""
    c = Checks('reconcile-only')
    d = SANDBOX / 'reconcile' / 'modified'
    if d.exists():
        shutil.rmtree(d)
    moddir = d / 'mod'
    outdir = d / 'out'
    moddir.mkdir(parents=True)
    outdir.mkdir(parents=True)
    shutil.copy2(MODIFIED, moddir / 'scrape_under_test.py')
    rec = outdir / 'records.jsonl'
    rec.write_text('\n'.join(json.dumps(_doc(call)) for call in CALLS[:50]) + '\n',
                   encoding='utf-8')
    # 一份最小可用的 progress.json（--reconcile-only 要求 load_progress() 成功）
    (outdir / 'progress.json').write_text(json.dumps({
        'schema': 'v10', 'todo': [{'p': 'A0', 'total': None}],
        'stats': {'leaves': 0, 'records': 0}, 'gaps': [], 'done': [], 'recovery': [],
        'seeds': ['A'], 'torn_requeue': [], 'started': '2026-09-15T20:00:00',
    }, ensure_ascii=False), encoding='utf-8')
    env = dict(os.environ, SCRAPE_OUT_DIR=str(outdir), PYTHONUNBUFFERED='1',
               PYTHONDONTWRITEBYTECODE='1')
    p = subprocess.run([sys.executable, str(moddir / 'scrape_under_test.py'), '--reconcile-only'],
                       cwd=str(moddir), env=env, capture_output=True, text=True, timeout=120)
    ok_json = False
    payload = {}
    try:
        payload = json.loads(p.stdout.strip().splitlines()[-1])
        ok_json = payload.get('ok') is True
    except Exception:
        ok_json = False
    c.add(p.returncode == 0 and ok_json, '--reconcile-only 退出 0 且输出 ok:true',
          f"rc={p.returncode} out={p.stdout.strip()[-200:]!r}")
    c.add(payload.get('todo') == 1 or payload.get('done') == 0,
          'reconcile 摘要含结构化读数（todo/done）',
          json.dumps({k: payload.get(k) for k in ('kept_lines', 'todo', 'done', 'gaps_resolved')},
                     ensure_ascii=False))
    report['reconcile_only'] = {'rc': p.returncode, 'stdout': p.stdout.strip()[-400:],
                                'payload': {k: payload.get(k) for k in
                                            ('ok', 'kept_lines', 'todo', 'done')}}
    return c


def scenario_serial(report):
    """e) 回归：SCRAPE_CONC=0 串行主循环（_serial_process_queue）与并发路径判据一致。"""
    c = Checks('serial-path')
    env = {'SCRAPE_CONC': '0', 'SCRAPE_W_MAX': '1'}
    n = run_variant('normal', 'modified', extra_env=env)
    d = run_variant('probe_all_dead', 'modified', extra_env=env)
    c.add(n['rc'] == 0 and n['records_unique'] == total_of('A'),
          f'串行路径：正常场景收敛 exit 0 且唯一 mms={total_of("A")}',
          f"rc={n['rc']} unique={n['records_unique']} conv={json.dumps({k: (n['conv'] or {}).get(k) for k in ('decided', 'exit_code')}, ensure_ascii=False)}")
    c.add(d['rc'] == 3 and d['audit_counts'].get('probe_all_dead', 0) >= 1,
          '串行路径：探测全灭 → exit 3 + probe_all_dead 审计（与并发路径一致）',
          f"rc={d['rc']} counts={d['audit_counts']}")
    c.add((d['conv'] or {}).get('exit_code') == 3,
          '串行路径：收敛报告 exit_code=3',
          json.dumps({k: (d['conv'] or {}).get(k) for k in ('decided', 'exit_code', 'reason')},
                     ensure_ascii=False))
    report['serial'] = {'normal': n, 'probe_all_dead': d}
    return c


# ===========================================================================
# 合流模式支持（R-4）：补丁链重放 / AST 区域代数 / 函数体归因
# ===========================================================================
def _patch_apply(patch_name, src, dst):
    """--fuzz=0 施加一条补丁（src → dst）。返回 (rc, stderr 摘要)。绝不吞失败。"""
    r = subprocess.run(['patch', '--fuzz=0', '-s', '-o', str(dst), '-i', str(HERE / patch_name),
                        str(src)], capture_output=True, text=True, timeout=120)
    return r.returncode, (r.stdout or '') + (r.stderr or '')


def _spans(path):
    """顶层定义 → (start, end)（与 scope-check.py 同算法）。"""
    import ast
    tree = ast.parse(Path(path).read_text(encoding='utf-8'))
    return {n.name: (n.lineno, getattr(n, 'end_lineno', n.lineno)) for n in tree.body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}


def _body(path, name):
    """顶层定义的函数体（含 def 行）逐行列表；不存在 ⇒ None。"""
    lines = Path(path).read_text(encoding='utf-8').splitlines(keepends=True)
    sp = _spans(path)
    if name not in sp:
        return None
    s, e = sp[name]
    return lines[s - 1:e]


def _changed_regions(a, b):
    """b 相对 a 的**变更区域集合**（顶层定义名；模块级行统一归 '<module-level>'）。"""
    import difflib
    al = Path(a).read_text(encoding='utf-8').splitlines(keepends=True)
    bl = Path(b).read_text(encoding='utf-8').splitlines(keepends=True)
    sm = difflib.SequenceMatcher(None, al, bl, autojunk=False)
    sp = _spans(b)
    out = set()
    for tag, _i1, _i2, j1, j2 in sm.get_opcodes():
        if tag == 'equal':
            continue
        for j in range(j1 + 1, j2 + 1):
            name = None
            for n, (s, e) in sp.items():
                if s <= j <= e:
                    name = n
                    break
            out.add(name or '<module-level>')
    return out


def _new_defs(a, b):
    return set(_spans(b)) - set(_spans(a))


def build_merge_refs():
    """按补丁链重建分支件/合流件（fail-closed）。返回 (refs, errors)。"""
    refs, errors = {}, []
    d = SANDBOX / 'merge'
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    base = d / 'r2cand.py'
    shutil.copy2(BASELINE, base)
    # 分支 C：converge 单槽
    cur, names = base, []
    for pn in MERGE_CHAIN_CONV:
        nxt = d / ('conv_%d.py' % (len(names) + 1))
        rc, err = _patch_apply(pn, cur, nxt)
        if rc != 0:
            errors.append('%s 施加失败（rc=%d）：%s' % (pn, rc, err.strip()[:200]))
            break
        cur = nxt
        names.append(pn)
    refs['conv_branch'] = cur
    refs['conv_chain'] = names
    # 分支 R：retry 单槽
    cur, names = base, []
    for pn in MERGE_CHAIN_RETRY:
        nxt = d / ('retry_%d.py' % (len(names) + 1))
        rc, err = _patch_apply(pn, cur, nxt)
        if rc != 0:
            errors.append('%s 施加失败（rc=%d）：%s' % (pn, rc, err.strip()[:200]))
            break
        cur = nxt
        names.append(pn)
    refs['retry_branch'] = cur
    refs['retry_chain'] = names
    # 合流 V2.0 →（可选）v2.1 增量 → 本目录 scrape.py
    cur, names = base, []
    for pn in MERGE_CHAIN_MERGED:
        nxt = d / ('merged_%d.py' % (len(names) + 1))
        rc, err = _patch_apply(pn, cur, nxt)
        if rc != 0:
            errors.append('%s 施加失败（rc=%d）：%s' % (pn, rc, err.strip()[:200]))
            break
        cur = nxt
        names.append(pn)
    refs['merged_v20'] = cur
    refs['merged_chain'] = names
    delta = HERE / MERGE_DELTA
    if delta.exists():
        nxt = d / 'merged_v21.py'
        rc, err = _patch_apply(MERGE_DELTA, cur, nxt)
        if rc != 0:
            errors.append('%s 施加失败（rc=%d）：%s' % (MERGE_DELTA, rc, err.strip()[:200]))
        else:
            cur = nxt
            refs['chain_with_delta'] = MERGE_CHAIN_MERGED + [MERGE_DELTA]
    refs['chain_end'] = cur            # 链的终点：应当逐字节等于本目录 scrape.py
    refs['delta_present'] = delta.exists()
    refs['dir'] = d
    # 只读参照逐字节核对（缺席 ⇒ 记 skipped，不算失败，但读数里显式标注）
    checks = {}
    for key, path in MERGE_REF_SLOTS.items():
        p = Path(path)
        if not p.exists():
            checks[key] = {'ref': path, 'status': 'skipped_missing'}
            continue
        rel = {'conv_slot': refs['conv_chain'][:1] and d / 'conv_1.py',
               'conv_f1_slot': refs['conv_branch'],
               'retry_slot': d / 'retry_1.py',
               'retry_fatal_slot': refs['retry_branch']}.get(key)
        same = bool(rel and rel.exists() and rel.read_bytes() == p.read_bytes())
        checks[key] = {'ref': path, 'status': 'match' if same else 'mismatch',
                       'ours': (_md5(rel) if rel and rel.exists() else None),
                       'slot': _md5(p)}
    refs['slot_crosscheck'] = checks
    return refs, errors


def scenario_scope(report):
    """改动区域机器核对：全部变更必须落在登记区域内，且 fetch/get_total 函数体逐字节未变。

    合流模式（TARGET_MODE == 'merge'）下换为合流作用域不变量，见文件头「靶模式」说明。"""
    if TARGET_MODE == 'merge':
        return _scenario_scope_merge(report)
    c = Checks('scope-discipline')
    p = subprocess.run([sys.executable, str(HERE / 'scope-check.py'), '--json'],
                       capture_output=True, text=True, timeout=120)
    try:
        rep = json.loads(p.stdout)
    except ValueError:
        rep = {}
    c.add(p.returncode == 0 and rep.get('ok') is True,
          'scope-check：改动全部在登记区域（probe_children/_conc_*/process_queue/… + F2/F3 最小落点）',
          f"rc={p.returncode} violations={len(rep.get('violations') or [])} "
          f"regions={len(rep.get('regions') or {})}")
    c.add(all((rep.get('forbidden_untouched') or {}).values()) is True,
          'fetch / get_total 函数体逐字节未变（impl-retry 区域零改动）',
          json.dumps(rep.get('forbidden_untouched'), ensure_ascii=False))
    c.add(len(rep.get('new_defs') or []) >= 20,
          '新增顶层定义（R3 助手区）', f"{len(rep.get('new_defs') or [])} 个")
    report['scope'] = {k: rep.get(k) for k in ('ok', 'forbidden_untouched', 'violations',
                                               'changed_mod_lines')}
    report['scope']['target_mode'] = 'single'
    return c


def _scenario_scope_merge(report):
    """合流靶的区域纪律：分支自重 + 区域并集闭合 + fetch/get_total 归因（R-4）。"""
    c = Checks('scope-discipline')
    refs, errors = build_merge_refs()
    if errors:
        c.add(False, '合流补丁链重放（fail-closed）', '; '.join(errors)[:300])
        report['scope'] = {'target_mode': 'merge', 'chain_errors': errors}
        return c
    conv, retry = refs['conv_branch'], refs['retry_branch']
    # ① converge 分支件仍过**同一套单槽纪律**（同一算法，靶换成重建出的分支件）
    p = subprocess.run([sys.executable, str(HERE / 'scope-check.py'), '--json',
                        '--ref', str(conv)], capture_output=True, text=True, timeout=120)
    try:
        rep = json.loads(p.stdout)
    except ValueError:
        rep = {}
    c.add(p.returncode == 0 and rep.get('ok') is True,
          '§合流① converge 分支件自身仍全在登记区域（scope-check 同算法换靶；合流不改写 converge 纪律）',
          f"ref={Path(str(conv)).name} rc={p.returncode} "
          f"violations={len(rep.get('violations') or [])} regions={len(rep.get('regions') or {})}")
    # ② 区域并集**恰好闭合**：MOD 的变更区域 == converge 分支 ∪ retry 分支（无外来改动）
    r_base = _changed_regions(BASELINE, MODIFIED)
    r_conv = _changed_regions(BASELINE, conv)
    r_retry = _changed_regions(BASELINE, retry)
    extra, missing = sorted(r_base - (r_conv | r_retry)), sorted((r_conv | r_retry) - r_base)
    c.add(not extra and not missing,
          '§合流② MOD 的变更区域集合 == converge 分支 ∪ retry 分支（无外来改动、无遗漏）',
          f"MOD={len(r_base)} conv={len(r_conv)} retry={len(r_retry)} "
          f"外来={extra[:6]} 未覆盖={missing[:6]}")
    nd_base = _new_defs(BASELINE, MODIFIED)
    nd_union = _new_defs(BASELINE, conv) | _new_defs(BASELINE, retry)
    c.add(nd_base == nd_union and len(nd_base) >= 20,
          '§合流②′ 新增顶层定义集合 == 两分支新增之并（合流未凭空增删定义）',
          f"MOD={len(nd_base)} 并集={len(nd_union)} 差={sorted(nd_base ^ nd_union)[:6]}")
    # ③ fetch / get_total 归因：get_total 未变（原判据）+ fetch 与 retry 分支件逐字节相同
    gt_same = _body(BASELINE, 'get_total') == _body(MODIFIED, 'get_total')
    fetch_same = _body(MODIFIED, 'fetch') == _body(retry, 'fetch')
    fetch_moved = _body(BASELINE, 'fetch') != _body(MODIFIED, 'fetch')
    c.add(gt_same and fetch_same and fetch_moved,
          '§合流③ get_total 逐字节未变 + fetch 体 == retry 分支件（converge 对 fetch 零贡献）',
          f"get_total_untouched={gt_same} fetch==retry_branch={fetch_same} "
          f"fetch_changed_vs_baseline={fetch_moved}")
    report['scope'] = {
        'target_mode': 'merge',
        'conv_branch_md5': _md5(conv), 'retry_branch_md5': _md5(retry),
        'conv_chain': refs['conv_chain'], 'retry_chain': refs['retry_chain'],
        'regions': {'mod': sorted(r_base), 'conv': sorted(r_conv), 'retry': sorted(r_retry),
                    'foreign': extra, 'uncovered': missing},
        'new_defs': {'mod': len(nd_base), 'union': len(nd_union)},
        'forbidden_attribution': {'get_total_untouched': gt_same, 'fetch_eq_retry_branch': fetch_same,
                                  'fetch_changed_vs_baseline': fetch_moved},
        'conv_branch_scope': {k: rep.get(k) for k in ('ok', 'violations', 'changed_mod_lines')},
        'slot_crosscheck': refs['slot_crosscheck'],
    }
    return c


def scenario_patch(report):
    """产物自洽：converge-f1.patch 由 R3 产物生成，且能**干净应用**回 R3 产物并逐字节还原 scrape.py。

    补丁本体由任务书指定命令生成（cd /tmp/lib-opt-work && diff -u R3/impl/slot-converge/scrape.py
    R3p4/slot-converge-f1/scrape.py > converge-f1.patch）⇒ 头部标签即这两个相对路径；
    本用例在沙箱内重建同名目录树并用 `patch -p0 -o` 落到指定输出文件后逐字节比对。

    合流模式（TARGET_MODE == 'merge'）下换为**全链重放**（见 _scenario_patch_merge）。"""
    if TARGET_MODE == 'merge':
        return _scenario_patch_merge(report)
    c = Checks('patch-roundtrip')
    patch = HERE / 'converge-f1.patch'
    PREV = Path('/tmp/lib-opt-work/R3/impl/slot-converge/scrape.py')   # 只读：改前产物（= 补丁左端）
    d = SANDBOX / 'patch'
    if d.exists():
        shutil.rmtree(d)
    (d / 'R3' / 'impl' / 'slot-converge').mkdir(parents=True)
    shutil.copy2(PREV, d / 'R3' / 'impl' / 'slot-converge' / 'scrape.py')
    if not patch.exists():
        c.add(False, 'converge-f1.patch 已生成', f'{patch} 缺失')
        return c
    ap = subprocess.run(['patch', '-p0', '--dry-run', '-i', str(patch)],
                        cwd=str(d), capture_output=True, text=True)
    c.add(ap.returncode == 0, 'converge-f1.patch 可干净应用到 R3 产物（patch --dry-run rc=0）',
          f"rc={ap.returncode} out={(ap.stdout or '').strip()[:160]}")
    out = d / 'applied.py'
    ap2 = subprocess.run(['patch', '-p0', '-o', str(out), '-i', str(patch)],
                         cwd=str(d), capture_output=True, text=True)
    same = out.exists() and out.read_bytes() == MODIFIED.read_bytes()
    c.add(ap2.returncode == 0 and same, '应用后与 scrape.py 逐字节一致（重放验证）',
          f"rc={ap2.returncode} identical={same} sha={_md5(out) if out.exists() else '-'}"
          f"/{_md5(MODIFIED)}")
    c.add(patch.stat().st_size > 5000, 'converge-f1.patch 已生成',
          f"{patch.stat().st_size} bytes, {len(patch.read_text().splitlines())} 行")
    report['patch'] = {'bytes': patch.stat().st_size, 'apply_rc': ap.returncode,
                       'identical': same, 'target_mode': 'single'}
    return c


def _scenario_patch_merge(report):
    """合流靶的产物自洽（R-4）：**全链重放**逐字节还原本目录 scrape.py。

    单槽断言"converge-f1.patch 结果 == 本目录 scrape.py"在合流树上必然失败（自指血缘不变量）。
    合流模式下换成更强、也更贴合的陈述：把 r2cand 按**记录在案的补丁链**重放
    （retry → retry-fatal → converge → converge-f1 → audit-align-converge → v2.1 增量），
    终点必须与本目录 scrape.py **逐字节一致**，且能与只读槽产物对上的中间件必须对上。
    任一步 --fuzz=0 施加失败或不一致 ⇒ FAIL（fail-closed）。"""
    c = Checks('patch-roundtrip')
    refs, errors = build_merge_refs()
    if errors:
        c.add(False, '合流补丁链可干净重放（每步 --fuzz=0）', '; '.join(errors)[:300])
        report['patch'] = {'target_mode': 'merge', 'chain_errors': errors}
        return c
    end = Path(refs['chain_end'])
    same = end.read_bytes() == MODIFIED.read_bytes()
    c.add(same, '§合流④ 全链重放（r2cand + 5 条补丁%s）逐字节还原本目录 scrape.py'
          % (' + v2.1 增量' if refs.get('delta_present') else ''),
          f"chain={'->'.join(Path(p).name for p in (refs.get('chain_with_delta')
                                                    or refs['merged_chain']))} "
          f"sha={_md5(end)}/{_md5(MODIFIED)} identical={same}")
    xc = refs['slot_crosscheck']
    matched = [k for k, v in xc.items() if v['status'] == 'match']
    mismatched = [k for k, v in xc.items() if v['status'] == 'mismatch']
    c.add(not mismatched and len(matched) >= 2,
          '§合流④′ 分支重建件与只读槽产物逐字节一致（converge/converge-f1/retry/retry-fatal）',
          f"match={matched} mismatch={mismatched} "
          f"skipped={[k for k, v in xc.items() if v['status'].startswith('skipped')]}")
    total = sum((HERE / p).stat().st_size for p in MERGE_CHAIN_MERGED if (HERE / p).exists())
    c.add(total > 5000 and not refs.get('chain_with_delta') is None,
          '§合流④″ 补丁链留档齐备（各补丁文件在场且含实质内容）',
          f"{len(refs['merged_chain'])} 条 / {total} bytes"
          f"{' + ' + MERGE_DELTA if refs.get('delta_present') else '（无 v2.1 增量文件）'}")
    report['patch'] = {'target_mode': 'merge',
                       'chain': refs.get('chain_with_delta') or refs['merged_chain'],
                       'chain_end_md5': _md5(end), 'modified_md5': _md5(MODIFIED),
                       'identical': same, 'slot_crosscheck': xc,
                       'merged_v20_md5': _md5(refs['merged_v20'])}
    return c


def scenario_e2e(report):
    """e) 回归（端到端）：改后 scrape.py 跑在**真实 HTTP 栈** + R2 loopback mock 上。"""
    c = Checks('e2e-loopback')
    p = subprocess.run([sys.executable, str(HERE / 'e2e-regression.py'), '--records', '1200'],
                       capture_output=True, text=True, timeout=600)
    res = None
    jf = SANDBOX / 'e2e' / 'e2e-result.json'
    if jf.exists():
        try:
            res = json.loads(jf.read_text())
        except ValueError:
            res = None
    if not res:
        c.add(False, 'e2e：读数文件缺失', (p.stdout or '')[-300:] + (p.stderr or '')[-200:])
        return c
    b, m = res['runs']['baseline'], res['runs']['modified']
    exp = (res['info'].get('expected_crawl') or {}).get('predicted_unique')
    c.add(bool(res.get('ok')) is True, 'e2e：改后与基线在真实 HTTP 路径上抓取结果一致且 exit 0',
          f"baseline unique={b['records_unique']} rc={b['rc']} / modified unique="
          f"{m['records_unique']} rc={m['rc']} / mock 预测 unique={exp}")
    c.add(m['rewrite']['n_host'] == 1 and m['rewrite']['n_base'] == 1,
          'e2e：被测源码 HOST/BASE 已重写（无生产路径字面量）',
          json.dumps(m['rewrite'], ensure_ascii=False))
    c.add((m['conv'] or {}).get('exit_code') == 0 and (m['conv'] or {}).get('decided') == 'done',
          'e2e：真实运行也产出结构化收敛报告（decided=done/exit 0）',
          json.dumps(m['conv'], ensure_ascii=False))
    report['e2e'] = {'info': {k: res['info'][k] for k in ('port', 'expected_crawl')},
                     'baseline': {k: b[k] for k in ('rc', 'records_unique', 'wall_s')},
                     'modified': {k: m[k] for k in ('rc', 'records_unique', 'wall_s', 'conv')}}
    return c


def resolve_target_mode(requested):
    """靶模式解析（R-4）。auto = 同目录同时有 converge.patch 与 retry.patch ⇒ merge。"""
    if requested in ('single', 'merge'):
        return requested, 'explicit(--target-mode %s)' % requested
    merged = (HERE / 'converge.patch').exists() and (HERE / 'retry.patch').exists()
    return ('merge' if merged else 'single',
            'auto(converge.patch=%s retry.patch=%s)'
            % ((HERE / 'converge.patch').exists(), (HERE / 'retry.patch').exists()))


def main():
    global TARGET_MODE
    ap = argparse.ArgumentParser()
    ap.add_argument('--child')
    ap.add_argument('--module')
    ap.add_argument('--out')
    ap.add_argument('--target-mode', default='auto', choices=('auto', 'single', 'merge'),
                    help='single=单槽不变量（原判据）；merge=合流作用域不变量（R-4）；'
                         'auto=按同目录补丁留档自动判定')
    args = ap.parse_args()
    if args.child:
        return child_main(args)

    TARGET_MODE, mode_src = resolve_target_mode(args.target_mode)
    SANDBOX.mkdir(parents=True, exist_ok=True)
    READINGS.mkdir(parents=True, exist_ok=True)
    report = {'ts': time.time(), 'baseline_md5': _md5(BASELINE), 'modified_md5': _md5(MODIFIED),
              'target_mode': TARGET_MODE, 'target_mode_source': mode_src,
              'catalog': {'seed': CATALOG_SEED, 'n': CATALOG_N, 'total_A': total_of('A'),
                          'children_A_nonzero': sum(1 for ch in string.digits + string.ascii_uppercase
                                                    if total_of('A' + ch) > 0)}}
    suites = [scenario_normal(report), scenario_probe_dead(report), scenario_f2(report),
              scenario_entry_total_undetermined(report), scenario_item_exc_budget(report),
              scenario_stall(report), scenario_outdir(report), scenario_serial(report),
              scenario_scope_and_reconcile(report), scenario_scope(report),
              scenario_patch(report), scenario_e2e(report)]
    print('=' * 100)
    print('R3·slot-converge 测试读数（基线 = R3/refs/scrape-r2cand.py，改后 = 本目录 scrape.py）')
    print('靶模式 TARGET_MODE=%s（来源 %s）：%s'
          % (TARGET_MODE, mode_src,
             '本目录含 converge.patch+retry.patch ⇒ 三条单槽不变量换为合流作用域等价判据（R-4）'
             if TARGET_MODE == 'merge' else '单槽靶 ⇒ 原判据（与 slot-converge-f1 一致）'))
    print('=' * 100)
    n_fail = 0
    for s in suites:
        print(f"\n## {s.name}  {'PASS' if s.ok else 'FAIL'}")
        for it in s.items:
            mark = 'PASS' if it['ok'] else 'FAIL'
            if not it['ok']:
                n_fail += 1
            print(f"  [{mark}] {it['label']}")
            if it['reading']:
                print(f"         {it['reading']}")
    report['suites'] = [{'name': s.name, 'ok': s.ok, 'items': s.items} for s in suites]
    report['pass'] = n_fail == 0
    report['n_fail'] = n_fail
    report['target_mode_resolved'] = TARGET_MODE
    slim = _slim(report)
    (READINGS / 'readings.json').write_text(json.dumps(slim, ensure_ascii=False, indent=1),
                                            encoding='utf-8')
    print('\n' + '=' * 100)
    print(f"TOTAL: {'PASS' if n_fail == 0 else 'FAIL'}（失败断言 {n_fail} 条，靶模式 {TARGET_MODE}）"
          f"  读数文件: {READINGS / 'readings.json'}")
    print('=' * 100)
    return 0 if n_fail == 0 else 1


def _md5(p):
    import hashlib
    return hashlib.md5(Path(p).read_bytes()).hexdigest()[:8]


def _slim(report):
    """读数文件去掉超长的 stdout/stderr（保留尾部），控制在可读体量。"""
    out = json.loads(json.dumps(report, ensure_ascii=False, default=str))
    for key in ('normal', 'probe_all_dead', 'f2', 'outdir_gone'):
        for v in (out.get(key) or {}).values():
            if isinstance(v, dict):
                v.pop('audit', None)
                v['stdout'] = (v.get('stdout') or '')[-1500:]
                v['stderr'] = (v.get('stderr') or '')[-1500:]
    for v in (out.get('stall') or {}).values():
        if isinstance(v, dict):
            v.pop('audit', None)
            v['stdout'] = (v.get('stdout') or '')[-1200:]
            v['stderr'] = (v.get('stderr') or '')[-1200:]
    return out


if __name__ == '__main__':
    sys.exit(main())
