#!/usr/bin/env python3
"""区域纪律机器检查（R3·slot-converge）

任务书边界：
  · 改动集中在 probe_children / process_queue / _serial_process_queue / _conc_* /
    final_recheck_round / main 区（含本轮新增的 R3 自愈助手区）；
  · **不动 fetch / get_total**（属 impl-retry 区域）；
  · 边界修复 F2/F3 需要的最小落点（log / save_progress / records 写入路径）显式登记；
  · 新增 import 一律函数内 import（本产物未新增任何 import）。

判定：把 diff -u 的每个变更行归到「顶层定义（函数/类）」或「模块级插入段」，
      逐行核对是否落在允许集合内；**任何一行落在 fetch/get_total 内即 FAIL**。

用法：python3 scope-check.py [--json]   （退出码 0 = 合规）
"""
import ast
import difflib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
BASE = Path('/tmp/lib-opt-work/R3/refs/scrape-r2cand.py')
MOD = HERE / 'scrape.py'

# 允许改动（本轮任务书区域 + 明确登记的边界修复落点）
ALLOWED = {
    # —— 任务书指定区域 ——
    'probe_children', 'recheck_chars', 'process_queue', '_serial_process_queue',
    'final_recheck_round', 'main', '_cli', '_conc_run_item', '_conc_worker',
    '_conc_log_periodic', '_conc_defer', '_WorkQueue', '_ConcurrencyBudget',
    # —— R3 新增助手（插入在 probe_children 之前的 R3 区块内）——
    '_r3_run_id', '_audit_fd_close', '_audit_fd_get', 'audit_event', 'audit_read',
    'audit_counts', '_ensure_out_dir_raw', '_note_dir_recreated', 'ensure_out_dir',
    '_classify_exc', '_audit_exc', '_safe_get_total', 'ProbeOutcome',
    '_probe_outcome_from_exc', '_probe_dead_policy', '_find_degraded_gap',
    '_probe_dead_degrade', 'progress_watermark', 'stall_state', 'stall_tick',
    'graceful_stop', '_stall_monitor_loop', '_stall_monitor_start', '_stall_monitor_stop',
    'open_gaps', 'convergence_decision', 'write_convergence_report',
    # —— R3p4·P4-A（收敛残留门闭环 C-1/C-2/C-3）新增助手：条目级未定论的统一降级记账 ——
    '_find_undetermined_gap', '_undetermined_retry', '_undetermined_degrade',
    '_keep_resolved_gap',
    # —— R3p4·P4-A 的 L2 最小落点：reconcile 里「容差内补齐的缺口」也留档 resolved_gaps[]
    #    （仅 3 行；reconcile 的判定逻辑与返回语义未变）——
    'reconcile',
    # —— 边界修复 F2/F3 的最小落点（任务书第 4 条）——
    'log', 'save_progress', '_write_progress_atomic', '_records_fd_get',
    'append_records', '_records_write_resilient', '_records_pending_flush',
    'flush_pending_records', '_fd_alive',
}
# 绝对禁区：impl-retry 区域（本 slot 不得改动其函数体）
FORBIDDEN = {'fetch', 'get_total'}
# 模块级插入段的锚点（R3 区块的 banner 行；该段内的模块级新增行视为合规）
MODULE_BLOCK_MARK = 'R3·slot-converge —— 块级兜底'


def spans(path):
    """顶层定义 → 真实 (起始行, 结束行, kind)（**不吞并定义之间的模块级行**）。"""
    src = path.read_text(encoding='utf-8')
    lines = src.splitlines()
    tree = ast.parse(src)
    out = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            out[node.name] = {'start': node.lineno,
                              'end': getattr(node, 'end_lineno', node.lineno),
                              'kind': 'class' if isinstance(node, ast.ClassDef) else 'def'}
    return out, lines


def owner(sp, lineno):
    """该行归属的顶层定义名（真实 span 内才算归属；否则 None = 模块级）。"""
    for name, s in sp.items():
        if s['start'] <= lineno <= s['end']:
            return name
    return None


def r3_module_block(mod_lines):
    """R3 模块级插入段的行区间 [起, 止]（banner 行 → banner 后第一个顶层定义前一行）。"""
    banner = None
    for i, line in enumerate(mod_lines, 1):
        if MODULE_BLOCK_MARK in line:
            banner = i
    if banner is None:
        return None
    start = banner
    prev = mod_lines[banner - 2].strip() if banner >= 2 else ''
    if prev.startswith('# ===='):          # 段首的 `# =====` 分隔行也算本段
        start = banner - 1
    tree = ast.parse('\n'.join(mod_lines))
    nxt = [n.lineno for n in tree.body
           if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.lineno > banner]
    return (start, (min(nxt) - 1) if nxt else len(mod_lines))


def changed_lines():
    a = BASE.read_text(encoding='utf-8').splitlines(keepends=True)
    b = MOD.read_text(encoding='utf-8').splitlines(keepends=True)
    sm = difflib.SequenceMatcher(None, a, b, autojunk=False)
    base_changed, mod_changed = set(), set()
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == 'equal':
            continue
        base_changed.update(range(i1 + 1, i2 + 1))
        mod_changed.update(range(j1 + 1, j2 + 1))
    return sorted(base_changed), sorted(mod_changed)


def main():
    global MOD
    as_json = '--json' in sys.argv[1:]
    # R3-P4·P4.5（R-4）：`--ref PATH` 把被检文件从 HERE/scrape.py 换成任意文件。
    #   用途：合流靶上**分支件**仍需逐一过同一套单槽纪律判据（test_converge.py 合流模式会
    #   重建 converge 分支件 `refs + converge.patch + converge-f1.patch` 后再检）。判据逻辑、
    #   ALLOWED/FORBIDDEN、退出码语义**完全不变**；被检对象由调用方显式给出（绝不静默换靶）。
    ref_override = None
    argv = sys.argv[1:]
    if '--ref' in argv:
        i = argv.index('--ref')
        if i + 1 >= len(argv):
            sys.stderr.write('usage: scope-check.py [--json] [--ref PATH]\n')
            return 2
        ref_override = Path(argv[i + 1])
        if not ref_override.exists():
            sys.stderr.write('scope-check: --ref 指向的文件不存在：%s\n' % ref_override)
            return 2
        MOD = ref_override
    mod_sp, mod_lines = spans(MOD)
    base_sp, base_lines = spans(BASE)
    base_changed, mod_changed = changed_lines()

    new_defs = sorted(set(mod_sp) - set(base_sp))
    gone_defs = sorted(set(base_sp) - set(mod_sp))

    regions = {}          # region 名 → {'mod': n, 'base': n}
    violations = []
    r3blk = r3_module_block(mod_lines)
    for ln in mod_changed:
        name = owner(mod_sp, ln)
        if name is None:
            body = mod_lines[ln - 1].strip()
            if not body or body.startswith('#'):
                # 定义之间的空行/注释分隔行：插入新函数必然带来的结构性行（无任何可执行语义）
                name = '<module-level:注释/空行>'
            elif r3blk and r3blk[0] <= ln <= r3blk[1]:
                name = '<module-level:R3-block>'
            else:
                name = '<module-level:其它>'
        regions.setdefault(name, {'mod': 0, 'base': 0})['mod'] += 1
        if name.startswith('<module-level:注释/空行'):
            continue
        if name in FORBIDDEN:
            violations.append(f'mod L{ln} 落在禁区 {name} 内: {mod_lines[ln-1].strip()[:90]}')
        elif name not in ALLOWED and not name.startswith('<module-level:R3-block>'):
            violations.append(f'mod L{ln} 落在未登记区域 {name}: {mod_lines[ln-1].strip()[:90]}')
    for ln in base_changed:
        name = owner(base_sp, ln) or '<module-level>'
        regions.setdefault(name, {'mod': 0, 'base': 0})['base'] += 1
        if name in FORBIDDEN:
            violations.append(f'base L{ln} 禁区 {name} 被删改: {base_lines[ln-1].strip()[:90]}')

    # 额外断言：fetch / get_total 的函数体逐字节未变
    same_body = {}
    for name in FORBIDDEN:
        if name in mod_sp and name in base_sp:
            s_m, e_m = mod_sp[name]['start'], mod_sp[name]['end']
            s_b, e_b = base_sp[name]['start'], base_sp[name]['end']
            same_body[name] = (mod_lines[s_m - 1:e_m] == base_lines[s_b - 1:e_b])
        else:
            same_body[name] = False

    report = {'baseline': str(BASE), 'modified': str(MOD),
              'ref_override': (str(ref_override) if ref_override else None),
              'mode': ('ref' if ref_override else 'default'),
              'changed_mod_lines': len(mod_changed), 'changed_base_lines': len(base_changed),
              'new_defs': new_defs, 'removed_defs': gone_defs,
              'regions': dict(sorted(regions.items(), key=lambda kv: -(kv[1]['mod'] + kv[1]['base']))),
              'forbidden_untouched': same_body,
              'violations': violations, 'ok': not violations and all(same_body.values())}
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=1))
    else:
        print(f"baseline : {BASE}")
        print(f"modified : {MOD}")
        print(f"变更行   : 改后 {len(mod_changed)} 行 / 基线 {len(base_changed)} 行")
        print(f"新增顶层定义（{len(new_defs)}）：{', '.join(new_defs)}")
        if gone_defs:
            print(f"移除顶层定义：{gone_defs}")
        print('改动分布（顶层定义 → 改后行数/基线行数）：')
        for name, v in report['regions'].items():
            print(f"  {name:28s} mod={v['mod']:4d} base={v['base']:4d}")
        print('禁区函数体逐字节未变：' + json.dumps(same_body, ensure_ascii=False))
        if violations:
            print(f'违规 {len(violations)} 条：')
            for v in violations[:20]:
                print('  ! ' + v)
        print('SCOPE: ' + ('PASS（改动全部在登记区域内，fetch/get_total 未动）'
                           if report['ok'] else 'FAIL'))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    sys.exit(main())
