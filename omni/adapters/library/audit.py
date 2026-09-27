#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""R3 自愈轮 · 结构化审计模块 v2（三级事件统一出口 · 契约 r3-audit-contract-v2）

用途
----
按 `R3/refs/r3-audit-schema.md` 写入 `audit.jsonl`（每行一个 JSON 对象）：
request / block / process 三级自愈事件共用本模块。本槽（slot-retry）只产出
**request 级**事件（`retry_scheduled` / `retry_exhausted` / `error_classified`），
但接口对三级通用，供其它槽复用（可加字段，不破坏既有解析）。

字段（契约 v2 §1 + v2.1 §8，三写者统一布局）
------------------------------------------
`schema`("r3-audit-v2") `ts_epoch`(epoch float，**权威时间**) `ts_iso`(ISO 字符串)
`pid`(int) **`writer`**(string，写者身份) `seq`(**写者内**从 1 严格 +1)
`level`("request"|"block"|"process") `event`(事件名) `detail`(对象) `run`(可选)
遗留键 `ts` **不再写入**（契约 §1 裁决：历史 ts 三侧语义不一）。

writer 分组键（契约 v2.1 §8，KI-1 修复）
--------------------------------------
同一个进程内可以有**多个写者**（`scrape.py` 的块级 `audit_event()` 与经本模块的请求级
写者共用同一个 `pid` 与同一个 `OUT/audit.jsonl`），二者各有独立计数器 ⇒ 只按 `pid`
分组会出现 `seq_duplicate`（P4-verify 在真实 S4 上复现）。故 v2.1 引入 `writer`：

· 本模块（请求级/retry 侧）写 `writer="retry"`（默认值 `WRITER`，可用
  `SCRAPE_AUDIT_WRITER` 或 `init(writer=...)` 覆盖，供其它写者复用本模块）。
· `scrape.py` 的块级审计段写 `writer="converge"`；`keeper-v3.sh` 写 `writer="keeper"`。
· 校验器按 `(文件, pid, writer)` 分组 ⇒ 同 pid 双写者合流时各组独立连续（契约 §8.2/§8.4）。
· **新写入一律带 writer**（契约 §8.5）；v2.0 历史产物（无 writer）只能用
  `audit-verify-v2.py --compat-v1` 对照读取。

R3-P4 修复（crit-correct R-7 / M4）
----------------------------------
· **F3 inode 存活校验**：`_ensure_fd()` 每次写前用 `st_nlink + (dev,ino)` 核对 fd 指向的
  inode 是否仍是路径上的那个文件。目录被 `rm -rf` / 文件被 unlink / 被外部轮转改名后，
  旧 fd 的 `os.write` 会"成功"但数据进黑洞 —— 现在改为：检测 → 关闭 → **一次自愈**
  （重建目录 + 重开）→ 仍失败则 `errors += 1` 并返回 None（**绝不静默成功**）。
· **轮转多写者语义（显式豁免）**：单写者保持原语义（超限 → `audit.jsonl.1`）；
  一旦发现真实文件大小与本人计数不符（存在外部写者），**放弃轮转**（避免 `os.replace`
  覆盖他人数据），改为采纳真实大小 + 计数 `rotate_skipped_shared`。理由：共享 sink 上的
  rename 轮转无法在不加锁的前提下保证不丢数据；本模块的定位是"旁路观测面"，宁可单调增长
  也不静默丢事件。豁免由 `selftest: rotate.multi_writer_no_rotate` 覆盖。

设计约束
--------
1. **绝不抛异常**：审计是旁路，写失败只计数（`stats()['errors']`），不得影响抓取主流程。
2. **行级原子追加**：整行序列化 → 单次 `os.write` 到 `O_APPEND` fd（与
   `scrape.append_records` 同一先例），kill -9 下不产生半写行。
3. **有界**：`seq/bytes` 为定长计数器；`detail` 超长时截断；可选按大小轮转
   （`SCRAPE_AUDIT_ROTATE_MB`，默认 32MB，轮转产物 `audit.jsonl.1`）。
4. **零依赖**：只用标准库；不 import scrape.py（可独立给 keeper/drill 使用）。

开关与路径
----------
`SCRAPE_AUDIT`：
  · 未设置 / 非禁用值 ⇒ 默认路径 `$SCRAPE_OUT_DIR/audit.jsonl`，否则 `./audit.jsonl`
  · `off|0|false|no|none|disable|disabled` ⇒ **彻底关闭**（emit 全部 no-op）
  · 其它字符串 ⇒ 视为审计文件路径
调用方（scrape.py）显式传入 `OUT/audit.jsonl`，因此生产默认落在输出目录内。

自测：`python3 audit.py`（内置 selftest）
"""
import json
import os
import sys
import threading
import time
from datetime import datetime

__all__ = ['LEVELS', 'WRITER', 'init', 'emit', 'emit_request', 'emit_block', 'emit_process',
           'path', 'is_enabled', 'stats', 'close', 'reset', 'DEFAULT_NAME']

SCHEMA = 'r3-audit-v2'          # 契约 v2 §1：三写者统一 schema 值
LEVELS = ('request', 'block', 'process')
ENV_AUDIT = 'SCRAPE_AUDIT'
ENV_OUT_DIR = 'SCRAPE_OUT_DIR'
ENV_ROTATE_MB = 'SCRAPE_AUDIT_ROTATE_MB'
DEFAULT_NAME = 'audit.jsonl'
WRITER = 'retry'               # 契约 v2.1 §8.1：本模块 = 请求级（retry 侧）写者身份
ENV_WRITER = 'SCRAPE_AUDIT_WRITER'
DISABLE_TOKENS = ('0', 'off', 'false', 'no', 'none', 'disable', 'disabled', 'null')
MAX_LINE_BYTES = 8192          # 单行上限（超出 ⇒ detail 截断为 raw 字符串）
DETAIL_MAX_CHARS = 4000
ROTATE_MB_DEFAULT = 32.0

_ST = {
    'path': None,          # 当前审计文件路径（str）或 None（未初始化/禁用）
    'fd': None,            # 已打开的 O_APPEND fd
    'seq': 0,              # **写者内**单调事件序号（含写失败的那些，保证严格递增）
    'writer': WRITER,      # 契约 v2.1 §8.1：写者分组键（默认 retry 侧）
    'written': 0,          # 成功落盘行数
    'errors': 0,           # 写失败次数（审计自身故障，不影响主流程）
    'last_error': None,
    'run': None,           # 运行标识（boot_id / progress.started / keeper run_id）
    'enabled': True,
    'bytes': 0,            # 本文件已写字节（轮转判据）
    'rotations': 0,
    'by_event': {},
    'by_level': {},
    'last_ts': None,
    'rotate_bytes': int(ROTATE_MB_DEFAULT * 1024 * 1024),
    'init_n': 0,
    'calls': 0,            # emit 调用次数（含被禁用/失败的）
    'dropped': 0,          # 检测到写不进去而**主动丢弃**的事件数（R-7：不得静默成功）
    'fd_reopens': 0,       # inode 失活/换代导致的 fd 重开次数
    'heals': 0,            # 目录重建（F3 自愈）次数
    'rotate_skipped_shared': 0,   # 因检测到多写者而放弃轮转的次数
    'external_writes_seen': 0,    # 真实大小 ≠ 本人计数的观测次数
}
_LOCK = threading.RLock()


# ---------------------------------------------------------------------------
# 路径 / 开关解析
# ---------------------------------------------------------------------------
def _env_disabled(raw):
    return (raw or '').strip().lower() in DISABLE_TOKENS


def default_path():
    """默认审计路径：SCRAPE_AUDIT（非禁用值）> $SCRAPE_OUT_DIR/audit.jsonl > ./audit.jsonl。"""
    raw = os.environ.get(ENV_AUDIT)
    if raw and not _env_disabled(raw):
        return raw.strip()
    out = (os.environ.get(ENV_OUT_DIR) or '').strip()
    if out:
        return os.path.join(out, DEFAULT_NAME)
    return DEFAULT_NAME


def enabled_by_env():
    """SCRAPE_AUDIT 是否为「关闭」语义。"""
    return not _env_disabled(os.environ.get(ENV_AUDIT))


def path():
    """当前审计文件路径（未启用 ⇒ None）。"""
    return _ST['path']


def is_enabled():
    return bool(_ST['enabled'] and _ST['path'])


# ---------------------------------------------------------------------------
# 生命周期
# ---------------------------------------------------------------------------
def _rotate_bytes_from_env():
    raw = (os.environ.get(ENV_ROTATE_MB) or '').strip()
    if not raw:
        return int(ROTATE_MB_DEFAULT * 1024 * 1024)
    try:
        mb = float(raw)
    except ValueError:
        return int(ROTATE_MB_DEFAULT * 1024 * 1024)
    if mb <= 0:                       # <=0 ⇒ 关闭轮转
        return 0
    return int(mb * 1024 * 1024)


def _writer_from_env():
    """写者身份（契约 v2.1 §8.1）：`SCRAPE_AUDIT_WRITER` 非空 ⇒ 用它，否则默认 `WRITER`。"""
    raw = (os.environ.get(ENV_WRITER) or '').strip()
    return raw or WRITER


def writer():
    """当前生效的写者身份（写入每行的 `writer` 字段）。"""
    return _ST['writer']


def init(path=None, run=None, enabled=None, rotate_bytes=None, reset_seq=False, writer=None):
    """初始化/改绑审计出口。返回生效路径（禁用/无路径 ⇒ None）。**绝不抛异常**。

    path: 审计文件路径；None ⇒ default_path()。传空串 '' ⇒ 显式禁用。
    enabled: 强制开关；None ⇒ 由 SCRAPE_AUDIT 决定。
    reset_seq: 归零 seq（仅测试用，保证每个用例的 seq 断言独立）。
    writer: 写者身份（契约 v2.1 §8.1）；None ⇒ `SCRAPE_AUDIT_WRITER` > 默认 `"retry"`。
            **恒不为空**（空串/纯空白 ⇒ 回落默认值）——契约 §8.5 要求新写入必带 writer。
    """
    try:
        with _LOCK:
            new_path = default_path() if path is None else str(path)
            _ST['writer'] = (str(writer).strip() if writer is not None else '') \
                or _writer_from_env()
            if reset_seq:
                _ST['seq'] = 0
                _ST['written'] = 0
                _ST['errors'] = 0
                _ST['bytes'] = 0
                _ST['rotations'] = 0
                _ST['by_event'] = {}
                _ST['by_level'] = {}
                _ST['calls'] = 0
                _ST['last_error'] = None
                _ST['dropped'] = 0
                _ST['fd_reopens'] = 0
                _ST['heals'] = 0
                _ST['rotate_skipped_shared'] = 0
                _ST['external_writes_seen'] = 0
            if enabled is None:
                enabled = enabled_by_env() and bool(new_path)
            if not new_path:
                enabled = False
            if _ST['fd'] is not None and (new_path != _ST['path'] or not enabled):
                _close_fd()
            # 禁用时不留路径（path()/stats() 的读数与「是否真的会写」一致）
            _ST['path'] = (new_path or None) if enabled else None
            _ST['enabled'] = bool(enabled)
            if run is not None:
                _ST['run'] = run
            _ST['rotate_bytes'] = (_rotate_bytes_from_env() if rotate_bytes is None
                                   else int(rotate_bytes))
            _ST['init_n'] += 1
            if _ST['enabled']:
                # 文件大小用于轮转判据（不存在 ⇒ 0）
                try:
                    _ST['bytes'] = os.path.getsize(_ST['path'])
                except OSError:
                    _ST['bytes'] = 0
            else:
                _ST['bytes'] = 0
            return _ST['path'] if _ST['enabled'] else None
    except Exception as e:                      # 初始化失败也绝不抛出
        _ST['errors'] += 1
        _ST['last_error'] = '%s: %s' % (type(e).__name__, e)
        _ST['enabled'] = False
        return None


def _close_fd():
    fd = _ST.get('fd')
    _ST['fd'] = None
    if fd is not None:
        try:
            os.close(fd)
        except OSError:
            pass


def close():
    """关闭 fd（不改变路径与开关；下一次 emit 会自动重开）。"""
    with _LOCK:
        _close_fd()


def reset():
    """清空全部计数与 fd（测试用）。"""
    with _LOCK:
        _close_fd()
        _ST.update({'path': None, 'seq': 0, 'written': 0, 'errors': 0, 'bytes': 0,
                    'rotations': 0, 'by_event': {}, 'by_level': {}, 'calls': 0,
                    'last_error': None, 'enabled': True, 'last_ts': None, 'init_n': 0,
                    'writer': WRITER,
                    'dropped': 0, 'fd_reopens': 0, 'heals': 0,
                    'rotate_skipped_shared': 0, 'external_writes_seen': 0})


def stats():
    """计数器快照（只读、纯读，供测试/验收读数）。"""
    with _LOCK:
        s = dict(_ST)
    s['by_event'] = dict(s.get('by_event') or {})
    s['by_level'] = dict(s.get('by_level') or {})
    s['fd_open'] = s.pop('fd', None) is not None
    return s


# ---------------------------------------------------------------------------
# 写入
# ---------------------------------------------------------------------------
def _iso(ts):
    try:
        return datetime.fromtimestamp(ts).isoformat(timespec='milliseconds')
    except (OverflowError, OSError, ValueError):
        return None


def _fd_alive(fd):
    """fd 指向的 inode 是否仍是当前路径上的那个文件（R-7/M4：F3 的静默丢数据路径）。

    `rm -rf <out>` 或 `os.replace(path, path + '.1')` 之后，已打开的 fd 依然"可写"，
    但 `os.write` 返回的字节数照常 ⇒ 旧实现会把它记为 written+=1、errors=0，而文件不存在。
    这里用 `st_nlink` + `(st_dev, st_ino)` 双重核对，把这条路径变成**显式失败**。
    """
    if fd is None:
        return False
    try:
        st = os.fstat(fd)
    except OSError:
        return False
    if st.st_nlink <= 0:                 # 已 unlink（无目录项）
        return False
    try:
        cur = os.stat(str(_ST['path']))
    except OSError:
        return False                     # 目录/文件没了
    return (cur.st_dev, cur.st_ino) == (st.st_dev, st.st_ino)


def _ensure_fd():
    fd = _ST.get('fd')
    if fd is not None and not _fd_alive(fd):
        # inode 已失活（被删/被改名/被外部轮转）⇒ 旧 fd 写入进黑洞，必须先换代
        _close_fd()
        _ST['fd_reopens'] += 1
        fd = None
    if fd is None:
        fd = os.open(str(_ST['path']), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        _ST['fd'] = fd
    return fd


def _rotate_if_needed():
    """按大小轮转：audit.jsonl → audit.jsonl.1（覆盖旧 .1）。仅在锁内调用。

    **多写者语义（R-7 · 显式豁免，见模块 docstring）**：若真实文件大小 ≠ 本人计数
    （有外部写者/别的实例/keeper 也在写同一个 sink），则**放弃轮转** —— 共享 sink 上的
    `os.replace` 轮转会覆盖他人数据，且本人 fd 会指向已改名的 inode。此时改为采纳真实
    大小并计数 `rotate_skipped_shared`，由上层按"文件单调增长"处理。
    """
    limit = _ST.get('rotate_bytes') or 0
    if limit <= 0:
        return False
    try:
        real = os.stat(str(_ST['path'])).st_size
    except OSError:
        real = _ST['bytes']
    if real != _ST['bytes']:
        _ST['external_writes_seen'] += 1
        _ST['bytes'] = real              # 采纳真实大小（本人计数不可信时不轮转）
    if _ST['bytes'] < limit:
        return False
    if _ST['external_writes_seen']:
        _ST['rotate_skipped_shared'] += 1
        return False                     # 多写者 ⇒ 显式放弃轮转（不覆盖他人数据）
    try:
        _close_fd()
        os.replace(str(_ST['path']), str(_ST['path']) + '.1')
        _ST['bytes'] = 0
        _ST['rotations'] += 1
        return True
    except OSError as e:
        _ST['last_error'] = '%s: %s' % (type(e).__name__, e)
        return False


def _write_blob(data):
    """写一行（含 F3 自愈重试）。成功 True；失败 False ⇒ 调用方计 dropped/errors。"""
    try:
        os.write(_ensure_fd(), data)            # 单次 O_APPEND 原子追加
        return True
    except OSError:
        pass
    _close_fd()                                 # 句柄可能指向失活 inode ⇒ 换新
    _ST['fd'] = None
    return _heal_and_retry(data)


def _heal_and_retry(data):
    """F3 自愈：重建审计文件所在目录 → 重开 fd → 重写一次。成功返回 True。"""
    if not _ST.get('enabled') or not _ST.get('path'):
        return False
    try:
        d = os.path.dirname(os.path.abspath(str(_ST['path'])))
        if d:
            os.makedirs(d, exist_ok=True)
            _ST['heals'] += 1
        fd = _ensure_fd()
        os.write(fd, data)
        return True
    except OSError as e:
        _ST['last_error'] = '%s: %s' % (type(e).__name__, e)
        return False


def emit(level, event, detail=None, run=None, **fields):
    """写一条审计事件。返回写入的记录 dict，失败/禁用 ⇒ None。**绝不抛异常**。

    level: 'request'|'block'|'process'（未知值原样写入，由校验器判定）
    event: 事件名（如 retry_scheduled）
    detail: 事件特定字段（dict；非 dict 会被包成 {'value': ...}）
    fields: 追加进 detail 的键值对（可选写法）
    """
    rec = None
    try:
        with _LOCK:
            _ST['calls'] += 1
            if not (_ST['enabled'] and _ST['path']):
                return None
            ts = time.time()
            _ST['seq'] += 1
            if isinstance(detail, dict):
                d = dict(detail)
            elif detail is None:
                d = {}
            else:
                d = {'value': detail}
            if fields:
                d.update(fields)
            rec = {'schema': SCHEMA, 'ts_epoch': round(float(ts), 6), 'ts_iso': _iso(ts),
                   'pid': os.getpid(), 'writer': _ST['writer'] or WRITER,
                   'seq': _ST['seq'], 'level': level, 'event': event,
                   'detail': d, 'run': run if run is not None else _ST['run']}
            try:
                line = json.dumps(rec, ensure_ascii=False, sort_keys=False, default=str) + '\n'
            except Exception:
                rec['detail'] = {'_unserializable': True,
                                 'repr': repr(detail)[:DETAIL_MAX_CHARS]}
                line = json.dumps(rec, ensure_ascii=False, default=str) + '\n'
            data = line.encode('utf-8', 'replace')
            if len(data) > MAX_LINE_BYTES:      # 截断 detail，保住 schema 骨架（有界）
                try:
                    raw = json.dumps(d, ensure_ascii=False, default=str)[:DETAIL_MAX_CHARS]
                except Exception:
                    raw = repr(d)[:DETAIL_MAX_CHARS]
                rec['detail'] = {'_truncated': True, 'raw': raw}
                data = (json.dumps(rec, ensure_ascii=False, default=str) + '\n').encode('utf-8')
            if not _write_blob(data):           # R-7/M4：失败绝不静默"成功"
                _ST['dropped'] += 1
                _ST['errors'] += 1
                return None
            _ST['written'] += 1
            _ST['bytes'] += len(data)
            _ST['by_event'][event] = _ST['by_event'].get(event, 0) + 1
            _ST['by_level'][level] = _ST['by_level'].get(level, 0) + 1
            _ST['last_ts'] = rec['ts_epoch']
            _rotate_if_needed()
    except Exception as e:
        _ST['errors'] += 1
        _ST['last_error'] = '%s: %s' % (type(e).__name__, e)
        return None
    return rec


def emit_request(event, detail=None, **fields):
    return emit('request', event, detail, **fields)


def emit_block(event, detail=None, **fields):
    return emit('block', event, detail, **fields)


def emit_process(event, detail=None, **fields):
    return emit('process', event, detail, **fields)


# ---------------------------------------------------------------------------
# selftest：python3 audit.py
# ---------------------------------------------------------------------------
def _selftest():
    import shutil
    import tempfile
    ok = 0
    bad = 0
    tmp = tempfile.mkdtemp(prefix='audit-selftest-')
    p = os.path.join(tmp, 'audit.jsonl')

    def chk(name, cond, extra=''):
        nonlocal ok, bad
        if cond:
            ok += 1
            print('PASS  %s %s' % (name, extra))
        else:
            bad += 1
            print('FAIL  %s %s' % (name, extra))

    reset()
    init(path=p, run='selftest', reset_seq=True)
    chk('init.path', path() == p, p)
    emit_request('retry_scheduled', {'attempt': 1, 'status_or_exc': 'HTTP 503',
                                     'wait_s': 5.0, 'q': 'x'})
    emit_request('retry_exhausted', {'attempts': 6, 'q': 'x', 'last_error': 'HTTP 503'})
    emit('block', 'probe_all_dead', {'prefix': 'A', 'missing_chars': ['1']})
    emit('process', 'keeper_restart', {'from_pid': 1, 'to_pid': 2, 'reason': 'test'})
    with open(p, encoding='utf-8') as f:
        recs = [json.loads(l) for l in f if l.strip()]
    chk('lines', len(recs) == 4, 'n=%d' % len(recs))
    chk('schema.keys', all(set(r) >= {'schema', 'ts_epoch', 'ts_iso', 'pid', 'writer', 'seq',
                                      'level', 'event', 'detail', 'run'} for r in recs))
    chk('schema.value', all(r['schema'] == 'r3-audit-v2' for r in recs))
    chk('no_legacy_ts', all('ts' not in r for r in recs))
    # 契约 v2.1 §8.1/§8.5：新写入必须带 writer，且本模块默认写 "retry"
    chk('writer.default_retry', all(r.get('writer') == 'retry' for r in recs)
        and WRITER == 'retry', str({r.get('writer') for r in recs}))
    chk('writer.nonempty_str', all(isinstance(r.get('writer'), str) and r['writer'].strip()
                                   for r in recs))
    chk('seq.monotonic', [r['seq'] for r in recs] == [1, 2, 3, 4])
    chk('level.enum', all(r['level'] in LEVELS for r in recs))
    chk('detail.obj', all(isinstance(r['detail'], dict) for r in recs))
    chk('ts_epoch.float', all(isinstance(r['ts_epoch'], float) and r['ts_epoch'] > 0
                              for r in recs))
    chk('run.propagated', all(r['run'] == 'selftest' for r in recs))
    st = stats()
    chk('stats.written', st['written'] == 4 and st['errors'] == 0, str(st['by_event']))

    # writer 覆盖语义（契约 v2.1 §8.1：其它写者可复用本模块，身份必须可显式指定）
    close()
    reset()
    init(path=p, run='w2', reset_seq=True, writer='keeper')
    emit_request('keeper_exit')
    rec_w = [json.loads(l) for l in open(p, encoding='utf-8') if l.strip()]
    chk('writer.explicit_override', rec_w and rec_w[-1].get('writer') == 'keeper'
        and writer() == 'keeper', str(rec_w[-1].get('writer') if rec_w else None))
    reset()
    init(path=p, run='w3', reset_seq=True, writer='   ')
    emit_request('keeper_exit')
    rec_w = [json.loads(l) for l in open(p, encoding='utf-8') if l.strip()]
    chk('writer.blank_falls_back', rec_w and rec_w[-1].get('writer') == WRITER,
        str(rec_w[-1].get('writer') if rec_w else None))
    os.environ[ENV_WRITER] = 'envwriter'
    reset()
    init(path=p, run='w4', reset_seq=True)
    emit_request('keeper_exit')
    rec_w = [json.loads(l) for l in open(p, encoding='utf-8') if l.strip()]
    chk('writer.env_override', rec_w and rec_w[-1].get('writer') == 'envwriter',
        str(rec_w[-1].get('writer') if rec_w else None))
    os.environ.pop(ENV_WRITER, None)
    reset()

    # 禁用语义
    close()
    reset()
    os.environ[ENV_AUDIT] = 'off'
    init(reset_seq=True)
    chk('disable.no_path', path() is None and emit_request('x') is None)
    os.environ.pop(ENV_AUDIT, None)

    # 写失败不得抛异常
    reset()
    init(path='/proc/definitely/not/here/audit.jsonl', reset_seq=True)
    r = emit_request('retry_scheduled', {'attempt': 1})
    chk('failopen', r is None and stats()['errors'] >= 1)
    reset()

    # ---- R-7/M4：F3 场景（目录被删）不得静默"成功" ----
    reset()
    d2 = os.path.join(tmp, 'f3')
    os.makedirs(d2, exist_ok=True)
    p2 = os.path.join(d2, 'audit.jsonl')
    init(path=p2, run='f3', reset_seq=True)
    emit_request('error_classified', {'class': 'x', 'action': 'retry'})
    assert os.path.getsize(p2) > 0
    shutil.rmtree(d2)                              # 模拟 rm -rf <out>
    rec2 = emit_request('retry_scheduled', {'attempt': 1, 'wait_s': 2.0})
    exists = os.path.exists(p2)
    size2 = os.path.getsize(p2) if exists else 0
    chk('f3.no_silent_success',
        (rec2 is not None and exists and size2 > 0)          # 自愈：真的落盘了
        or (rec2 is None and stats()['dropped'] >= 1),       # 或显式丢弃
        'rec=%s exists=%s size=%d dropped=%d' % (rec2 is not None, exists, size2,
                                                 stats()['dropped']))
    chk('f3.on_disk_visible',
        (not exists) or ('retry_scheduled' in open(p2, encoding='utf-8').read()))
    chk('f3.heal_or_drop', stats()['heals'] >= 1 or stats()['dropped'] >= 1,
        str({k: stats()[k] for k in ('heals', 'errors', 'dropped', 'fd_reopens')}))
    # 不可自愈路径（父目录不可创建）⇒ 返回 None + dropped>0
    init(path='/proc/definitely/not/here/audit.jsonl', run='f3b', reset_seq=True)
    r3 = emit_request('retry_scheduled', {'attempt': 1})
    chk('f3b.explicit_drop', r3 is None and stats()['dropped'] >= 1,
        str({k: stats()[k] for k in ('dropped', 'errors')}))
    reset()

    # ---- R-7：轮转的多写者语义（显式豁免：检测到外部写者 ⇒ 不再轮转、不覆盖） ----
    reset()
    d3 = os.path.join(tmp, 'rot')
    os.makedirs(d3, exist_ok=True)
    p3 = os.path.join(d3, 'audit.jsonl')
    init(path=p3, run='rot', reset_seq=True, rotate_bytes=200)
    for i in range(3):                             # 阶段1：本进程独占（轮转按原语义）
        emit_request('retry_scheduled', {'attempt': i, 'pad': 'x' * 40})
    rot_before = stats()['rotations']
    n_before = stats()['written']
    with open(p3, 'a', encoding='utf-8') as f:     # 外部写者插一行
        f.write(json.dumps({'schema': 'r3-audit-v2', 'ts_epoch': time.time(),
                            'ts_iso': _iso(time.time()), 'pid': 999999, 'writer': 'keeper',
                            'seq': 1, 'level': 'process', 'event': 'keeper_start',
                            'detail': {}}) + '\n')
    for i in range(3):                             # 阶段2：共享 sink
        emit_request('retry_scheduled', {'attempt': 10 + i, 'pad': 'y' * 40})
    st = stats()
    rows = [json.loads(l) for l in open(p3, encoding='utf-8') if l.strip()]
    mine_phase2 = [r for r in rows if (r.get('detail') or {}).get('attempt') in (10, 11, 12)]
    chk('rotate.multi_writer_no_rotate',
        st['rotate_skipped_shared'] >= 1 and st['rotations'] == rot_before,
        'rotations %d→%d skipped=%d' % (rot_before, st['rotations'],
                                        st['rotate_skipped_shared']))
    chk('rotate.no_loss_after_shared', len(mine_phase2) == 3,
        'phase2 rows=%d（阶段1 已轮转 %d 次，written=%d→%d）'
        % (len(mine_phase2), rot_before, n_before, st['written']))
    chk('rotate.peer_row_kept', any(r.get('pid') == 999999 for r in rows))
    chk('rotate.every_row_has_writer', all(r.get('writer') for r in rows),
        str(sorted({r.get('writer') for r in rows})))
    reset()

    # ---- 契约 v2.1 §8.4（KI-1 的模块内回归护栏）：同 pid 双写者合流 ----
    # 模拟 scrape.py 的块级写者（converge）与请求级写者（本模块 retry）共用同一进程 + 同一
    # sink：两个计数器各写 1..n ⇒ 按 (pid, writer) 分组必须各自从 1 连续；按 pid 单键分组
    # 必然出现重复号（这正是 P4-verify 在真实 S4 上复现的 KI-1 现象）。
    reset()
    d4 = os.path.join(tmp, 'dualwriter')
    os.makedirs(d4, exist_ok=True)
    p4 = os.path.join(d4, 'audit.jsonl')
    init(path=p4, run='dual', reset_seq=True)          # writer == WRITER == 'retry'
    for i in range(3):
        emit_request('retry_scheduled', {'attempt': i})
    twin_pid = os.getpid()
    with open(p4, 'a', encoding='utf-8') as f:         # 同 pid 的第二写者
        for i in (1, 2, 3, 4):
            f.write(json.dumps({'schema': 'r3-audit-v2', 'ts_epoch': time.time() + i,
                                'ts_iso': _iso(time.time() + i), 'pid': twin_pid,
                                'writer': 'converge', 'seq': i, 'level': 'block',
                                'event': 'run_start', 'detail': {}}) + '\n')
    rows4 = [json.loads(l) for l in open(p4, encoding='utf-8') if l.strip()]
    by_pid, by_pidw = {}, {}
    for r in rows4:
        by_pid.setdefault(r['pid'], []).append(r['seq'])
        by_pidw.setdefault((r['pid'], r.get('writer')), []).append(r['seq'])
    groups = {k[1]: sorted(v) for k, v in by_pidw.items()}
    chk('writer.dual_same_pid_groups_continuous',
        set(groups) == {WRITER, 'converge'}
        and all(v == list(range(1, len(v) + 1)) for v in groups.values()),
        '按 (pid,writer) 分组：%s' % json.dumps(groups, sort_keys=True))
    pid_only = sorted(by_pid.get(twin_pid) or [])
    chk('writer.pid_only_group_would_dup',
        len(pid_only) != len(set(pid_only)),
        '按 pid 单键分组 seq=%s ⇒ 重复 %d 个（KI-1 签名）'
        % (pid_only, len(pid_only) - len(set(pid_only))))
    reset()
    print('audit.py selftest: %d pass / %d fail' % (ok, bad))
    return 0 if bad == 0 else 1


if __name__ == '__main__':
    sys.exit(_selftest())
