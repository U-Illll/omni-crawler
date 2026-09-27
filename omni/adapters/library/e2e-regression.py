#!/usr/bin/env python3
"""R3·slot-converge · 端到端回归（loopback-only，零外网）

目的：把「改后的 scrape.py」跑在**真实 HTTP 栈**上（requests + 真实 mock 服务端），
      确认 R3 的块级兜底/收敛判据改动没有破坏正常抓取路径（探测→细分→叶子→落盘→收敛）。

红线：
  · 服务端是 R2 的 **loopback mock**（只允许 127.0.0.1，端口 0 自动分配）；
  · 被测源码先复制到本 slot 的 sandbox 下并**文本重写** HOST/BASE，
    重写后断言不再含 `/tmp/library-scrape` 字面量（否则中止）；
  · 只终止本脚本自己 spawn 的 mock 子进程（走 /__mock/shutdown）。

用法：python3 e2e-regression.py [--records 1500]
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
SANDBOX = HERE / 'sandbox' / 'e2e'          # R3p4/slot-converge-f1 路径适配
MODIFIED = HERE / 'scrape.py'
BASELINE = Path('/tmp/lib-opt-work/R3/refs/scrape-r2cand.py')
MOCK = Path('/tmp/lib-opt-work/R2/impl/slot-bench/mock_primo.py')
CORPUS = Path('/tmp/lib-opt-work/R2/impl/slot-bench/corpus-real-tree.json')


def rewrite(src, dst, port):
    """复制 + 重写 HOST/BASE（绝不 import 原文件；重写后断言无生产路径字面量）。"""
    text = Path(src).read_text(encoding='utf-8')
    text, n_host = re.subn(r'^HOST\s*=\s*"https?://[^"]+"',
                           'HOST = "http://127.0.0.1:%d"' % port, text, flags=re.M)
    text, n_base = re.subn(r'^BASE\s*=\s*Path\(.*\)$',
                           'BASE = Path(os.environ["SCRAPE_E2E_BASE"])', text, flags=re.M)
    if '/tmp/library-scrape' in text:
        raise SystemExit('重写不完整：仍残留生产路径字面量 → 中止')
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    Path(dst).write_text(text, encoding='utf-8')
    return {'n_host': n_host, 'n_base': n_base}


def start_mock(tag, records, seeds='A'):
    SANDBOX.mkdir(parents=True, exist_ok=True)
    ready = SANDBOX / f'{tag}.ready.json'
    stats = SANDBOX / f'{tag}.stats.json'
    reqlog = SANDBOX / f'{tag}.reqlog.jsonl'
    outp = SANDBOX / f'{tag}.mock.out'
    for p in (ready, stats, reqlog, outp):
        if p.exists():
            p.unlink()
    argv = [sys.executable, str(MOCK), '--port', '0', '--host', '127.0.0.1',
            '--profile', 'ideal', '--lat-p50', '0', '--lat-sigma', '0',
            '--lat-overhead', '0', '--records', str(records),
            '--corpus-file', str(CORPUS), '--seed-subset', seeds,
            '--ready-file', str(ready), '--stats-file', str(stats), '--log', str(reqlog)]
    fh = open(outp, 'w')
    proc = subprocess.Popen(argv, stdout=fh, stderr=subprocess.STDOUT, cwd=str(SANDBOX))
    t0 = time.time()
    while time.time() - t0 < 60:
        if ready.exists():
            try:
                info = json.loads(ready.read_text())
                if info.get('port'):
                    cfg = _get(info['port'], '/__mock/config')
                    return proc, {'port': info['port'], 'argv': argv,
                                  'expected_crawl': cfg.get('expected_crawl'),
                                  'corpus': cfg.get('corpus'), 'reqlog': str(reqlog)}
            except Exception:
                pass
        if proc.poll() is not None:
            raise SystemExit('mock 启动失败: ' + outp.read_text()[-600:])
        time.sleep(0.05)
    raise SystemExit('mock 启动超时')


def _get(port, path, timeout=15):
    try:
        with urllib.request.urlopen(f'http://127.0.0.1:{port}{path}', timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def stop_mock(proc, port):
    try:
        urllib.request.urlopen(f'http://127.0.0.1:{port}/__mock/shutdown', timeout=5).read()
    except Exception:
        pass
    try:
        proc.wait(timeout=15)
    except Exception:
        try:
            proc.terminate(); proc.wait(timeout=10)
        except Exception:
            proc.kill()


def run_one(variant, src, port, tag, outdir, base):
    d = SANDBOX / tag
    if d.exists():
        shutil.rmtree(d)
    outdir.mkdir(parents=True, exist_ok=True)
    base.mkdir(parents=True, exist_ok=True)
    dst = d / 'scrape_under_test.py'
    meta = rewrite(src, dst, port)
    env = dict(os.environ, SCRAPE_OUT_DIR=str(outdir), SCRAPE_E2E_BASE=str(base),
               PYTHONUNBUFFERED='1', PYTHONDONTWRITEBYTECODE='1',
               SCRAPE_THROTTLE='static', SCRAPE_LEAF_RETRY_S='1', SCRAPE_BRANCH_RECHECK_S='1')
    t0 = time.time()
    p = subprocess.run([sys.executable, str(dst), 'A'], cwd=str(d), env=env,
                       capture_output=True, text=True, timeout=600)
    wall = time.time() - t0
    m = re.search(r'__RC__', p.stdout or '')
    rec = outdir / 'records.jsonl'
    uniq = set()
    if rec.exists():
        for line in rec.read_text(encoding='utf-8', errors='replace').splitlines():
            try:
                uniq.add(json.loads(line).get('mms'))
            except ValueError:
                pass
    conv = None
    cp = outdir / 'convergence-report.json'
    if cp.exists():
        try:
            conv = json.loads(cp.read_text())
        except ValueError:
            conv = {'_parse': 'failed'}
    return {'variant': variant, 'src': str(src), 'rc': p.returncode, 'wall_s': round(wall, 1),
            'records_unique': len(uniq), 'rewrite': meta,
            'conv': {k: (conv or {}).get(k) for k in ('decided', 'exit_code', 'reason')},
            'log_tail': (p.stdout or '')[-800:], 'stderr_tail': (p.stderr or '')[-400:]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--records', type=int, default=1500)
    args = ap.parse_args()
    proc, info = start_mock('e2e', args.records)
    try:
        print(json.dumps({k: info[k] for k in ('port', 'expected_crawl', 'corpus')},
                         ensure_ascii=False))
        res = {}
        for variant, src in (('baseline', BASELINE), ('modified', MODIFIED)):
            out = SANDBOX / f'out-{variant}'
            base = SANDBOX / f'base-{variant}'
            res[variant] = run_one(variant, src, info['port'], f'run-{variant}', out, base)
            print(f"[{variant}] rc={res[variant]['rc']} unique={res[variant]['records_unique']} "
                  f"wall={res[variant]['wall_s']}s conv={json.dumps(res[variant]['conv'], ensure_ascii=False)}")
        exp = (info.get('expected_crawl') or {}).get('predicted_unique')
        ok = (res['modified']['rc'] == 0
              and res['modified']['records_unique'] == res['baseline']['records_unique']
              and (exp is None or res['modified']['records_unique'] >= exp - 2))
        print(json.dumps({'e2e_ok': ok, 'expected_unique': exp,
                          'baseline_unique': res['baseline']['records_unique'],
                          'modified_unique': res['modified']['records_unique'],
                          'modified_rc': res['modified']['rc']}, ensure_ascii=False))
        (SANDBOX / 'e2e-result.json').write_text(
            json.dumps({'info': info, 'runs': res, 'ok': ok}, ensure_ascii=False, indent=1),
            encoding='utf-8')
        return 0 if ok else 1
    finally:
        stop_mock(proc, info['port'])


if __name__ == '__main__':
    sys.exit(main())
