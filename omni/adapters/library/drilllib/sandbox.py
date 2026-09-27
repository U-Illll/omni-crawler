#!/usr/bin/env python3
"""drilllib/sandbox.py —— 把被测系统「影子化」进演练沙箱（fail-closed）。

职责：
  1. 复制被测 scrape.py 到 <run>/src/scrape_under_test.py，**文本重写**路径常量：
       BASE = Path("/tmp/library-scrape")  →  BASE = Path(os.environ["SCRAPE_SANDBOX_BASE"])
       OUT  = ...                          →  OUT  = Path(os.environ["DRILL_OUT_DIR"])
     并断言副本里不再出现 "/tmp/library-scrape" 字面量（否则拒绝运行）。
     ⇒ 即使被测版本不认 SCRAPE_OUT_DIR，也不可能写到生产路径。
  2. 复制 memguard.py / heartbeat.py 到同一目录（被测代码用函数内 import，与生产同布局）。
  3. 生成作业包装脚本 bin/run-scrape.sh：**不 exec**（保持 argv=包装脚本，keeper 的
     PID/argv 精确核对才成立），前置可选的 `ulimit -f`（S3 短写注入用，取值每次启动从
     <run>/drill-env/fsize_kb 读 ⇒ 演练台可在运行期收紧/放宽）。
  4. 生成 keeper-v2/v3 的 jobs.conf。
"""
import os
import re
import shutil
import stat
from pathlib import Path

PROD_LITERAL = "/tmp/library-scrape"

WRAPPER = """#!/usr/bin/env bash
# 演练台作业包装（自动生成，勿手改）：按需施加 RLIMIT_FSIZE 后前台运行被看护进程。
# 刻意不使用 exec：keeper 以「PID 文件 + /proc argv token 精确核对」判定存活，
# argv 必须始终是本包装脚本（见 refs/keeper-v2.sh verify_pid_for_job）。
set -u
FSIZE_FILE="${DRILL_FSIZE_FILE:-}"
if [ -n "$FSIZE_FILE" ] && [ -f "$FSIZE_FILE" ]; then
  KB="$(cat "$FSIZE_FILE" 2>/dev/null || printf 0)"
  case "$KB" in
    ''|*[!0-9]*) : ;;
    0) : ;;
    *) ulimit -f "$KB" 2>/dev/null || true ;;
  esac
fi
python3 "${DRILL_SHIM}" "$@"
rc=$?
exit $rc
"""

JOBS_CONF = """# 演练台作业表（自动生成，勿手改）—— 绝不指向生产路径
job {job} \\
  "{script}" \\
  "{cwd}" \\
  "{complog}" \\
  "{comppat}" \\
  "{outlog}" {extra}
job_exec {job} bash
job_cwd_strict {job} 1
"""


class SandboxError(RuntimeError):
    pass


def rewrite_source(src: Path, dst: Path) -> dict:
    """复制并重写路径常量；返回改写读数。"""
    text = src.read_text(encoding="utf-8")
    if not re.search(r"^\s*import .*\bos\b", text, re.M):
        text = re.sub(r"^(import .*)$", r"\1\nimport os", text, count=1, flags=re.M)
    n_base = n_out = 0
    out_lines = []
    for line in text.splitlines():
        if re.match(r"^BASE\s*=\s*Path\(", line):
            line = 'BASE = Path(os.environ["SCRAPE_SANDBOX_BASE"])'
            n_base += 1
        elif re.match(r"^OUT\s*=", line):
            line = 'OUT = Path(os.environ["DRILL_OUT_DIR"])'
            n_out += 1
        out_lines.append(line)
    patched = "\n".join(out_lines) + "\n"
    patched = patched.replace('"%s"' % PROD_LITERAL, 'os.environ["SCRAPE_SANDBOX_BASE"]')
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(patched, encoding="utf-8")
    leftovers = [i for i, ln in enumerate(patched.splitlines(), 1) if PROD_LITERAL in ln]
    if leftovers:
        raise SandboxError("副本仍含生产路径字面量（行 %s）→ 拒绝运行" % leftovers[:5])
    return {"base_rewritten": n_base, "out_rewritten": n_out,
            "prod_literal_left": len(leftovers), "sha256": _sha256(dst)}


def _sha256(p: Path) -> str:
    import hashlib
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def prepare(run_dir: Path, system: dict, seeds, slot_root: Path) -> dict:
    """搭好一次演练的沙箱骨架，返回路径字典。"""
    run_dir = Path(run_dir).resolve()
    slot_root = Path(slot_root).resolve()
    if not str(run_dir).startswith(str(slot_root)):
        raise SandboxError("演练目录越界：%s 不在 %s 内" % (run_dir, slot_root))

    src_dir = run_dir / "src"
    out_dir = run_dir / "out"
    work = run_dir / "work"
    bin_dir = run_dir / "bin"
    state = run_dir / "state"
    env_dir = run_dir / "drill-env"
    for d in (src_dir, out_dir, work, bin_dir, state, env_dir):
        d.mkdir(parents=True, exist_ok=True)

    scrape_src = Path(system["scrape"])
    if not scrape_src.exists():
        raise SandboxError("被测 scrape 不存在：%s" % scrape_src)
    target = src_dir / "scrape_under_test.py"
    rw = rewrite_source(scrape_src, target)

    copied = {}
    for key in ("memguard", "heartbeat"):
        p = system.get(key)
        if p and Path(p).exists():
            shutil.copy2(p, src_dir / Path(p).name)
            copied[key] = str(Path(p).name)
    # R3 各槽可能提供额外的同目录模块（如 audit.py / retry 模块）——一并影子化
    for extra in system.get("extra_modules") or []:
        p = Path(extra)
        if p.exists():
            shutil.copy2(p, src_dir / p.name)
            copied[p.name] = p.name

    shim = Path(__file__).resolve().parent / "shim.py"
    wrapper = bin_dir / "run-scrape.sh"
    wrapper.write_text(WRAPPER, encoding="utf-8")
    wrapper.chmod(wrapper.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    fsize_file = env_dir / "fsize_kb"
    fsize_file.write_text("0\n", encoding="utf-8")

    extra = " ".join(seeds) if seeds else ""
    conf = run_dir / "jobs.conf"
    conf.write_text(JOBS_CONF.format(
        job=system.get("job_name", "v8"), script=str(wrapper), cwd=str(work),
        complog=str(out_dir / "scrape.log"), comppat=system.get("complete_marker", "全部完成"),
        outlog=str(out_dir / "run.log"), extra=extra), encoding="utf-8")

    return {
        "run_dir": str(run_dir), "src_dir": str(src_dir), "out_dir": str(out_dir),
        "work": str(work), "bin": str(bin_dir), "state": str(state),
        "drill_env": str(env_dir), "fsize_file": str(fsize_file),
        "scrape_copy": str(target), "shim": str(shim), "wrapper": str(wrapper),
        "jobs_conf": str(conf), "rewrite": rw, "copied": copied,
        "scrape_src": str(scrape_src), "scrape_src_sha256": _sha256(scrape_src),
        "scrape_copy_sha256": rw["sha256"],
    }


def env_for(run: dict, system: dict, mock_host: str, extra=None) -> dict:
    """构造被看护进程 + keeper 的环境（全部指向沙箱，绝无生产路径）。"""
    env = dict(os.environ)
    env.update({
        "SCRAPE_SANDBOX_BASE": run["work"],
        "DRILL_OUT_DIR": run["out_dir"],
        "SCRAPE_OUT_DIR": run["out_dir"],          # 与 DRILL_OUT_DIR 同值（双保险）
        "DRILL_SANDBOX_ROOT": run["run_dir"],
        "DRILL_SCRAPE": run["scrape_copy"],
        "DRILL_SHIM": run["shim"],
        "DRILL_FSIZE_FILE": run["fsize_file"],
        "DRILL_MOCK_HOST": mock_host,
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONUNBUFFERED": "1",
        # keeper 侧
        "KEEPER_CONF": run["jobs_conf"],
        "KEEPER_STATE_DIR": run["state"],
        "KEEPER_AUDIT": run["audit"],
        "KEEPER_STARTUP_AUDIT": run["startup_audit"],
        "KEEPER_LOG_SINKS": run["keeper_log"],
        "KEEPER_ENV_TAG": "sandbox-drill",
        "KEEPER_TICK": str(system.get("keeper_tick", 5)),
        "KEEPER_RECOVERY_DEADLINE": str(system.get("recovery_deadline", 180)),
        "KEEPER_SPAWN_VERIFY_S": str(system.get("spawn_verify_s", 5)),
        "KEEPER_MAX_BACKOFF": str(system.get("max_backoff", 20)),
        "KEEPER_RAPID_DEATH": str(system.get("rapid_death", 10)),
        "KEEPER_STRICT_ENV": "1",
        "KEEPER_RUN_ID": run.get("run_id", "drill"),
        "KEEPER_SETSID": "1",
    })
    for k, v in (system.get("env") or {}).items():
        env[k] = v
    if extra:
        env.update(extra)
    return env
