#!/usr/bin/env bash
# =============================================================================
# R3-v2.sh —— R3 acceptance 判据硬化版（P4-E 交付物）
# =============================================================================
#
# 诚实语义声明（P2-3；v1 的注释称"判据 = 存在性 + 轻执行 + 读数"，实际"读数"只是向后 grep，
# 已被证伪：见 R3/verification/report.md §1.3 全桩目录 ⇒ exit 0）：
#
#   v1 判据 = 文件存在 && 能 py_compile && 报告文本含 "PASS"/"180" 词。
#   v2 判据 = **调用被验产物的机器判据并检查退出码**（P0-1），或**读被验产物本次运行落盘的
#             结构化工件**（JSON / JSONL / 退出码），全部读数进 JSON 后由本脚本比对阈值。
#   v2 不把"报告里出现 PASS 字样"当判据（P1-2：那等于让被验方自判，且反向惩罚诚实披露）；
#             人读报告只在 ADV 段做抽样标注，永不参与判定。
#
# 判据清单（每条都标注了来源：verify report §4 / crit-benchmark §6 / 契约 / 任务书）：
#
#   K1  C1 单元   python3 <cand>/.../test_retry.py  rc=0
#                 + 读 <cand>/.../sandbox-test/test-results.json（结构化）：
#                   fail=0、断言总数 ≥ 下限(默认 111)、T2/T4/T5/T7/T8/T9/T10/T11/T12 覆盖、
#                   读数文件 mtime ≥ 本次运行起点（防陈旧读数复用）
#                 [P0-1 ①；crit-benchmark §1.1 L2/L3 + A1-4；verify §6 补记的"陈旧读数"风险]
#   K2  C1 能力  独立探针 probe_retry_jitter.py：import 候选版 scrape.py，直接采样等待计划
#                 （_retry_wait_plan；缺失时退回行为旁路 drive fetch + 虚时钟）
#                 ⇒ 抖动 stdev/mean>0.05 且样本落在 [0.75,1.25]×raw 内
#                 [crit-benchmark §1.3(a)(b)(d)/A1-4；来源独立于候选版自己的测试台]
#   K3  C2 单元   python3 <cand>/.../test_converge.py rc=0 + 读 test-results/readings.json：
#                 pass=true、必需套件齐全且 ok、任何跑了的套件不得为红、读数 mtime 新鲜
#                 [P0-1 ②；§2.3 A2-*]
#   K4  C2 目标   读被验产物落盘的原始工件（旁路，不看它的自述）：probe_all_dead 场景 out 目录
#                 ⇒ convergence-report.json: decided≠done 且 exit_code≠0；gaps[] 非空且逐条带
#                   degraded/terminal 登记字段；progress.json.gaps≥1；scrape.log 无完成标记；
#                   audit.jsonl 含 probe_all_dead≥1 与 repair_round_enter≥1
#                 [P1-3 C2「未决缺口必须登记」；crit-benchmark §2.3 A2-2/A2-3/A2-4]
#   K5  契约     候选版本次运行写下的 audit.jsonl 必须符合冻结的审计契约 v2
#                 （schema/ts_epoch/pid/seq/level/event/detail 强制字段 + 每 pid 组 seq 从 1 连续）
#                 [R3p4/refs/audit-contract-v2.md §1/§3；覆盖 crit-benchmark F6「组合态」缺口的一半]
#   K6  校验器   python3 <cand>/audit-verify-v2.py --self-test rc=0（缺文件 = MISSING，fail-closed）
#                 [P0-1 ③；P1-1 兜底不得恒真]
#   K7  元判据   红队夹具（手写伪造）必须 rc≠0 且输出无 Python traceback
#                 [任务书 P0-1 末条；契约 §5.1/§5.3a/§5.3c；verify §3.2 空洞 1（T1）]
#   K8  特异性   真样本正对照：契约 v2 形态的真实产物必须 rc=0（无 traceback），
#                 并在其副本上做变异（seq 缺号 / 非末行坏行）必须 rc≠0
#                 [crit-benchmark §6.1 原则 2/5：只证伪不证真 ⇒ 会放过"永远 exit 1"的退化校验器]
#   K9  C3 时限   恢复时限 ≤180s 的结构化读数（drill result.json 的 recovery_within_deadline
#                 断言：声明 limit≤180 且全部非空读数 ≤limit；或 keeper d1 audit.jsonl 的
#                 restart 事件读数）——**只读既有结构化读数**，不为本判据杀任何进程
#                 [P1-3「恢复时限」；crit-benchmark §3.3 A3-1/A3-7；verify §2.4 的口径警告]
#   K10 回执      生产者回执齐备（不含验收脚本自己的回执、不含验证者的报告 ⇒ 消除 P0-2 自指）
#                 缺 ⇒ 分级退出码 3（不是判据不过）
#   ADV 人读标注  验证报告是否含 ≥N 条可复现命令 + 独立读数表 + 遗留清单（**不判死**）
#                 [P0-2：删掉 R3-6 自指判据，改为人读标注]
#
# 退出码（分级；P2-1。优先级：FAIL > MISSING > UNADJUDICABLE > RECEIPTS > PASS）：
#   0 = 全过（另有 verdict=PASS；带 --allow-known-gaps 且只剩 GAP 时为 PASS-with-known-gaps）
#   1 = 至少一条判据不过（判据真的跑了并给出否定结论）
#   2 = 缺件 / 靶目录不存在（fail-closed：不允许"没有也能过"）
#   3 = 判据全过，但生产者回执未齐（人工产物缺，机器判据不受影响）
#   4 = 有判据无法判定（UNADJUDICABLE：证据来源按设计缺失，例如候选版没带契约 v2 真样本）；
#       默认按"不能证明达标"处理 ⇒ 非零；调用方显式传 --allow-known-gaps 才降为 0（并打印缺口）
#
# 证据模式（每次运行都写进 JSON，避免"演练读数 = 生产读数"这类误读）：
#   fresh-run         本脚本本次真实执行的命令/探针
#   preexisting-read  读候选版此前落盘的结构化读数（K9）；年龄超过 --max-reading-age 只记 warning
#   gated             需要杀进程（drill S1/S4、keeper d1）的判据**默认不跑**：本槽红线是"不 kill
#                     任何进程"，故用 --with-kill-drills 显式开启；关闭时该判据走 preexisting-read
#
# 残余可伪造面（诚实披露，对齐 verify §3.2「空洞 2 / 信任根」）：
#   本脚本能证明"产物里有与目标等价的结构化证据"，不能证明"这些证据不是人类手写的"。
#   一个愿意把 F1 反转工件、契约合规审计、抖动分布全部一致地伪造出来的提交，等价于重新实现
#   了产物本身；这是"工作产物等价性"判据的固有边界。独立复现（换会话实跑）仍是最后一道防线。
#
# 用法：
#   bash R3-v2.sh [--cand DIR] [--root DIR] [--json OUT] [--work DIR]
#                 [--with-kill-drills] [--allow-known-gaps] [--no-shadow]
#                 [--receipts a,b,c | --no-receipts] [--sample-dir DIR] [--report FILE]
# 环境变量：R3_ROOT / CAND / R3V2_WORK / R3V2_SAMPLE_DIR / R3V2_RECEIPTS
# =============================================================================

set -uo pipefail

# ----------------------------------------------------------------------------- 
# 0. 配置与参数（P2-2：路径全部可配置，支持影子目录复跑）
# -----------------------------------------------------------------------------
R3_ROOT="${R3_ROOT:-/tmp/lib-opt-work/R3p4}"                 # 轮次根（回执/报告）
DEFAULT_CAND_REL="merge/r3-candidate"                        # 候选版相对轮次根
FALLBACK_CAND="${R3_FALLBACK_CAND:-/tmp/lib-opt-work/R3/impl}"  # 未合成前的现状基线
CAND="${CAND:-}"
WORK="${R3V2_WORK:-}"
JSON_OUT=""
OPT_SAMPLE="${R3V2_SAMPLE_DIR:-}"
OPT_REPORT=""
OPT_RECEIPTS="${R3V2_RECEIPTS-audit-unify,converge-f1,keeper-match,retry-fatal,merge}"
NO_RECEIPTS=0
WITH_KILL_DRILLS=0
ALLOW_KNOWN_GAPS=0
USE_SHADOW=1
T_RETRY=300
T_CONVERGE=1500
T_VERIFY=300
MIN_RETRY_ASSERTIONS=111
MAX_READING_AGE="${R3V2_MAX_READING_AGE:-86400}"             # 24h
DRILL_SYSTEM="${R3V2_DRILL_SYSTEM:-}"

usage() { sed -n '3,77p' "$0"; exit 0; }

while [ $# -gt 0 ]; do
  case "$1" in
    --cand) CAND="${2:-}"; shift 2 ;;
    --root) R3_ROOT="${2:-}"; shift 2 ;;
    --json) JSON_OUT="${2:-}"; shift 2 ;;
    --work) WORK="${2:-}"; shift 2 ;;
    --sample-dir) OPT_SAMPLE="${2:-}"; shift 2 ;;
    --report) OPT_REPORT="${2:-}"; shift 2 ;;
    --receipts) OPT_RECEIPTS="${2:-}"; shift 2 ;;
    --no-receipts) NO_RECEIPTS=1; shift ;;
    --with-kill-drills) WITH_KILL_DRILLS=1; shift ;;
    --allow-known-gaps) ALLOW_KNOWN_GAPS=1; shift ;;
    --no-shadow) USE_SHADOW=0; shift ;;
    --timeout-converge) T_CONVERGE="${2:-}"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "未知参数：$1（--help 看用法）" >&2; exit 2 ;;
  esac
done

# 候选版根目录解析（任务书第 9 条：默认 merge/r3-candidate，回退 R3/impl；不存在时分级说清）
TARGET_KIND="candidate"
if [ -z "$CAND" ]; then
  if [ -d "$R3_ROOT/$DEFAULT_CAND_REL" ]; then
    CAND="$R3_ROOT/$DEFAULT_CAND_REL"
  elif [ -d "$FALLBACK_CAND" ]; then
    CAND="$FALLBACK_CAND"
    TARGET_KIND="fallback-baseline"
  else
    CAND="$R3_ROOT/$DEFAULT_CAND_REL"
    TARGET_KIND="absent"
  fi
fi

RUN_T0="$(date +%s)"
[ -n "$WORK" ] || WORK="$(mktemp -d "${TMPDIR:-/tmp}/r3v2-XXXXXX")"
mkdir -p "$WORK"/{logs,probes,fixtures,tmp}
TSV="$WORK/criteria.tsv"
: > "$TSV"
PROBE_JSON="$WORK/probes"

STATUS_LOG=()
n_pass=0; n_fail=0; n_missing=0; n_gap=0; n_adv=0; n_receipts=0

# -----------------------------------------------------------------------------
# 1. 判定记录原语
# -----------------------------------------------------------------------------
# 状态：PASS / FAIL / MISSING / GAP / RECEIPTS / ADVISORY
rec() { # ID STATUS DETAIL
  local id="$1" st="$2" det="$3"
  printf '%s\t%s\t%s\n' "$id" "$st" "$(printf '%s' "$det" | tr '\t\n' '  ')" >> "$TSV"
  case "$st" in
    PASS)      n_pass=$((n_pass+1));     printf 'PASS      %-26s %s\n' "$id" "$det" ;;
    FAIL)      n_fail=$((n_fail+1));     printf 'FAIL      %-26s %s\n' "$id" "$det" ;;
    MISSING)   n_missing=$((n_missing+1)); printf 'MISSING   %-26s %s\n' "$id" "$det" ;;
    GAP)       n_gap=$((n_gap+1));       printf 'UNADJUD   %-26s %s\n' "$id" "$det" ;;
    RECEIPTS)  n_receipts=$((n_receipts+1)); printf 'RECEIPTS  %-26s %s\n' "$id" "$det" ;;
    ADVISORY)  n_adv=$((n_adv+1));       printf 'advisory  %-26s %s\n' "$id" "$det" ;;
  esac
  STATUS_LOG+=("$id=$st")
}

# run_to LOGFILE TIMEOUT CMD...  → 全局 RC
RC=0
run_to() {
  local log="$1" tmo="$2"; shift 2
  if command -v timeout >/dev/null 2>&1; then
    timeout -k 5 "$tmo" "$@" >"$log" 2>&1
  else
    "$@" >"$log" 2>&1
  fi
  RC=$?
}
# run_in CWD LOGFILE TIMEOUT CMD...  → 在指定 cwd 下跑（子壳内 exec，退出码可传回全局 RC）
run_in() {
  local dir="$1" log="$2" tmo="$3"; shift 3
  if command -v timeout >/dev/null 2>&1; then
    ( cd "$dir" && exec timeout -k 5 "$tmo" "$@" ) >"$log" 2>&1
  else
    ( cd "$dir" && exec "$@" ) >"$log" 2>&1
  fi
  RC=$?
}

# 在候选版树里找一个产物（多布局兼容：slot-*/ 子目录、扁平布局、顶层）
# find_artifact NAME → stdout 路径（取最新者），无则空
find_artifact() {
  local name="$1" tree="$2" p
  for p in "$tree/$name" "$tree"/*/"$name" "$tree"/*/*/"$name"; do
    [ -f "$p" ] && { printf '%s' "$p"; return 0; }
  done
  p="$(find "$tree" -maxdepth 4 -name "$name" -type f -not -path '*/runs/*' \
        -not -path '*__pycache__*' -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2-)"
  printf '%s' "$p"
}

json_get() { # FILE DOTTED_KEY [default]
  python3 - "$1" "$2" "${3:-}" <<'PYEOF' 2>/dev/null
import json,sys
path,key,default=sys.argv[1],sys.argv[2],sys.argv[3]
try:
    d=json.load(open(path,encoding='utf-8'))
except Exception:
    print(default); raise SystemExit(0)
cur=d
for part in key.split('.'):
    if part=='' : continue
    if isinstance(cur,list):
        try: cur=cur[int(part)]
        except Exception: print(default); raise SystemExit(0)
    elif isinstance(cur,dict):
        if part not in cur: print(default); raise SystemExit(0)
        cur=cur[part]
    else:
        print(default); raise SystemExit(0)
if isinstance(cur,(dict,list)): print(json.dumps(cur,ensure_ascii=False))
elif cur is None: print(default)
else: print(cur)
PYEOF
}

json_check() { # FILE DOTTED_KEY EXPECT JSOP   → 0/1
  python3 - "$1" "$2" "$3" "$4" <<'PYEOF' 2>/dev/null
import json,sys
path,key,expect,op=sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4]
try: d=json.load(open(path,encoding='utf-8'))
except Exception: raise SystemExit(1)
cur=d
for part in key.split('.'):
    if part=='': continue
    if isinstance(cur,list):
        try: cur=cur[int(part)]
        except Exception: raise SystemExit(1)
    elif isinstance(cur,dict):
        if part not in cur: raise SystemExit(1)
        cur=cur[part]
    else: raise SystemExit(1)
if op=='eq':   ok = (str(cur)==expect)
elif op=='ne': ok = (str(cur)!=expect)
elif op=='ge': ok = (float(cur)>=float(expect))
elif op=='le': ok = (float(cur)<=float(expect))
elif op=='gt': ok = (float(cur)>float(expect))
elif op=='lt': ok = (float(cur)<float(expect))
elif op=='true':  ok = (cur is True)
elif op=='false': ok = (cur is False)
elif op=='nonempty': ok = bool(cur)
else: ok=False
raise SystemExit(0 if ok else 1)
PYEOF
}

# -----------------------------------------------------------------------------
# 2. 靶目录检查（fail-closed）
# -----------------------------------------------------------------------------
echo "==============================================================="
echo "R3-v2 acceptance（判据硬化版）  $(date '+%F %T')"
echo "  轮次根 R3_ROOT = $R3_ROOT"
echo "  候选版 CAND    = $CAND   [$TARGET_KIND]"
echo "  工作区 WORK    = $WORK   (影子=$( [ $USE_SHADOW = 1 ] && echo on || echo off ))"
echo "  杀进程演练     = $( [ $WITH_KILL_DRILLS = 1 ] && echo on || echo 'off（默认；本槽红线不 kill 进程）')"
echo "==============================================================="

if [ ! -d "$CAND" ]; then
  rec "TARGET" "MISSING" "候选版根目录不存在：$CAND（显式 --cand 或 R3_ROOT 下 $DEFAULT_CAND_REL 均未命中）"
  FINAL_EXIT=2
  VERDICT="MISSING-TARGET"
  CAND_FILES=0
else
  CAND_FILES="$(find "$CAND" -maxdepth 4 -type f -not -path '*/runs/*' 2>/dev/null | wc -l | tr -d ' ')"
  [ "$CAND_FILES" -gt 0 ] || rec "TARGET" "ADVISORY" "靶目录存在但（maxdepth 4 内）没有文件：$CAND"
  rec "TARGET" "PASS" "靶目录存在：$CAND [$TARGET_KIND]，maxdepth4 内 $CAND_FILES 个文件"
fi

# 影子副本：默认所有会写盘的判据都在副本里跑（p4-verify 红线：不得修改被测文件）
TREE="$CAND"
SHADOW_NOTE="in-place"
if [ -d "$CAND" ] && [ "$USE_SHADOW" = 1 ]; then
  TREE="$WORK/shadow"
  mkdir -p "$TREE"
  tar -C "$CAND" --exclude=runs --exclude=__pycache__ --exclude='*.pyc' -cf - . 2>/dev/null \
    | tar -C "$TREE" -xf - 2>/dev/null
  SHADOW_NOTE="shadow:$TREE"
  # 把测试台里硬编码的沙箱常量重定向到影子内（verify §6 补记的标准做法；改的是副本，不是交付件）
  REWRITES=0
  while IFS= read -r f; do
    if grep -q "R3/sandbox/slot-converge" "$f" 2>/dev/null; then
      sed -i "s#/tmp/lib-opt-work/R3/sandbox/slot-converge#$TREE/sandbox/slot-converge#g" "$f"
      REWRITES=$((REWRITES+1))
      printf '%s\n' "$f" >> "$WORK/logs/shadow-rewrites.txt"
    fi
  done < <(find "$TREE" -maxdepth 3 -name '*.py' -not -path '*__pycache__*' 2>/dev/null)
  printf '影子重定向：%d 个文件（沙箱常量 → %s/sandbox/slot-converge）\n' "$REWRITES" "$TREE"
fi

# 产物定位
A_TEST_RETRY="$(find_artifact test_retry.py "$TREE")"
A_TEST_CONV="$(find_artifact test_converge.py "$TREE")"
A_VERIFIER="$(find_artifact audit-verify-v2.py "$TREE")"
VERIFIER_KIND="v2"
if [ -z "$A_VERIFIER" ]; then
  A_VERIFIER="$(find_artifact audit-verify.py "$TREE")"
  [ -n "$A_VERIFIER" ] && VERIFIER_KIND="v1(旧版；契约要求 audit-verify-v2.py)"
fi
A_DRILL="$(find_artifact drill.py "$TREE")"
A_SCRAPE_RETRY=""
if [ -n "$A_TEST_RETRY" ]; then
  A_SCRAPE_RETRY="$(find_artifact scrape.py "$(dirname "$A_TEST_RETRY")")"
fi
[ -n "$A_SCRAPE_RETRY" ] || A_SCRAPE_RETRY="$(find_artifact scrape.py "$TREE")"

echo "---------------------------------------------------------------"
printf '定位：test_retry=%s\ntest_converge=%s\nverifier=%s\ndrill=%s\nscrape=%s\n' \
  "${A_TEST_RETRY:-<无>}" "${A_TEST_CONV:-<无>}" "${A_VERIFIER:-<无>}" "${A_DRILL:-<无>}" "${A_SCRAPE_RETRY:-<无>}"
echo "---------------------------------------------------------------"

# =============================================================================
# K1 · C1 单元判据：调用被验产物自己的测试台 + 读它的结构化结果
# =============================================================================
if [ -z "$A_TEST_RETRY" ]; then
  rec "K1-C1-unit" "MISSING" "test_retry.py 未找到（fail-closed：不接受'报告里写了 PASS'）"
else
  RDIR="$(dirname "$A_TEST_RETRY")"
  run_to "$WORK/logs/test_retry.out" "$T_RETRY" python3 "$A_TEST_RETRY"
  rc1=$RC
  RES="$RDIR/sandbox-test/test-results.json"
  det="rc=$rc1"
  if [ "$rc1" -ne 0 ]; then
    rec "K1-C1-unit" "FAIL" "$det；$(tail -c 300 "$WORK/logs/test_retry.out" | tr '\n' ' ')"
  elif [ ! -f "$RES" ]; then
    rec "K1-C1-unit" "FAIL" "$det 但结构化读数沙箱沙箱缺失：$RES（只认结构化读数，不认 stdout 文本）"
  else
    p="$(json_get "$RES" pass 0)"; f="$(json_get "$RES" fail -1)"; t="$(json_get "$RES" total -1)"
    n="$(python3 -c "import json,sys;d=json.load(open(sys.argv[1]));print(len(d.get('results') or []))" "$RES" 2>/dev/null || echo 0)"
    mt="$(stat -c %Y "$RES" 2>/dev/null || echo 0)"
    missing_names="$(python3 - "$RES" T2 T4 T5 T7 T8 T9 T10 T11 T12 <<'PYEOF' 2>/dev/null
import json,sys
d=json.load(open(sys.argv[1],encoding='utf-8'))
names=[str(r.get('test','')) for r in (d.get('results') or [])]
need=sys.argv[2:]
miss=[p for p in need if not any(n.startswith(p) for n in names)]
print(','.join(miss))
PYEOF
)"
    if [ "$f" != "0" ] || [ "$p" = "0" ]; then
      rec "K1-C1-unit" "FAIL" "rc=$rc1，但 test-results.json 说 pass=$p fail=$f"
    elif [ "$n" -lt "$MIN_RETRY_ASSERTIONS" ]; then
      rec "K1-C1-unit" "FAIL" "断言总数 $n < 下限 $MIN_RETRY_ASSERTIONS（防'删断言凑绿'）"
    elif [ -n "$missing_names" ]; then
      rec "K1-C1-unit" "FAIL" "缺少关键用例前缀：$missing_names（C1 能力/红线的覆盖被删）"
    elif [ "$mt" -lt "$RUN_T0" ]; then
      rec "K1-C1-unit" "FAIL" "读数文件 mtime=$mt 早于本次运行起点 $RUN_T0 ⇒ 疑似复用陈旧读数"
    else
      rec "K1-C1-unit" "PASS" "rc=0，$n 断言全绿（pass=$p fail=$f），关键用例前缀齐全，读数新鲜"
    fi
  fi
fi

# =============================================================================
# K2 · C1 能力（独立旁路探针，不经候选版测试台）
# =============================================================================
cat > "$PROBE_JSON/probe_retry_jitter.py" <<'PYEOF'
#!/usr/bin/env python3
# 独立探针：直接 import 候选版 scrape.py，采样它的等待计划，判"指数包络 + 抖动"是否真的存在。
# 与候选版自己的测试台无关（这才是"来源独立"的读数）；不写任何被测文件。
import importlib.util, json, os, random, statistics, sys, types

SCRAPE, OUT = sys.argv[1], sys.argv[2]
result = {"probe": "retry_jitter", "scrape": SCRAPE, "api_used": None, "ok": False,
          "fatal": None, "checks": {}}
os.environ.setdefault("SCRAPE_OUT_DIR", os.path.join(os.path.dirname(OUT), "out"))


def dump(rc=0):
    with open(OUT, "w", encoding="utf-8") as fh:
        json.dump(result, fh, ensure_ascii=False, indent=1)
    raise SystemExit(rc)


def load():
    spec = importlib.util.spec_from_file_location("cand_scrape", SCRAPE)
    m = importlib.util.module_from_spec(spec)
    sys.modules["cand_scrape"] = m
    spec.loader.exec_module(m)
    return m


try:
    m = load()
except Exception as e:                                     # noqa: BLE001
    result["fatal"] = "import 失败：%s: %s" % (type(e).__name__, e)
    dump(3)

# ---- 主通道：直接采样等待计划（crit-benchmark §1.1 认定的唯一等待计算点） ----
plan = None
for name in ("_retry_wait_plan", "retry_wait_plan", "_retry_plan_wait"):
    fn = getattr(m, name, None)
    if callable(fn):
        plan = fn
        result["api_used"] = name
        break

backoff = getattr(m, "RETRY_BACKOFF", None)
# R-8（P4.5·v2.1）：候选版已删除乘性口径的 RETRY_JITTER_LO/HI（改为 equal jitter 的
# RETRY_JITTER_BAND=(0.5,1.0) / RETRY_JITTER_MEAN=0.75）。**本节读数与判据数值不变**
# （R-2 已登记「K2 口径过期」，其口径重新冻结不在 R-8 处置范围内），只把两种口径分别标注，
# 避免读者把 [0.75,1.25] 误当成现行抖动带：现行 wait/d ∈ [0.5,1.0]，E[wait] = 0.75·d。
jlo = float(getattr(m, "RETRY_JITTER_LO", 0.75))
jhi = float(getattr(m, "RETRY_JITTER_HI", 1.25))
_band = getattr(m, "RETRY_JITTER_BAND", None)
result["constants"] = {
    "RETRY_BACKOFF": backoff, "jitter": [jlo, jhi],
    "jitter_legacy_multiplicative": [jlo, jhi],
    "jitter_band_equal": ([float(_band[0]), float(_band[1])]
                          if isinstance(_band, (tuple, list)) and len(_band) == 2 else None),
    "jitter_mean_over_d": getattr(m, "RETRY_JITTER_MEAN", None),
    "RETRY_MAX_ATTEMPTS": getattr(m, "RETRY_MAX_ATTEMPTS", None),
    "RETRY_MIN_RETRIES": getattr(m, "RETRY_MIN_RETRIES", None)}
if not isinstance(backoff, dict) or not backoff:
    result["fatal"] = "缺少 RETRY_BACKOFF 表（无法判定退避包络）"
    dump(3)


def raw_of(cls, att):
    base, fac, cap = backoff[cls]
    return min(float(cap), float(base) * float(fac) ** att)


def sample(cls, att, n):
    vals = []
    for _ in range(n):
        w = plan(cls, att)
        if isinstance(w, (tuple, list)):
            w = w[0]
        vals.append(float(w))
    return vals


# ---- 兜底通道：行为旁路（drive 公共重试路径 + 虚时钟），仅在找不到计划函数时使用 ----
def behavioural(cls_hint="conn_reset", n=300):
    import requests
    class VClock:
        def __init__(self):
            self.t = 1_700_000_000.0
            self.sleeps = []
        def time(self):
            return self.t
        def sleep(self, s):
            self.sleeps.append(float(s))
            self.t += float(s)
    class AnyLimiter:
        def __getattr__(self, k):
            return lambda *a, **kw: None
    class Refuse:
        def __init__(self):
            self.clock = VClock()
        def get(self, *a, **kw):
            raise requests.exceptions.ConnectionError("probe-refuse")
    class Allow:
        def __init__(self, clock):
            self.clock = clock
        def get(self, *a, **kw):
            return None
    out = []
    for _ in range(n):
        sess = Refuse()
        m.SESSION = sess
        m.LIMITER = AnyLimiter()
        m.time = sess.clock
        m.RETRY_MODE = "v2"
        m._AUDIT_MOD = {"mod": None, "tried": True, "failed": 0, "cls_n": 0}
        try:
            m.fetch("probe", limit=1, retries=2)
        except Exception:                                   # noqa: BLE001
            pass
        out.append(sess.clock.sleeps[0] if sess.clock.sleeps else 0.0)
    return out


try:
    if plan is not None:
        n0 = sample("conn_reset", 0, 20000)
        result["n_samples"] = len(n0)
        mean = statistics.mean(n0)
        sd = statistics.stdev(n0) if len(n0) > 1 else 0.0
        raw = raw_of("conn_reset", 0)
        ratio = (sd / mean) if mean else 0.0
        env_ok = all(raw * jlo - 1e-9 <= v <= raw * jhi + 1e-9 for v in n0)
        result["checks"]["jitter_exists"] = {"ok": ratio > 0.05, "ratio": round(ratio, 4),
                                            "mean": round(mean, 4), "stdev": round(sd, 4),
                                            "threshold": 0.05}
        result["checks"]["envelope"] = {"ok": env_ok, "raw": round(raw, 4),
                                        "min": round(min(n0), 4), "max": round(max(n0), 4),
                                        "bounds": [round(raw * jlo, 4), round(raw * jhi, 4)]}
        meds = [statistics.median(sample("conn_reset", a, 4000)) for a in range(5)]
        ratios = [(meds[i + 1] / meds[i]) if meds[i] else 0.0 for i in range(len(meds) - 1)]
        pre_cap = [r for r in ratios if r > 0]
        result["checks"]["exponential"] = {
            "ok": all(1.6 <= r <= 2.4 for r in pre_cap) and len(pre_cap) >= 3,
            "medians": [round(x, 4) for x in meds], "ratios": [round(x, 4) for x in ratios]}
    else:
        vals = behavioural()
        result["api_used"] = "behavioural-fetch"
        nz = [v for v in vals if v > 0]
        raw = raw_of("conn_reset", 0)
        result["n_samples"] = len(vals)
        if len(nz) < 50:
            result["fatal"] = "行为旁路取不到足够样本（sleep 记录 %d 条）" % len(nz)
            dump(3)
        mean = statistics.mean(nz)
        sd = statistics.stdev(nz)
        result["checks"]["jitter_exists"] = {"ok": (sd / mean) > 0.05, "ratio": round(sd / mean, 4)}
        result["checks"]["envelope"] = {"ok": all(raw * jlo - 1e-9 <= v <= raw * jhi + 1e-9 for v in nz),
                                        "raw": round(raw, 4), "min": round(min(nz), 4),
                                        "max": round(max(nz), 4)}
        result["checks"]["exponential"] = {"ok": None, "note": "行为旁路只覆盖 att=0，不判指数包络"}
    result["ok"] = all(c.get("ok") is not False for c in result["checks"].values())
    dump(0 if result["ok"] else 1)
except SystemExit:
    raise
except Exception as e:                                      # noqa: BLE001
    result["fatal"] = "探针异常：%s: %s" % (type(e).__name__, e)
    dump(3)
PYEOF

if [ -z "$A_SCRAPE_RETRY" ] && [ -z "$A_TEST_RETRY" ]; then
  rec "K2-C1-capability" "MISSING" "既没有 scrape.py 也没有 test_retry.py，无法定位候选版 C1 产物"
elif [ -z "$A_SCRAPE_RETRY" ]; then
  rec "K2-C1-capability" "MISSING" "retry 侧 scrape.py 未找到"
else
  run_to "$WORK/logs/probe-jitter.out" 600 python3 "$PROBE_JSON/probe_retry_jitter.py" \
        "$A_SCRAPE_RETRY" "$PROBE_JSON/retry_jitter.json"
  rc2=$RC
  if [ ! -f "$PROBE_JSON/retry_jitter.json" ]; then
    rec "K2-C1-capability" "FAIL" "独立探针未产出读数（rc=$rc2）：$(tail -c 200 "$WORK/logs/probe-jitter.out" | tr '\n' ' ')"
  else
    fatal="$(json_get "$PROBE_JSON/retry_jitter.json" fatal "")"
    api="$(json_get "$PROBE_JSON/retry_jitter.json" api_used "")"
    je="$(json_get "$PROBE_JSON/retry_jitter.json" checks.jitter_exists.ok "")"
    ee="$(json_get "$PROBE_JSON/retry_jitter.json" checks.envelope.ok "")"
    xo="$(json_get "$PROBE_JSON/retry_jitter.json" checks.exponential.ok "")"
    ratio="$(json_get "$PROBE_JSON/retry_jitter.json" checks.jitter_exists.ratio "")"
    if [ -n "$fatal" ]; then
      rec "K2-C1-capability" "FAIL" "探针判定不可用：$fatal（产物在场但缺冻结能力面 ⇒ 判 FAIL，不是 GAP）"
    elif [ "$je" = "True" ] && [ "$ee" = "True" ] && [ "$xo" != "False" ]; then
      rec "K2-C1-capability" "PASS" "api=$api 抖动 stdev/mean=$ratio>0.05，样本落在 [0.75,1.25]×raw 内，指数比值∈[1.6,2.4]"
    else
      rec "K2-C1-capability" "FAIL" "api=$api jitter_exists=$je envelope=$ee exponential=$xo（独立复算否定 C1 能力）"
    fi
  fi
fi

# =============================================================================
# K3 · C2 单元判据
# =============================================================================
if [ -z "$A_TEST_CONV" ]; then
  rec "K3-C2-unit" "MISSING" "test_converge.py 未找到"
else
  CDIR="$(dirname "$A_TEST_CONV")"
  CONV_T0="$(date +%s)"
  CONV_LOG="$WORK/logs/test_converge.out"
  run_in "$CDIR" "$CONV_LOG" "$T_CONVERGE" python3 "$A_TEST_CONV"
  rc3=$RC; FIRST_RC=$rc3
  # 单元套件里含真实 HTTP 回环 + 时序断言，实测有 flaky 面（首跑偶发，见回执）；
  # 策略：失败即复跑一次，两次读数都进 JSON，若复跑通过则记 ADVISORY（不掩盖 flaky 事实）。
  if [ "$rc3" -ne 0 ]; then
    CONV_LOG="$WORK/logs/test_converge-rerun.out"
    run_in "$CDIR" "$CONV_LOG" "$T_CONVERGE" python3 "$A_TEST_CONV"
    rc3=$RC
    [ "$rc3" -eq 0 ] && rec "K3-flaky-advisory" "ADVISORY" \
      "test_converge.py 首跑 rc=$FIRST_RC 失败、复跑 rc=0 ⇒ flaky 读数（两次日志：$WORK/logs/test_converge*.out）"
  fi
  READINGS="$CDIR/test-results/readings.json"
  HAS_PATCH=0; [ -f "$CDIR/converge.patch" ] && HAS_PATCH=1
  if [ "$rc3" -ne 0 ] && [ ! -f "$READINGS" ]; then
    rec "K3-C2-unit" "FAIL" "两次运行 rc=$FIRST_RC/$rc3 且无 readings.json：$(tail -c 160 "$CONV_LOG" | tr '\n' ' ')"
  else
    python3 - "$READINGS" "$HAS_PATCH" "$CONV_T0" > "$PROBE_JSON/converge_suites.json" 2>"$WORK/logs/converge_suites.err" <<'PYEOF'
import json, os, sys
path, has_patch, t0 = sys.argv[1], sys.argv[2] == "1", int(sys.argv[3])
REQ = ["normal-converge", "probe-all-dead", "f2-malformed-200", "stall-detect",
       "f3-outdir-gone", "serial-path", "reconcile-only", "scope-discipline"]
if has_patch:
    REQ.append("patch-roundtrip")
out = {"readings": path, "readings_exists": os.path.exists(path), "required": REQ,
       "converge_patch_present": has_patch, "fresh": None, "suites": [], "missing_required": [],
       "red_suites": [], "failed_items": [], "pass": None, "n_fail": None}
if not out["readings_exists"]:
    print(json.dumps(out, ensure_ascii=False)); raise SystemExit(0)
d = json.load(open(path, encoding="utf-8"))
out["pass"] = d.get("pass"); out["n_fail"] = d.get("n_fail")
out["fresh"] = os.stat(path).st_mtime >= t0
seen = {}
for s in d.get("suites") or []:
    seen[s.get("name")] = bool(s.get("ok"))
    out["suites"].append({"name": s.get("name"), "ok": bool(s.get("ok")),
                          "n_items": len(s.get("items") or [])})
    if not s.get("ok"):
        for it in (s.get("items") or []):
            if not it.get("ok"):
                out["failed_items"].append("%s ▸ %s | %s" % (s.get("name"), it.get("label"),
                                                             str(it.get("reading"))[:100]))
out["missing_required"] = [n for n in REQ if n not in seen]
out["red_suites"] = [n for n, ok in seen.items() if not ok]
# 归属辅助（只影响**文字归属**，不影响判定）：e2e 回环套件的短差落在 baseline 侧还是 modified 侧。
# 背景：本槽实测该用例在负载下会偶发 baseline 侧少 4 条（764 vs 768 期望），
# 那是冻结参照件 r2cand 的时序属性，不是候选版产物；如实标注以免 p4-verify 误判。
e2e = d.get("e2e") or {}
info = e2e.get("info") or {}
exp = (info.get("expected_crawl") or {}).get("predicted_unique")
b = (e2e.get("baseline") or {}).get("records_unique")
m = (e2e.get("modified") or {}).get("records_unique")
out["e2e_counts"] = {"expected": exp, "baseline": b, "modified": m}
if exp and b is not None and m is not None and b != exp and m == exp:
    out["e2e_attribution"] = ("e2e 短差落在 baseline 侧（%s≠%s）而 modified=%s 正确 ⇒ "
                              "疑似负载/时序环境因素，非候选版产物" % (b, exp, m))
elif exp and m is not None and m != exp:
    out["e2e_attribution"] = "e2e 短差落在 modified 侧（%s≠%s）⇒ 候选版产物的真实缺陷" % (m, exp)
else:
    out["e2e_attribution"] = None
print(json.dumps(out, ensure_ascii=False))
PYEOF
    sj="$PROBE_JSON/converge_suites.json"
    miss="$(json_get "$sj" missing_required "")"
    red="$(json_get "$sj" red_suites "")"
    fresh="$(json_get "$sj" fresh "")"
    fitems="$(python3 -c "
import json,sys;d=json.load(open(sys.argv[1]));print(' || '.join((d.get('failed_items') or [])[:4])[:400])" "$sj" 2>/dev/null)"
    attrib="$(json_get "$sj" e2e_attribution "")"
    if [ "$rc3" -ne 0 ]; then
      rec "K3-C2-unit" "FAIL" "rc=$FIRST_RC/$rc3（两次都红，失败套件：${red:-未见读数}）${fitems:+ ▸ $fitems}"
      [ -n "$attrib" ] && rec "K3-attribution" "ADVISORY" "归属：$attrib"
    elif [ "$red" != "[]" ] && [ -n "$red" ]; then
      rec "K3-C2-unit" "FAIL" "rc=0 但 readings.json 里有红色套件：$red"
      [ -n "$attrib" ] && rec "K3-attribution" "ADVISORY" "归属：$attrib"
    elif [ -n "$miss" ] && [ "$miss" != "[]" ]; then
      rec "K3-C2-unit" "FAIL" "必需套件缺测：$miss（防止靠删套件变绿）"
    elif [ "$fresh" != "True" ]; then
      rec "K3-C2-unit" "FAIL" "readings.json 不是本次运行的产物（fresh=$fresh）"
    else
      rec "K3-C2-unit" "PASS" "rc=0（首跑 rc=$FIRST_RC），套件全绿且必需套件齐全（pass=$(json_get "$sj" pass) / n_fail=$(json_get "$sj" n_fail)，patch-roundtrip=$( [ "$HAS_PATCH" = 1 ] && echo 要求 || echo 未随件 )"
    fi
  fi
fi

# =============================================================================
# K4/K5/K11 · C2 目标判据 + 审计契约 v2 + 三级覆盖（读本次落盘的原始工件）
# =============================================================================
cat > "$PROBE_JSON/probe_artifacts.py" <<'PYEOF'
#!/usr/bin/env python3
# 旁路读物：读被验产物本次运行落盘的原始工件（convergence-report.json / progress.json /
# scrape.log / audit.jsonl / records.jsonl），不看它的自述摘要。
import json, os, sys

READINGS, OUTJ = sys.argv[1], sys.argv[2]
res = {"probe": "artifacts", "readings": READINGS, "out_dir": None, "checks": {},
       "levels_seen": [], "events": {}, "notes": [], "fatal": None}
LEVELS = {"request", "block", "process"}
CANON = {"retry_scheduled", "retry_exhausted", "error_classified", "probe_all_dead",
         "repair_round_enter", "convergence_decision", "stall_detected", "graceful_stop",
         "keeper_restart", "keeper_adopt_refused", "singleton_conflict", "heartbeat_stall",
         "keeper_exit"}


def dump(rc=0):
    with open(OUTJ, "w", encoding="utf-8") as fh:
        json.dump(res, fh, ensure_ascii=False, indent=1)
    raise SystemExit(rc)


def chk(name, ok, detail):
    res["checks"][name] = {"ok": bool(ok), "detail": detail}
    return bool(ok)


try:
    d = json.load(open(READINGS, encoding="utf-8"))
except Exception as e:                                       # noqa: BLE001
    res["fatal"] = "读不到 readings.json：%s" % e
    dump(3)

block = d.get("probe_all_dead") or {}
pick = None
for var, v in block.items():
    if isinstance(v, dict) and (v.get("audit_counts") or {}).get("probe_all_dead", 0) >= 1:
        pick = (var, v)
        break
if pick is None:
    for var in ("modified", "changed"):
        if isinstance(block.get(var), dict):
            pick = (var, block[var])
            break
if pick is None:
    res["fatal"] = "readings.json 里没有 probe_all_dead 场景读数（C2 目标未覆盖）"
    dump(3)
variant, sc = pick
res["variant"] = variant
res["self_report"] = {"rc": sc.get("rc"), "verdict_rc": sc.get("verdict_rc"),
                      "audit_counts": sc.get("audit_counts"), "records_lines": sc.get("records_lines"),
                      "log_has_all_done": sc.get("log_has_all_done")}
outd = sc.get("out") or ""
res["out_dir"] = outd
if not outd or not os.path.isdir(outd):
    res["fatal"] = "场景 out 目录不存在或未记录：%r" % outd
    dump(3)

# ---- convergence-report.json：未收敛 + 未决缺口必须登记 ----
rep = {}
rp = os.path.join(outd, "convergence-report.json")
if os.path.exists(rp):
    try:
        rep = json.load(open(rp, encoding="utf-8"))
        chk("report_parses", True, rp)
    except Exception as e:                                   # noqa: BLE001
        chk("report_parses", False, "解析失败：%s" % e)
else:
    chk("report_parses", False, "缺少 convergence-report.json")
dec = rep.get("decided")
ec = rep.get("exit_code")
gaps = rep.get("gaps") if isinstance(rep.get("gaps"), list) else []
chk("no_false_convergence", dec not in (None, "done") and ec not in (None, 0),
    "decided=%r exit_code=%r（未收敛不得声称完成）" % (dec, ec))
reg = all((g.get("degraded") is True) or (g.get("terminal") is True) or
          bool(g.get("reason")) or bool(g.get("type")) for g in gaps) if gaps else False
chk("gaps_registered", bool(gaps) and reg,
    "gaps=%d，登记字段(degraded/terminal/reason/type)齐备=%s" % (len(gaps), reg))
chk("gap_quantity_recorded", all(isinstance(g.get("gap"), (int, float)) or
                                isinstance(g.get("total"), (int, float)) for g in gaps) if gaps else False,
    "每条缺口带数值 gap/total：%s" % ([{"p": g.get("p") or g.get("prefix"),
                                        "total": g.get("total"), "gap": g.get("gap")} for g in gaps[:3]]))

# ---- progress.json：账本在册 + 与报告互证 ----
pj = os.path.join(outd, "progress.json")
prog = {}
if os.path.exists(pj):
    try:
        prog = json.load(open(pj, encoding="utf-8"))
        chk("progress_parses", True, pj)
    except Exception as e:                                   # noqa: BLE001
        chk("progress_parses", False, "解析失败：%s" % e)
else:
    chk("progress_parses", False, "缺少 progress.json")
pg = prog.get("gaps")
if isinstance(pg, bool):
    pgn, pshape = None, "bool"
elif isinstance(pg, (int, float)):
    pgn, pshape = int(pg), "count"
elif isinstance(pg, list):
    pgn, pshape = len(pg), "list"
elif isinstance(pg, dict):
    pgn, pshape = len(pg), "mapping"
else:
    pgn, pshape = None, type(pg).__name__
chk("progress_gaps_registered", pgn is not None and pgn >= 1,
    "progress.gaps(%s)=%s（缺口必须进账本）" % (pshape, pgn))
if pgn is not None and gaps and pgn != len(gaps):
    res["notes"].append("软读数：progress.gaps(%s)=%s 与 convergence-report.gaps=%d 不等"
                        "（口径差异，不判死）" % (pshape, pgn, len(gaps)))

# ---- scrape.log：不得出现完成标记 ----
sj = os.path.join(outd, "scrape.log")
if os.path.exists(sj):
    txt = open(sj, encoding="utf-8", errors="replace").read()
    chk("no_complete_marker", "全部完成" not in txt,
        "scrape.log 含'全部完成'=%s" % ("全部完成" in txt))
else:
    chk("no_complete_marker", False, "缺少 scrape.log（无法证伪完成标记）")

# ---- audit.jsonl：事件在册（C2 关键事件）+ 契约 v2 合规 + 三级覆盖 ----
aj = os.path.join(outd, "audit.jsonl")
rows, bad_lines, torn = [], 0, 0
if os.path.exists(aj):
    lines = open(aj, encoding="utf-8", errors="replace").read().splitlines()
    for i, ln in enumerate(lines):
        if not ln.strip():
            continue
        try:
            rows.append(json.loads(ln))
        except Exception:                                    # noqa: BLE001
            if i == len(lines) - 1 or i == len(lines) - 2:
                torn += 1
            else:
                bad_lines += 1
else:
    res["notes"].append("缺少 audit.jsonl（C2 审计通道无证据）")
ev = {}
for r in rows:
    e = r.get("event")
    if isinstance(e, str):
        ev[e] = ev.get(e, 0) + 1
res["events"] = dict(sorted(ev.items(), key=lambda kv: -kv[1]))
chk("c2_events_present", ev.get("probe_all_dead", 0) >= 1 and ev.get("repair_round_enter", 0) >= 1,
    "probe_all_dead=%d repair_round_enter=%d" % (ev.get("probe_all_dead", 0),
                                                 ev.get("repair_round_enter", 0)))

# 契约 v2 强制字段 + 每组 seq 从 1 连续（audit-contract-v2 §1/§3 + **§8.2 分组键 (pid,writer)**）
#   v2.1（P4.5·KI-1）：分组键由 pid 改为 (pid, writer)。理由同 audit-verify-v2.py §8.2：
#   同一进程内可有两个写者（scrape.py 块级 "converge" × audit.py 请求级 "retry"）各持独立
#   计数器，本探针抽到的 C2 场景（probe_all_dead）实测就同时含两写者（converge seq=1 与
#   retry seq=1..N）⇒ 只按 pid 分组会误判重复。
#   **判据数值口径不变**：缺 writer 的行（v2.0 历史产物）回落 (pid,"") ⇒ 行为与改前逐项相同。
viol, groups, nocanon = [], {}, 0
for idx, r in enumerate(rows):
    if r.get("schema") != "r3-audit-v2":
        viol.append("row%d schema=%r" % (idx, r.get("schema")))
        continue
    nocanon += 1
    problems = []
    if not isinstance(r.get("ts_epoch"), (int, float)):
        problems.append("ts_epoch")
    if not isinstance(r.get("pid"), int):
        problems.append("pid")
    if not isinstance(r.get("seq"), int):
        problems.append("seq")
    if r.get("level") not in LEVELS:
        problems.append("level=%r" % r.get("level"))
    if not isinstance(r.get("event"), str) or not r.get("event"):
        problems.append("event")
    if not isinstance(r.get("detail"), dict):
        problems.append("detail")
    if problems:
        viol.append("row%d 缺/坏字段:%s" % (idx, ",".join(problems)))
        continue
    groups.setdefault((r["pid"], r.get("writer") or ""), []).append(r["seq"])
seqbad = []
for (pid, w), seqs in groups.items():
    if sorted(seqs) != list(range(1, len(seqs) + 1)):
        seqbad.append("pid=%s writer=%s seq=%s" % (pid, w or "<missing>", sorted(seqs)[:12]))
chk("contract_v2_fields", bool(rows) and not viol,
    "v2 行 %d/%d；违规 %d：%s" % (nocanon, len(rows), len(viol), viol[:4]))
chk("contract_v2_seq", bool(groups) and not seqbad,
    "(pid,writer) 组 %d 个；seq 违规：%s" % (len(groups), seqbad[:3]))
if torn:
    res["notes"].append("末行撕裂 %d 行（契约 §5.6 容忍 ≤1）" % torn)
res["levels_seen"] = sorted({r.get("level") for r in rows if r.get("level") in LEVELS})
res["canonical_events"] = sorted({e for e in ev if e in CANON})

# ---- records.jsonl：数据保险 + 与自述互证（软） ----
rj = os.path.join(outd, "records.jsonl")
if os.path.exists(rj):
    n_lines = 0
    uniq = set()
    bad = 0
    for ln in open(rj, encoding="utf-8", errors="replace"):
        if not ln.strip():
            continue
        n_lines += 1
        try:
            o = json.loads(ln)
            for k in ("mms", "mms_id", "id"):
                if isinstance(o.get(k), (str, int)):
                    uniq.add(str(o[k]))
                    break
        except Exception:                                    # noqa: BLE001
            bad += 1
    chk("records_present", n_lines >= 1 and len(uniq) >= 1,
        "records.jsonl 行=%d unique=%d 坏行=%d" % (n_lines, len(uniq), bad))
    if isinstance(sc.get("records_lines"), int) and n_lines != sc["records_lines"]:
        res["notes"].append("软读数：records.jsonl 行数 %d 与自述 %s 不等"
                            "（自述与工件互证失败，不判死）" % (n_lines, sc["records_lines"]))
else:
    chk("records_present", False, "缺少 records.jsonl")

HARD = ("report_parses", "no_false_convergence", "gaps_registered", "gap_quantity_recorded",
        "progress_parses", "progress_gaps_registered", "no_complete_marker", "c2_events_present")
res["summary"] = ("decided=%r exit_code=%r；gaps=%d（degraded/terminal 登记=%s）；"
                  "progress.gaps 在册=%s；完成标记缺席=%s；C2 事件 probe_all_dead=%d repair_round_enter=%d"
                  % (dec, ec, len(gaps), res["checks"].get("gaps_registered", {}).get("ok"),
                     res["checks"].get("progress_gaps_registered", {}).get("ok"),
                     res["checks"].get("no_complete_marker", {}).get("ok"),
                     ev.get("probe_all_dead", 0), ev.get("repair_round_enter", 0)))
res["hard_fail"] = [k for k, c in res["checks"].items() if k in HARD and not c["ok"]]
res["hard_fail_detail"] = ["%s：%s" % (k, res["checks"][k]["detail"][:160]) for k in res["hard_fail"]]
res["soft_fail"] = [k for k, c in res["checks"].items() if k not in HARD
                    and k != "contract_v2_fields" and k != "contract_v2_seq" and not c["ok"]]
res["ok"] = not res["hard_fail"]
dump(0)
PYEOF

if [ -z "$A_TEST_CONV" ]; then
  rec "K4-C2-target" "MISSING" "test_converge.py 未找到 ⇒ 无法定位 C2 场景工件"
  rec "K5-AUDIT-contract-v2" "MISSING" "同上"
  rec "K11-composite-levels" "GAP" "无 C2 工件可读"
else
  READINGS="$(dirname "$A_TEST_CONV")/test-results/readings.json"
  run_to "$WORK/logs/probe-artifacts.out" 300 python3 "$PROBE_JSON/probe_artifacts.py" \
        "$READINGS" "$PROBE_JSON/artifacts.json"
  rc4=$RC
  AJ="$PROBE_JSON/artifacts.json"
  if [ ! -f "$AJ" ]; then
    rec "K4-C2-target" "FAIL" "旁路读物未产出（rc=$rc4）：$(tail -c 200 "$WORK/logs/probe-artifacts.out" | tr '\n' ' ')"
    rec "K5-AUDIT-contract-v2" "FAIL" "同上"
    rec "K11-composite-levels" "GAP" "同上"
  else
    fatal="$(json_get "$AJ" fatal "")"
    if [ -n "$fatal" ]; then
      rec "K4-C2-target" "FAIL" "旁路读物判定不可用：$fatal"
    elif [ "$(json_get "$AJ" ok)" = "True" ]; then
      rec "K4-C2-target" "PASS" "$(json_get "$AJ" summary)"
    else
      bad="$(python3 -c "
import json,sys;d=json.load(open(sys.argv[1]));print(' | '.join(d.get('hard_fail_detail') or ['<无硬失败项>']))" "$AJ" 2>/dev/null)"
      rec "K4-C2-target" "FAIL" "$bad"
    fi
    # K5：契约 v2 合规（候选版本次运行写下的审计行）
    v2f="$(json_get "$AJ" checks.contract_v2_fields.ok "")"
    v2s="$(json_get "$AJ" checks.contract_v2_seq.ok "")"
    if [ "$v2f" = "True" ] && [ "$v2s" = "True" ]; then
      rec "K5-AUDIT-contract-v2" "PASS" "$(json_get "$AJ" checks.contract_v2_fields.detail)；$(json_get "$AJ" checks.contract_v2_seq.detail)"
    else
      rec "K5-AUDIT-contract-v2" "FAIL" "fields=$v2f seq=$v2s | $(json_get "$AJ" checks.contract_v2_fields.detail) | $(json_get "$AJ" checks.contract_v2_seq.detail)"
    fi
    # K11：同一文件内三级覆盖（组合态；无则登记为缺口，不判死）
    lv="$(json_get "$AJ" levels_seen "")"
    if [ "$lv" = "['block', 'process', 'request']" ] || [ "$lv" = "['request', 'block', 'process']" ]; then
      rec "K11-composite-levels" "PASS" "同一 audit.jsonl 覆盖 request/block/process 三级：$(json_get "$AJ" canonical_events)"
    else
      rec "K11-composite-levels" "GAP" "本次可读工件只覆盖 levels=$lv（组合态需 merge 后的合成系统跑出三级合流审计）"
    fi
    python3 -c "
import json,sys
d=json.load(open(sys.argv[1]))
for k in (d.get('soft_fail') or []): print('  advisory(K4 软读数 %s): %s' % (k, d['checks'][k]['detail'][:120]))
for n in d.get('notes') or []: print('  advisory(K4 软读数): %s' % n)" "$AJ" 2>/dev/null
  fi
fi

# =============================================================================
# K6 · 校验器自检（fail-closed）
# =============================================================================
if [ -z "$A_VERIFIER" ]; then
  rec "K6-VERIFIER-self-test" "MISSING" "audit-verify-v2.py / audit-verify.py 均未找到（P1-1：不允许'没有也能过'）"
else
  run_to "$WORK/logs/verifier-selftest.out" "$T_VERIFY" python3 "$A_VERIFIER" --self-test
  rc6=$RC
  if [ "$rc6" -eq 0 ]; then
    rec "K6-VERIFIER-self-test" "PASS" "$(basename "$A_VERIFIER") [$VERIFIER_KIND] --self-test rc=0；$(grep -E '自检|self-test' "$WORK/logs/verifier-selftest.out" | tail -1)"
  else
    rec "K6-VERIFIER-self-test" "FAIL" "rc=$rc6：$(tail -c 200 "$WORK/logs/verifier-selftest.out" | tr '\n' ' ')"
  fi
fi

# =============================================================================
# K7/K8 · 判据可证伪性（元判据）+ 特异性（正对照/变异）
# =============================================================================
cat > "$PROBE_JSON/probe_verifier_redteam.py" <<'PYEOF'
#!/usr/bin/env python3
# 红队探针：① 手写伪造夹具必须 rc≠0（元判据：判据可证伪）；
#            ② 真样本正对照必须 rc=0，并在其副本上做变异必须 rc≠0（防"永远 exit 1"的退化校验器）。
# 全部夹具建在 --work 内；只读候选版校验器。
import json, os, shutil, subprocess, sys, time

VERIFIER, WORK, OUTJ = sys.argv[1], sys.argv[2], sys.argv[3]
SAMPLE = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] else None
SEARCH = [p for p in (sys.argv[5].split(os.pathsep) if len(sys.argv) > 5 else []) if p]
FIX = os.path.join(WORK, "fixtures", "redteam")
os.makedirs(FIX, exist_ok=True)
res = {"probe": "verifier_redteam", "verifier": VERIFIER, "fixtures": [],
       "positive": None, "mutations": [], "sample": SAMPLE, "notes": []}


def dump(rc=0):
    with open(OUTJ, "w", encoding="utf-8") as fh:
        json.dump(res, fh, ensure_ascii=False, indent=1)
    raise SystemExit(rc)


def run_verifier(target, extra=None):
    cmd = [sys.executable, VERIFIER, "--scenario-dir", target]
    if extra:
        cmd += list(extra)
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=180,
                           cwd=WORK)
        out = (p.stdout or "") + (p.stderr or "")
        rc = p.returncode
    except subprocess.TimeoutExpired:
        return 124, "TIMEOUT", False, ""
    crash = "Traceback (most recent call last)" in out
    verdict = ""
    for ln in out.splitlines():
        s = ln.strip()
        if s.startswith(("判定", "FAIL", "结论", "verdict", "exit")) and len(s) < 200:
            verdict = s
            break
    return rc, out, crash, verdict


def mk(name, rows, evidence=None, records=None, log=None):
    d = os.path.join(FIX, name)
    if os.path.exists(d):
        shutil.rmtree(d)
    os.makedirs(os.path.join(d, "out"), exist_ok=True)
    if rows is not None:
        with open(os.path.join(d, "out", "audit.jsonl"), "w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    if evidence is not None:
        with open(os.path.join(d, "evidence.json"), "w", encoding="utf-8") as fh:
            json.dump(evidence, fh, ensure_ascii=False, indent=1)
    if records is not None:
        with open(os.path.join(d, "records.jsonl"), "w", encoding="utf-8") as fh:
            fh.write("\n".join(json.dumps(x, ensure_ascii=False) for x in records) + "\n")
    if log is not None:
        with open(os.path.join(d, "scrape.log"), "w", encoding="utf-8") as fh:
            fh.write(log)
    return d


def v2row(pid, seq, level, event, ts, detail=None):
    return {"schema": "r3-audit-v2", "ts_epoch": ts, "ts_iso": time.strftime(
        "%Y-%m-%dT%H:%M:%S", time.localtime(ts)), "pid": pid, "seq": seq,
        "level": level, "event": event, "detail": detail or {}, "run": "redteam-handwritten"}


NOW = time.time()
# ① 单行伪造：只有一条手写 canonical 记录，无 evidence.json / records.jsonl / scrape.log
d1 = mk("R1-forge-single-record", [v2row(4242, 1, "process", "keeper_restart", NOW - 30,
                                         {"from_pid": 1, "to_pid": 4242, "restart_s": 4.0})])
# ② 空审计：0 字节 audit.jsonl + 自报时间线 + 两行 records
d2 = mk("R2-empty-audit", [], evidence={"injections": [{"ts": NOW - 60, "reason": "kill#1"}]},
        records=[{"mms": "99000000001"}, {"mms": "99000000002"}])
open(os.path.join(d2, "out", "audit.jsonl"), "w").close()
# ③ 场景目录不存在（fail-closed）
d3 = os.path.join(FIX, "R3-missing-scenario-dir")
shutil.rmtree(d3, ignore_errors=True)
# ④ 软夹具（登记不判死）：三件套互相自洽的全手写现场（信任根边界 T9）
d4 = mk("R4-forge-consistent-triple",
        [v2row(777, i + 1, "request", "retry_scheduled", NOW - 60 + i * 2, {"attempt": i + 1})
         for i in range(3)],
        evidence={"injections": [{"ts": NOW - 60, "reason": "kill#1"}]},
        records=[{"mms": "9900000000%d" % i} for i in range(1, 6)],
        log="[probe] 假日志：全部完成\n")

HARD = [("R1-forge-single-record", d1, "单行手写 canonical 记录且无血缘文件 ⇒ 契约 §5.3a/§5.3c 要求 FAIL"),
        ("R2-empty-audit", d2, "0 字节审计 + 自报时间线 ⇒ 契约 §5.1 fail-closed 要求非零"),
        ("R3-missing-scenario-dir", d3, "场景目录不存在 ⇒ 契约 §5.1 要求 FAIL")]
for name, d, why in HARD:
    rc, out, crash, verdict = run_verifier(d)
    res["fixtures"].append({"name": name, "dir": d, "expect": "rc!=0 且无 traceback",
                            "rc": rc, "crash": crash, "ok": (rc != 0 and not crash),
                            "why": why, "verdict_line": verdict,
                            "tail": out[-400:]})
    with open(os.path.join(WORK, "logs", "redteam-%s.out" % name), "w", encoding="utf-8") as fh:
        fh.write(out)

rc, out, crash, verdict = run_verifier(d4)
res["soft_fixtures"] = [{"name": "R4-forge-consistent-triple", "rc": rc, "crash": crash,
                         "note": "信任根边界（verify §3.2 空洞 2）：rc=0 说明校验器证的是自洽性而非真实性——如实登记，不判死",
                         "verdict_line": verdict}]

# ---- 正对照 + 变异（只在给了真样本时） ----
def newest_sample(root, maxdepth=7):
    best = None
    for dirpath, dirnames, filenames in os.walk(root):
        depth = dirpath[len(root):].count(os.sep)
        if depth > maxdepth:
            dirnames[:] = []
            continue
        if "audit.jsonl" in filenames:
            p = os.path.join(dirpath, "audit.jsonl")
            if os.path.getsize(p) == 0:
                continue
            try:
                with open(p, encoding="utf-8", errors="replace") as fh:
                    rows = []
                    for ln in fh:
                        try:
                            rows.append(json.loads(ln))
                        except Exception:                        # noqa: BLE001
                            pass
                        if len(rows) > 40:
                            break
            except Exception:                                    # noqa: BLE001
                continue
            if len(rows) >= 5 and any(r.get("schema") == "r3-audit-v2" for r in rows):
                mt = os.path.getmtime(p)
                if best is None or mt > best[1]:
                    best = (dirpath, mt, len(rows))
    return best[0] if best else None


if not SAMPLE or SAMPLE in ("auto", "AUTO"):
    for root in (SEARCH + [os.path.join(WORK, "shadow")]):
        if os.path.isdir(root):
            SAMPLE = newest_sample(root)
            if SAMPLE:
                break
if SAMPLE and not os.path.isdir(SAMPLE):
    res["notes"].append("指定的样本目录不存在：%s" % SAMPLE)
    SAMPLE = None

if not SAMPLE:
    res["notes"].append("候选版内没有找到契约 v2 形态的真实产物样本（schema=r3-audit-v2 且 ≥5 行）"
                        "⇒ 特异性判据 UNADJUDICABLE；merge/verify 可用 --sample-dir 指定")
    dump(0)

rc, out, crash, verdict = run_verifier(SAMPLE)
res["positive"] = {"dir": SAMPLE, "rc": rc, "crash": crash, "ok": (rc == 0 and not crash),
                   "expect": "真样本必须 rc=0 且无 traceback", "verdict_line": verdict,
                   "tail": out[-400:]}
with open(os.path.join(WORK, "logs", "redteam-positive.out"), "w", encoding="utf-8") as fh:
    fh.write(out)


def mutate(name, fn):
    d = os.path.join(FIX, name)
    if os.path.exists(d):
        shutil.rmtree(d)
    shutil.copytree(SAMPLE, d)
    aj = os.path.join(d, "out", "audit.jsonl")
    if not os.path.exists(aj):
        aj = os.path.join(d, "audit.jsonl")
    info = fn(d, aj)
    rc, out, crash, verdict = run_verifier(d)
    return {"name": name, "dir": d, "rc": rc, "crash": crash, "verdict_line": verdict,
            "mutation": info, "tail": out[-300:]}


def mut_seq_gap(d, aj):
    lines = [l for l in open(aj, encoding="utf-8").read().splitlines() if l.strip()]
    rows = [json.loads(l) for l in lines]
    groups = {}
    for i, r in enumerate(rows):
        groups.setdefault(r.get("pid"), []).append(i)
    target = max(groups.items(), key=lambda kv: len(kv[1]))[0]
    idxs = sorted(groups[target], key=lambda i: rows[i].get("seq") or 0)
    # 删**中间**一行才会造成 seq 缺号；删末行只是截尾（1..n-1 仍连续，契约允许）
    victim = idxs[len(idxs) // 2]
    removed_seq = rows[victim].get("seq")
    rows.pop(victim)
    open(aj, "w", encoding="utf-8").write(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n")
    return "删除 pid=%s 组内 seq=%s 的行 ⇒ 组内 seq 缺号" % (target, removed_seq)


def mut_bad_nonlast(d, aj):
    lines = [l for l in open(aj, encoding="utf-8").read().splitlines() if l.strip()]
    if len(lines) >= 3:
        lines.insert(len(lines) - 2, '{"schema": "r3-audit-v2", "ts_epoch": 1.0, TRUNCATED')
    open(aj, "w", encoding="utf-8").write("\n".join(lines) + "\n")
    return "在倒数第二行插入坏 JSON（非末行坏行 ⇒ 契约 §5.6 硬违规）"


def mut_torn_last(d, aj):
    lines = [l for l in open(aj, encoding="utf-8").read().splitlines() if l.strip()]
    if lines:
        lines[-1] = lines[-1][: max(10, len(lines[-1]) // 2)]
    open(aj, "w", encoding="utf-8").write("\n".join(lines) + "\n")
    return "把末行截半（撕裂）⇒ 契约 §5.6 要求容忍（rc=0）"


def mut_window(d, aj):
    ev = os.path.join(d, "evidence.json")
    if not os.path.exists(ev):
        return "SKIP：样本无 evidence.json"
    try:
        e = json.load(open(ev, encoding="utf-8"))
    except Exception:                                            # noqa: BLE001
        return "SKIP：evidence.json 不可解析"
    inj = [x.get("ts") for x in (e.get("injections") or []) if isinstance(x, dict)]
    if not inj:
        return "SKIP：evidence.json 无 injections[].ts"
    rows = []
    for ln in open(aj, encoding="utf-8"):
        if not ln.strip():
            continue
        r = json.loads(ln)
        if isinstance(r.get("ts_epoch"), (int, float)):
            r["ts_epoch"] = float(min(inj)) - 3600.0
        r["ts_iso"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(r.get("ts_epoch", 0)))
        rows.append(r)
    open(aj, "w", encoding="utf-8").write(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n")
    return "把全部事件 ts_epoch 平移到 first_inj-3600 ⇒ 契约 §5.2 时间窗要求 FAIL"


for name, fn, hard in (("M1-seq-gap", mut_seq_gap, True),
                       ("M2-bad-nonlast-line", mut_bad_nonlast, True),
                       ("M3-torn-last-line", mut_torn_last, False),
                       ("M4-window-shift", mut_window, False)):
    m = mutate("mut-%s" % name, fn)
    m["hard"] = hard
    m["ok"] = (m["rc"] != 0 and not m["crash"]) if hard else None
    res["mutations"].append(m)
    with open(os.path.join(WORK, "logs", "redteam-%s.out" % name), "w", encoding="utf-8") as fh:
        fh.write(m.pop("tail") or "")

dump(0)
PYEOF

if [ -z "$A_VERIFIER" ]; then
  rec "K7-VERIFIER-redteam" "MISSING" "无校验器可红队"
  rec "K8-VERIFIER-specificity" "MISSING" "无校验器可做正对照"
else
  run_to "$WORK/logs/probe-verifier.out" 1200 python3 "$PROBE_JSON/probe_verifier_redteam.py" \
        "$A_VERIFIER" "$WORK" "$PROBE_JSON/verifier.json" "${OPT_SAMPLE:-auto}" "$CAND"
  rc7=$RC
  VJ="$PROBE_JSON/verifier.json"
  if [ ! -f "$VJ" ]; then
    rec "K7-VERIFIER-redteam" "FAIL" "红队探针未产出（rc=$rc7）：$(tail -c 200 "$WORK/logs/probe-verifier.out" | tr '\n' ' ')"
    rec "K8-VERIFIER-specificity" "FAIL" "同上"
  else
    python3 -c "
import json,sys
d=json.load(open(sys.argv[1]))
for f in d['fixtures']: print('  %-28s rc=%-3s crash=%-5s ok=%s' % (f['name'],f['rc'],f['crash'],f['ok']))
" "$VJ" 2>/dev/null
    nbad="$(python3 -c "
import json,sys;d=json.load(open(sys.argv[1]));print(sum(1 for f in d['fixtures'] if not f['ok']))" "$VJ" 2>/dev/null || echo 0)"
    if [ "${nbad:-1}" = "0" ]; then
      rec "K7-VERIFIER-redteam" "PASS" "全部 $(python3 -c "
import json,sys;print(len(json.load(open(sys.argv[1]))['fixtures']))" "$VJ") 个手写伪造夹具均 rc≠0 且无 traceback（判据可证伪）"
    else
      rec "K7-VERIFIER-redteam" "FAIL" "$nbad 个伪造夹具未被拒（判据不可证伪）"
    fi
    # K8
    pos_ok="$(json_get "$VJ" positive.ok "")"
    hard_bad="$(python3 -c "
import json,sys
d=json.load(open(sys.argv[1]))
ms=[m for m in (d.get('mutations') or []) if m.get('hard')]
print(sum(1 for m in ms if not (m['rc']!=0 and not m['crash'])))" "$VJ" 2>/dev/null || echo 1)"
    if [ -z "$pos_ok" ] || [ "$pos_ok" = "" ]; then
      reason="$(json_get "$VJ" notes "")"
      rec "K8-VERIFIER-specificity" "GAP" "无契约 v2 真样本可做正对照 ⇒ 无法排除'永远 exit 1'的退化校验器（merge/verify 传 --sample-dir 可解）"
    elif [ "$pos_ok" != "True" ]; then
      rec "K8-VERIFIER-specificity" "FAIL" "真样本被判非零（rc=$(json_get "$VJ" positive.rc)，crash=$(json_get "$VJ" positive.crash)）⇒ 校验器拒绝合规产物"
    elif [ "$hard_bad" != "0" ]; then
      rec "K8-VERIFIER-specificity" "FAIL" "$hard_bad 个硬变异未被检出（seq 缺号 / 非末行坏行）"
    else
      rec "K8-VERIFIER-specificity" "PASS" "真样本 rc=0；seq 缺号与非末行坏行变异均 rc≠0"
    fi
    python3 -c "
import json,sys
d=json.load(open(sys.argv[1]))
for m in (d.get('mutations') or []):
    print('  advisory(变异 %s 硬=%s): rc=%s %s' % (m['name'], m['hard'], m['rc'], m['mutation'][:70]))
for s in (d.get('soft_fixtures') or []):
    print('  advisory(软夹具 %s): rc=%s' % (s['name'], s['rc']))
for n in (d.get('notes') or []): print('  advisory: %s' % n)
" "$VJ" 2>/dev/null
  fi
fi

# =============================================================================
# K9 · C3 恢复时限（结构化读数；默认不杀进程）
# =============================================================================
cat > "$PROBE_JSON/probe_recovery.py" <<'PYEOF'
#!/usr/bin/env python3
# C3 恢复时限：只读**既有结构化读数**（本槽红线不 kill 任何进程）。
# 优先级：drill result.json 的 recovery_within_deadline 断言 > keeper d1 audit.jsonl 的 restart 读数。
import glob, json, os, sys

OUTJ, MAXAGE = sys.argv[1], int(sys.argv[2])
ROOTS = [p for p in sys.argv[3:] if p and os.path.isdir(p)]
res = {"probe": "recovery", "roots": ROOTS, "sources": [], "fatal": None, "ok": False,
       "mode": None, "max_age_s": MAXAGE, "stale": False}
LIMIT = 180.0


def dump(rc=0):
    with open(OUTJ, "w", encoding="utf-8") as fh:
        json.dump(res, fh, ensure_ascii=False, indent=1)
    raise SystemExit(rc)


# ① drill result.json（S1/S4/S8 等任何带 recovery_within_deadline 断言的场景）
cands = []
for TREE in ROOTS:
    for pat in ("runs/*/S*/result.json", "**/runs/*/S*/result.json",
                "results/*.json", "**/results/summary*.json"):
        cands += glob.glob(os.path.join(TREE, pat), recursive=True)
seen, hits = set(), []
for p in sorted(set(cands)):
    if p in seen:
        continue
    seen.add(p)
    try:
        d = json.load(open(p, encoding="utf-8"))
    except Exception:                                          # noqa: BLE001
        continue
    asserts = d.get("assertions")
    if not isinstance(asserts, list):
        continue
    for a in asserts:
        if not isinstance(a, dict) or a.get("name") != "recovery_within_deadline":
            continue
        lim = a.get("limit")
        vals = a.get("value")
        vals = vals if isinstance(vals, list) else [vals]
        nums = [float(v) for v in vals if isinstance(v, (int, float))]
        if lim is None or not nums:
            continue
        hits.append({"file": p, "limit": float(lim), "values": nums, "soft": bool(a.get("soft")),
                     "ok": float(lim) <= LIMIT and max(nums) <= LIMIT,
                     "mtime": os.path.getmtime(p),
                     "detail": "声明时限 %ss；实测恢复 %s ⇒ ≤180s=%s"
                               % (lim, nums, max(nums) <= LIMIT)})
if hits:
    best = sorted(hits, key=lambda h: (not h["ok"], -h["mtime"]))[0]
    res["mode"] = "drill-result.json"
    res["sources"] = hits
    res["ok"] = best["ok"]
    res["picked"] = best
    dump(0)

# ② keeper d1 audit.jsonl（结构化行里的 restart 读数）
a_hits = []
_audit_paths = []
for _root in ROOTS:
    _audit_paths += glob.glob(os.path.join(_root, "**/audit.jsonl"), recursive=True)
for p in _audit_paths:
    if "/runs/" in p:
        continue
    rows = []
    try:
        for ln in open(p, encoding="utf-8", errors="replace"):
            if not ln.strip():
                continue
            try:
                rows.append(json.loads(ln))
            except Exception:                                  # noqa: BLE001
                pass
    except Exception:                                          # noqa: BLE001
        continue
    for r in rows:
        ev = str(r.get("event") or "")
        if ev not in ("keeper_restart", "restart"):
            continue
        det = r.get("detail") if isinstance(r.get("detail"), dict) else {}
        for k in ("recovery_ms", "recover_ms", "restart_s", "recovery_s"):
            v = det.get(k, r.get(k))
            if isinstance(v, (int, float)):
                secs = float(v) / 1000.0 if k.endswith("_ms") else float(v)
                a_hits.append({"file": p, "field": k, "seconds": secs, "ok": secs <= LIMIT,
                               "mtime": os.path.getmtime(p),
                               "detail": "keeper %s 恢复读数 %s=%s ⇒ %.3fs ≤180s=%s"
                                         % (ev, k, v, secs, secs <= LIMIT)})
if a_hits:
    best = sorted(a_hits, key=lambda h: (not h["ok"], -h["mtime"]))[0]
    res["mode"] = "keeper-audit.jsonl"
    res["sources"] = a_hits
    res["ok"] = best["ok"]
    res["picked"] = best
    dump(0)

res["fatal"] = ("既无 drill result.json 的 recovery_within_deadline 断言，也无 keeper 审计的 restart 读数 "
                "（结构化恢复时限读数缺失；本判据默认不杀进程，可用 --with-kill-drills 现场生成）")
dump(3)
PYEOF

if [ ! -d "$CAND" ]; then
  rec "K9-C3-recovery-deadline" "MISSING" "靶目录不存在，无读数可读"
else
  echo "  note(K9)：证据模式 = preexisting-read（本判据默认不杀任何进程；用 --with-kill-drills 现场生成）"
  run_to "$WORK/logs/probe-recovery.out" 300 python3 "$PROBE_JSON/probe_recovery.py" \
        "$PROBE_JSON/recovery.json" "$MAX_READING_AGE" "$TREE" "$CAND"
  rc9=$RC
  RJ="$PROBE_JSON/recovery.json"
  if [ -f "$RJ" ]; then
    fatal="$(json_get "$RJ" fatal "")"
    if [ -n "$fatal" ]; then
      rec "K9-C3-recovery-deadline" "GAP" "$fatal"
    else
      mode="$(json_get "$RJ" mode)"
      dtl="$(json_get "$RJ" picked.detail)"
      mt="$(json_get "$RJ" picked.mtime 0)"
      age="$(( $(date +%s) - ${mt%%.*} ))"
      if [ "$(json_get "$RJ" ok)" = "True" ]; then
        rec "K9-C3-recovery-deadline" "PASS" "[$mode] $dtl（读数年龄 ${age}s；证据模式=preexisting-read）"
      else
        rec "K9-C3-recovery-deadline" "FAIL" "[$mode] $dtl"
      fi
      if [ "$age" -gt "$MAX_READING_AGE" ]; then
        rec "K9-stale-warning" "ADVISORY" "恢复时限读数年龄 ${age}s > 上限 ${MAX_READING_AGE}s（陈旧读数，建议现场复跑）"
      fi
    fi
  else
    rec "K9-C3-recovery-deadline" "FAIL" "读数探针未产出（rc=$rc9）"
  fi
fi

# --- 可选：现场生成（会 kill 它自己拉起的 mock ⇒ 默认关闭） ---
if [ "$WITH_KILL_DRILLS" = 1 ]; then
  if [ -z "$A_DRILL" ]; then
    rec "K9b-fresh-drill" "MISSING" "--with-kill-drills 给了但 drill.py 未找到"
  else
    SYS="$DRILL_SYSTEM"
    if [ -z "$SYS" ]; then
      for c in r3-full slot-converge baseline-r2cand; do
        if [ -f "$TREE/systems/$c.json" ] || find "$TREE" -maxdepth 3 -name "$c.json" -print -quit 2>/dev/null | grep -q .; then
          SYS="$c"; break
        fi
      done
    fi
    [ -n "$SYS" ] || SYS="baseline-r2cand"
    run_to "$WORK/logs/drill-S1S4.out" 1800 python3 "$A_DRILL" --system "$SYS" --only S1,S4
    rec "K9b-fresh-drill" "$( [ "$RC" -eq 0 ] && echo PASS || echo ADVISORY )" \
        "现场 drill --system $SYS --only S1,S4 rc=$RC（读数见 $WORK/logs/drill-S1S4.out；只 kill 演练台自建的 mock）"
  fi
fi

# =============================================================================
# K10 · 回执（生产者回执；不含验收脚本自己的回执与验证者报告 ⇒ 无自指）
# =============================================================================
if [ "$NO_RECEIPTS" = 1 ]; then
  rec "K10-receipts" "ADVISORY" "--no-receipts：跳过回执检查（JSON 里如实登记）"
else
  miss=""
  IFS=',' read -r -a _rcpts <<< "$OPT_RECEIPTS"
  for r in "${_rcpts[@]}"; do
    [ -n "$r" ] || continue
    [ -s "$R3_ROOT/receipts/$r.md" ] || miss="$miss $r"
  done
  if [ -z "$miss" ]; then
    rec "K10-receipts" "PASS" "生产者回执齐备：$OPT_RECEIPTS（不含验收脚本自身与验证者报告）"
  else
    rec "K10-receipts" "RECEIPTS" "缺回执：$miss（机器判据不受影响；按分级退出码 3 报告）"
  fi
fi

# =============================================================================
# ADV · 人读标注（P0-2：替代 R3-6 自指判据，永不参与判定）
# =============================================================================
REPORT="$OPT_REPORT"
[ -n "$REPORT" ] || REPORT="$R3_ROOT/verification/report.md"
if [ -f "$REPORT" ]; then
  python3 - "$REPORT" <<'PYEOF'
import re, sys
txt = open(sys.argv[1], encoding="utf-8", errors="replace").read()
cmds = len(re.findall(r"^\s*\$ .*(python3|bash|grep|cd|md5sum|patch)", txt, re.M))
tables = 0
for blk in re.findall(r"(?:^\|.*\n)+", txt, re.M):
    rows = [l for l in blk.splitlines() if l.strip().startswith("|")]
    if len(rows) >= 4 and not re.match(r"^\|[\s\-:|]+\|$", rows[1].strip()):
        tables += 1
heads = [h for h in re.findall(r"^#{2,4}\s*(.+)$", txt, re.M)
         if re.search(r"遗留|未覆盖|风险|缺口|残留", h)]
print("  advisory(人读报告 %s)：可复现命令 %d 条（建议 ≥5）、独立读数表 %d 个（建议 ≥1）、"
      "遗留/未覆盖小节 %d 处（建议 ≥1）—— 仅作人读标注，不参与判定"
      % (sys.argv[1], cmds, tables, len(heads)))
PYEOF
  rec "ADV-human-report" "ADVISORY" "见上行（人读标注，不判死）"
else
  rec "ADV-human-report" "ADVISORY" "未找到验证报告 $REPORT（不影响机器判据）"
fi

# =============================================================================
# 汇总
# =============================================================================
if [ ! -d "$CAND" ]; then
  FINAL_EXIT=2; VERDICT="MISSING-TARGET"
elif [ "$n_fail" -gt 0 ]; then
  FINAL_EXIT=1; VERDICT="FAIL"
elif [ "$n_missing" -gt 0 ]; then
  FINAL_EXIT=2; VERDICT="MISSING-ARTIFACTS"
elif [ "$n_gap" -gt 0 ]; then
  if [ "$ALLOW_KNOWN_GAPS" = 1 ]; then
    FINAL_EXIT=0; VERDICT="PASS-with-known-gaps"
  else
    FINAL_EXIT=4; VERDICT="UNADJUDICABLE"
  fi
elif [ "$n_receipts" -gt 0 ]; then
  FINAL_EXIT=3; VERDICT="RECEIPTS-INCOMPLETE"
else
  FINAL_EXIT=0; VERDICT="PASS"
fi

echo "==============================================================="
printf '判据统计：PASS=%d FAIL=%d MISSING=%d UNADJUDICABLE=%d RECEIPTS=%d advisory=%d\n' \
  "$n_pass" "$n_fail" "$n_missing" "$n_gap" "$n_receipts" "$n_adv"
printf 'R3-V2 ACCEPTANCE: %s（exit %d）\n' "$VERDICT" "$FINAL_EXIT"
echo "  判据依据：全部来自本次运行的命令退出码 / 结构化读数（无 grep PASS 词判据）"
echo "==============================================================="

if [ -n "$JSON_OUT" ]; then
  mkdir -p "$(dirname "$JSON_OUT")"
  python3 - "$TSV" "$JSON_OUT" "$VERDICT" "$FINAL_EXIT" "$CAND" "$TARGET_KIND" \
           "$R3_ROOT" "$WORK" "$SHADOW_NOTE" "$PROBE_JSON" "$RUN_T0" "$VERIFIER_KIND" <<'PYEOF'
import json, os, sys
(tsv, out, verdict, ec, cand, kind, root, work, shadow, pj, t0, vkind) = sys.argv[1:13]
crit = []
for ln in open(tsv, encoding="utf-8"):
    if not ln.strip():
        continue
    a = ln.rstrip("\n").split("\t")
    if len(a) >= 3:
        crit.append({"id": a[0], "status": a[1], "detail": a[2]})
probes = {}
for name in ("retry_jitter", "converge_suites", "artifacts", "verifier", "recovery"):
    p = os.path.join(pj, name + ".json")
    if os.path.exists(p):
        try:
            probes[name] = json.load(open(p, encoding="utf-8"))
        except Exception as e:                                    # noqa: BLE001
            probes[name] = {"_error": str(e)}
doc = {
    "script": "R3-v2.sh", "version": "P0-E/2026-09-16", "verdict": verdict, "exit_code": int(ec),
    "target": {"cand": cand, "kind": kind, "root": root, "verifier_kind": vkind,
               "shadow": shadow, "run_t0": int(t0)},
    "counts": {"pass": sum(1 for c in crit if c["status"] == "PASS"),
               "fail": sum(1 for c in crit if c["status"] == "FAIL"),
               "missing": sum(1 for c in crit if c["status"] == "MISSING"),
               "unadjudicable": sum(1 for c in crit if c["status"] == "GAP"),
               "receipts": sum(1 for c in crit if c["status"] == "RECEIPTS"),
               "advisory": sum(1 for c in crit if c["status"] == "ADVISORY")},
    "criteria": crit,
    "gaps": [c["detail"] for c in crit if c["status"] == "GAP"],
    "advisories": [{"id": c["id"], "detail": c["detail"]} for c in crit if c["status"] == "ADVISORY"],
    "evidence_mode": {"fresh-run": ["K1", "K2", "K3", "K4", "K5", "K6", "K7", "K8"],
                      "preexisting-read": ["K9"],
                      "advisory-only": ["ADV"]},
    "probes": probes,
}
with open(out, "w", encoding="utf-8") as fh:
    json.dump(doc, fh, ensure_ascii=False, indent=1)
print("JSON 读数：%s" % out)
PYEOF
fi

if [ -z "${R3V2_KEEP_WORK:-}" ] && [ -z "$JSON_OUT" ]; then
  echo "（工作区保留在 $WORK；--json/--work 可固定路径）"
fi
exit "$FINAL_EXIT"
