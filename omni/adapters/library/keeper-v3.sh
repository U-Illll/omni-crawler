#!/usr/bin/env bash
# =============================================================================
# keeper-v3.sh — 进程级看护 v3   (R3 自愈轮 / C3 进程级自愈 + R3 审计 schema)
# 血缘：v2（R1 长程自动化，slot-C，A2 + C3）→ v3（R3，本条以下 [A][B][C] 为 v2 原有语义）
#
# 相对 v1 (refs/keeper.sh) 的三处加固，逐条对应 2026-09-15 事故教训：
#
#  [A] 进程检测 = PID 文件 + /proc/<pid>/cmdline 精确核对
#      判定链：PID 文件记录 pid + starttime(+) → /proc/<pid> 存在 →
#      argv token **相等**（不是子串！）→ /proc/<pid>/cwd 与作业 cwd 一致 →
#      /proc/<pid>/exe 解释器匹配 → starttime 未变（防 PID 复用）。
#      彻底消除 `pgrep -f "scrape\.py"` 的子串/自匹配误判（事故教训 a）。
#      歧义（同时匹配到多个候选）时 **绝不 spawn**，只审计 —— 宁可晚恢复，
#      不可重复起抓取进程（A2 要求重复抓取 ≤1 个叶子块）。
#
#  [B] 启动即记录运行环境指纹（事故教训 b：keeper 曾运行在异常挂载视图而孤儿化）
#      /tmp 挂载视图关键行、mount namespace inode、cwd、作业路径可见性探针，
#      写入启动审计（人类可读块 + JSONL 事件）。挂载指纹相对上一次启动发生变化
#      → env_changed 事件；作业路径不可见 → env_probe_fail 事件；
#      KEEPER_STRICT_ENV=1 时直接非零退出，绝不静默空转。
#
#  [C] 结构化审计：每次自愈事件写一行 JSONL
#      ts / event / from_pid / to_pid / evidence（+ run_id / env / ns_mnt / job）
#      覆盖：keeper_start / env_probe / heartbeat / process_dead / restart /
#      restart_failed / restart_backoff / restart_flapping / guard_suppressed /
#      pidfile_stale / adopt / detector_ambiguous / deadline_breached / keeper_stop
#
# 守卫语义（与 v1 一致，刻意保持）：完成标记存在 ⇒ 永不重启。
#   仅当「完成标记缺失 **且** 进程经上述核对确认不在」才重启。
#
# 恢复时限（A2 ≤180s）：worst case = KEEPER_TICK + spawn + verify
#   （默认 15 + ~2s ≈ 17s）；崩溃循环退避上限 KEEPER_MAX_BACKOFF 亦被夹到
#   DEADLINE 以下（启动时自检，越界则夹紧并告警）。
#
# 用法：
#   keeper-v3.sh start         守护循环（前台；生产用 daemon 或 setsid nohup）
#                              启动即抢 flock 单例锁；已有在位 keeper ⇒ 退出码 4
#   keeper-v3.sh daemon        自后台化运行 start（单例锁由 start 进程持有）
#   keeper-v3.sh once          只跑一轮巡检（测试/演练用，可配合 KEEPER_DRY_RUN=1）
#                              带 advisory 单例探测：冲突只审计不退出
#   keeper-v3.sh status        打印各作业状态（只读；含 TRUST / HB_CLASS 列）
#   keeper-v3.sh stop          请求优雅停止（写停止标志，不发送任何信号）
#   keeper-v3.sh resume [JOB]  解除机器判据造成的终态/停机（默认 all；清预算 + 审计）
#   keeper-v3.sh fingerprint   打印环境指纹 JSON（只读，不落审计）
#   keeper-v3.sh hbclass       打印一次心跳分类结果（只读，不落审计）
#   keeper-v3.sh lock-probe    探测单例锁持有者（只读，不落审计）
#   keeper-v3.sh selftest      检测器/认领策略/rail/单例/心跳分类 回归自测（自清理）
#
# 配置：默认 source $KEEPER_DIR/jobs.conf（可用 KEEPER_CONF 指定；/dev/null = 零作业）
# 依赖：bash 4+ / coreutils / flock / python3（心跳分类用）；**不依赖 pgrep / jq**
# =============================================================================

# =============================================================================
# keeper-v3.1 —— R3p4/P4-D 修订（F6 完成判据机器化 + c1 冒充防护 + 锁族加固）。
#   相对 v3.0 的增补（对应 crit-correct H5/K-3、crit-adversarial c1、K-1/K-2/K-5）：
#
#  [J] 完成/失败判据机器化（F6 闭环；audit-selfheal §4.3.2「只认结构化终态」）：
#      · 世代记录：每次 spawn 由 keeper 亲写 gen/<job>.rec（pid + starttime + token 哈希
#        + spawn 时刻 + keeper pid）。这是"这个进程是 keeper 拉起的"的唯一机器指纹。
#      · 退出码：作业是 keeper 的直接子进程（LAUNCH_SHIM 结尾 exec；setsid 不 fork），
#        进程从 /proc 消失后 `wait <pid>` 立即返回真实退出码（信号死 = 128+N），
#        同时回收僵尸；读到的码写入持久化终态 terminal/<job>.json（跨 keeper 重启可复核）。
#      · 结构化终态：读 convergence-report.json 的 decided/terminal/exit_code/pid/run
#        （新鲜度门槛：mtime ≥ 本次 spawn 时刻，或报告内 pid == 被判定 pid；
#         过期报告一律标 stale 并审计，绝不采信陈旧结论）。
#      · 退出码处置矩阵（KEEPER_MACHINE_VERDICT=1 为主判据）：
#          0  收敛完成       → 终态停机（terminal/<job>.json + keeper_job_stop 审计，不再重启）
#          3  未收敛         → 有限重启 KEEPER_INCOMPLETE_RETRIES 次；超限 ⇒ keeper_job_halt
#                             （停机告警：审计 + halt/<job>.json + 状态文件），防空转循环
#                             —— H5「exit 3 被当崩溃 ⇒ 无界重启」在此闭环
#          4  优雅自停/stall → 重启（resume 语义），受同一重启预算约束
#          信号死/其他非零   → 崩溃路径（process_dead + keeper_restart）+ 重启预算
#      · 重启预算（supervisord startretries 式）：连续快速失败（活不过 KEEPER_RAPID_DEATH
#        秒）达 KEEPER_START_RETRIES ⇒ 停止重启 + keeper_job_halt 升级告警
#        （v3.0 的 FLAP 只审计不停止 ⇒ 现已闭环）。
#      · grep 完成标记退化为兼容降级通道：KEEPER_COMPAT_GREP 默认 0（关闭）；开启时
#        仅作兜底判据，且必须以 evidence_level=low + 显式审计标注。
#
#  [K] 冒充防护（c1 / crit-adversarial C1）：token 单证不得静默认领——
#      · 认领 = 世代记录（pid+starttime+token 哈希）∧ 身份证据（argv/cwd/exe）∧ 进程存活；
#      · 世代记录存在时**只有** pid+starttime 与记录逐字相符的进程是本作业；偷 token 的
#        陌生进程（token 哈希相符而 pid/starttime 不符）⇒ keeper_adopt_refused
#        （reason=stranger_token / stranger_candidate / pidfile_generation_mismatch），
#        不认领、不静默、不写 pidfile；真作业缺席则照常重启（c1b 后果修正）；
#      · 无世代记录（keeper 首次上任 / state 被清）⇒ 仅允许"token+路径+cwd+exe 全命中"的
#        legacy 认领，且必须写 adopt_unverified（reason=token_without_generation_record）；
#      · 后果修正（c1b）：真作业死亡不再被冒充者静默——陌生候选被拒 ⇒ 真作业缺席 ⇒
#        走重启路径（process_dead + keeper_restart），恢复时限仍受 A2 ≤180s 约束。
#
#  [L] 锁族加固（K-1/K-2/K-5）：
#      · K-1 分裂脑：持锁后记录锁文件 dev:inode 并每轮校验；锁被删/重建 ⇒ 先尝试在新
#        inode 上重新 flock：拿到 ⇒ keeper_lock_rotated{action=reacquired} 续持有；
#        拿不到 ⇒ singleton_conflict{reason=lock_replaced_held_elsewhere} 并以退出码 6
#        让位（绝不两个实例同时"持锁"）。flock 失败区分「冲突」(rc=4) 与「不可用」
#        (rc=5 + errno 文本)；冲突取证不再只信落盘 holder 记录——用 /proc/<pid>/fdinfo
#        交叉定位真实持有者（C2b：陈旧记录 15/24 错指认）。
#      · K-2 fd 泄漏：心跳读取子进程、tick 内 sleep、spawn 核对轮询、TERM 宽限等**长命/可预测**
#        外部子命令显式 9>&-（作业本体 v3.0 已如此）。残留面：bash 为命令替换 fork 的短命子壳
#        仍会在毫秒级窗口持有继承来的 fd 9（实测单持有者 ≤~100ms，见 test-report §7.1；
#        真"零继承"须把锁交独立 holder 进程，bash 无法对已开 fd 设 CLOEXEC）。
#      · K-5 PID 复用：status / lock-probe / holder 校验把 keeper 自己写的 starttime
#        一并核对，cmdline 亦须命中本 keeper。
# =============================================================================
# keeper-v3.0 —— R3 自愈轮升级（C3 进程级自愈 + R3 审计 schema）。相对 v2 的增补：
#
#  [E] flock 单例锁（R1 移交 5）：start/daemon 独占 $KEEPER_STATE_DIR/keeper-v3.lock；
#      第二个 keeper 非阻塞抢锁失败 ⇒ 审计 singleton_conflict{holder_pid,self_pid} 后
#      以退出码 4 退出，绝不与在位 keeper 互踩（双 spawn / 双 pidfile 写 / 双审计）。
#      持有者身份另写 keeper-v3.holder（原子写）供冲突方取证；锁 fd 持有到进程退出。
#
#  [F] 信号处理 + 优雅收尾（R1 移交 6）：SIGTERM/SIGINT ⇒ 置停止标志（下一检查点即停
#      重启循环，不再拉起新进程）→ 有界等待 keeper 自建的在飞子进程（KEEPER_STOP_GRACE）
#      → 写 checkpoint（state + keeper-v3.checkpoint.json）→ 审计 graceful_stop
#      {signal,todo,done,...} 与 keeper_stop → 退出 0。
#      **刻意不杀被看护作业**：setsid 脱离的作业在 keeper 停机期间继续跑（审计列出
#      detached_job_pids），这是"keeper 可随时重启、抓取不中断"的前提。
#
#  [G] adopt 反向风险修正（R1 移交 9 / R1 crit-adversarial S1+S2）：认领不再是"argv 里
#      出现同名 basename"，改为身份证据分级（env token > 绝对路径+cwd+exe > 宽松），
#      并区分「pidfile 核对」与「扫描认领」两套判据：
#        · spawn 时注入 SCRAPE_KEEPER_TOKEN=kv3:<family>:<job>:<nonce>（环境指纹，
#          读 /proc/<pid>/environ 校验）⇒ 认领有可验证凭据；
#        · token 存在但 job 字段不匹配 ⇒ **拒绝认领**（keeper_adopt_refused），
#          冒充者不得被认领 ⇒ 真作业照常拉起；
#        · starttime 缺失/`?` 不再"跳过校验" ⇒ pidfile 降级为不可信，走扫描+分级证据；
#        · pidfile 存在但 starttime 不符且无 token ⇒ 不再静默认领（拒 pidfile）；
#        · 无 token 的合法老进程走 legacy 通道，但**必须显式记 adopt_unverified**
#          （绝不静默 running），并在 status 里标 TRUST=legacy；
#        · basename-only 匹配 ⇒ 仅当 cwd 也吻合才可信（S1 冒充者的死穴）。
#
#  [H] 心跳消费增强（R2 移交 13 / crit-correct H3 / crit-adversarial C3）：每轮读心跳
#      runtime 块（gate_open / gate_closed_s / delay_s / strikes / backoff_active /
#      mem.level / eff_workers / base_workers…），把「数据没动」正确分类为
#      退避中 / 闸门冻结 / 预算降档 / 真卡死（判据表见 test-report.md §2）。
#      合法退避与闸门冻结**绝不触发重启**；真卡死需连续 KEEPER_HB_CONFIRM 轮确认，
#      且默认只告警（KEEPER_STALL_ACTION=audit）；restart 需 env rail 显式放行。
#
#  [I] 配置挡板（R1 移交 10）：job_cwd_strict NAME 0、KEEPER_SINGLETON=0、
#      KEEPER_STALL_ACTION=restart 三条 "rail" 在**读取 conf 之前**取 env 快照，
#      部署模板（jobs.conf）里写什么都无法关闭，只能由启动 env 放行 + 审计
#      config_rail_refused / config_rail_override。堵死"部署模板关掉唯一挡板"。
#
#  审计（R3 schema 兼容）：既有 v2 事件名与字段**一个不改**（子集兼容），只增字段
#      （ts_epoch / level / seq / r3_event / detail）。新增 process 级事件：
#      keeper_restart（= restart 的 R3 别名 r3_event）/ keeper_adopt_refused /
#      singleton_conflict / heartbeat_stall / graceful_stop
#      （辅助事件：heartbeat_class / config_rail_refused / config_rail_override /
#        adopt_unverified / pidfile_untrusted / stall_restart_refused）。
#      详见 refs/r3-audit-schema.md 与 test-report.md §2-§3。
# =============================================================================

set -uo pipefail

KEEPER_VERSION="keeper-v3.1"
KEEPER_SELF="$(readlink -f "${BASH_SOURCE[0]}" 2>/dev/null || printf '%s' "${BASH_SOURCE[0]}")"
KEEPER_DIR="$(dirname "$KEEPER_SELF")"

# ------------------------------------------------------------------ 默认参数 --
: "${KEEPER_TICK:=15}"                  # 巡检周期(s)
: "${KEEPER_HEARTBEAT:=300}"            # 心跳审计周期(s)
: "${KEEPER_ENV_RECHECK:=300}"          # 环境探针复核周期(s)
: "${KEEPER_RECOVERY_DEADLINE:=180}"    # A2 硬指标：恢复 ≤180s
: "${KEEPER_MAX_BACKOFF:=120}"          # 崩溃循环退避上限（启动时夹到 < DEADLINE）
: "${KEEPER_RAPID_DEATH:=20}"           # 新进程活不过 N 秒 ⇒ 视为"秒死"(崩溃循环)
: "${KEEPER_FLAP_THRESHOLD:=3}"         # 连续秒死 N 次 ⇒ restart_flapping 告警
: "${KEEPER_STRICT_ENV:=0}"             # 1=环境探针失败即拒绝运行
: "${KEEPER_DRY_RUN:=0}"                # 1=只审计不 spawn
: "${KEEPER_SETSID:=1}"                 # 1=用 setsid 脱离进程组（生产用）
: "${KEEPER_SPAWN_VERIFY_S:=10}"        # spawn 后核对 /proc 的等待上限(s)
: "${KEEPER_ENV_TAG:=prod}"             # 审计来源标签：prod / sandbox-drill / ...
: "${KEEPER_STATE_DIR:=$KEEPER_DIR/state}"
: "${KEEPER_AUDIT:=$KEEPER_DIR/audit.jsonl}"                    # 可空格分隔多sink
: "${KEEPER_STARTUP_AUDIT:=$KEEPER_DIR/startup-audit.log}"      # 可空格分隔多sink
: "${KEEPER_LOG_SINKS:=$KEEPER_STATE_DIR/keeper-v3.log}"
: "${KEEPER_SELFTEST_DIR:=$KEEPER_STATE_DIR/selftest}"
: "${KEEPER_CONF:=$KEEPER_DIR/jobs.conf}"
: "${KEEPER_RUN_ID:=}"
# ---------------- v3 新增：单例锁 / 优雅停机 / 心跳 / 认领策略 -------------------
: "${KEEPER_SINGLETON:=1}"              # 1=start 独占 flock 单例锁（rail I1，conf 关不掉）
: "${KEEPER_LOCK_FILE:=$KEEPER_STATE_DIR/keeper-v3.lock}"
: "${KEEPER_HOLDER_FILE:=$KEEPER_STATE_DIR/keeper-v3.holder}"
: "${KEEPER_FAMILY_FILE:=$KEEPER_STATE_DIR/keeper-v3.family}"
: "${KEEPER_STOP_GRACE:=10}"            # SIGTERM 后有界等待自建在飞子进程的秒数
: "${KEEPER_HB_ENABLE:=1}"              # 1=消费心跳（runtime 块分类）
: "${KEEPER_HEARTBEAT_PY:=$KEEPER_DIR/heartbeat.py}"   # 部署时与 keeper 同目录
: "${KEEPER_HEARTBEAT_FILE:=}"          # 全局默认心跳文件；作业可用 job_heartbeat 覆盖
: "${KEEPER_HB_TIMEOUT:=10}"            # 单次心跳读取超时(s)
: "${KEEPER_HB_STALL_S:=}"              # 空=用 heartbeat.py 默认(900，env 可覆盖)
: "${KEEPER_HB_CONFIRM:=2}"             # 连续 N 轮同判据才升级为 heartbeat_stall
: "${KEEPER_HB_REAUDIT:=5}"             # 卡死持续期间每 N 轮重复审计一次
: "${KEEPER_STALL_ACTION:=audit}"       # audit|restart（restart 需 env rail I3 放行）
: "${KEEPER_STALL_KILL_GRACE:=10}"      # restart 动作：TERM 后等 N 秒再 KILL
: "${KEEPER_ADOPT_LEGACY:=1}"           # 1=允许无 token 的 legacy 认领（必审计）；0=拒绝
: "${KEEPER_ADOPT_LOOSE:=1}"            # 1=允许"绝对路径+exe+唯一候选"的宽松认领（必审计）
: "${KEEPER_TOKEN_ENFORCE:=1}"          # 1=spawn 时注入 SCRAPE_KEEPER_TOKEN
# ------- v3.1 [J][K][L]：机器化终态判据 / 冒充防护 / 锁族 -------------------------
: "${KEEPER_MACHINE_VERDICT:=1}"        # 1=以 exit code + 结构化终态为完成/失败主判据（F6）
: "${KEEPER_COMPAT_GREP:=0}"            # 1=允许 grep 完成标记作为兼容降级判据（默认关）
: "${KEEPER_EXIT_GRACE_MS:=1500}"       # 观测到进程消失后等待结构化报告落盘的宽限(ms)
: "${KEEPER_INCOMPLETE_RETRIES:=2}"     # exit 3（未收敛）有限重启次数，超限 ⇒ 停机告警
: "${KEEPER_START_RETRIES:=5}"          # 连续快速失败上限（startretries 式），超限 ⇒ 停止重启
: "${KEEPER_HALT_ON_FLAP:=1}"           # 1=超限即停止重启 + flap 升级告警；0=仅告警（v3.0 语义）
: "${KEEPER_CONV_REPORT:=}"             # 全局结构化终态报告路径；空=按作业推导，"-"=禁用
: "${KEEPER_GEN_BIND:=1}"               # 1=认领须与世代记录（pid+starttime+token 哈希）绑定
: "${KEEPER_LOCK_INODE_GUARD:=1}"       # 1=每轮校验锁文件 inode（防「删锁重建」分裂脑）
# ---- rail 快照：必须在 source conf 之前读取（部署模板改不了这三条挡板）----------
KEEPER_RAIL_ALLOW_CWD_STRICT_OFF="${KEEPER_ALLOW_CWD_STRICT_OFF:-0}"
KEEPER_RAIL_ALLOW_SINGLETON_OFF="${KEEPER_ALLOW_SINGLETON_OFF:-0}"
KEEPER_RAIL_ALLOW_STALL_RESTART="${KEEPER_ALLOW_STALL_RESTART:-0}"

mkdir -p "$KEEPER_STATE_DIR/pid" "$KEEPER_STATE_DIR" 2>/dev/null

# ------------------------------------------------------------- 作业表结构 ----
JOB_NAME=(); JOB_SCRIPT=(); JOB_CWD=(); JOB_COMPLOG=(); JOB_COMPPAT=()
JOB_OUTLOG=(); JOB_EXTRA=(); JOB_BASENAME=(); JOB_RELPATH=(); JOB_PIDFILE=()
JOB_EXE_RE=(); JOB_HB_FILE=()                 # v3：每作业心跳文件（消费 runtime 块用）
JOB_CONV_REPORT=()             # v3.1：每作业结构化终态报告路径覆盖（"-"=禁用该通道）
declare -A JOB_EXEC_OVERRIDE=()
declare -A JOB_CWD_STRICT=()   # 每作业 cwd 强核对开关，默认 1（v3：由 rail 守护）
JOB_HB_SRC=()                  # 心跳来源标注：conf/job_heartbeat | global | none

# job NAME SCRIPT CWD COMPLOG COMPPAT OUTLOG [EXTRA_ARGS...]
job() {
  local name="$1" script="$2" cwd="$3" complog="$4" comppat="$5" outlog="$6"; shift 6
  JOB_NAME+=("$name"); JOB_SCRIPT+=("$script"); JOB_CWD+=("$cwd")
  JOB_COMPLOG+=("$complog"); JOB_COMPPAT+=("$comppat"); JOB_OUTLOG+=("$outlog")
  JOB_EXTRA+=("$*")
  JOB_BASENAME+=("$(basename "$script")")
  case "$script" in
    "$cwd"/*) JOB_RELPATH+=("${script#"$cwd"/}") ;;
    *)        JOB_RELPATH+=("$(basename "$script")") ;;
  esac
  JOB_PIDFILE+=("$KEEPER_STATE_DIR/pid/$name.pid")
  JOB_HB_FILE+=("${KEEPER_HEARTBEAT_FILE:-}")
  JOB_HB_SRC+=("$([ -n "${KEEPER_HEARTBEAT_FILE:-}" ] && printf global || printf none)")
  JOB_CONV_REPORT+=("")
  case "$script" in
    *.py) JOB_EXE_RE+=('^(python[0-9.]*|.*/python[0-9.]*)$') ;;
    *.sh) JOB_EXE_RE+=('^(ba)?sh$|.*/(ba)?sh$') ;;
    *)    JOB_EXE_RE+=('') ;;
  esac
}
# job_exec NAME EXE —— 覆盖推导出的启动解释器（须在 job 之后调用）
job_exec() { JOB_EXEC_OVERRIDE["$1"]="$2"; }
# job_cwd_strict NAME 0|1 —— 是否强制核对 /proc/<pid>/cwd（默认 1）
#   0 适用于"历史进程 cwd 已不可考"的作业：此时仍要求 argv token 绝对路径精确相等，
#   精度足够，且避免因 cwd 不一致而误判为不在 ⇒ 重复起进程。
#
#   v3 rail（R1 移交 10 / 任务书第 10 条）：部署模板 jobs.conf 里写 `job_cwd_strict X 0`
#   **不再生效** —— 该开关是 R1 crit 实证的"唯一挡板"（关掉后 basename 冒充者被直接认领）。
#   只有启动 env `KEEPER_ALLOW_CWD_STRICT_OFF=1`（在 source conf 之前取快照）才放行，
#   且放行/拒绝都写审计。注意 v3 的身份判据已把 cwd 纳入 argv 相对路径匹配的前提，
#   所以即便放行 cwd_strict=0，basename-only 的冒充者依然认领不了（纵深防御）。
job_cwd_strict() {
  local n="$1" v="$2"
  if [ "$v" != 1 ] && [ "$v" != 0 ]; then v=1; fi
  if [ "$v" = 0 ] && [ "${KEEPER_RAIL_ALLOW_CWD_STRICT_OFF:-0}" != 1 ]; then
    JOB_CWD_STRICT["$n"]=1
    RAIL_TRIPPED+=("cwd_strict:$n")
    audit config_rail_refused "" - "$(ev rail=cwd_strict job="$n" requested=0 forced=1 source=conf allow_env=KEEPER_ALLOW_CWD_STRICT_OFF env_seen="${KEEPER_RAIL_ALLOW_CWD_STRICT_OFF:-0}" note="部署模板不得关闭 cwd 挡板；需 env 显式放行")"
    return 0
  fi
  JOB_CWD_STRICT["$n"]="$v"
  if [ "$v" = 0 ]; then
    RAIL_OVERRIDES+=("cwd_strict:$n")
    audit config_rail_override "" - "$(ev rail=cwd_strict job="$n" value=0 source=env allow_env=KEEPER_ALLOW_CWD_STRICT_OFF=1 note="env 显式放行 cwd 挡板关闭，已扩权审计")"
  fi
}
# job_heartbeat NAME FILE —— 绑定该作业的心跳文件（v3：消费 runtime 块分类退避/闸门/预算）
job_heartbeat() {
  local n="$1" f="$2" i=0
  for ((i=0; i<${#JOB_NAME[@]}; i++)); do
    if [ "${JOB_NAME[$i]}" = "$n" ]; then JOB_HB_FILE[$i]="$f"; JOB_HB_SRC[$i]=conf; return 0; fi
  done
  return 1
}
# job_convreport NAME PATH —— v3.1：该作业的结构化终态报告路径（convergence-report.json）
#   PATH="-" ⇒ 禁用报告通道（只认 exit code / 兼容 grep）；未声明 ⇒ 由完成标记日志目录推导
job_convreport() {
  local n="$1" f="$2" i=0
  for ((i=0; i<${#JOB_NAME[@]}; i++)); do
    if [ "${JOB_NAME[$i]}" = "$n" ]; then JOB_CONV_REPORT[$i]="$f"; return 0; fi
  done
  return 1
}
cwd_strict_of() { printf '%s' "${JOB_CWD_STRICT[$1]:-1}"; }
job_count() { printf '%s' "${#JOB_NAME[@]}"; }
job_exec_of() {
  local i="$1" n="${JOB_NAME[$1]}"
  if [ -n "${JOB_EXEC_OVERRIDE[$n]:-}" ]; then printf '%s' "${JOB_EXEC_OVERRIDE[$n]}"; return; fi
  case "${JOB_SCRIPT[$i]}" in
    *.py) printf 'python3' ;;
    *.sh) printf 'bash' ;;
    *)    printf '%s' "${JOB_SCRIPT[$i]}" ;;
  esac
}

# ------------------------------------------------------------------ 小工具 --
CUR_JOB="-"
RAIL_TRIPPED=(); RAIL_OVERRIDES=()      # v3：被拒/被放行的挡板（rails_apply 汇总审计）
RUN_ID="${KEEPER_RUN_ID:-$(date +%Y%m%dT%H%M%S)-$$}"
HOST_S="$(hostname 2>/dev/null || printf 'unknown')"
NS_MNT="$(readlink /proc/self/ns/mnt 2>/dev/null || printf 'unknown')"

hlog() {
  local f line; line="$(date '+%F %T') $*"
  for f in $KEEPER_LOG_SINKS; do
    mkdir -p "$(dirname "$f")" 2>/dev/null
    printf '%s\n' "$line" >>"$f" 2>/dev/null
  done
  printf '%s\n' "$line"
}

# 毫秒时间戳（优先 bash 内建 EPOCHREALTIME，无则退回 date %s%3N）
now_ms() {
  if [ -n "${EPOCHREALTIME:-}" ]; then
    local r="${EPOCHREALTIME}" frac
    frac="${r#*.}"; frac="${frac:0:3}"
    printf '%s%s' "${r%.*}" "$frac"
  else
    date +%s%3N
  fi
}
# epoch 秒（3 位小数），R3 审计的 ts_epoch 字段用
epoch_s() {
  if [ -n "${EPOCHREALTIME:-}" ]; then
    printf '%s.%s' "${EPOCHREALTIME%.*}" "$(printf '%s' "${EPOCHREALTIME#*.}" | cut -c1-3)"
  else
    date +%s
  fi
}
now_iso() {
  if [ -n "${EPOCHREALTIME:-}" ]; then
    local r="${EPOCHREALTIME}" frac
    frac="${r#*.}"; frac="${frac:0:3}"
    printf '%s.%sZ' "$(date -u -d "@${r%.*}" '+%Y-%m-%dT%H:%M:%S' 2>/dev/null || date -u '+%Y-%m-%dT%H:%M:%S')" "$frac"
  else
    date -u '+%Y-%m-%dT%H:%M:%SZ'
  fi
}
# JSON 字符串转义（纯 bash + tr，不依赖 jq）
jesc() {
  local s="$1"
  s="${s//\\/\\\\}"; s="${s//\"/\\\"}"
  s="${s//$'\n'/\\n}"; s="${s//$'\r'/\\r}"; s="${s//$'\t'/\\t}"
  s="${s//$'\b'/\\b}"; s="${s//$'\f'/\\f}"
  s="$(printf '%s' "$s" | tr -d '\000-\010\013\014\016-\037' 2>/dev/null)"
  printf '%s' "$s"
}
# ev key=value [key=value ...] → JSON object（纯数字值按数字输出；无 = 的参数记为布尔旗标）
ev() {
  local out='{' sep='' a k v
  for a in "$@"; do
    case "$a" in
      *=*) k="${a%%=*}"; v="${a#*=}" ;;
      *)   k="$a"; v="true" ;;
    esac
    [ -n "$k" ] || continue
    if [[ "$v" =~ ^-?[0-9]+(\.[0-9]+)?$ ]]; then
      out+="$sep\"$(jesc "$k")\":$v"
    else
      out+="$sep\"$(jesc "$k")\":\"$(jesc "$v")\""
    fi
    sep=','
  done
  printf '%s}' "$out"
}
# evcat '{..}' '{..}' → 合并两个 JSON 对象（用于把子检测器的 evidence 嵌进外层）
evcat() {
  local out='{' sep='' a inner
  for a in "$@"; do
    [ -n "$a" ] || continue
    inner="${a#\{}"; inner="${inner%\}}"
    [ -n "$inner" ] || continue
    out+="$sep$inner"; sep=','
  done
  printf '%s}' "$out"
}

# ---------------------------------------------------------------- 审计写入 --
# R3-P4 契约 v2（refs/audit-contract-v2.md §1/§2）—— 行布局与三写者统一：
#   · 顶层 `schema` 写 "r3-audit-v2"（旧值 keeper-audit/1 降为私有键 keeper_schema 保留）
#   · `ts`(ISO) → **`ts_iso`**（重命名，契约 §1 遗留键裁决）；`ts_epoch` 保留
#   · 补 `pid`（= keeper 自身 pid，与 keeper_pid 同值，契约 §1 强制字段）
#   · 写入侧一律用 canonical 事件名：restart → keeper_restart、adopt → keeper_adopt；
#     原始事件名降为私有键 `keeper_event` 保留（老解析器仍可读 event/from_pid/to_pid/evidence）
#   · 契约 v2.1 §8.1（KI-1 修复）：补 **`writer`** 分组键 = "keeper"（本文件是 keeper 侧
#     写者；scrape 侧写 converge/retry）。校验器按 (文件, pid, writer) 分组 ⇒ 同一 sink 上
#     多个写者各持独立 seq 时不会误判为 seq_duplicate。
#   · 其余既有字段（keeper / run_id / run / env / host / ns_mnt / job / level / seq /
#     evidence / detail / r3_event / r3_schema）一个不删
AUDIT_SEQ=0
# 契约 v2.1 §8.1：本写者身份（写入每行的 writer 字段）
AUDIT_WRITER="keeper"
# canonical 事件名（R3p4/refs/audit-contract-v2.md §2）：左=写入名，右=r3_event 别名。
# v3.1 新增的进程级事件一律**直接用 canonical 名写入**（identity 映射），便于校验器与
# 合并槽按统一口径识别；旧事件（restart 等）保持写入名不变 + 别名映射（子集兼容）。
R3_ALIAS_EVENTS="keeper_restart:restart keeper_adopt_refused:keeper_adopt_refused singleton_conflict:singleton_conflict heartbeat_stall:heartbeat_stall graceful_stop:graceful_stop keeper_exit:keeper_exit keeper_lock_unavailable:keeper_lock_unavailable keeper_lock_rotated:keeper_lock_rotated keeper_job_stop:keeper_job_stop keeper_job_halt:keeper_job_halt keeper_terminal_report_stale:keeper_terminal_report_stale keeper_exit_observed:keeper_exit_observed keeper_restart_budget_exhausted:keeper_restart_budget_exhausted keeper_job_resume:keeper_job_resume"
r3_event_of() { # EVENT → R3 规范事件名（非 R3 事件返回空）
  local e="$1" pair
  for pair in $R3_ALIAS_EVENTS; do
    case "$pair" in
      "$e":*) printf '%s' "${pair#*:}"; return 0 ;;
    esac
    case "$pair" in
      *:"$e") printf '%s' "${pair%%:*}"; return 0 ;;
    esac
  done
  printf ''
}
# audit EVENT FROM_PID TO_PID EVIDENCE_JSON [LEVEL] [DETAIL_JSON]
audit() {
  local evn="$1" fp="${2:-}" tp="${3:-}" ex="${4:-}" lvl="${5:-process}" det="${6:-}"
  case "$fp" in ''|-) fp=null ;; esac
  case "$tp" in ''|-) tp=null ;; esac
  case "$ex" in ''|-) ex='{}' ;; esac
  case "$lvl" in request|block|process) : ;; *) lvl=process ;; esac
  case "$det" in ''|-) det="$ex" ;; esac
  local r3 evc line f
  r3="$(r3_event_of "$evn")"
  # 写入侧 canonical 名（契约 §2）：r3 别名优先；adopt 是唯一需要额外映射的旧名
  case "${r3:-$evn}" in
    adopt) evc=keeper_adopt ;;
    *)     evc="${r3:-$evn}" ;;
  esac
  AUDIT_SEQ=$((AUDIT_SEQ + 1))
  # [merge R3p4] 行布局裁决：采纳 audit-align-keeper（audit-contract-v2 §1 权威写入侧）。
  #   契约 §1 明令 v2 行不再写入 `ts` 键 ⇒ 不保留 keeper-match 的 `ts`/`ts_iso` 并列形态；
  #   keeper-match 侧实质增补（扩展 R3_ALIAS_EVENTS 与 v3.1 全部逻辑）均已保留。
  # [P4.5 v2.1] 契约 §8.1：`writer` 为**字面量** "keeper"（无占位符 ⇒ printf 参数表不变）。
  line="$(printf '{"schema":"r3-audit-v2","ts_epoch":%s,"ts_iso":"%s","pid":%s,"writer":"%s","keeper_schema":"keeper-audit/1","r3_schema":"r3-audit/1","keeper":"%s","run_id":"%s","run":"%s","env":"%s","host":"%s","keeper_pid":%s,"ns_mnt":"%s","job":"%s","level":"%s","event":"%s","seq":%s,"from_pid":%s,"to_pid":%s,"evidence":%s,"detail":%s,"r3_event":%s,"keeper_event":"%s"}' \
    "$(epoch_s)" "$(now_iso)" "$$" "$AUDIT_WRITER" "$KEEPER_VERSION" "$RUN_ID" "$RUN_ID" "$KEEPER_ENV_TAG" "$HOST_S" "$$" \
    "$(jesc "$NS_MNT")" "$(jesc "$CUR_JOB")" "$lvl" "$(jesc "$evc")" "$AUDIT_SEQ" "$fp" "$tp" "$ex" "$det" \
    "$([ -n "$evc" ] && printf '"%s"' "$evc" || printf 'null')" "$(jesc "$evn")")"
  for f in $KEEPER_AUDIT; do
    mkdir -p "$(dirname "$f")" 2>/dev/null
    printf '%s\n' "$line" >>"$f" 2>/dev/null || hlog "WARN audit 写入失败: $f"
  done
  hlog "AUDIT $CUR_JOB $evn from=${fp} to=${tp} $ex"
}

# ------------------------------------------------------------ /proc 事实层 --
proc_exists() { [ -d "/proc/$1" ]; }
proc_cmdline_tokens() { # PID → 每行一个 argv token；解析失败返回 1
  local p="$1" raw
  [ -r "/proc/$p/cmdline" ] || return 1
  raw="$(tr '\0' '\n' <"/proc/$p/cmdline" 2>/dev/null)" || return 1
  [ -n "$raw" ] || return 1
  printf '%s\n' "$raw"
}
proc_starttime() { # PID → /proc/<pid>/stat 第 22 字段（自系统启动以来的 jiffies）
  local p="$1" s rest
  s="$(cat "/proc/$p/stat" 2>/dev/null)" || return 1
  rest="${s##*)}"          # 去掉 "pid (comm)"（comm 可能含空格/括号）
  # shellcheck disable=SC2086
  set -- $rest             # 第 1 项 = state(字段3) ⇒ 字段 22 == 第 20 项
  [ "$#" -ge 20 ] || return 1
  printf '%s' "${20}"
}
proc_cwd() { readlink -f "/proc/$1/cwd" 2>/dev/null || printf ''; }
proc_exe() { readlink -f "/proc/$1/exe" 2>/dev/null || printf ''; }
proc_cmdline_text() { # PID → 人类可读 argv（审计证据用，截断）
  local t; t="$(proc_cmdline_tokens "$1" 2>/dev/null | tr '\n' ' ')" || { printf '(unreadable)'; return; }
  printf '%s' "${t:0:200}"
}

# ------------------------------------------- 进程身份证据层（v3 [G] 核心） ----
# 同一个 /proc 事实集既服务「pidfile 核对」也服务「扫描认领」，避免两套判据漂移。
# 证据（factors）：
#   IDF_ABS     argv 里出现 == 作业脚本**绝对路径**的 token        （强）
#   IDF_REL     argv 里出现 == 作业脚本相对路径/basename 的 token  （弱，需 cwd 佐证）
#   IDF_CWD_OK  1=cwd 与作业 cwd 一致 / 0=不一致 / ""=不可读（未知）
#   IDF_EXE_OK  /proc/<pid>/exe 解释器与作业类型匹配
#   IDF_TOKEN   none | match | mismatch | forged？  —— SCRAPE_KEEPER_TOKEN 环境指纹
#               kv3:<family>:<job>:<nonce>；job 字段不匹配 = 冒充（拒绝认领）
#   IDF_START_OK pidfile 记录的 starttime 与 /proc 一致；"" = pidfile 无有效 starttime
# 身份等级（VERIFY_TRUST）：
#   token           —— env token job 匹配（最强，可跨 keeper 重启/跨 PID 复用）
#   path_verified   —— 绝对路径 token + cwd 吻合 + exe 吻合
#   legacy_loose    —— 绝对路径 token + exe 吻合（cwd 不吻合/不可读）—— 必有审计
#   relative_only   —— 仅相对/basename token 且 cwd 不吻合 ⇒ **不可信**（S1 冒充者）
VERIFY_WHY=""
IDF_ABS=0; IDF_REL=0; IDF_CWD_OK=""; IDF_EXE_OK=1; IDF_TOKEN=none
IDF_TOKEN_JOB=""; IDF_TOKEN_FAMILY=""; IDF_TOKEN_RAW=""; IDF_START_OK=""
VERIFY_TRUST=""
IDF_JSON='{}'

# proc_environ_field PID KEY —— 从 /proc/<pid>/environ 取环境变量值（不存在返回空）
proc_environ_field() {
  local p="$1" key="$2" raw
  [ -r "/proc/$p/environ" ] || return 1
  raw="$(tr '\0' '\n' <"/proc/$p/environ" 2>/dev/null)" || return 1
  local l
  while IFS= read -r l; do
    case "$l" in "$key"=*) printf '%s' "${l#*=}"; return 0 ;; esac
  done <<<"$raw"
  return 1
}
# proc_keeper_token PID —— 解析 SCRAPE_KEEPER_TOKEN → IDF_TOKEN_RAW/JOB/FAMILY
proc_keeper_token() {
  local p="$1" raw rest
  IDF_TOKEN=none; IDF_TOKEN_RAW=""; IDF_TOKEN_JOB=""; IDF_TOKEN_FAMILY=""
  if ! raw="$(proc_environ_field "$p" SCRAPE_KEEPER_TOKEN)"; then
    # environ 不可读/无该变量：区分"读不了"（降级）与"确实没有"（legacy）
    [ -r "/proc/$p/environ" ] || IDF_TOKEN=unreadable
    return 0
  fi
  [ -n "$raw" ] || return 0
  IDF_TOKEN_RAW="${raw:0:120}"
  rest="${raw#kv3:}"
  if [ "$rest" = "$raw" ]; then IDF_TOKEN=mismatch; return 0; fi   # 非本代 token 格式
  IDF_TOKEN_FAMILY="${rest%%:*}"
  rest="${rest#*:}"
  IDF_TOKEN_JOB="${rest%%:*}"
  IDF_TOKEN=match
}
# identity_factors IDX PID —— 采集全部身份证据（只读 /proc，不写任何东西）
identity_factors() {
  local i="$1" p="$2" t cwd exe base
  IDF_ABS=0; IDF_REL=0; IDF_CWD_OK=""; IDF_EXE_OK=1; IDF_START_OK=""
  if [ -n "${JOB_CWD[$i]}" ]; then
    cwd="$(proc_cwd "$p")"
    if [ -n "$cwd" ]; then
      if [ "$cwd" = "${JOB_CWD[$i]}" ]; then IDF_CWD_OK=1; else IDF_CWD_OK=0; fi
    fi
  fi
  while IFS= read -r t; do
    [ -n "$t" ] || continue
    [ "$t" = "${JOB_SCRIPT[$i]}" ] && IDF_ABS=1
    if [ "$t" = "${JOB_BASENAME[$i]}" ] || [ "$t" = "${JOB_RELPATH[$i]}" ]; then IDF_REL=1; fi
  done < <(proc_cmdline_tokens "$p")
  if [ -n "${JOB_EXE_RE[$i]}" ]; then
    exe="$(proc_exe "$p")"; base="${exe##*/}"
    [[ "$base" =~ ${JOB_EXE_RE[$i]} ]] || IDF_EXE_OK=0
  fi
  proc_keeper_token "$p"
  IDF_JSON="$(ev argv_abs="$IDF_ABS" argv_rel="$IDF_REL" cwd_ok="${IDF_CWD_OK:-unknown}" exe_ok="$IDF_EXE_OK" token="$IDF_TOKEN" token_job="${IDF_TOKEN_JOB:-}" token_family="${IDF_TOKEN_FAMILY:-}" cwd_strict="$(cwd_strict_of "${JOB_NAME[$i]}")")"
}
# identity_trust IDX —— 由证据推导身份等级（不判 OK/NG，只分级）
identity_trust() {
  local i="$1"
  if [ "$IDF_TOKEN" = match ] && [ "$IDF_TOKEN_JOB" = "${JOB_NAME[$i]}" ]; then
    VERIFY_TRUST="token"; return 0
  fi
  if [ "$IDF_ABS" = 1 ] && [ "$IDF_CWD_OK" = 1 ] && [ "$IDF_EXE_OK" = 1 ]; then
    VERIFY_TRUST="path_verified"; return 0
  fi
  if [ "$IDF_ABS" = 1 ] && [ "$IDF_EXE_OK" = 1 ]; then VERIFY_TRUST="legacy_loose"; return 0; fi
  if [ "$IDF_REL" = 1 ] && [ "$IDF_CWD_OK" = 1 ] && [ "$IDF_EXE_OK" = 1 ]; then
    # 相对路径/basename 启动（v1 风格 `cd X && python3 scrape.py`）：cwd 是它的身份佐证
    VERIFY_TRUST="relative_cwd_ok"; return 0
  fi
  VERIFY_TRUST="insufficient"
  return 1
}
# verify_pid_for_job IDX PID —— 该 pid 在 argv/cwd/exe 三轴上是该作业的进程吗？（0=是）
# v3 收紧（R1 crit S1）：basename-only 匹配**必须**有 cwd 佐证；token job 不匹配 ⇒ 直接否。
verify_pid_for_job() {
  local i="$1" p="$2" cwd
  VERIFY_WHY=""; VERIFY_TRUST=""
  IDF_ABS=0; IDF_REL=0; IDF_CWD_OK=""; IDF_EXE_OK=1; IDF_TOKEN=none
  IDF_TOKEN_JOB=""; IDF_TOKEN_FAMILY=""; IDF_TOKEN_RAW=""; IDF_JSON='{}'
  [[ "$p" =~ ^[0-9]+$ ]] || { VERIFY_WHY="pid_not_numeric"; return 1; }
  [ "$p" != "1" ] || { VERIFY_WHY="pid_1_refused"; return 1; }
  proc_exists "$p" || { VERIFY_WHY="proc_missing"; return 1; }
  identity_factors "$i" "$p"
  # (0) 环境指纹：token 存在但 job 字段不是本作业 ⇒ 冒充/他作业进程，一票否决
  if [ "$IDF_TOKEN" = match ] && [ "$IDF_TOKEN_JOB" != "${JOB_NAME[$i]}" ]; then
    VERIFY_WHY="token_job_mismatch(${IDF_TOKEN_JOB:-?})"; return 1
  fi
  if [ "$IDF_TOKEN" = mismatch ]; then
    VERIFY_WHY="token_format_mismatch"; return 1
  fi
  # (1) argv：绝对路径命中，或（相对/basename 命中 且 cwd 佐证）
  identity_trust "$i" || {
    if [ "$IDF_REL" = 1 ] && [ "$IDF_CWD_OK" = 0 ]; then
      VERIFY_WHY="argv_basename_only_cwd_mismatch"
    elif [ "$IDF_EXE_OK" = 0 ]; then VERIFY_WHY="exe_mismatch"
    elif [ "$IDF_ABS" = 0 ] && [ "$IDF_REL" = 0 ]; then VERIFY_WHY="argv_token_mismatch"
    else VERIFY_WHY="identity_insufficient"; fi
    return 1
  }
  # (2) cwd 强核对（rail 守护；相对路径级别已在 (1) 强制要求 cwd 吻合）
  if [ "$(cwd_strict_of "${JOB_NAME[$i]}")" = 1 ] && [ -n "${JOB_CWD[$i]}" ]; then
    cwd="$(proc_cwd "$p")"
    if [ -n "$cwd" ] && [ "$cwd" != "${JOB_CWD[$i]}" ]; then
      VERIFY_WHY="cwd_mismatch($cwd)"; return 1
    fi
  fi
  # (3) 解释器核对（identity_trust 里已查，这里给显式理由）
  if [ "$IDF_EXE_OK" = 0 ]; then
    VERIFY_WHY="exe_mismatch($(proc_exe "$p"))"; return 1
  fi
  VERIFY_WHY="ok"
  return 0
}

# PID 文件：`pid=N starttime=N exe=... script=... job=... ts=... [trust=... adopted=1]`
PF_PID=""; PF_START=""; PF_RAW=""; PF_START_VALID=0; PF_TRUST=""; PF_ADOPTED=""
pidfile_read() { # IDX → 解析成功(0/1)，结果写入 PF_*
  PF_PID=""; PF_START=""; PF_RAW=""; PF_START_VALID=0; PF_TRUST=""; PF_ADOPTED=""
  local f="${JOB_PIDFILE[$1]}" w k v
  [ -r "$f" ] || return 1
  PF_RAW="$(head -c 512 "$f" 2>/dev/null | tr -d '\n')"
  [ -n "$PF_RAW" ] || return 1
  for w in $PF_RAW; do
    k="${w%%=*}"; v="${w#*=}"
    case "$k" in
      pid) PF_PID="$v" ;;
      starttime) PF_START="$v" ;;
      trust) PF_TRUST="$v" ;;
      adopted) PF_ADOPTED="$v" ;;
    esac
  done
  [[ "$PF_PID" =~ ^[0-9]+$ ]] || return 1
  # v3：starttime 是否"有效"是独立事实（v2 把 `?`/缺失当"跳过校验"⇒ R1 crit S2 的洞）
  [[ "$PF_START" =~ ^[0-9]+$ ]] && PF_START_VALID=1
  return 0
}
pidfile_write() { # IDX PID [extra_kv...] —— 原子写
  local i="$1" p="$2"; shift 2
  local f="${JOB_PIDFILE[$i]}" tmp st
  st="$(proc_starttime "$p" 2>/dev/null || printf '?')"
  tmp="$f.tmp.$$"
  mkdir -p "$(dirname "$f")" 2>/dev/null
  printf 'pid=%s starttime=%s exe=%s script=%s job=%s ts=%s %s\n' \
    "$p" "$st" "$(proc_exe "$p")" "${JOB_SCRIPT[$i]}" "${JOB_NAME[$i]}" \
    "$(now_iso)" "$*" >"$tmp" 2>/dev/null && mv -f "$tmp" "$f" 2>/dev/null
}

# scan_matches IDX —— 全 /proc 精确扫描（排除自身与父进程），结果写入 SCAN_PIDS
SCAN_PIDS=""
scan_matches() {
  local i="$1" d p
  SCAN_PIDS=""
  for d in /proc/[0-9]*; do
    p="${d#/proc/}"
    if [ "$p" = "$$" ] || [ "$p" = "${PPID:-0}" ]; then continue; fi   # 排除自身/父进程
    verify_pid_for_job "$i" "$p" || continue
    SCAN_PIDS+="$p "
  done
  SCAN_PIDS="${SCAN_PIDS% }"
}

# scan_matches IDX —— 全 /proc 中**通过完整身份核对**的 pid（v2 语义：selftest/清理用）
SCAN_PIDS=""
scan_matches() {
  local i="$1" d p
  SCAN_PIDS=""
  for d in /proc/[0-9]*; do
    p="${d#/proc/}"
    if [ "$p" = "$$" ] || [ "$p" = "${PPID:-0}" ]; then continue; fi   # 排除自身/父进程
    verify_pid_for_job "$i" "$p" || continue
    SCAN_PIDS+="$p "
  done
  SCAN_PIDS="${SCAN_PIDS% }"
}
# scan_candidates IDX —— "看起来像该作业"的**全部** pid（argv 命中 abs/relpath/basename）
#   v3 新增：认领判决要能对**被拒**的候选出证（keeper_adopt_refused），所以先收全候选
#   再逐个过身份分级，而不是像 v2 那样先过滤掉再看不见拒绝理由。
CAND_PIDS=""
scan_candidates() {
  local i="$1" d p t hit
  CAND_PIDS=""
  for d in /proc/[0-9]*; do
    p="${d#/proc/}"
    if [ "$p" = "$$" ] || [ "$p" = "${PPID:-0}" ]; then continue; fi
    proc_exists "$p" || continue
    hit=0
    while IFS= read -r t; do
      [ -n "$t" ] || continue
      if [ "$t" = "${JOB_SCRIPT[$i]}" ] || [ "$t" = "${JOB_BASENAME[$i]}" ] \
         || [ "$t" = "${JOB_RELPATH[$i]}" ]; then hit=1; break; fi
    done < <(proc_cmdline_tokens "$p")
    [ "$hit" = 1 ] && CAND_PIDS+="$p "
  done
  CAND_PIDS="${CAND_PIDS% }"
}

# --------------------------------------------------------------- 检测器输出 --
DET_STATE="absent"      # running | absent | ambiguous
DET_PID=""
DET_WHY=""
DET_EV='{}'
DET_TRUST=""            # token | path_verified | relative_cwd_ok | legacy_loose | ""
DET_REFUSED_JSON='[]'   # v3.1：本轮被拒认领的候选（c1 冒充防护证据）
DET_REFUSED_N=0
PF_REJECT=""            # PID 文件被否掉的原因（保留进认领证据，便于追责）
PF_REJECT_PID=""
# refuse_adopt CAND_PID REASON SOURCE EXTRA_EV —— 拒绝认领一个候选（R3 事件）
refuse_adopt() {
  local cand="$1" reason="$2" src="$3" extra="${4:-}"
  audit keeper_adopt_refused "$cand" - \
    "$(evcat "$(ev candidate_pid="$cand" reason="$reason" source="$src" action=refuse_adopt no_pidfile_write=1)" "$extra")" \
    process "$(ev candidate_pid="$cand" reason="$reason" source="$src")"
}
# note_adopt_unverified CAND_PID TRUST SOURCE —— 无 token 的 legacy 认领：必须显式出证
note_adopt_unverified() {
  local cand="$1" trust="$2" src="$3" extra="${4:-}"
  audit adopt_unverified "$cand" "$cand" \
    "$(evcat "$(ev candidate_pid="$cand" trust="$trust" source="$src" action=adopt_with_flag note="无环境指纹 token：按 legacy 通道认领，已标注不可验证，绝不静默 running")" "$extra")"
}
# detect_job IDX —— 只在"看到的事实"上作判断，绝不推断
detect_job() {
  local i="$1" n cand
  DET_STATE="absent"; DET_PID=""; DET_WHY=""; DET_EV='{}'; DET_TRUST=""
  PF_REJECT=""; PF_REJECT_PID=""; DET_REFUSED_JSON='[]'; DET_REFUSED_N=0
  # ---------------- 路径 1：PID 文件 ----------------
  if pidfile_read "$i"; then
    n="${PF_PID}"
    if verify_pid_for_job "$i" "$n"; then
      local st; st="$(proc_starttime "$n" 2>/dev/null || printf '?')"
      # v3.1 [K]：世代绑定——世代记录存在时，pidfile 指向的 pid/starttime 必须与记录相符，
      # 否则该 pidfile 是伪造/陈旧的（偷 token 的冒充者最可能的入口）⇒ 拒绝并落到扫描路径。
      gen_match "$i" "$n" "$st" "$(tok_hash "$IDF_TOKEN_RAW")" >/dev/null
      if [ "$GEN_MATCH" = token_only ] || [ "$GEN_MATCH" = stale ]; then
        PF_REJECT="pidfile_generation_mismatch"; PF_REJECT_PID="$n"
        refuse_adopt "$n" "pidfile_generation_mismatch" pidfile \
          "$(ev gen_match="$GEN_MATCH" gen_note="$GEN_MATCH_NOTE" gen_pid="$GEN_PID" gen_starttime="$GEN_START" pf_pid="$n" pf_starttime="${PF_START:-?}" proc_starttime="$st" trust="${VERIFY_TRUST:-none}" id_factors="$(printf '%s' "$IDF_JSON")" cmdline="$(proc_cmdline_text "$n")" action=fallthrough_scan note="pidfile 指向的进程不在 keeper 亲启世代（token 单证不足以认领）")"
      else
      if [ "$PF_START_VALID" = 1 ] && [ "$st" = "$PF_START" ]; then
        DET_STATE="running"; DET_PID="$n"; DET_WHY="pidfile_verified"; DET_TRUST="$VERIFY_TRUST"
        DET_EV="$(ev source=pidfile pid="$n" starttime="$PF_START" trust="$VERIFY_TRUST" gen_match="$GEN_MATCH" id_factors="$(printf '%s' "$IDF_JSON")" cmdline="$(proc_cmdline_text "$n")")"
        return
      fi
      if [ "$VERIFY_TRUST" = token ]; then
        # token 环境指纹吻合 + 世代相符 ⇒ 即便 starttime 记录失效/不符也是"我们的这个作业"
        # （PID 复用 + keeper 重启场景）。认领并**修复** pidfile。
        # v3.1 [K]：无世代记录时不再静默 refresh ⇒ 见下面 gen_match=none 分支。
        DET_STATE="running"; DET_PID="$n"; DET_TRUST=token
        if [ "$PF_START_VALID" = 1 ]; then DET_WHY="pidfile_token_verified_starttime_refresh"
        else DET_WHY="pidfile_token_verified_starttime_repaired"; fi
        DET_EV="$(ev source=pidfile pid="$n" pf_starttime="${PF_START:-?}" proc_starttime="$st" trust=token gen_match="$GEN_MATCH" decision=adopt_token_repair cmdline="$(proc_cmdline_text "$n")")"
        if [ "$GEN_MATCH" = none ]; then
          note_adopt_unverified "$n" "$VERIFY_TRUST" "pidfile_generation_unbound" \
            "$(ev gen_match=none gen_note="$GEN_MATCH_NOTE" note="token 单证（无 keeper 亲启世代记录）：认领但显式标注不可验证，非静默")"
        fi
        return
      fi
      # 无 token 而 starttime 无效/不符 ⇒ pidfile **不可信**（v2 在此静默认领 ⇒ S2 洞）
      if [ "$PF_START_VALID" = 1 ]; then
        PF_REJECT="pidfile_starttime_mismatch"; PF_REJECT_PID="$n"
        audit pidfile_untrusted "$n" - \
          "$(ev source=pidfile pid="$n" pf_starttime="$PF_START" proc_starttime="$st" reason=starttime_mismatch_no_token trust="${VERIFY_TRUST:-none}" gen_match="$GEN_MATCH" id_factors="$(printf '%s' "$IDF_JSON")" action=fallthrough_scan note="v2 在此按 PID 复用静默认领（R1 crit S2）；v3 降级为不可信，改走扫描+分级证据")"
      else
        PF_REJECT="pidfile_starttime_invalid"; PF_REJECT_PID="$n"
        audit pidfile_untrusted "$n" - \
          "$(ev source=pidfile pid="$n" pf_starttime="${PF_START:-empty}" reason=starttime_invalid_no_token trust="${VERIFY_TRUST:-none}" gen_match="$GEN_MATCH" id_factors="$(printf '%s' "$IDF_JSON")" action=fallthrough_scan note="starttime 缺失/\`?\` 不再被当作'跳过校验'（R1 crit E3.3）")"
      fi
      fi   # end v3.1 世代绑定
      # 落到路径 2
    else
      # PID 文件指向的进程不成立：stale / PID 复用 / 无关进程
      if proc_exists "$n"; then
        PF_REJECT="pidfile_stale_${VERIFY_WHY}"; PF_REJECT_PID="$n"
        audit pidfile_stale "$n" - "$(ev pid="$n" reason="${VERIFY_WHY}" id_factors="$(printf '%s' "$IDF_JSON")" cmdline="$(proc_cmdline_text "$n")" pf_raw="${PF_RAW:0:160}")"
      else
        PF_REJECT="pidfile_dead"; PF_REJECT_PID="$n"
      fi
    fi
  fi
  # ---------------- 路径 2：扫描认领（分级证据 + 拒绝出证） ----------------
  scan_candidates "$i"
  local accepted="" acc_trust="" strong="" strong_trust="" refused_ev="" cnt=0 acc_cnt=0
  local gen_strong=0 cand_st cand_th
  for cand in $CAND_PIDS; do
    cnt=$((cnt+1))
    if ! verify_pid_for_job "$i" "$cand"; then
      case "$VERIFY_WHY" in
        token_job_mismatch*)      # 冒充者：token 说它是别的作业
          refuse_adopt "$cand" "token_job_mismatch" scan "$(ev id_factors="$(printf '%s' "$IDF_JSON")" cmdline="$(proc_cmdline_text "$cand")")"
          refused_ev+="$(ev pid="$cand" reason=token_job_mismatch) "
          continue ;;
        argv_basename_only_cwd_mismatch)
          refuse_adopt "$cand" "basename_only_cwd_mismatch" scan "$(ev id_factors="$(printf '%s' "$IDF_JSON")" cmdline="$(proc_cmdline_text "$cand")")"
          refused_ev+="$(ev pid="$cand" reason=basename_only_cwd_mismatch) "
          continue ;;
        token_format_mismatch)
          refuse_adopt "$cand" "token_format_mismatch" scan "$(ev id_factors="$(printf '%s' "$IDF_JSON")" cmdline="$(proc_cmdline_text "$cand")")"
          refused_ev+="$(ev pid="$cand" reason=token_format_mismatch) "
          continue ;;
      esac
      continue      # 其他不符（argv 不匹配等）：不算候选，静默跳过
    fi
    # ---- v3.1 [K]：世代绑定（token 单证不得静默认领）----
    cand_st="$(proc_starttime "$cand" 2>/dev/null || printf '?')"
    cand_th="$(tok_hash "$IDF_TOKEN_RAW")"
    gen_match "$i" "$cand" "$cand_st" "$cand_th" >/dev/null
    if [ "$KEEPER_GEN_BIND" = 1 ] && [ "$GEN_MATCH" != exact ] && [ "$GEN_MATCH" != none ]; then
      # 身份证据看起来像本作业（argv/cwd/exe 乃至 token 都命中），但不在 keeper 亲启世代里：
      # 典型形态 = 偷了 token 的冒充者 / 手工另起的副本 / PID+starttime 不可关联的进程。
      # 拒绝认领（绝不静默），真作业若缺席则由 cycle 的重启路径照常拉起。
      local rsn="stranger_candidate"
      [ "$GEN_MATCH" = token_only ] && rsn="stranger_token"
      refuse_adopt "$cand" "$rsn" scan \
        "$(ev gen_match="$GEN_MATCH" gen_note="$GEN_MATCH_NOTE" gen_pid="$GEN_PID" gen_starttime="$GEN_START" gen_spawn_ms="$GEN_MS" cand_pid="$cand" cand_starttime="$cand_st" trust="$VERIFY_TRUST" id_factors="$(printf '%s' "$IDF_JSON")" cmdline="$(proc_cmdline_text "$cand")" action=refuse_adopt note="世代记录存在且不一致：token/路径证据不足以认领（c1 冒充防护）")"
      refused_ev+="$(ev pid="$cand" reason="$rsn" gen_match="$GEN_MATCH") "
      continue
    fi
    case "$VERIFY_TRUST" in
      token)           strong+="$cand "; strong_trust=token ;;
      path_verified|relative_cwd_ok)
        if [ "$GEN_MATCH" = exact ]; then strong+="$cand "; strong_trust="$VERIFY_TRUST"; gen_strong=1
        else accepted+="$cand "; acc_trust="$VERIFY_TRUST"; acc_cnt=$((acc_cnt+1)); fi ;;
      legacy_loose)
        if [ "$KEEPER_ADOPT_LEGACY" = 1 ] && [ "$KEEPER_ADOPT_LOOSE" = 1 ]; then
          # 不在此出证：认领与否要等"唯一候选"判决；出证统一放在 cycle 的认领分支
          # （保证每次认领恰好一条 adopt_unverified，且扫描/pidfile 两条路都不漏）
          accepted+="$cand "; acc_trust=legacy_loose; acc_cnt=$((acc_cnt+1))
        else
          refuse_adopt "$cand" "loose_adoption_denied" scan "$(ev id_factors="$(printf '%s' "$IDF_JSON")" keeper_adopt_legacy="$KEEPER_ADOPT_LEGACY" keeper_adopt_loose="$KEEPER_ADOPT_LOOSE")"
          refused_ev+="$(ev pid="$cand" reason=loose_adoption_denied) "
        fi ;;
      *)  refuse_adopt "$cand" "identity_insufficient(${VERIFY_WHY})" scan "$(ev id_factors="$(printf '%s' "$IDF_JSON")")"
          refused_ev+="$(ev pid="$cand" reason=identity_insufficient) " ;;
    esac
  done
  local scnt=0; for cand in $strong; do scnt=$((scnt+1)); done
  local refused_json='[]'; [ -n "$refused_ev" ] && refused_json="[$refused_ev]"
  DET_REFUSED_JSON="$refused_json"
  DET_REFUSED_N="$(printf '%s' "$refused_json" | grep -o '"pid":' | wc -l | tr -d ' ')"
  if [ "$scnt" -eq 1 ]; then
    DET_STATE="running"; DET_PID="${strong% }"; DET_TRUST="$strong_trust"
    if [ "$gen_strong" = 1 ]; then DET_WHY="adopt_generation"; else DET_WHY="adopt_token"; fi
    DET_EV="$(ev source=proc_scan adopted=1 pid="$DET_PID" trust="$strong_trust" gen_bound="$([ "$gen_strong" = 1 ] && printf exact || printf token)" pidfile_rejected="${PF_REJECT:-none}" refused="$refused_json" cmdline="$(proc_cmdline_text "$DET_PID")")"
    return
  fi
  if [ "$scnt" -gt 1 ]; then
    DET_STATE="ambiguous"; DET_PID="${strong% }"; DET_WHY="multiple_token_matches"
    DET_EV="$(ev source=proc_scan candidates="${strong% }" count="$scnt" trust=token pidfile_rejected="${PF_REJECT:-none}")"
    return
  fi
  if [ "$acc_cnt" -eq 0 ]; then
    DET_STATE="absent"; DET_PID=""
    local why="${PF_REJECT:-not_found}"
    [ "$cnt" -gt 0 ] && why="${PF_REJECT:+${PF_REJECT}+}no_accepted_candidate(seen=$cnt)"
    DET_WHY="$why"
    DET_EV="$(ev source=proc_scan result=no_accepted_candidate candidates_seen="$cnt" pidfile_rejected="${PF_REJECT:-none}" refused="$refused_json" decoys="${CAND_PIDS:-none}")"
    return
  fi
  if [ "$acc_cnt" -eq 1 ]; then
    DET_STATE="running"; DET_PID="${accepted% }"; DET_TRUST="$acc_trust"
    case "$acc_trust" in
      legacy_loose) DET_WHY="adopt_legacy_loose" ;;
      *)            DET_WHY="adopt_scan" ;;
    esac
    DET_EV="$(ev source=proc_scan adopted=1 pid="$DET_PID" trust="$acc_trust" pidfile_rejected="${PF_REJECT:-none}" refused="$refused_json" cmdline="$(proc_cmdline_text "$DET_PID")")"
    return
  fi
  DET_STATE="ambiguous"; DET_PID="$accepted"; DET_WHY="multiple_matches"
  DET_EV="$(ev source=proc_scan candidates="${accepted% }" count="$acc_cnt" pidfile_rejected="${PF_REJECT:-none}" refused="$refused_json")"
}

# ================= v3.1 [J] 机器化终态判据（F6 闭环）======================
# 契约依据：audit-selfheal §4.3.2「只认结构化终态，禁止 grep 日志」；crit-correct K-3。
# 五条事实来源（优先级从高到低，全部落结构化记录）：
#   1) gen/<job>.rec    —— keeper 亲启世代（pid + starttime + token 哈希 + spawn 时刻）
#   2) wait <pid>       —— 直接子进程的真实退出码（信号死 = 128+N），并回收僵尸
#   3) jobexit/<job>.json —— 上一次 keeper 实测到的退出码（跨 keeper 重启可复核）
#   4) convergence-report.json —— decided/terminal/exit_code（新鲜度门槛见 conv_report_read）
#   5) 兼容降级（默认关）：grep 完成标记，仅当 KEEPER_COMPAT_GREP=1
GEN_PRESENT=0; GEN_PID=""; GEN_START=""; GEN_TOKHASH=""; GEN_MS=0; GEN_FILE=""; GEN_KEEPER_PID=""
GEN_MATCH="none"; GEN_MATCH_NOTE=""
CONV_OK=0; CONV_PATH=""; CONV_DECIDED=""; CONV_EXIT=""; CONV_TERMINAL=""; CONV_PID=""
CONV_RUN=""; CONV_REASON=""; CONV_MTIME_MS=0; CONV_STALE=0
V_VERDICT=""; V_EXIT=""; V_SOURCE="none"; V_DECIDED=""; V_REASON=""; V_CONFLICT=0
TERM_FINAL=0; TERM_VERDICT=""; TERM_EXIT=""; TERM_SOURCE=""; TERM_DECIDED=""; COMPAT_GREP_HIT=0
declare -A CHILD_OWNED=()          # 本 keeper 进程亲启的子进程 pid（wait 取退出码的前提）
_skey() { printf '%s/%s' "$KEEPER_STATE_DIR" "$1"; }
gen_rec_path()   { printf '%s/gen/%s.rec' "$KEEPER_STATE_DIR" "${JOB_NAME[$1]}"; }
terminal_path()  { printf '%s/terminal/%s.json' "$KEEPER_STATE_DIR" "${JOB_NAME[$1]}"; }
halt_path()      { printf '%s/halt/%s.json' "$KEEPER_STATE_DIR" "${JOB_NAME[$1]}"; }
jobexit_path()   { printf '%s/jobexit/%s.json' "$KEEPER_STATE_DIR" "${JOB_NAME[$1]}"; }
tok_hash() { # TOKEN → 16 位摘要（不落明文 token，避免 state 目录二次泄密）
  local t="${1:-}"
  [ -n "$t" ] || { printf ''; return 0; }
  printf '%s' "$t" | sha256sum 2>/dev/null | cut -c1-16
}
# gen_rec_write IDX PID TOKEN —— 记录"keeper 亲启世代"。原子写。
gen_rec_write() {
  local i="$1" p="$2" tok="${3:-}" f tmp h
  f="$(gen_rec_path "$i")"; tmp="$f.tmp.$$"; h="$(tok_hash "$tok")"
  mkdir -p "$(dirname "$f")" 2>/dev/null
  printf 'pid=%s starttime=%s tok_hash=%s spawn_ms=%s keeper_pid=%s job=%s ts=%s\n' \
    "$p" "$(proc_starttime "$p" 2>/dev/null || printf '?')" "$h" "$(now_ms)" "$$" "${JOB_NAME[$i]}" "$(now_iso)" \
    >"$tmp" 2>/dev/null && mv -f "$tmp" "$f" 2>/dev/null
  return 0
}
# gen_rec_read IDX —— 解析世代记录（缺失时 GEN_PRESENT=0，不报错）
gen_rec_read() {
  local i="$1" f w k v raw
  GEN_PRESENT=0; GEN_PID=""; GEN_START=""; GEN_TOKHASH=""; GEN_MS=0; GEN_FILE=""; GEN_KEEPER_PID=""
  f="$(gen_rec_path "$i")"; GEN_FILE="$f"
  [ -r "$f" ] || return 1
  raw="$(head -c 512 "$f" 2>/dev/null | tr -d '\n')"; [ -n "$raw" ] || return 1
  for w in $raw; do
    k="${w%%=*}"; v="${w#*=}"
    case "$k" in
      pid) GEN_PID="$v" ;; starttime) GEN_START="$v" ;; tok_hash) GEN_TOKHASH="$v" ;;
      spawn_ms) GEN_MS="$v" ;; keeper_pid) GEN_KEEPER_PID="$v" ;;
    esac
  done
  [[ "$GEN_PID" =~ ^[0-9]+$ ]] || return 1
  GEN_PRESENT=1; return 0
}
# gen_match IDX CAND_PID CAND_START CAND_TOKHASH —— 候选进程与该作业世代的符合度
#   打印 exact | stale | token_only | none（并设置 GEN_MATCH + GEN_MATCH_NOTE）
gen_match() {
  local i="$1" cp="$2" cs="${3:-}" ch="${4:-}"
  GEN_MATCH="none"; GEN_MATCH_NOTE=""
  gen_rec_read "$i" || { GEN_MATCH="none"; GEN_MATCH_NOTE="no_generation_record"; printf 'none'; return 0; }
  if [ "$cp" = "$GEN_PID" ] && [ -n "$cs" ] && [ "$cs" = "$GEN_START" ]; then
    if [ -n "$ch" ] && [ -n "$GEN_TOKHASH" ] && [ "$ch" != "$GEN_TOKHASH" ]; then
      GEN_MATCH="none"; GEN_MATCH_NOTE="same_pid_starttime_but_token_hash_differs"; printf 'none'; return 0
    fi
    GEN_MATCH="exact"; GEN_MATCH_NOTE="pid+starttime(+token_hash)_match"; printf 'exact'; return 0
  fi
  if [ -n "$ch" ] && [ -n "$GEN_TOKHASH" ] && [ "$ch" = "$GEN_TOKHASH" ]; then
    GEN_MATCH="token_only"; GEN_MATCH_NOTE="token_hash_match_pid_or_starttime_mismatch(rec_pid=$GEN_PID rec_start=$GEN_START cand_pid=$cp cand_start=${cs:-?})"
    printf 'token_only'; return 0
  fi
  GEN_MATCH="stale"; GEN_MATCH_NOTE="neither_pid_starttime_nor_token_hash(rec_pid=$GEN_PID cand_pid=$cp)"
  printf 'stale'
}
# gen_binding_ok IDX CAND_PID CAND_START CAND_TOKHASH STRENGTH —— 该候选是否可被认领为"我们的作业"
#   0=可以（exact / 无世代记录的 legacy 通道）；1=拒绝（并给出 GEN_MATCH_NOTE）
gen_binding_ok() {
  local i="$1" m
  [ "$KEEPER_GEN_BIND" = 1 ] || { GEN_MATCH="unbound"; GEN_MATCH_NOTE="gen_bind_disabled"; return 0; }
  m="$(gen_match "$i" "$2" "${3:-}" "${4:-}")"
  case "$m" in
    exact)     return 0 ;;
    none)      return 0 ;;   # 无世代记录：legacy 通道（调用方必须写 adopt_unverified）
    token_only|stale) return 1 ;;
    *)         return 1 ;;
  esac
}
# conv_report_path IDX —— 作业的结构化终态报告路径（KEEPER_CONV_REPORT 覆盖；"-"=禁用）
conv_report_path() {
  local i="$1" p="${JOB_CONV_REPORT[$i]:-}"
  [ "$p" = "-" ] && { printf ''; return 0; }
  if [ -z "$p" ]; then
    p="${KEEPER_CONV_REPORT:-}"
    [ "$p" = "-" ] && { printf ''; return 0; }
  fi
  [ -n "$p" ] || p="$(dirname "${JOB_COMPLOG[$i]}")/convergence-report.json"
  printf '%s' "$p"
}
# conv_report_read IDX SPAWN_MS JUDGE_PID —— 读结构化终态（decided/terminal/exit_code/pid/run）
#   新鲜度：mtime ≥ SPAWN_MS 或 报告内 pid == JUDGE_PID；否则 CONV_STALE=1（绝不采信陈旧结论）
conv_report_read() {
  local i="$1" spawn_ms="${2:-0}" jpid="${3:-}" kv mt
  CONV_OK=0; CONV_DECIDED=""; CONV_EXIT=""; CONV_TERMINAL=""; CONV_PID=""; CONV_RUN=""
  CONV_REASON=""; CONV_MTIME_MS=0; CONV_STALE=0; CONV_PATH="$(conv_report_path "$i")"
  [ -n "$CONV_PATH" ] && [ -r "$CONV_PATH" ] || return 1
  command -v python3 >/dev/null 2>&1 || return 1
  mt="$(stat -c %Y "$CONV_PATH" 2>/dev/null || printf 0)"; CONV_MTIME_MS=$(( mt * 1000 ))
  kv="$(timeout "$KEEPER_HB_TIMEOUT" python3 - "$CONV_PATH" <<'PY' 2>/dev/null
import json, sys
try:
    d = json.load(open(sys.argv[1], encoding="utf-8"))
except Exception:
    sys.exit(0)
if not isinstance(d, dict):
    sys.exit(0)
def g(k):
    v = d.get(k)
    return "" if v is None else v
for k in ("schema", "decided", "exit_code", "pid", "run", "reason"):
    print("conv_%s=%s" % (k, g(k)))
print("conv_terminal=%s" % (1 if d.get("terminal") else 0))
PY
)" || kv=""
  [ -n "$kv" ] || return 1
  local line k v
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    k="${line%%=*}"; v="${line#*=}"
    case "$k" in
      conv_decided) CONV_DECIDED="$v" ;; conv_exit_code) CONV_EXIT="$v" ;;
      conv_terminal) CONV_TERMINAL="$v" ;; conv_pid) CONV_PID="$v" ;;
      conv_run) CONV_RUN="$v" ;; conv_reason) CONV_REASON="$v" ;;
    esac
  done <<<"$kv"
  CONV_OK=1
  [[ "$CONV_EXIT" =~ ^-?[0-9]+$ ]] || CONV_EXIT=""
  if [ -n "$jpid" ] && [ "$CONV_PID" = "$jpid" ]; then
    CONV_STALE=0
  elif [ "$CONV_MTIME_MS" -ge "${spawn_ms:-0}" ] 2>/dev/null && [ "${spawn_ms:-0}" -gt 0 ]; then
    CONV_STALE=0
  else
    CONV_STALE=1
  fi
  return 0
}
# proc_exited PID —— 0=已退出（/proc 消失或僵尸）；1=仍在运行
proc_exited() {
  local p="$1" st
  [ -n "$p" ] || return 1
  [ -d "/proc/$p" ] || return 0
  st="$(awk '{print $3}' "/proc/$p/stat" 2>/dev/null)"
  [ "$st" = "Z" ] && return 0
  return 1
}
# job_exit_capture IDX PID —— 取退出码（wait 优先，其次持久化 jobexit 记录）；打印码或空
job_exit_capture() {
  local i="$1" p="$2" rc="" sig=0 fid
  V_SOURCE="none"
  [ -n "$p" ] || return 1
  proc_exited "$p" || return 1
  if [ "${CHILD_OWNED[$p]:-0}" = 1 ]; then
    wait "$p" 2>/dev/null; rc=$?
    if [ "$rc" != 127 ]; then
      [ "$rc" -ge 128 ] && sig=$(( rc - 128 ))
      jobexit_write "$i" "$p" "$rc" "$sig" wait
      V_SOURCE="wait"; printf '%s' "$rc"; return 0
    fi
  fi
  fid="$(jobexit_read "$i" "$p")"
  if [ -n "$fid" ]; then V_SOURCE="jobexit_record"; printf '%s' "$fid"; return 0; fi
  return 1
}
jobexit_write() { # IDX PID EXIT SIGNAL SOURCE
  local i="$1" p="$2" rc="$3" sig="$4" src="$5" f tmp
  f="$(jobexit_path "$i")"; tmp="$f.tmp.$$"
  mkdir -p "$(dirname "$f")" 2>/dev/null
  printf '{"schema":"keeper-jobexit/1","job":"%s","pid":%s,"starttime":"%s","exit_code":%s,"signal":%s,"source":"%s","keeper_pid":%s,"ts":"%s","ts_epoch":%s}\n' \
    "$(jesc "${JOB_NAME[$i]}")" "$p" "$(proc_starttime "$p" 2>/dev/null || printf '?')" "$rc" "$sig" "$(jesc "$src")" "$$" "$(now_iso)" "$(epoch_s)" \
    >"$tmp" 2>/dev/null && mv -f "$tmp" "$f" 2>/dev/null
}
# jobexit_read IDX PID —— 记录里的退出码（pid 必须与记录一致；记录缺失/不符返回空）
jobexit_read() {
  local i="$1" p="$2" f raw rp re
  f="$(jobexit_path "$i")"; [ -r "$f" ] || return 1
  raw="$(head -c 512 "$f" 2>/dev/null | tr -d '\n')"; [ -n "$raw" ] || return 1
  rp="$(printf '%s' "$raw" | sed -n 's/.*"pid":\([0-9]*\).*/\1/p')"
  re="$(printf '%s' "$raw" | sed -n 's/.*"exit_code":\([0-9]*\).*/\1/p')"
  [ -n "$re" ] || return 1
  [ -n "$p" ] && [ "$rp" != "$p" ] && return 1
  printf '%s' "$re"
}
# terminal_write IDX VERDICT EXIT SOURCE DECIDED REASON —— 终态记录（原子写；JSON 单行扁平）
terminal_write() {
  local i="$1" v="$2" x="$3" src="$4" dec="$5" rsn="$6" f tmp
  f="$(terminal_path "$i")"; tmp="$f.tmp.$$"
  mkdir -p "$(dirname "$f")" 2>/dev/null
  printf '{"schema":"keeper-job-terminal/1","job":"%s","verdict":"%s","exit_code":%s,"source":"%s","decided":"%s","reason":"%s","pid":%s,"starttime":"%s","incomplete_restarts":%s,"resume_restarts":%s,"restarts":%s,"ts":"%s","ts_epoch":%s}\n' \
    "$(jesc "${JOB_NAME[$i]}")" "$(jesc "$v")" "${x:--1}" "$(jesc "$src")" "$(jesc "${dec:-none}")" "$(jesc "$rsn")" \
    "${GEN_PID:-0}" "${GEN_START:-?}" "$(st_get "incomplete_restarts_${JOB_NAME[$i]}")" "$(st_get "resume_restarts_${JOB_NAME[$i]}")" "$(st_get "restarts_${JOB_NAME[$i]}")" \
    "$(now_iso)" "$(epoch_s)" >"$tmp" 2>/dev/null && mv -f "$tmp" "$f" 2>/dev/null
}
# terminal_read IDX —— 从磁盘终态记录恢复事实（keeper 换 state 目录/首次上任时用）
terminal_read() {
  local i="$1" f raw
  TERM_FINAL=0; TERM_VERDICT=""; TERM_EXIT=""; TERM_DECIDED=""; TERM_SOURCE=""
  f="$(terminal_path "$i")"; [ -r "$f" ] || return 1
  raw="$(head -c 1024 "$f" 2>/dev/null | tr -d '\n')"; [ -n "$raw" ] || return 1
  TERM_VERDICT="$(printf '%s' "$raw" | sed -n 's/.*"verdict":"\([a-z_]*\)".*/\1/p')"
  [ -n "$TERM_VERDICT" ] || return 1
  TERM_EXIT="$(printf '%s' "$raw" | sed -n 's/.*"exit_code":\(-\{0,1\}[0-9]*\).*/\1/p')"
  TERM_DECIDED="$(printf '%s' "$raw" | sed -n 's/.*"decided":"\([a-z_-]*\)".*/\1/p')"
  TERM_SOURCE=record; TERM_FINAL=1
  return 0
}
halt_write() { # IDX REASON DETAIL DEAD_PID
  local i="$1" rsn="$2" det="$3" dead="$4" f tmp
  f="$(halt_path "$i")"; tmp="$f.tmp.$$"
  mkdir -p "$(dirname "$f")" 2>/dev/null
  printf '{"schema":"keeper-job-halt/1","job":"%s","reason":"%s","detail":"%s","dead_pid":"%s","budgets":{"startretries":%s,"incomplete_retries":%s,"fail_count":%s,"incomplete_restarts":%s},"ts":"%s","ts_epoch":%s,"resume_hint":"bash keeper-v3.sh resume %s"}\n' \
    "$(jesc "${JOB_NAME[$i]}")" "$(jesc "$rsn")" "$(jesc "$det")" "$(jesc "${dead:-0}")" \
    "$KEEPER_START_RETRIES" "$KEEPER_INCOMPLETE_RETRIES" "$(st_get "fail_${JOB_NAME[$i]}")" "$(st_get "incomplete_restarts_${JOB_NAME[$i]}")" \
    "$(now_iso)" "$(epoch_s)" "$(jesc "${JOB_NAME[$i]}")" >"$tmp" 2>/dev/null && mv -f "$tmp" "$f" 2>/dev/null
}
# job_settled IDX —— 0=已终态/已停机（不再巡检重启）；设置 TERM_* / COMPAT_GREP_HIT
job_settled() {
  local i="$1" job="${JOB_NAME[$i]}" st
  TERM_FINAL=0; TERM_VERDICT=""; TERM_EXIT=""; TERM_SOURCE=""; TERM_DECIDED=""; COMPAT_GREP_HIT=0
  st="$(st_get "terminal_${job}")"
  case "$st" in
    converged|halted)
      TERM_FINAL=1; TERM_VERDICT="$st"; TERM_EXIT="$(st_get "exit_code_${job}")"; TERM_SOURCE=state; return 0 ;;
  esac
  if [ "$(st_get "halted_${job}")" = 1 ]; then
    TERM_FINAL=1; TERM_VERDICT="halted"; TERM_EXIT="$(st_get "exit_code_${job}")"; TERM_SOURCE=state; return 0
  fi
  if terminal_read "$i"; then     # 磁盘终态（state 被清/新 keeper 上任）
    st_set "terminal_${job}" "$TERM_VERDICT"; st_set "exit_code_${job}" "${TERM_EXIT:--1}"
    return 0
  fi
  if [ "$KEEPER_COMPAT_GREP" = 1 ] && job_complete "$i"; then
    TERM_FINAL=1; TERM_VERDICT="completed_marker"; TERM_SOURCE=compat_grep; COMPAT_GREP_HIT=1; return 0
  fi
  return 1
}
job_settled_state() { # IDX → completed | halted
  local i="$1"
  case "$(st_get "terminal_${JOB_NAME[$i]}")" in
    halted) printf 'halted' ;;
    *)      [ "$(st_get "halted_${JOB_NAME[$i]}")" = 1 ] && printf 'halted' || printf 'completed' ;;
  esac
}
# job_verdict_compute IDX —— 汇总"这个作业为什么结束了"（退出码 + 结构化终态 + 冲突标注）
job_verdict_compute() {
  local i="$1" job="${JOB_NAME[$i]}" rc="" conv_code="" gpid gstart
  V_VERDICT="unknown"; V_EXIT=""; V_SOURCE="none"; V_DECIDED=""; V_REASON=""; V_CONFLICT=0
  gen_rec_read "$i"; gpid="${GEN_PID:-}"; gstart="${GEN_START:-}"
  # (1) 退出码：wait（OS 事实）→ jobexit 记录
  if rc="$(job_exit_capture "$i" "$gpid")"; then :; else rc=""; fi
  # (2) 结构化终态报告（宽限窗口内等它落盘）
  conv_report_read "$i" "${GEN_MS:-0}" "$gpid" || true
  if [ "$CONV_OK" = 1 ] && [ "$CONV_STALE" != 1 ] && [ "$CONV_TERMINAL" = 1 ] && [ -z "$rc" ]; then
    local w=0
    while [ "$w" -lt $(( KEEPER_EXIT_GRACE_MS )) ] && [ -z "$CONV_EXIT" ]; do
      sleep 0.25 9>&-; w=$(( w + 250 ))
      conv_report_read "$i" "${GEN_MS:-0}" "$gpid" || break
      [ "$CONV_TERMINAL" = 1 ] || break
    done
  fi
  [ -n "$rc" ] && { V_EXIT="$rc"; V_SOURCE="$( [ "${V_SOURCE:-none}" = none ] && printf wait || printf '%s' "$V_SOURCE" )"; }
  if [ "$CONV_OK" = 1 ] && [ "$CONV_STALE" = 1 ] && [ -n "$CONV_DECIDED" ]; then
    # 审计节流：同一 mtime 只出证一次（否则每轮巡检都会刷同一条 stale 告警）
    local key_stale="stale_report_mtime_${job}"
    if [ "$(st_get "$key_stale")" != "$CONV_MTIME_MS" ]; then
      st_set "$key_stale" "$CONV_MTIME_MS"
      audit keeper_terminal_report_stale "$gpid" - \
        "$(ev job="$job" report="$CONV_PATH" report_pid="${CONV_PID:-0}" report_decided="${CONV_DECIDED:-none}" spawn_ms="${GEN_MS:-0}" mtime_ms="$CONV_MTIME_MS" action=ignore note="结构化终态报告早于本次 spawn 或 pid 不可关联 ⇒ 标 stale，不采信（同 mtime 只审计一次）")"
    fi
  fi
  if [ "$CONV_OK" = 1 ] && [ "$CONV_STALE" != 1 ] && [ "$CONV_TERMINAL" = 1 ]; then
    V_DECIDED="$CONV_DECIDED"; V_REASON="${CONV_REASON:-report_terminal}"
    conv_code="$CONV_EXIT"
    if [ -z "$V_EXIT" ] && [ -n "$conv_code" ]; then V_EXIT="$conv_code"; V_SOURCE="convergence_report"; fi
    [ -n "$V_EXIT" ] && [ -n "$conv_code" ] && [ "$V_EXIT" != "$conv_code" ] && V_CONFLICT=1
  fi
  [ -n "$V_SOURCE" ] || V_SOURCE="none"
  if [ "$V_DECIDED" = done ] && [ "$CONV_EXIT" = 0 ] && [ "$CONV_OK" = 1 ] && [ "$CONV_STALE" != 1 ]; then
    # converge 自己的终态报告（decided=done & exit 0）是权威机器结论：即便被信号打断，
    # 数据也已收敛（仅记 channel_conflict 证据，不误判为崩溃而重启）。
    V_VERDICT="converged"; [ -z "$V_EXIT" ] && V_EXIT=0
    [ "$V_EXIT" != 0 ] && V_CONFLICT=1
    return 0
  fi
  case "${V_EXIT:-}" in
    '')  V_VERDICT="unknown" ;;
    0)   V_VERDICT="converged" ;;
    3)   V_VERDICT="incomplete" ;;
    4)   V_VERDICT="stall_stop" ;;
    12[6-9]|1[3-9][0-9]|2[0-5][0-9]) V_VERDICT="signal" ;;
    *)   V_VERDICT="crash" ;;
  esac
  [ "$V_VERDICT" = converged ] && [ "$V_DECIDED" != done ] && [ -n "$V_DECIDED" ] && V_CONFLICT=1
  return 0
}
# job_stop_converged IDX EXIT SOURCE DECIDED —— 完成停止（终态审计 + 状态文件）
job_stop_converged() {
  local i="$1" x="$2" src="$3" dec="$4" job="${JOB_NAME[$i]}"
  st_set "terminal_${job}" converged; st_set "exit_code_${job}" "${x:--1}"
  st_set "last_state_${job}" completed; st_set "fail_${job}" 0
  terminal_write "$i" converged "$x" "$src" "$dec" "converged_p0"
  audit keeper_job_stop "${GEN_PID:-}" - \
    "$(evcat "$(ev job="$job" verdict=converged exit_code="${x:--1}" decided="${dec:-none}" source="$src" action=stop_no_restart generation="${GEN_PID:-0}" gen_starttime="${GEN_START:-?}" terminal_file="$(terminal_path "$i")" note="机器判据：退出码 0 / decided=done ⇒ 完成停机（不再重启）；grep 标记不作为主判据")" "$(comp_evidence "$i")")" \
    process "$(ev job="$job" verdict=converged exit_code="${x:--1}" decided="${dec:-none}" source="$src")"
}
# job_halt IDX REASON DETAIL DEAD_PID —— 停机告警（停止重启 + 状态文件 + 审计）
job_halt() {
  local i="$1" rsn="$2" det="$3" dead="${4:-}" job="${JOB_NAME[$i]}"
  st_set "halted_${job}" 1; st_set "halt_reason_${job}" "$rsn"; st_set "last_state_${job}" halted
  halt_write "$i" "$rsn" "$det" "$dead"
  audit keeper_job_halt "$dead" - \
    "$(ev job="$job" reason="$rsn" detail="$det" dead_pid="${dead:-0}" action=stop_restarting halted=1 halt_file="$(halt_path "$i")" startretries="$KEEPER_START_RETRIES" incomplete_retries="$KEEPER_INCOMPLETE_RETRIES" fail_count="$(st_get "fail_${job}")" note="重启预算/有限重启预算耗尽 ⇒ 停止重启并升级告警（防空转循环）；解除：keeper-v3.sh resume $job")" \
    process "$(ev job="$job" reason="$rsn" action=halt)"
}
# job_restart_audit IDX FROM_PID TO_PID REASON KIND EXTRA_EV —— 统一的 keeper_restart 审计
job_restart_audit() {
  local i="$1" from="$2" to="$3" rsn="$4" kind="$5" extra="${6:-}"
  audit restart "$from" "$to" \
    "$(evcat "$(ev reason="$rsn" kind="$kind" script="${JOB_SCRIPT[$i]}" total_restarts="$(st_get "restarts_${JOB_NAME[$i]}")" spawn_verify_ms="${SPAWN_ALIVE_MS:-0}" deadline_ms=$((KEEPER_RECOVERY_DEADLINE*1000)) spawn_trust="${SPAWN_TRUST:-unknown}")" "$extra")" \
    process "$(ev from_pid="${from:-0}" to_pid="$to" reason="$rsn" kind="$kind" recovery_ms="${SPAWN_ALIVE_MS:-0}")"
}
# job_limited_restart IDX DEAD_PID KIND(exit3|exit4) EXIT DECIDED —— 有限重启；超限 ⇒ 停机告警
#   0=已重启 1=已停机（调用方据此 continue）
job_limited_restart() {
  local i="$1" dead="$2" kind="$3" x="$4" dec="$5" job="${JOB_NAME[$i]}"
  local key limit counter rc t0 t1
  case "$kind" in
    exit3) key="incomplete_restarts_${job}"; limit="$KEEPER_INCOMPLETE_RETRIES" ;;
    exit4) key="resume_restarts_${job}";     limit="$KEEPER_START_RETRIES" ;;
    *)     key="incomplete_restarts_${job}"; limit="$KEEPER_INCOMPLETE_RETRIES" ;;
  esac
  counter="$(st_get "$key")"
  if [ "$counter" -ge "$limit" ] 2>/dev/null; then
    st_set "exit_code_${job}" "${x:--1}"
    job_halt "$i" "${kind}_budget_exhausted" "attempts=$counter/$limit exit_code=${x:--1} decided=${dec:-none}" "$dead"
    audit keeper_restart_budget_exhausted "$dead" - \
      "$(ev job="$job" kind="$kind" attempts="$counter" limit="$limit" exit_code="${x:--1}" action=halt note="有限重启预算耗尽：停机告警而非无界重启（K-3 的'无界重启循环'在此闭环）")" \
      process "$(ev job="$job" kind="$kind" attempts="$counter" limit="$limit")"
    return 1
  fi
  st_set "$key" "$(( counter + 1 ))"
  st_set "fail_${job}" 0            # 判据重启不是崩溃，重置秒死计数
  audit keeper_restart_budget "$dead" - \
    "$(ev job="$job" kind="$kind" attempt="$(( counter + 1 ))" limit="$limit" exit_code="${x:--1}" decided="${dec:-none}" note="按退出码判据的有限重启（resume/未收敛重试）")"
  t0="$(now_ms)"; spawn_job "$i"; rc=$?; t1="$(now_ms)"; SPAWN_ALIVE_MS=$(( t1 - t0 ))
  if [ "$rc" = 0 ]; then
    pidfile_write "$i" "$SPAWN_PID" spawned_by=keeper
    st_set "restarts_${job}" "$(( $(st_get "restarts_${job}") + 1 ))"
    st_set "last_spawn_ms_${job}" "$t1"; st_set "last_pid_${job}" "$SPAWN_PID"; st_set "last_alive_ms_${job}" "$t1"
    st_set "last_state_${job}" running
    job_restart_audit "$i" "$dead" "$SPAWN_PID" "${kind}_verdict(exit=${x:--1})" "$kind" \
      "$(ev exit_code="${x:--1}" decided="${dec:-none}" verdict_source="${V_SOURCE:-none}" note="退出码判据驱动的重启（非崩溃路径）")"
    CYCLE_SPAWNS=$((CYCLE_SPAWNS+1))
    heartbeat_cycle "$i" "$SPAWN_PID" "${SPAWN_TRUST:-unknown}"
  elif [ "$rc" = 2 ]; then
    st_set "last_state_${job}" dry_run
    audit restart_dry_run "$dead" - "$(ev reason="${kind}_verdict" detail="$SPAWN_WHY")"
  else
    st_set "last_state_${job}" spawn_failed; st_set "last_spawn_ms_${job}" "$t1"
    audit restart_failed "$dead" - "$(ev reason="${kind}_verdict" spawn_why="$SPAWN_WHY" spawn_ms="$SPAWN_ALIVE_MS")"
  fi
  return 0
}
SPAWN_ALIVE_MS=0; SPAWN_TOKEN=""

# --------------------------------------------------------------- 完成守卫 --
# 守卫语义（与 v1 一致，刻意保持）：完成标记存在 ⇒ 永不重启
job_complete() {
  local i="$1" cl="${JOB_COMPLOG[$1]}" cp="${JOB_COMPPAT[$1]}"
  [ -n "$cp" ] || return 1
  [ -f "$cl" ] || return 1
  grep -qF -- "$cp" "$cl" 2>/dev/null
}
comp_evidence() { # IDX → evidence json
  local i="$1" cl="${JOB_COMPLOG[$1]}" cp="${JOB_COMPPAT[$1]}" lines=0 present=no
  [ -f "$cl" ] || { printf '%s' "$(ev comp_log="$cl" exists=no pattern="$cp")"; return; }
  lines="$(wc -l <"$cl" 2>/dev/null | tr -d ' ')"
  grep -qF -- "$cp" "$cl" 2>/dev/null && present=yes
  printf '%s' "$(ev comp_log="$cl" exists=yes lines="${lines:-0}" pattern="$cp" marker_present="$present")"
}

# ---------------------------------------------------------------- 状态持久化 --
declare -A STATE=()
state_load() {
  local f="$KEEPER_STATE_DIR/keeper-v3.state" line k v imported=no
  if [ ! -r "$f" ] && [ -r "$KEEPER_STATE_DIR/keeper-v2.state" ]; then
    # v3 上任首次启动：继承 v2 状态（重启计数/崩溃循环历史/最后 pid），
    # 避免"换了 keeper 就忘了抖动历史"；只读继承，随后一律写 v3 自己的状态文件。
    f="$KEEPER_STATE_DIR/keeper-v2.state"; imported=yes
  fi
  [ -r "$f" ] || return 0
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    k="${line%%=*}"; v="${line#*=}"
    STATE["$k"]="$v"
  done <"$f"
  if [ "$imported" = yes ]; then
    audit state_imported "" - "$(ev from=keeper-v2.state to=keeper-v3.state keys="${#STATE[@]}" note="v3 首次上任继承 v2 状态（只读），随后写入 v3 状态文件")"
  fi
}
state_save() {
  local f="$KEEPER_STATE_DIR/keeper-v3.state" tmp="$KEEPER_STATE_DIR/keeper-v3.state.tmp.$$" k
  : >"$tmp" 2>/dev/null || return 0
  for k in "${!STATE[@]}"; do printf '%s=%s\n' "$k" "${STATE[$k]}" >>"$tmp"; done
  mv -f "$tmp" "$f" 2>/dev/null
}
st_get() { printf '%s' "${STATE[$1]:-0}"; }
st_set() { STATE["$1"]="$2"; }

# --------------------------------------------------------------- 环境指纹 --
mount_lines() { grep -E '[[:space:]]/tmp(/[^[:space:]]*)?[[:space:]]' /proc/self/mountinfo 2>/dev/null | head -8; }
env_fingerprint_json() {
  local cwd ns_pid mlines joined="" l n=0 probes='{' sep='' i
  cwd="$(pwd -P 2>/dev/null)"
  ns_pid="$(readlink /proc/self/ns/pid 2>/dev/null || printf 'unknown')"
  mlines="$(mount_lines)"
  while IFS= read -r l; do
    [ -n "$l" ] || continue
    n=$((n+1)); joined+="${sep}$(jesc "$l")"; sep='|'
  done <<<"$mlines"
  sep=''
  for ((i=0; i<${#JOB_NAME[@]}; i++)); do
    local se=no ce=no cw=no
    [ -f "${JOB_SCRIPT[$i]}" ] && se=yes
    [ -f "${JOB_COMPLOG[$i]}" ] && ce=yes
    [ -d "${JOB_CWD[$i]}" ] && cw=yes
    probes+="$sep\"$(jesc "${JOB_NAME[$i]}")\":$(ev script_visible="$se" comp_log_visible="$ce" cwd_visible="$cw")"
    sep=','
  done
  probes+='}'
  printf '{"cwd":"%s","script_dir":"%s","ns_mnt":"%s","ns_pid":"%s","pid":%s,"ppid":%s,"user":"%s","host":"%s","mount_line_count":%s,"mount_fp":"%s","mount_lines":"%s","jobs":%s}' \
    "$(jesc "$cwd")" "$(jesc "$KEEPER_DIR")" "$(jesc "$NS_MNT")" "$(jesc "$ns_pid")" \
    "$$" "${PPID:-0}" "$(jesc "$(id -un 2>/dev/null || printf '?')")" "$(jesc "$HOST_S")" \
    "$n" "$(printf '%s' "$joined" | sha256sum 2>/dev/null | cut -c1-16)" "$joined" "$probes"
}
startup_audit_block() {
  # 逐行装进数组后统一 printf 输出：避免 $(printf ...) 吃掉行尾换行导致整块挤成一行
  local f i line
  local -a L=()
  L+=("===== $KEEPER_VERSION  $(now_iso) run_id=$RUN_ID env=$KEEPER_ENV_TAG =====")
  L+=("pid=$$ ppid=${PPID:-0} user=$(id -un 2>/dev/null) host=$HOST_S tick=${KEEPER_TICK}s deadline=${KEEPER_RECOVERY_DEADLINE}s max_backoff=${KEEPER_MAX_BACKOFF}s")
  L+=("cwd(realpath)=$(pwd -P)")
  L+=("script_dir=$KEEPER_DIR")
  L+=("conf=$KEEPER_CONF")
  L+=("mount namespace: mnt=$NS_MNT pid=$(readlink /proc/self/ns/pid 2>/dev/null)")
  L+=("-- /tmp 挂载视图关键行 (mountinfo) --")
  while IFS= read -r line; do [ -n "$line" ] && L+=("  $line"); done < <(mount_lines)
  L+=("-- 作业可见性探针 --")
  for ((i=0; i<${#JOB_NAME[@]}; i++)); do
    L+=("  [${JOB_NAME[$i]}] script=${JOB_SCRIPT[$i]} script_visible=$([ -f "${JOB_SCRIPT[$i]}" ] && echo yes || echo NO) cwd=${JOB_CWD[$i]} cwd_visible=$([ -d "${JOB_CWD[$i]}" ] && echo yes || echo NO) comp_log=${JOB_COMPLOG[$i]} comp_log_visible=$([ -f "${JOB_COMPLOG[$i]}" ] && echo yes || echo NO) completed=$(job_complete "$i" && echo yes || echo no)")
  done
  L+=("=========================================================")
  for f in $KEEPER_STARTUP_AUDIT; do
    mkdir -p "$(dirname "$f")" 2>/dev/null
    printf '%s\n' "${L[@]}" >>"$f" 2>/dev/null
  done
  printf '%s\n' "${L[@]}"
}
# 返回 0=环境正常；1=有作业路径不可见（写入全局 ENV_MISSING）
ENV_MISSING=""
env_probe() {
  local i miss=""
  for ((i=0; i<${#JOB_NAME[@]}; i++)); do
    # 已终态/已停机作业（守卫判定无需再动作）不参与探针：其脚本被归档/改名属正常，
    # 不能因此判定"环境异常"（否则 STRICT_ENV=1 会在收尾阶段误退出）。
    # v3.1：判据与 cycle 一致（job_settled = 机器终态 / 停机 / 兼容 grep）。
    job_settled "$i" && continue
    [ -f "${JOB_SCRIPT[$i]}" ] || miss+="${JOB_NAME[$i]}:script(${JOB_SCRIPT[$i]}) "
    [ -d "${JOB_CWD[$i]}" ] || miss+="${JOB_NAME[$i]}:cwd(${JOB_CWD[$i]}) "
  done
  ENV_MISSING="${miss% }"
  [ -z "$ENV_MISSING" ]
}

# ------------------------------------------------------------------- spawn --
SPAWN_WHY=""; SPAWN_PID=""; SPAWN_CHILD_PID=""; SPAWN_TRUST=""
# 子进程先把自己的 pid + starttime 写进 PID 文件再 exec（规避 setsid fork 后 $! 非目标 pid
# 的坑）。v3 增补：写 starttime（v2 只写 pid/job/ts ⇒ 读侧成了 `starttime=?` 的失效状态）
# 与 trust 字段；starttime 用 /proc/self/stat 第 22 字段（= 去掉 "pid (comm) " 后的第 20 项）。
LAUNCH_SHIM='pf=$1; shift; job=$1; shift; st=$(sed "s/.*) //" /proc/self/stat 2>/dev/null | cut -d" " -f20); printf "pid=%s starttime=%s job=%s ts=%s trust=%s token=%s\n" "$$" "${st:-?}" "$job" "$(date -u "+%Y-%m-%dT%H:%M:%S")" "${SCRAPE_KEEPER_TRUST:-spawned}" "${SCRAPE_KEEPER_TOKEN:0:40}" > "$pf.tmp.$$" 2>/dev/null && mv -f "$pf.tmp.$$" "$pf" 2>/dev/null; exec "$@"'
# keeper family id：跨 keeper 重启保持稳定（state 目录内持久化），使存活作业的 token
# 在新 keeper 上任后仍可被识别为"本 family 的作业"，而不是被当成冒充者拒绝（防重复 spawn）
KEEPER_FAMILY=""
keeper_family() {
  local f="$KEEPER_FAMILY_FILE" v=""
  if [ -r "$f" ]; then v="$(head -c 64 "$f" 2>/dev/null | tr -d '\n')"; fi
  if [ -z "$v" ]; then
    v="kf$(date +%s)-$$"
    mkdir -p "$(dirname "$f")" 2>/dev/null
    printf '%s\n' "$v" >"$f.tmp.$$" 2>/dev/null && mv -f "$f.tmp.$$" "$f" 2>/dev/null
  fi
  KEEPER_FAMILY="$v"; printf '%s' "$v"
}
# spawn_generation_bind IDX PID TOKEN —— keeper 亲启世代的机器指纹 + 子进程登记
#   · gen/<job>.rec 是"这个进程由 keeper 拉起"的唯一凭据（c1 冒充防护的锚点）；
#   · CHILD_OWNED 用于后续 `wait` 取真实退出码（作业是 keeper 的直接子进程；setsid 不 fork）。
spawn_generation_bind() {
  local i="$1" p="$2" tok="${3:-}"
  gen_rec_write "$i" "$p" "$tok"
  if [ -n "${SPAWN_CHILD_PID:-}" ] && [ "$SPAWN_CHILD_PID" = "$p" ]; then
    CHILD_OWNED["$p"]=1
  fi
  return 0
}
spawn_job() { # IDX → 0=成功(SPAWN_PID)；2=dry-run；1/3=失败
  local i="$1" exe script cwd out pf tok
  SPAWN_WHY=""; SPAWN_PID=""; SPAWN_CHILD_PID=""; SPAWN_TRUST=""; SPAWN_TOKEN=""
  script="${JOB_SCRIPT[$i]}"; cwd="${JOB_CWD[$i]}"; out="${JOB_OUTLOG[$i]}"
  pf="${JOB_PIDFILE[$i]}"; exe="$(job_exec_of "$i")"
  [ -f "$script" ] || { SPAWN_WHY="script_not_visible($script)"; return 1; }
  [ -d "$cwd" ] || { SPAWN_WHY="cwd_not_visible($cwd)"; return 1; }
  command -v "$exe" >/dev/null 2>&1 || { SPAWN_WHY="exec_not_found($exe)"; return 1; }
  if [ "$KEEPER_DRY_RUN" = 1 ]; then SPAWN_WHY="dry_run"; return 2; fi
  mkdir -p "$(dirname "$out")" "$(dirname "$pf")" 2>/dev/null
  rm -f "$pf" 2>/dev/null
  local launcher="setsid nohup"
  [ "$KEEPER_SETSID" = 1 ] || launcher=""
  [ -n "$KEEPER_FAMILY" ] || keeper_family >/dev/null
  tok="kv3:$KEEPER_FAMILY:${JOB_NAME[$i]}:${RANDOM}${RANDOM}"
  SPAWN_TOKEN="$tok"
  # shellcheck disable=SC2086
  (
    cd "$cwd" || exit 1
    exec 9>&-                 # v3 [E]：绝不把单例锁 fd 传给被看护作业（否则 job 会替 keeper 持锁）
    if [ "$KEEPER_TOKEN_ENFORCE" = 1 ]; then
      export SCRAPE_KEEPER_TOKEN="$tok" SCRAPE_KEEPER_JOB="${JOB_NAME[$i]}" \
             SCRAPE_KEEPER_RUN="$RUN_ID" SCRAPE_KEEPER_TRUST=spawned
    fi
    exec $launcher bash -c "$LAUNCH_SHIM" _ "$pf" "${JOB_NAME[$i]}" "$exe" "$script" ${JOB_EXTRA[$i]:-} >>"$out" 2>&1
  ) &
  SPAWN_CHILD_PID=$!
  local waited=0 p
  while [ "$waited" -lt $((KEEPER_SPAWN_VERIFY_S * 4)) ]; do
    sleep 0.25 9>&-            # v3.1 K-2：spawn 核对轮询不继承单例锁 fd
    waited=$((waited+1))
    if [ "$STOP_REQUESTED" = 1 ]; then SPAWN_WHY="stop_requested_during_spawn"; return 3; fi
    if pidfile_read "$i"; then
      p="$PF_PID"
      if verify_pid_for_job "$i" "$p"; then
        SPAWN_PID="$p"; SPAWN_TRUST="$VERIFY_TRUST"; SPAWN_WHY=""
        spawn_generation_bind "$i" "$p" "$tok"
        return 0
      fi
    fi
  done
  # v2 在此直接判失败；v3 兜底：pidfile 未被写好/被抢占时，扫描一次确认进程真的起来了
  # （避免"起来了但 keeper 认为失败 ⇒ 下一轮再 spawn"的重复抓取风险）
  local cand scnt=0 scpid=""
  scan_matches "$i"
  for cand in $SCAN_PIDS; do scnt=$((scnt+1)); scpid="$cand"; done
  if [ "$scnt" -eq 1 ] && [ -n "$scpid" ]; then
    SPAWN_PID="$scpid"; SPAWN_TRUST="scan_fallback"
    SPAWN_WHY="pidfile_missing_after_spawn(pf_raw=${PF_RAW:0:60})"
    spawn_generation_bind "$i" "$scpid" "$tok"
    return 0
  fi
  SPAWN_WHY="spawn_unverified(pf_raw=${PF_RAW:0:80} last_verify=${VERIFY_WHY:-none} proc_exists=$([ -n "${PF_PID:-}" ] && proc_exists "$PF_PID" && printf yes || printf no))"
  return 3
}

# ---------------------------------------------------------------- 一轮巡检 --
# 返回通过 spawn 的次数（写入 CYCLE_SPAWNS）
CYCLE_SPAWNS=0
cycle() {
  local i n; n="${#JOB_NAME[@]}"; CYCLE_SPAWNS=0
  for ((i=0; i<n; i++)); do
    CUR_JOB="${JOB_NAME[$i]}"
    # v3 [F]：停机请求 ⇒ 立即停重启循环（本轮剩余作业不再处理，绝不新拉进程）
    if [ "$STOP_REQUESTED" = 1 ]; then
      audit cycle_aborted "" - "$(ev reason=stop_requested signal="$STOP_SIGNAL" at_job="${JOB_NAME[$i]}" remaining_jobs="$(( n - i ))")"
      break
    fi
    local key_fail="fail_${JOB_NAME[$i]}" key_spawn="last_spawn_ms_${JOB_NAME[$i]}"
    local key_alive="last_alive_ms_${JOB_NAME[$i]}" key_pid="last_pid_${JOB_NAME[$i]}"
    local key_state="last_state_${JOB_NAME[$i]}" key_restarts="restarts_${JOB_NAME[$i]}"
    local key_trust="last_trust_${JOB_NAME[$i]}"      # v3：身份信任等级（token/legacy…）

    # ---------- 0) [J] 终态守卫（机器化 F6；grep 仅作可选兼容降级通道） ----------
    # 已收敛（exit 0 / decided=done / 终态记录）或已停机（halt）⇒ 不再重启、不再巡检重启。
    if job_settled "$i"; then
      local settled_state; settled_state="$(job_settled_state "$i")"
      if [ "$(st_get "$key_state")" != "$settled_state" ]; then
        detect_job "$i"
        audit guard_suppressed "" "${DET_PID:--}" \
          "$(evcat "$(ev reason=terminal_final action=no_restart source="$TERM_SOURCE" verdict="${TERM_VERDICT:-none}" exit_code="${TERM_EXIT:--1}" settled_state="$settled_state" halt_reason="$(st_get "halt_reason_${JOB_NAME[$i]}")" evidence_level="$([ "$TERM_SOURCE" = compat_grep ] && printf low || printf high)" note="机器终态判据命中（KEEPER_COMPAT_GREP=${KEEPER_COMPAT_GREP}；grep 标记默认不作为主判据）")" "$(comp_evidence "$i")")"
        [ "$settled_state" = halted ] && st_set "last_state_${JOB_NAME[$i]}" halted
      fi
      st_set "$key_state" "$settled_state"
      continue
    fi

    # ---------- 1) 守卫：兼容降级通道（grep 完成标记；仅 KEEPER_COMPAT_GREP=1 时可达） ----------
    if [ "$KEEPER_COMPAT_GREP" = 1 ] && job_complete "$i"; then
      detect_job "$i"
      if [ "$DET_STATE" = "running" ]; then
        audit guard_suppressed "" "$DET_PID" \
          "$(evcat "$(ev reason=completed action=no_restart channel=compat_grep evidence_level=low)" "$(comp_evidence "$i")" "$(ev still_running_pid="$DET_PID")")"
      else
        audit guard_suppressed "" - \
          "$(evcat "$(ev reason=completed action=no_restart channel=compat_grep evidence_level=low)" "$(comp_evidence "$i")")"
      fi
      st_set "$key_state" completed
      continue
    fi

    # ---------- 2) 进程真实性核对 ----------
    detect_job "$i"
    case "$DET_STATE" in
      running)
        st_set "$key_alive" "$(now_ms)"
        st_set "$key_pid" "$DET_PID"
        if [ "$(st_get "$key_state")" != "running" ] || [ "$(st_get "$key_pid")" != "$DET_PID" ] \
           || [ "$DET_WHY" != "pidfile_verified" ]; then
          case "$DET_WHY" in
            adopt_scan|adopt_token|adopt_generation|adopt_legacy_loose|pidfile_token_verified_starttime_refresh|pidfile_token_verified_starttime_repaired)
              pidfile_write "$i" "$DET_PID" adopted=1 "trust=${DET_TRUST:-legacy}"
              audit adopt "$DET_PID" "$DET_PID" \
                "$(evcat "$(ev reason="$DET_WHY" action=write_pidfile trust="${DET_TRUST:-legacy}" starttime="$(proc_starttime "$DET_PID" 2>/dev/null)")" "$DET_EV")"
              # 无环境指纹 token 的认领：**必须显式出证**（R1 crit 建议 3：绝不静默 running）
              # v3.1：即便 token 命中，只要没有 keeper 亲启世代记录支撑，同样显式出证
              # （token 单证不得静默通过；c1 冒充防护）
              if [ "${DET_TRUST:-}" != token ] && [ -n "${DET_TRUST:-}" ]; then
                audit adopt_unverified "$DET_PID" "$DET_PID" \
                  "$(evcat "$(ev candidate_pid="$DET_PID" trust="$DET_TRUST" source="${DET_WHY}" action=adopt_with_flag note="无环境指纹 token：按 legacy/世代通道认领，已标注不可验证；仅当 token/路径+exe+cwd 证据充分时才走到这里")" "$DET_EV")"
              elif [ "$DET_WHY" = adopt_token ] && [ "$KEEPER_GEN_BIND" = 1 ]; then
                gen_rec_read "$i" || audit adopt_unverified "$DET_PID" "$DET_PID" \
                  "$(evcat "$(ev candidate_pid="$DET_PID" trust=token source="${DET_WHY}" reason=token_without_generation_record action=adopt_with_flag evidence_level=low note="token 单证（无 keeper 亲启世代记录可交叉验证）：认领但显式标注不可验证，绝不静默")" "$DET_EV")"
              fi ;;
            *)
              audit process_alive "$DET_PID" "$DET_PID" "$DET_EV" ;;
          esac
        fi
        st_set "$key_state" running
        st_set "$key_trust" "${DET_TRUST:-unknown}"
        st_set "$key_fail" 0
        # v3 [H]：进程活着 ⇒ 消费心跳，区分「退避中/闸门冻结/预算降档/真卡死」
        heartbeat_cycle "$i" "$DET_PID" "${DET_TRUST:-unknown}"
        ;;
      ambiguous)
        # 宁可晚恢复，绝不重复 spawn（A2：重复抓取 ≤1 叶子块）
        audit detector_ambiguous "" - \
          "$(evcat "$(ev action=refuse_spawn why=ambiguous)" "$DET_EV")"
        st_set "$key_state" ambiguous
        ;;
      absent)
        # 秒死/崩溃循环判定
        local last_spawn rap=0
        last_spawn="$(st_get "$key_spawn")"
        if [ "$last_spawn" -gt 0 ] 2>/dev/null; then
          rap=$(( $(now_ms) - last_spawn ))
          if [ "$rap" -lt $((KEEPER_RAPID_DEATH * 1000)) ]; then
            st_set "$key_fail" "$(( $(st_get "$key_fail") + 1 ))"
          else
            st_set "$key_fail" 0
          fi
        fi
        local from_pid dead_pid dead_ev
        from_pid="$(st_get "$key_pid")"
        dead_pid="$from_pid"; [ "$dead_pid" = "0" ] && dead_pid=""
        dead_ev="$DET_EV"
        # v3.1 [K]：被拒认领的陌生候选记入状态文件（c1 冒充防护的可观测面；
        # 拒绝事件本身由 refuse_adopt 写入 keeper_adopt_refused 审计）
        if [ "${DET_REFUSED_N:-0}" != 0 ]; then
          st_set "refused_${JOB_NAME[$i]}" "$DET_REFUSED_JSON"
          st_set "refused_count_${JOB_NAME[$i]}" "$(( $(st_get "refused_count_${JOB_NAME[$i]}") + DET_REFUSED_N ))"
        fi
        # ---------- 2b) [J] 机器化终态判据：exit code + 结构化报告（F6 闭环） ----------
        local v_kind="" v_reason="${DET_WHY}"
        if [ "$KEEPER_MACHINE_VERDICT" = 1 ]; then
          job_verdict_compute "$i"
          # 世代记录里的 pid 才是"我们的作业"；DET_PID 可能是扫描到的别的进程
          [ -n "${GEN_PID:-}" ] && [ "${DET_PID:-}" != "${GEN_PID}" ] && [ -z "$dead_pid" ] && dead_pid="$GEN_PID"
          audit keeper_exit_observed "$dead_pid" "$dead_pid" \
            "$(evcat "$(ev job="${JOB_NAME[$i]}" verdict="$V_VERDICT" exit_code="${V_EXIT:--1}" decided="${V_DECIDED:-none}" source="$V_SOURCE" channel_conflict="$V_CONFLICT" gen_pid="${GEN_PID:-0}" gen_present="$GEN_PRESENT" gen_starttime="${GEN_START:-?}" detector_why="$DET_WHY" action="$([ "$V_VERDICT" = converged ] && printf stop || printf evaluate_matrix)")" \
            "$(ev report="${CONV_PATH:-none}" report_stale="$CONV_STALE" report_decided="${CONV_DECIDED:-none}")")" \
            process "$(ev job="${JOB_NAME[$i]}" verdict="$V_VERDICT" exit_code="${V_EXIT:--1}" source="$V_SOURCE")"
          [ "$V_CONFLICT" = 1 ] && audit keeper_terminal_channel_conflict "$dead_pid" - \
            "$(ev job="${JOB_NAME[$i]}" wait_exit="${V_EXIT:--1}" report_exit="${CONV_EXIT:-none}" report_decided="${CONV_DECIDED:-none}" action=prefer_report_terminal note="OS 退出码与结构化终态不一致：以 converge 自己的终态报告为收敛结论，同时保留冲突证据")"
          case "$V_VERDICT" in
            converged)
              job_stop_converged "$i" "${V_EXIT:-0}" "$V_SOURCE" "$V_DECIDED"
              continue ;;
            incomplete)
              job_limited_restart "$i" "$dead_pid" exit3 "${V_EXIT:-3}" "$V_DECIDED" || true
              continue ;;
            stall_stop)
              job_limited_restart "$i" "$dead_pid" exit4 "${V_EXIT:-4}" "$V_DECIDED" || true
              continue ;;
            *) v_kind="$V_VERDICT" ;;   # crash / signal / unknown ⇒ 既有崩溃路径 + 预算
          esac
        fi
        # 退避（崩溃循环保护，上限被夹在 DEADLINE 之内）
        local fail backoff_ms waited
        fail="$(st_get "$key_fail")"
        # ---------- 2c) 重启预算（supervisord startretries 式）：超限 ⇒ 停机告警，不再重启 ----------
        if [ "$KEEPER_HALT_ON_FLAP" = 1 ] && [ "$fail" -ge "$KEEPER_START_RETRIES" ] 2>/dev/null; then
          audit keeper_restart_budget_exhausted "$dead_pid" - \
            "$(evcat "$(ev job="${JOB_NAME[$i]}" kind=rapid_death attempts="$fail" limit="$KEEPER_START_RETRIES" verdict="${v_kind:-crash}" action=halt note="连续快速失败超限：停止重启并升级告警（v3.0 的 FLAP 只审计不停止）")" "$dead_ev")" \
            process "$(ev job="${JOB_NAME[$i]}" kind=rapid_death attempts="$fail" limit="$KEEPER_START_RETRIES")"
          job_halt "$i" "restart_budget_exhausted" "rapid_deaths=$fail/$KEEPER_START_RETRIES verdict=${v_kind:-crash}" "$dead_pid"
          continue
        fi
        backoff_ms=0
        if [ "$fail" -gt 0 ]; then
          backoff_ms=$(( KEEPER_TICK * 1000 ))
          local k; for ((k=1; k<fail; k++)); do backoff_ms=$((backoff_ms*2)); done
          [ "$backoff_ms" -le $((KEEPER_MAX_BACKOFF*1000)) ] || backoff_ms=$((KEEPER_MAX_BACKOFF*1000))
          waited=$(( $(now_ms) - last_spawn ))
          if [ "$waited" -lt "$backoff_ms" ]; then
            audit restart_backoff "$dead_pid" - \
              "$(evcat "$(ev reason=crash_loop consecutive_rapid_deaths="$fail" backoff_ms="$backoff_ms" waited_ms="$waited")" "$dead_ev")"
            st_set "$key_state" backoff
            continue
          fi
        fi
        if [ "$(st_get "$key_state")" != "dead" ]; then
          local ago la; la="$(st_get "$key_alive")"
          if [ "$la" = 0 ]; then ago=-1; else ago=$(( $(now_ms) - la )); fi
          audit process_dead "$dead_pid" - \
            "$(evcat "$(ev reason="${DET_WHY}" last_alive_ago_ms="$ago")" "$dead_ev")"
        fi
        # ---------- 3) 重启 ----------
        local t0 t1 rc alive_ms
        t0="$(now_ms)"
        spawn_job "$i"; rc=$?
        t1="$(now_ms)"
        alive_ms=$(( t1 - t0 ))
        if [ "$rc" = 0 ]; then
          pidfile_write "$i" "$SPAWN_PID" spawned_by=keeper
          st_set "$key_restarts" "$(( $(st_get "$key_restarts") + 1 ))"
          st_set "$key_spawn" "$t1"; st_set "$key_pid" "$SPAWN_PID"; st_set "$key_alive" "$t1"
          st_set "$key_state" running
          audit restart "$dead_pid" "$SPAWN_PID" \
            "$(evcat "$(ev reason="${DET_WHY}" script="${JOB_SCRIPT[$i]}" cwd="${JOB_CWD[$i]}" spawn_verify_ms="$alive_ms" starttime="$(proc_starttime "$SPAWN_PID" 2>/dev/null)" total_restarts="$(st_get "$key_restarts")" deadline_ms=$((KEEPER_RECOVERY_DEADLINE*1000)) spawn_trust="${SPAWN_TRUST:-unknown}" spawn_child_pid="${SPAWN_CHILD_PID:-0}" verdict="${v_kind:-unknown}" exit_code="${V_EXIT:--1}" verdict_source="${V_SOURCE:-none}" restart_kind="crash_path" fail_count="$(st_get "$key_fail")")" "$(ev detection="$dead_ev")")" \
            process "$(ev from_pid="${dead_pid:-0}" to_pid="$SPAWN_PID" reason="${DET_WHY}" recovery_ms="$alive_ms" ticks_note="deadline=${KEEPER_RECOVERY_DEADLINE}s" verdict="${v_kind:-unknown}" exit_code="${V_EXIT:--1}" evidence="$(evcat "$(ev detection="$dead_ev")")")"
          CYCLE_SPAWNS=$((CYCLE_SPAWNS+1))
          # 拉起后立刻做一次心跳分类（否则要等下一轮才知道是否处于退避/闸门冻结）
          heartbeat_cycle "$i" "$SPAWN_PID" "${SPAWN_TRUST:-unknown}"
          if [ "$alive_ms" -gt $((KEEPER_RECOVERY_DEADLINE*1000)) ]; then
            audit deadline_breached "" "$SPAWN_PID" "$(ev deadline_ms=$((KEEPER_RECOVERY_DEADLINE*1000)) observed_ms="$alive_ms")"
          fi
          if [ "$(st_get "$key_fail")" -ge "$KEEPER_FLAP_THRESHOLD" ]; then
            audit restart_flapping "" "$SPAWN_PID" \
              "$(ev consecutive_rapid_deaths="$(st_get "$key_fail")" restart_count="$(st_get "$key_restarts")" halt_threshold="$KEEPER_START_RETRIES" halt_on_flap="$KEEPER_HALT_ON_FLAP" note="进程反复秒死（告警档）；再连续失败至 $KEEPER_START_RETRIES 次将停止重启并停机告警（KEEPER_HALT_ON_FLAP=1）")"
          fi
        elif [ "$rc" = 2 ]; then
          st_set "$key_state" dry_run
          audit restart_dry_run "$dead_pid" - "$(evcat "$(ev reason="${DET_WHY}" detail="$SPAWN_WHY")" "$dead_ev")"
        else
          st_set "$key_state" spawn_failed
          st_set "$key_spawn" "$t1"
          audit restart_failed "$dead_pid" - \
            "$(evcat "$(ev reason="${DET_WHY}" spawn_why="$SPAWN_WHY" spawn_ms="$alive_ms")" "$dead_ev")"
        fi
        ;;
    esac
  done
  st_set run_count "$(( $(st_get run_count) + 1 ))"
  state_save
}

# ------------------------------------------------------------- 环境探针复核 --
env_recheck() {
  local fp_now prev
  CUR_JOB="-"
  fp_now="$(env_fingerprint_json)"
  local fp_short; fp_short="$(printf '%s' "$fp_now" | sed 's/.*"mount_fp":"\([^"]*\)".*/\1/')"
  prev="$(st_get mount_fp)"
  if [ -n "$prev" ] && [ "$prev" != "0" ] && [ "$prev" != "$fp_short" ]; then
    audit env_changed "" - "$(ev prev_mount_fp="$prev" now_mount_fp="$fp_short" ns_mnt="$NS_MNT" note="/tmp 挂载视图相对上次启动已变化，确认是否处于异常挂载视图")"
  fi
  st_set mount_fp "$fp_short"
  if ! env_probe; then
    audit env_probe_fail "" - "$(ev missing="$ENV_MISSING" cwd="$(pwd -P)" ns_mnt="$NS_MNT" mount_fp="$fp_short" strict="$KEEPER_STRICT_ENV" note="待动作作业的路径在当前挂载视图不可见（孤儿化征兆）；已完成作业已跳过探针")"
    if [ "$KEEPER_STRICT_ENV" = 1 ]; then
      audit keeper_abort "" - "$(ev reason=env_probe_fail missing="$ENV_MISSING" exit_code=3)"
      hlog "FATAL 环境探针失败，KEEPER_STRICT_ENV=1 → 退出（拒绝静默空转）"
      state_save
      exit 3
    fi
    return 1
  fi
  return 0
}

# ------------------------------------------------------------------ 子命令 --
on_signal() { # SIGNAL —— 只置标志，不做任何重活（bash 在命令结束后才执行 trap 体）
  STOP_REQUESTED=1
  STOP_SIGNAL="$1"
  hlog "收到 SIG$1：置停机标志（不再拉起新进程；当前一轮结束后收尾）"
}
cmd_start() {
  # 配置自检：保证 worst-case 恢复时间 < A2 硬指标
  if [ $((KEEPER_MAX_BACKOFF + KEEPER_TICK + 10)) -gt "$KEEPER_RECOVERY_DEADLINE" ]; then
    local clamped=$(( KEEPER_RECOVERY_DEADLINE - KEEPER_TICK - 10 ))
    [ "$clamped" -lt 10 ] && clamped=10
    audit config_warning "" - "$(ev reason=max_backoff_exceeds_deadline old_max_backoff="$KEEPER_MAX_BACKOFF" clamped_to="$clamped" deadline="$KEEPER_RECOVERY_DEADLINE")"
    KEEPER_MAX_BACKOFF="$clamped"
  fi
  # v3.1 [J]：绝不允许"零完成判据"运行（机器判据关 + 兼容 grep 关 ⇒ 作业永不休止）
  if [ "$KEEPER_MACHINE_VERDICT" != 1 ] && [ "$KEEPER_COMPAT_GREP" != 1 ]; then
    audit config_warning "" - \
      "$(ev reason=no_completion_criterion machine_verdict="$KEEPER_MACHINE_VERDICT" compat_grep="$KEEPER_COMPAT_GREP" action=force_compat_grep note="机器终态判据与兼容降级判据同时关闭 ⇒ 强制启用 grep 兼容通道（fail-safe：不允许无完成判据的看护）")"
    KEEPER_COMPAT_GREP=1
  fi
  # v3.1 [L]/K-6：停止标志的清理提前到抢锁之前（v3.0 在抢锁/状态/审计之后 ⇒ 启动窗口内的
  # stop 请求会被删除并吞掉）
  local stop_flag="$KEEPER_STATE_DIR/keeper-v3.stop"
  if [ -f "$stop_flag" ]; then
    hlog "启动前发现停止标志（启动窗口内的 stop 请求）：立即优雅收尾，不进入看护循环"
    rm -f "$stop_flag" 2>/dev/null
    audit keeper_exit "" - "$(ev code=0 reason=stop_flag_at_startup note="启动窗口内收到 stop：按请求退出，不吞掉停止意图")"
    return 0
  fi
  # v3 [E]：先抢单例锁再干任何事（state/pid 文件/审计都由在位 keeper 独占写）
  if [ "$KEEPER_SINGLETON" = 1 ]; then
    singleton_acquire || return $?
  fi
  keeper_family >/dev/null
  state_load
  # keeper 自身 PID 文件（自我登记，starttime 防复用）
  local kp="$KEEPER_STATE_DIR/keeper-v3.pid"
  printf 'pid=%s starttime=%s exe=%s script=%s run_id=%s ts=%s\n' "$$" "$(proc_starttime $$ 2>/dev/null)" \
    "$(proc_exe $$)" "$KEEPER_SELF" "$RUN_ID" "$(now_iso)" >"$kp" 2>/dev/null
  startup_audit_block >/dev/null
  audit keeper_start "" - "$(ev jobs="$(printf '%s ' "${JOB_NAME[@]:-}")" tick="$KEEPER_TICK" heartbeat="$KEEPER_HEARTBEAT" deadline="$KEEPER_RECOVERY_DEADLINE" max_backoff="$KEEPER_MAX_BACKOFF" dry_run="$KEEPER_DRY_RUN" setsid="$KEEPER_SETSID" strict_env="$KEEPER_STRICT_ENV" conf="$KEEPER_CONF" singleton="$KEEPER_SINGLETON" lock_file="$KEEPER_LOCK_FILE" family="$KEEPER_FAMILY" hb_enable="$KEEPER_HB_ENABLE" hb_py="$KEEPER_HEARTBEAT_PY" stall_action="$KEEPER_STALL_ACTION" fingerprint="$(env_fingerprint_json)")"
  env_recheck || true
  hlog "keeper-v3 启动 pid=$$ run_id=$RUN_ID family=$KEEPER_FAMILY jobs=$(job_count) tick=${KEEPER_TICK}s singleton=$KEEPER_SINGLETON"
  local last_hb last_env now
  last_hb="$(now_ms)"; last_env="$(now_ms)"
  # v3 [F]：信号 ⇒ 优雅收尾（停重启循环 + 排空自建子进程 + checkpoint + 审计）
  trap 'on_signal TERM' TERM
  trap 'on_signal INT' INT
  while :; do
    # v3.1 [L]/K-1：每轮校验锁文件 inode（锁被删/重建 ⇒ 重新抢锁，抢不到则让位退出 6）
    if ! singleton_check_inode; then
      CUR_JOB="-"
      audit keeper_exit "" - "$(ev code=6 reason=singleton_yield_lock_replaced note="锁文件被删/重建且新 inode 已被他人持有：让位退出，绝不双活（K-1 分裂脑防线）")"
      hlog "keeper-v3 让位退出（rc=6）：锁文件被重建且新持有者已就位"
      return 6
    fi
    cycle
    now="$(now_ms)"
    if [ $(( now - last_hb )) -ge $((KEEPER_HEARTBEAT*1000)) ]; then
      CUR_JOB="-"
      audit heartbeat "" - "$(ev uptime_ms="$(( now - $(st_get first_ms) ))" run_count="$(st_get run_count)" alive_pids="$(for ((i=0;i<${#JOB_NAME[@]};i++)); do printf '%s ' "${JOB_NAME[$i]}=$(st_get last_pid_${JOB_NAME[$i]})"; done)" hb_classes="$(for ((i=0;i<${#JOB_NAME[@]};i++)); do printf '%s ' "${JOB_NAME[$i]}=$(st_get hb_class_${JOB_NAME[$i]})"; done)" ns_mnt="$NS_MNT")"
      last_hb="$now"
    fi
    if [ $(( now - last_env )) -ge $((KEEPER_ENV_RECHECK*1000)) ]; then
      env_recheck || true
      last_env="$now"
    fi
    if [ "$STOP_REQUESTED" = 1 ]; then
      CUR_JOB="-"
      audit keeper_signal "" - "$(ev signal="$STOP_SIGNAL" action=graceful_stop)"
      graceful_stop "$STOP_SIGNAL" "signal_$STOP_SIGNAL"
      audit keeper_exit "" - "$(ev code=0 reason=signal_$STOP_SIGNAL run_count="$(st_get run_count)" uptime_ms="$(( now - $(st_get first_ms) ))")"
      hlog "keeper-v3 停止（SIG$STOP_SIGNAL 优雅收尾）"
      return 0
    fi
    local s
    for ((s=0; s<KEEPER_TICK; s++)); do
      if [ -f "$stop_flag" ]; then
        CUR_JOB="-"
        graceful_stop "stop_flag" "stop_flag"
        audit keeper_exit "" - "$(ev code=0 reason=stop_flag run_count="$(st_get run_count)")"
        hlog "keeper-v3 停止（stop 标志）"; rm -f "$stop_flag" 2>/dev/null; return 0
      fi
      if [ "$STOP_REQUESTED" = 1 ]; then break; fi
      sleep 1 9>&-          # v3.1 K-2：tick 内的 sleep 不继承单例锁 fd 9
    done
  done
}
cmd_once() {
  state_load
  [ "$(st_get first_ms)" = "0" ] && st_set first_ms "$(now_ms)"
  keeper_family >/dev/null
  # once 属测试/演练模式：不抢锁（避免与在位 keeper 争抢），但**探测并出证**
  local prc=0 pout
  pout="$(singleton_probe)"; prc=$?
  if [ "$prc" = 1 ]; then
    audit singleton_conflict "" - \
      "$(ev holder_pid="$(printf '%s' "$pout" | sed -n 's/.*pid=\([0-9]*\).*/\1/p' | head -1)" self_pid="$$" probe="$pout" mode=advisory action=proceed note="once 模式：探测到在位 keeper，仅出证不退出（演练需要能在在位 keeper 存在时跑单轮巡检）")" \
      process "$(ev holder_pid="$(printf '%s' "$pout" | sed -n 's/.*pid=\([0-9]*\).*/\1/p' | head -1)" self_pid="$$")"
  fi
  startup_audit_block >/dev/null
  audit keeper_start "" - "$(ev mode=once jobs="$(printf '%s ' "${JOB_NAME[@]:-}")" dry_run="$KEEPER_DRY_RUN" conf="$KEEPER_CONF" singleton_probe="$pout" hb_enable="$KEEPER_HB_ENABLE" stall_action="$KEEPER_STALL_ACTION" fingerprint="$(env_fingerprint_json)")"
  env_recheck || true
  cycle
  # 机器可读的一轮摘要（演练脚本据此断言，不必解析审计文件）
  local i sum='[' sep=''
  for ((i=0; i<${#JOB_NAME[@]}; i++)); do
    CUR_JOB="${JOB_NAME[$i]}"
    detect_job "$i"
    local settled=no sverdict=none ssource=none
    if job_settled "$i"; then settled=yes; sverdict="${TERM_VERDICT:-none}"; ssource="${TERM_SOURCE:-none}"; fi
    gen_rec_read "$i" || true
    local gmatch=none
    if [ -n "$DET_PID" ] && [ "$GEN_PRESENT" = 1 ]; then
      gmatch="$(gen_match "$i" "$DET_PID" "$(proc_starttime "$DET_PID" 2>/dev/null || printf '?')" "$(tok_hash "$IDF_TOKEN_RAW")")"
    elif [ "$GEN_PRESENT" = 1 ]; then gmatch=stale; fi
    sum+="$sep$(ev job="${JOB_NAME[$i]}" state="$DET_STATE" pid="${DET_PID:-0}" why="$DET_WHY" trust="${DET_TRUST:-none}" hb_class="$(st_get hb_class_${JOB_NAME[$i]})" hb_stall_streak="$(st_get hb_stall_streak_${JOB_NAME[$i]})" complete="$(job_complete "$i" && printf yes || printf no)" restarts="$(st_get restarts_${JOB_NAME[$i]})" settled="$settled" settled_verdict="$sverdict" settled_source="$ssource" exit_code="$(st_get exit_code_${JOB_NAME[$i]})" halted="$(st_get halted_${JOB_NAME[$i]})" halt_reason="$(st_get halt_reason_${JOB_NAME[$i]})" incomplete_restarts="$(st_get incomplete_restarts_${JOB_NAME[$i]})" resume_restarts="$(st_get resume_restarts_${JOB_NAME[$i]})" fail_count="$(st_get fail_${JOB_NAME[$i]})" gen_present="$GEN_PRESENT" gen_pid="${GEN_PID:-0}" gen_match="$gmatch" refused_count="$(st_get refused_count_${JOB_NAME[$i]})")"
    sep=','
  done
  sum+=']'; CUR_JOB="-"
  printf 'CYCLE_SPAWNS=%s\n' "$CYCLE_SPAWNS"
  printf 'ONCE_SUMMARY={"schema":"keeper-once/1","spawns":%s,"jobs":%s}\n' "$CYCLE_SPAWNS" "$sum"
  return 0
}
cmd_status() {
  state_load
  printf '%-10s %-9s %-10s %-8s %-10s %-22s %-16s %s\n' JOB COMPLETED STATE PID TRUST PID_SRC HB_CLASS EVIDENCE
  local i
  for ((i=0; i<${#JOB_NAME[@]}; i++)); do
    local completed=no; job_settled "$i" && completed=yes
    detect_job "$i"
    hb_classify_file "$(hb_path_of "$i")"
    printf '%-10s %-9s %-10s %-8s %-10s %-22s %-16s %s\n' \
      "${JOB_NAME[$i]}" "$completed" "$DET_STATE" "${DET_PID:- -}" "${DET_TRUST:-none}" "$DET_WHY" "$HB_CLASS" "$(printf '%s' "$DET_EV" | cut -c1-90)"
    # v3.1 [J]：机器化终态面（退出码判据 / 停机告警 / 重启预算）
    if [ "$completed" = yes ]; then
      printf '           terminal: verdict=%s settled_source=%s exit_code=%s decided=%s\n' \
        "${TERM_VERDICT:-none}" "${TERM_SOURCE:-none}" "$(st_get "exit_code_${JOB_NAME[$i]}")" "${TERM_DECIDED:-none}"
    fi
    if [ "$(st_get "halted_${JOB_NAME[$i]}")" = 1 ]; then
      printf '           HALTED: reason=%s fail_count=%s incomplete_restarts=%s resume: keeper-v3.sh resume %s\n' \
        "$(st_get "halt_reason_${JOB_NAME[$i]}")" "$(st_get "fail_${JOB_NAME[$i]}")" \
        "$(st_get "incomplete_restarts_${JOB_NAME[$i]}")" "${JOB_NAME[$i]}"
    fi
    if [ "$(st_get "refused_count_${JOB_NAME[$i]}")" != "" ] && [ "$(st_get "refused_count_${JOB_NAME[$i]}")" != 0 ]; then
      printf '           adopt_refused: count=%s last=%s\n' "$(st_get "refused_count_${JOB_NAME[$i]}")" "$(st_get "refused_${JOB_NAME[$i]}" | cut -c1-140)"
    fi
  done
  printf '\nTRUST=none ⇒ 身份不可验证（R1 crit S1/S2 的冒充者通道已被拒绝认领）；legacy ⇒ 无 token 的老进程\n'
  printf '判据：KEEPER_MACHINE_VERDICT=%s（exit code + 结构化终态）KEEPER_COMPAT_GREP=%s（grep 兼容降级通道）KEEPER_GEN_BIND=%s（世代绑定认领）\n' \
    "$KEEPER_MACHINE_VERDICT" "$KEEPER_COMPAT_GREP" "$KEEPER_GEN_BIND"
  local kp="$KEEPER_STATE_DIR/keeper-v3.pid"
  if [ -r "$kp" ]; then
    printf '\nkeeper 自身 PID 文件: %s\n' "$(head -c 200 "$kp")"
    # v3.1 [L]/K-5：存活判定必须同时核对 starttime 与 cmdline（防 PID 复用误报"keeper 存活"）
    local kpid kst now_st verd
    kpid="$(sed -n 's/^pid=\([0-9]*\).*/\1/p' "$kp")"
    kst="$(sed -n 's/.*starttime=\([0-9]*\).*/\1/p' "$kp")"
    if [ -n "$kpid" ] && [ -d "/proc/$kpid" ]; then
      now_st="$(proc_starttime "$kpid" 2>/dev/null || printf '?')"
      if [ -n "$kst" ] && [ "$now_st" != "$kst" ]; then
        verd="no_pid_reuse"
        printf 'keeper 存活: NO（pid=%s 存在但 starttime 不符：pidfile=%s /proc=%s ⇒ PID 复用，非本 keeper）\n' "$kpid" "$kst" "$now_st"
      else
        case "$(proc_cmdline_text "$kpid")" in
          *keeper-v3.sh*) verd=yes ;;
          *) verd="no_cmdline" ;;
        esac
        if [ "$verd" = yes ]; then
          printf 'keeper 存活: yes (pid=%s starttime=%s 核对通过 cmdline=%s)\n' "$kpid" "${now_st:-?}" "$(proc_cmdline_text "$kpid" | cut -c1-80)"
        else
          printf 'keeper 存活: NO（pid=%s starttime 相符但 cmdline 不含 keeper-v3.sh：%s）\n' "$kpid" "$(proc_cmdline_text "$kpid" | cut -c1-80)"
        fi
      fi
    else
      printf 'keeper 存活: no\n'
    fi
  fi
  printf '单例锁: %s\n' "$(singleton_probe)"
}
cmd_stop() {
  local f="$KEEPER_STATE_DIR/keeper-v3.stop"
  mkdir -p "$KEEPER_STATE_DIR" 2>/dev/null
  printf '%s\n' "$(now_iso)" >"$f"
  hlog "已写入停止标志：$f（不发送任何信号；在位 keeper 在下一个检查点优雅收尾）"
}
cmd_fingerprint() { env_fingerprint_json; printf '\n'; }
cmd_hbclass() { # hbclass [FILE] —— 只读打印一次心跳分类（不落审计）
  if [ -n "${1:-}" ]; then
    hb_classify_file "$1"
    printf 'class=%s\n' "$HB_CLASS"
    printf 'HB_EVIDENCE=%s\n' "$HB_EV"
    return 0
  fi
  local i
  for ((i=0; i<${#JOB_NAME[@]}; i++)); do
    hb_classify_file "$(hb_path_of "$i")"
    printf '%-10s class=%-16s file=%s\n' "${JOB_NAME[$i]}" "$HB_CLASS" "$(hb_path_of "$i")"
    printf '           %s\n' "$HB_EV"
  done
  return 0
}
cmd_lock_probe() { singleton_probe "${1:-$KEEPER_LOCK_FILE}"; return $?; }
cmd_identity() { # identity PID [JOB] —— 只读打印某 pid 对各作业的身份证据（排障/审计用）
  local p="${1:-}" want="${2:-}" i
  [[ "$p" =~ ^[0-9]+$ ]] || { printf '用法: keeper-v3.sh identity <pid> [job]\n'; return 2; }
  for ((i=0; i<${#JOB_NAME[@]}; i++)); do
    [ -n "$want" ] && [ "${JOB_NAME[$i]}" != "$want" ] && continue
    if verify_pid_for_job "$i" "$p"; then
      printf 'job=%-12s VERIFY=yes trust=%-16s factors=%s\n' "${JOB_NAME[$i]}" "$VERIFY_TRUST" "$IDF_JSON"
    else
      printf 'job=%-12s VERIFY=no  why=%-38s trust=%-16s factors=%s\n' "${JOB_NAME[$i]}" "$VERIFY_WHY" "${VERIFY_TRUST:-none}" "$IDF_JSON"
    fi
  done
  return 0
}
cmd_spawn() { # spawn JOBNAME —— 诊断/演练用：立即拉起某作业并打印核对结果（写 pidfile）
  local want="${1:-}" i found=-1 rc
  for ((i=0; i<${#JOB_NAME[@]}; i++)); do [ "${JOB_NAME[$i]}" = "$want" ] && found=$i; done
  if [ "$found" -lt 0 ]; then printf '未知作业: %s（可用: %s）\n' "$want" "$(printf '%s ' ${JOB_NAME[@]+"${JOB_NAME[@]}"})" >&2; return 2; fi
  state_load
  # v3.1 (K-6)：spawn 是唯一的"写 pidfile 特权动作"，在位 keeper 存在时默认拒绝
  # （避免两实例同时巡检/拉起/写 pidfile）；演练需要时用 KEEPER_ALLOW_SPAWN_WITH_KEEPER=1 放行。
  local pout prc=0
  pout="$(singleton_probe)"; prc=$?
  if [ "$prc" = 1 ] && [ "${KEEPER_ALLOW_SPAWN_WITH_KEEPER:-0}" != 1 ]; then
    audit spawn_refused "" - \
      "$(ev job="$want" self_pid="$$" probe="$pout" action=refuse_spawn allow_env=KEEPER_ALLOW_SPAWN_WITH_KEEPER note="探测到在位 keeper：spawn 会绕过单例锁并可能双写 pidfile（K-6）⇒ 默认拒绝")"
    printf 'SPAWN_RC=4 pid=none trust=none why=in_position_keeper(%s)\n' "$pout"
    return 4
  fi
  keeper_family >/dev/null
  spawn_job "$found"; rc=$?
  printf 'SPAWN_RC=%s pid=%s trust=%s why=%s\n' "$rc" "${SPAWN_PID:-none}" "${SPAWN_TRUST:-none}" "${SPAWN_WHY:-none}"
  if [ "$rc" = 0 ]; then pidfile_write "$found" "$SPAWN_PID" spawned_by=keeper "trust=${SPAWN_TRUST:-spawned}"; fi
  return $rc
}
# cmd_resume JOB|all —— 解除机器判据造成的终态/停机（运维显式介入；写审计 + 清状态）
cmd_resume() {
  local want="${1:-}" i n=0
  state_load
  for ((i=0; i<${#JOB_NAME[@]}; i++)); do
    [ -n "$want" ] && [ "$want" != all ] && [ "${JOB_NAME[$i]}" != "$want" ] && continue
    local job="${JOB_NAME[$i]}"
    local prev_term prev_halt rsn
    prev_term="$(st_get "terminal_${job}")"; prev_halt="$(st_get "halted_${job}")"
    rsn="$(st_get "halt_reason_${job}")"
    st_set "terminal_${job}" ""; st_set "halted_${job}" 0; st_set "halt_reason_${job}" ""
    st_set "fail_${job}" 0; st_set "incomplete_restarts_${job}" 0; st_set "resume_restarts_${job}" 0
    st_set "exit_code_${job}" ""
    rm -f "$(terminal_path "$i")" "$(halt_path "$i")" 2>/dev/null
    audit keeper_job_resume "" - \
      "$(ev job="$job" prev_terminal="${prev_term:-none}" prev_halted="${prev_halt:-0}" prev_halt_reason="${rsn:-none}" action=clear_terminal_and_budgets note="运维显式解除：清除终态记录与重启预算，下一轮巡检将按正常判据重新看护")"
    printf 'resume: job=%s 已清除 terminal=%s halted=%s reason=%s\n' "$job" "${prev_term:-none}" "${prev_halt:-0}" "${rsn:-none}"
    n=$((n+1))
  done
  state_save
  [ "$n" -gt 0 ] || { printf '未匹配任何作业: %s（可用: %s）\n' "${want:-<空>}" "$(printf '%s ' ${JOB_NAME[@]+"${JOB_NAME[@]}"})" >&2; return 2; }
  return 0
}
cmd_daemon() {
  local dlog="$KEEPER_STATE_DIR/keeper-v3.daemon.log"
  mkdir -p "$KEEPER_STATE_DIR" 2>/dev/null
  ( setsid nohup bash "$KEEPER_SELF" start </dev/null >>"$dlog" 2>&1 & ) 2>/dev/null \
    || ( nohup bash "$KEEPER_SELF" start </dev/null >>"$dlog" 2>&1 & )
  hlog "keeper-v3 daemon 已拉起（self pid 以 $KEEPER_STATE_DIR/keeper-v3.pid 为准；单例锁由 start 进程持有）"
  sleep 0.3 9>&-              # v3.1 K-2：TERM 宽限不继承单例锁 fd
  singleton_probe >/dev/null 2>&1
  return 0
}

# ---------------------------------------------------------------- 自测套件 --
SELFTEST_RESULT=""; SELFTEST_FAILED=0
_st_case() { # name ok detail
  local name="$1" ok="$2" detail="$3"
  if [ "$ok" = 1 ]; then printf 'PASS %s  %s\n' "$name" "$detail"
  else printf 'FAIL %s  %s\n' "$name" "$detail"; SELFTEST_FAILED=$((SELFTEST_FAILED+1)); fi
  SELFTEST_RESULT+="${SELFTEST_RESULT:+,}{\"case\":\"$name\",\"ok\":$( [ "$ok" = 1 ] && echo true || echo false ),\"detail\":\"$(jesc "$detail")\"}"
  audit selftest_case "" - "$(ev case_name="$name" ok="$([ "$ok" = 1 ] && echo 1 || echo 0)" detail="$detail")"
}
# 仅允许结束本套件自己拉起的进程（先核对 /proc cmdline 含自测目录）
_st_kill_local() {
  local p="$1" t ok=0
  proc_exists "$p" || return 0
  while IFS= read -r t; do
    case "$t" in "$KEEPER_SELFTEST_DIR"*|*mock-target.sh*|*decoy*) ok=1 ;; esac
  done < <(proc_cmdline_tokens "$p")
  [ "$ok" = 1 ] || { printf 'REFUSE_KILL %s cmdline=%s\n' "$p" "$(proc_cmdline_text "$p")"; return 1; }
  kill -9 "$p" 2>/dev/null
  wait "$p" 2>/dev/null      # 立即回收，避免异步作业通知污染日志
  return 0
}
# _st_mk_hb FILE MODE PID —— 生成心跳夹具（自测用；判据仍由 refs/heartbeat.py 的读侧给出）
#   MODE: ok | done | gate | backoff | budget | stall
_st_mk_hb() {
  local f="$1" mode="$2" pid="${3:-0}"
  python3 - "$f" "$mode" "$pid" <<'PY'
import json, sys, time
f, mode, pid = sys.argv[1], sys.argv[2], int(sys.argv[3])
now = time.time()
wb = {"ok": 0, "done": 0, "gate": 1200, "backoff": 1200, "budget": 1200, "stall": 1200}.get(mode, 0)
wts = now - wb
mem = {"level": "NORMAL", "level_num": 0, "gate_open": True, "gate_closed_s": 0.0, "pct": 40.0,
       "eff_workers": {"leaf": 3, "probe": 4}, "base_workers": {"leaf": 3, "probe": 4}}
lim = {"mode": "auto", "interval_s": 0.5, "err_streak": 0, "cooldown_remaining_s": 0.0,
       "last_event": None}
if mode == "gate":
    mem.update({"level": "CRITICAL", "level_num": 3, "gate_open": False,
                "gate_closed_s": 400.0, "pct": 96.0})
elif mode == "backoff":
    lim.update({"interval_s": 4.0, "err_streak": 3})
elif mode == "budget":
    mem.update({"level": "PRESSURE", "level_num": 2, "pct": 91.0,
                "eff_workers": {"leaf": 1, "probe": 2}})
payload = {
    "schema": "heartbeat/v1", "state": "done" if mode == "done" else "running",
    "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
    "ts_epoch": round(now, 3), "round": 7, "writes": 7, "pid": pid, "boot_id": "fixture",
    "proc_start_epoch": None, "since_boot_s": 5000.0, "prefix": "A1",
    "todo": 3, "done": 5, "gaps": 0, "leaves": 10, "records": 100,
    "watermark": {"schema": "watermark/v1", "leaves": 10, "records": 100, "prefix": "A1",
                  "ts_epoch": round(wts, 3), "advance_count": 5, "clamped": False},
    "runtime": {"schema": "runtime/v1", "collected_at_epoch": round(now, 3),
                "gate_open": mem["gate_open"], "gate_closed_s": mem["gate_closed_s"],
                "mem_level": mem["level"], "delay_s": lim["interval_s"],
                "strikes": lim["err_streak"], "backoff_active": bool(mode == "backoff"),
                "backoff_reason": "err_streak=3" if mode == "backoff" else None,
                "mem": mem, "limiter": lim,
                "collected": {"memguard": True, "limiter": True, "provider": True}},
    "extra": {},
}
with open(f, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, ensure_ascii=False)
PY
  rm -f "$f.lock" "$f.wlock" 2>/dev/null   # 无锁文件 ⇒ 读侧退回 PID+启动时刻探活（夹具用活 pid）
}
cmd_selftest() {
  mkdir -p "$KEEPER_SELFTEST_DIR" 2>/dev/null
  local D="$KEEPER_SELFTEST_DIR"
  local mock="$D/mock-target.sh" cl="$D/complete.log" pf="$D/pid/x.pid"
  mkdir -p "$D/pid"
  # 内层 sleep 用 1s：父进程被 kill -9 后不会残留长命 sleep 子进程
  printf '#!/usr/bin/env bash\nwhile :; do sleep 1; done\n' >"$mock"
  : >"$cl"; rm -f "$pf"
  # 建立 1 个自测作业（仅检测用；只有 case 8 会 spawn）
  JOB_NAME=(stj); JOB_SCRIPT=("$mock"); JOB_CWD=("$D"); JOB_COMPLOG=("$cl")
  JOB_COMPPAT=("全部完成"); JOB_OUTLOG=("$D/out.log"); JOB_EXTRA=("")
  JOB_BASENAME=("$(basename "$mock")"); JOB_RELPATH=("mock-target.sh")
  JOB_PIDFILE=("$pf"); JOB_EXE_RE=('^(ba)?sh$|.*/(ba)?sh$')
  JOB_HB_FILE=("$D/hb.json"); JOB_HB_SRC=("selftest")

  local p1 p2 dec pgrep_out pgrep_rc targeted pp decoy_sh="$D/decoy.sh"
  # 伪装进程：argv 里"提到"被看护脚本路径（子串），但不是它本身。
  # 用脚本文件而非 bash -c：bash 对 `-c '单条命令'` 会 exec 优化掉自己的 argv。
  printf '#!/usr/bin/env bash\nsleep 1000\n' >"$decoy_sh"
  printf -- '-- case 0: 无进程无 PID 文件 ⇒ absent --\n'
  detect_job 0
  [ "$DET_STATE" = absent ] && _st_case "detector_absent_when_not_running" 1 "state=$DET_STATE why=$DET_WHY" \
                            || _st_case "detector_absent_when_not_running" 0 "state=$DET_STATE why=$DET_WHY"

  printf -- '-- case 1: 真实进程 + 正确 PID 文件 ⇒ running --\n'
  ( cd "$D" && exec bash "$mock" ) </dev/null >>"$D/out.log" 2>&1 & p1=$!
  sleep 0.4
  printf 'pid=%s starttime=%s script=%s job=stj ts=%s\n' "$p1" "$(proc_starttime "$p1")" "$mock" "$(now_iso)" >"$pf"
  detect_job 0
  [ "$DET_STATE" = running ] && [ "$DET_PID" = "$p1" ] && _st_case "detector_running_verified" 1 "state=$DET_STATE pid=$DET_PID why=$DET_WHY" \
                                      || _st_case "detector_running_verified" 0 "state=$DET_STATE pid=$DET_PID why=$DET_WHY"

  printf -- '-- case 2: 子串假阳性（v1 pgrep -f 的坑） --\n'
  ( cd "$D" && exec bash "$decoy_sh" "$mock.metrics" ) </dev/null >>"$D/decoy.log" 2>&1 & dec=$!
  sleep 0.4
  pgrep_out="$(pgrep -f "$(basename "$mock")" 2>/dev/null | tr '\n' ' ')"; pgrep_rc=$?
  printf 'v1 检测器 pgrep -f "%s" → rc=%s pids=[%s]  (真实目标=%s 伪装进程=%s)\n' \
    "$(basename "$mock")" "$pgrep_rc" "$pgrep_out" "$p1" "$dec"
  printf '伪装进程 argv: %s\n' "$(proc_cmdline_text "$dec")"
  targeted=0; for pp in $pgrep_out; do [ "$pp" = "$dec" ] && targeted=1; done
  printf '新检测器对该伪装进程的判定：'; verify_pid_for_job 0 "$dec" && printf 'running(误判!)\n' || printf 'NOT running（正确）理由=%s\n' "$VERIFY_WHY"
  [ "$targeted" = 1 ] && _st_case "pgrep_substring_false_positive_reproduced" 1 "无关进程 pid=$dec（argv 仅含 $mock.metrics 子串）被 pgrep -f 命中 ⇒ v1 会误判为在跑" \
                       || _st_case "pgrep_substring_false_positive_reproduced" 0 "decoy pid=$dec 未被 pgrep 命中"
  verify_pid_for_job 0 "$dec"; [ "$?" != 0 ] && _st_case "detector_rejects_substring_decoy" 1 "why=$VERIFY_WHY" \
                                          || _st_case "detector_rejects_substring_decoy" 0 "误判为 running"

  printf -- '-- case 3: PID 复用（argv/cwd 吻合但 starttime 变化）＋ 无 token ⇒ 拒绝 pidfile，改走扫描 --\n'
  printf 'pid=%s starttime=1 script=%s job=stj ts=%s\n' "$p1" "$mock" "$(now_iso)" >"$pf"
  detect_job 0
  { [ "$DET_STATE" = running ] && [ "$DET_PID" = "$p1" ] && [ "$DET_WHY" = adopt_scan ] \
    && [ "$PF_REJECT" = pidfile_starttime_mismatch ]; } \
    && _st_case "detector_starttime_mismatch_refuses_pidfile" 1 "state=$DET_STATE pid=$DET_PID why=$DET_WHY pf_reject=$PF_REJECT（v2 会静默认领 pid_reuse_adopted_starttime_refresh ⇒ R1 crit S2 洞已堵）" \
    || _st_case "detector_starttime_mismatch_refuses_pidfile" 0 "state=$DET_STATE pid=$DET_PID why=$DET_WHY pf_reject=$PF_REJECT"
  [ "$DET_WHY" != pid_reuse_adopted_starttime_refresh ] \
    && _st_case "detector_no_silent_pid_reuse_adopt" 1 "不再存在 starttime 不符即认领的分支（why=$DET_WHY）" \
    || _st_case "detector_no_silent_pid_reuse_adopt" 0 "仍走了 pid_reuse 静默认领"

  printf -- '-- case 4: PID 文件指向无关存活进程（argv 不匹配）⇒ 否掉 PID 文件并认领真进程 --\n'
  printf 'pid=%s starttime=%s script=%s job=stj ts=%s\n' "$dec" "$(proc_starttime "$dec")" "$mock" "$(now_iso)" >"$pf"
  detect_job 0
  { [ "$DET_STATE" = running ] && [ "$DET_PID" = "$p1" ] && [ "$PF_REJECT" = pidfile_stale_argv_token_mismatch ]; } \
    && _st_case "detector_stale_pidfile_other_process" 1 "state=$DET_STATE pid=$DET_PID pf_reject=$PF_REJECT why=$DET_WHY" \
    || _st_case "detector_stale_pidfile_other_process" 0 "state=$DET_STATE pid=$DET_PID pf_reject=$PF_REJECT why=$DET_WHY"

  printf -- '-- case 5: PID 文件指向已死 PID（真进程仍在）⇒ 否掉 PID 文件并认领 --\n'
  printf 'pid=999999 starttime=1 script=%s job=stj ts=%s\n' "$mock" "$(now_iso)" >"$pf"
  detect_job 0
  { [ "$DET_STATE" = running ] && [ "$DET_PID" = "$p1" ] && [ "$PF_REJECT" = pidfile_dead ]; } \
    && _st_case "detector_stale_pidfile_dead_pid" 1 "state=$DET_STATE pid=$DET_PID pf_reject=$PF_REJECT" \
    || _st_case "detector_stale_pidfile_dead_pid" 0 "state=$DET_STATE pid=$DET_PID pf_reject=$PF_REJECT why=$DET_WHY"

  printf -- '-- case 6: PID 文件缺失但进程活着 ⇒ running/adopt_scan --\n'
  rm -f "$pf"; detect_job 0
  [ "$DET_STATE" = running ] && [ "$DET_PID" = "$p1" ] && [ "$DET_WHY" = adopt_scan ] \
    && _st_case "detector_adopt_scan" 1 "state=$DET_STATE pid=$DET_PID why=$DET_WHY" \
    || _st_case "detector_adopt_scan" 0 "state=$DET_STATE pid=$DET_PID why=$DET_WHY"

  printf -- '-- case 7: 歧义（两个候选）⇒ ambiguous，绝不 spawn --\n'
  ( cd "$D" && exec bash "$mock" ) </dev/null >>"$D/out.log" 2>&1 & p2=$!
  sleep 0.4; detect_job 0
  [ "$DET_STATE" = ambiguous ] && _st_case "detector_ambiguous_refuses_spawn" 1 "state=$DET_STATE pids=${DET_PID:-} why=$DET_WHY" \
                               || _st_case "detector_ambiguous_refuses_spawn" 0 "state=$DET_STATE pids=${DET_PID:-} why=$DET_WHY"
  _st_kill_local "$p2"; sleep 0.3

  printf -- '-- case 8: 完成守卫语义（与 v1 一致） --\n'
  job_complete 0 && _st_case "guard_incomplete_without_marker" 0 "无标记却判为完成" \
                 || _st_case "guard_incomplete_without_marker" 1 "无标记 ⇒ 未完成（允许重启）"
  printf '全部完成！stats=x\n' >>"$cl"
  job_complete 0 && _st_case "guard_complete_with_marker" 1 "含标记 ⇒ 完成（永不重启）" \
                 || _st_case "guard_complete_with_marker" 0 "含标记却判为未完成"
  : >"$cl"

  printf -- '-- case 9: 陈旧 PID 文件 + 进程确实不在 ⇒ absent（该重启的判定） --\n'
  _st_kill_local "$p1"; sleep 0.4
  printf 'pid=%s starttime=1 script=%s job=stj ts=%s\n' "$p1" "$mock" "$(now_iso)" >"$pf"
  detect_job 0
  [ "$DET_STATE" = absent ] && _st_case "detector_absent_when_truly_dead" 1 "state=$DET_STATE why=$DET_WHY（应触发重启）" \
                            || _st_case "detector_absent_when_truly_dead" 0 "state=$DET_STATE pid=$DET_PID why=$DET_WHY"

  printf -- '-- case 10: spawn 后 PID 文件必须指向真实进程（setsid fork 坑） --\n'
  rm -f "$pf"; : >"$D/out.log"
  KEEPER_SETSID=1 KEEPER_DRY_RUN=0 spawn_job 0
  local rc=$?
  if [ "$rc" = 0 ] && [ -n "$SPAWN_PID" ] && proc_exists "$SPAWN_PID" \
     && verify_pid_for_job 0 "$SPAWN_PID"; then
    _st_case "spawn_pidfile_points_to_real_pid" 1 "setsid 启动后 PID 文件 pid=$SPAWN_PID 核对通过（starttime=$(proc_starttime "$SPAWN_PID")）"
    _st_kill_local "$SPAWN_PID"
  else
    _st_case "spawn_pidfile_points_to_real_pid" 0 "rc=$rc spawn_pid=${SPAWN_PID:-} why=$SPAWN_WHY"
  fi

  # ================= v3 新增用例（[G] 认领 / [I] 挡板 / [E] 单例 / [H] 心跳） =====
  # ---- v3.1 重写 case 12（世代绑定）：旧断言"token 单证即认领"正是 c1 冒充者通道 ----
  printf -- '-- case 12: 世代绑定——keeper 亲启 + token ⇒ 认领；偷 token 的陌生进程 ⇒ 拒绝 --\n'
  rm -f "$pf"
  # 12a：keeper 亲启的进程（spawn_job 写 gen/<job>.rec）⇒ 即使删掉 pidfile 也应认领
  : >"$D/out.log"
  KEEPER_SETSID=1 KEEPER_DRY_RUN=0 spawn_job 0; local rc12=$?
  local gp="${SPAWN_PID:-}"
  if [ "$rc12" = 0 ] && [ -n "$gp" ] && [ -d "/proc/$gp" ]; then
    rm -f "$pf"
    detect_job 0
    { [ "$DET_STATE" = running ] && [ "$DET_PID" = "$gp" ] && [ "$DET_WHY" = adopt_token ] && [ "$DET_TRUST" = token ]; } \
      && _st_case "identity_generation_bound_adopt" 1 "keeper 亲启世代 pid=$gp ⇒ state=$DET_STATE why=$DET_WHY trust=$DET_TRUST（token 哈希 + pid/starttime 与 gen 记录逐字相符）" \
      || _st_case "identity_generation_bound_adopt" 0 "state=$DET_STATE pid=${DET_PID:-} why=$DET_WHY trust=${DET_TRUST:-}"
  else
    _st_case "identity_generation_bound_adopt" 0 "spawn_job 未成功（rc=$rc12 pid=$gp）⇒ 无法验证"
  fi
  # 12a2：无 token 的 keeper 亲启进程（TOKEN_ENFORCE=0）⇒ 世代记录单独支撑认领（adopt_generation）
  _st_kill_local "$gp" 2>/dev/null; sleep 0.3; rm -f "$pf"
  local tok_keep="$KEEPER_TOKEN_ENFORCE"; KEEPER_TOKEN_ENFORCE=0
  KEEPER_SETSID=1 KEEPER_DRY_RUN=0 spawn_job 0; local rc12b=$?; KEEPER_TOKEN_ENFORCE="$tok_keep"
  local gp2="${SPAWN_PID:-}"
  if [ "$rc12b" = 0 ] && [ -n "$gp2" ] && [ -d "/proc/$gp2" ]; then
    rm -f "$pf"; detect_job 0
    { [ "$DET_STATE" = running ] && [ "$DET_PID" = "$gp2" ] && [ "$DET_WHY" = adopt_generation ]; } \
      && _st_case "identity_generation_without_token" 1 "无 token 但世代相符 ⇒ why=$DET_WHY trust=$DET_TRUST pid=$gp2（世代记录本身即是认领凭据）" \
      || _st_case "identity_generation_without_token" 0 "state=$DET_STATE pid=${DET_PID:-} why=$DET_WHY trust=${DET_TRUST:-}"
  else
    _st_case "identity_generation_without_token" 0 "spawn_job 未成功（rc=$rc12b pid=$gp2）"
  fi
  _st_kill_local "$gp2" 2>/dev/null; sleep 0.3; rm -f "$pf"; rm -f "$(gen_rec_path 0)" 2>/dev/null
  # 12b：偷 token 的陌生进程（argv/cwd/exe 逐轴复刻 + token 逐字复制）⇒ 必须被拒；
  #      与此同时真作业（keeper 亲启世代）仍被认领 —— c1a 攻击的直接反转
  rm -f "$pf"; : >"$D/out.log"
  KEEPER_SETSID=1 KEEPER_DRY_RUN=0 spawn_job 0; local rc12c=$?
  local real="${SPAWN_PID:-}" before_refuse0 after_refuse0 ref0 stolen0
  if [ "$rc12c" = 0 ] && [ -n "$real" ] && [ -d "/proc/$real" ]; then
    stolen0="$(tr '\0' '\n' <"/proc/$real/environ" 2>/dev/null | sed -n 's/^SCRAPE_KEEPER_TOKEN=//p' | head -1)"
    ( cd "$D" && SCRAPE_KEEPER_TOKEN="$stolen0" exec bash "$mock" ) </dev/null >>"$D/out.log" 2>&1 & local p3b=$!
    sleep 0.5
    # 复刻 c1a 攻击：伪造 pidfile 指向冒充者（pid+starttime 自洽，只有"世代"对不上）
    printf 'pid=%s starttime=%s script=%s job=stj ts=%s forged_by=selftest\n' \
      "$p3b" "$(proc_starttime "$p3b" 2>/dev/null || printf '?')" "$mock" "$(now_iso)" >"$pf"
    before_refuse0="$(grep -c '"event":"keeper_adopt_refused"' "$KEEPER_AUDIT" 2>/dev/null)"; before_refuse0="${before_refuse0:-0}"
    detect_job 0
    after_refuse0="$(grep -c '"event":"keeper_adopt_refused"' "$KEEPER_AUDIT" 2>/dev/null)"; after_refuse0="${after_refuse0:-0}"
    ref0="$(grep '"event":"keeper_adopt_refused"' "$KEEPER_AUDIT" 2>/dev/null | tail -1)"
    local pf_after=""
    local st_keep0; st_keep0="$(st_get "last_state_stj")"; st_set "last_state_stj" none
    cycle >/dev/null 2>&1
    st_set "last_state_stj" "$st_keep0"
    pf_after="$(sed -n 's/^pid=\([0-9]*\).*/\1/p' "$pf" 2>/dev/null | head -1)"
    { [ "$DET_PID" = "$real" ] && [ "$after_refuse0" -gt "$before_refuse0" ] \
      && printf '%s' "$ref0" | grep -Eq 'stranger_token|stranger_candidate' \
      && printf '%s' "$ref0" | grep -q "candidate_pid\":$p3b" \
      && [ "$pf_after" = "$real" ]; } \
      && _st_case "identity_stolen_token_stranger_refused" 1 "冒充者 pid=$p3b（token 逐字复制 ${stolen0:0:22}…、argv/cwd/exe 逐轴复刻）被拒：reason=$(printf '%s' "$ref0" | sed -n 's/.*"reason":"\([a-z_]*\)".*/\1/p' | head -1) gen_match=$(printf '%s' "$ref0" | sed -n 's/.*"gen_match":"\([a-z_]*\)".*/\1/p' | head -1)；真作业 pid=$real 仍被认领，pidfile→$pf_after（≠冒充者）" \
      || _st_case "identity_stolen_token_stranger_refused" 0 "未达标：DET_PID=${DET_PID:-} real=$real impostor=$p3b refused_delta=$((after_refuse0-before_refuse0)) pidfile=$pf_after ref=$ref0"
    _st_kill_local "$p3b" 2>/dev/null
    _st_kill_local "$real" 2>/dev/null
  else
    _st_case "identity_stolen_token_stranger_refused" 0 "spawn_job 未成功（rc=$rc12c pid=$real）⇒ 无法验证"
  fi
  sleep 0.3; rm -f "$pf" "$(gen_rec_path 0)" 2>/dev/null
  # 12c：无世代记录（state 被清/keeper 首次上任）+ token + 路径证据 ⇒ 认领但必须显式 adopt_unverified
  rm -f "$(gen_rec_path 0)" 2>/dev/null
  local before_uv; before_uv="$(grep -c '"event":"adopt_unverified"' "$KEEPER_AUDIT" 2>/dev/null)"; before_uv="${before_uv:-0}"
  ( cd "$D" && SCRAPE_KEEPER_TOKEN="kv3:${KEEPER_FAMILY:-kfX}:stj:nogen" exec bash "$mock" ) </dev/null >>"$D/out.log" 2>&1 & local p3c=$!
  sleep 0.4; rm -f "$pf"
  detect_job 0
  local uv_after; uv_after="$(grep -c '"event":"adopt_unverified"' "$KEEPER_AUDIT" 2>/dev/null)"; uv_after="${uv_after:-0}"
  local cycle_ok=no
  JOB_PIDFILE[0]="$pf"; detect_job 0 >/dev/null
  # 走一遍认领分支（cycle 会写 pidfile + adopt_unverified）
  local st_keep; st_keep="$(st_get "last_state_stj")"; st_set "last_state_stj" none
  cycle >/dev/null 2>&1
  st_set "last_state_stj" "$st_keep"
  uv_after="$(grep -c '"event":"adopt_unverified"' "$KEEPER_AUDIT" 2>/dev/null)"; uv_after="${uv_after:-0}"
  local uvline; uvline="$(grep '"event":"adopt_unverified"' "$KEEPER_AUDIT" 2>/dev/null | tail -1)"
  { [ "$uv_after" -gt "$before_uv" ] && printf '%s' "$uvline" | grep -q 'token_without_generation_record'; } \
    && _st_case "identity_token_without_generation_not_silent" 1 "token 单证（无世代记录）⇒ 认领但显式出证 adopt_unverified{reason=token_without_generation_record}（+$((uv_after-before_uv))）" \
    || _st_case "identity_token_without_generation_not_silent" 0 "未出证（adopt_unverified $before_uv→$uv_after，末条=$uvline）"
  _st_kill_local "$p3c"; sleep 0.3
  rm -f "$pf"
  ( cd "$D" && SCRAPE_KEEPER_TOKEN="kv3:${KEEPER_FAMILY:-kfX}:stj:nonce1" exec bash "$mock" ) </dev/null >>"$D/out.log" 2>&1 & p3=$!
  sleep 0.4; detect_job 0
  { [ "$DET_STATE" = running ] && [ "$DET_PID" = "$p3" ] && [ "$DET_TRUST" = token ]; } \
    && _st_case "identity_token_adopt" 1 "无世代记录时 token+路径 证据认领：state=$DET_STATE pid=$DET_PID why=$DET_WHY trust=$DET_TRUST（并已出 adopt_unverified）" \
    || _st_case "identity_token_adopt" 0 "state=$DET_STATE pid=${DET_PID:-} why=$DET_WHY trust=${DET_TRUST:-}"

  printf -- '-- case 13: token 存在但 job 字段不是本作业 ⇒ 拒绝认领（冒充者） --\n'
  _st_kill_local "$p3"; sleep 0.4; rm -f "$pf"
  ( cd "$D" && SCRAPE_KEEPER_TOKEN="kv3:${KEEPER_FAMILY:-kfX}:OTHERJOB:nonce2" exec bash "$mock" ) </dev/null >>"$D/out.log" 2>&1 & p4=$!
  sleep 0.4
  local before_refuse; before_refuse="$(grep -c '"event":"keeper_adopt_refused"' "$KEEPER_AUDIT" 2>/dev/null)"; before_refuse="${before_refuse:-0}"
  detect_job 0
  local after_refuse; after_refuse="$(grep -c '"event":"keeper_adopt_refused"' "$KEEPER_AUDIT" 2>/dev/null)"; after_refuse="${after_refuse:-0}"
  { [ "$DET_STATE" = absent ] && [ "$DET_PID" = "" ] && [ "$after_refuse" -gt "$before_refuse" ]; } \
    && _st_case "identity_token_job_mismatch_refused" 1 "state=$DET_STATE why=$DET_WHY refused_event+$((after_refuse-before_refuse))（冒充者未被认领）" \
    || _st_case "identity_token_job_mismatch_refused" 0 "state=$DET_STATE pid=${DET_PID:-} why=$DET_WHY refused_event_delta=$((after_refuse-before_refuse))"

  printf -- '-- case 14: R1 crit S1 复现——basename-only 冒充者（异 cwd）⇒ 拒绝认领，且 cwd_strict=0 也拒 --\n'
  _st_kill_local "$p4"; sleep 0.4; rm -f "$pf"
  mkdir -p "$D/decoydir"
  cp -f "$mock" "$D/decoydir/mock-target.sh"
  ( cd "$D/decoydir" && exec bash mock-target.sh ) </dev/null >>"$D/decoy.log" 2>&1 & p5=$!
  sleep 0.4
  local cs_keep; cs_keep="$(cwd_strict_of stj)"
  detect_job 0
  local s1_state="$DET_STATE" s1_why="$DET_WHY" s1_pid="${DET_PID:-}"
  JOB_CWD_STRICT[stj]=0            # 故意关挡板（模拟旧部署模板）：身份判据仍应拒绝
  detect_job 0
  local s1b_state="$DET_STATE" s1b_why="$DET_WHY"
  JOB_CWD_STRICT[stj]="$cs_keep"
  { [ "$s1_state" = absent ] && [ "$s1_pid" = "" ] && [ "$s1b_state" = absent ]; } \
    && _st_case "impostor_basename_only_refused" 1 "cwd_strict=1 ⇒ state=$s1_state why=$s1_why ; cwd_strict=0 ⇒ state=$s1b_state why=$s1b_why（v2 在 cwd_strict=0 时认领该冒充者）" \
    || _st_case "impostor_basename_only_refused" 0 "cwd_strict=1 state=$s1_state pid=$s1_pid ; cwd_strict=0 state=$s1b_state（冒充者被认领！）"
  _st_kill_local "$p5"; sleep 0.3

  printf -- '-- case 15: rail——部署模板里 job_cwd_strict X 0 不生效（须 env 放行） --\n'
  local rail_before="$KEEPER_RAIL_ALLOW_CWD_STRICT_OFF"
  KEEPER_RAIL_ALLOW_CWD_STRICT_OFF=0
  JOB_CWD_STRICT[stj]=1
  RAIL_TRIPPED=()
  job_cwd_strict stj 0 >/dev/null 2>&1
  local refused_val; refused_val="$(cwd_strict_of stj)"
  local refused_n; refused_n="${#RAIL_TRIPPED[@]}"
  KEEPER_RAIL_ALLOW_CWD_STRICT_OFF=1
  RAIL_OVERRIDES=()
  job_cwd_strict stj 0 >/dev/null 2>&1
  local allowed_val; allowed_val="$(cwd_strict_of stj)"
  local allowed_n; allowed_n="${#RAIL_OVERRIDES[@]}"
  KEEPER_RAIL_ALLOW_CWD_STRICT_OFF="$rail_before"; JOB_CWD_STRICT[stj]=1
  { [ "$refused_val" = 1 ] && [ "$refused_n" = 1 ] && [ "$allowed_val" = 0 ] && [ "$allowed_n" = 1 ]; } \
    && _st_case "rail_cwd_strict_conf_cannot_disable" 1 "conf 请求 0 ⇒ 实际 $refused_val（拒绝审计 $refused_n 条）；env 放行 ⇒ 实际 $allowed_val（扩权审计 $allowed_n 条）" \
    || _st_case "rail_cwd_strict_conf_cannot_disable" 0 "conf ⇒ $refused_val(tripped=$refused_n)；env ⇒ $allowed_val(override=$allowed_n)"

  printf -- '-- case 16: rail——KEEPER_STALL_ACTION=restart 无 env 放行时被降级为 audit --\n'
  local act_keep="$KEEPER_STALL_ACTION" rail_keep="$KEEPER_RAIL_ALLOW_STALL_RESTART"
  KEEPER_STALL_ACTION=restart; KEEPER_RAIL_ALLOW_STALL_RESTART=0; rails_apply >/dev/null 2>&1
  local forced_act="$KEEPER_STALL_ACTION"
  KEEPER_STALL_ACTION=restart; KEEPER_RAIL_ALLOW_STALL_RESTART=1; rails_apply >/dev/null 2>&1
  local allowed_act="$KEEPER_STALL_ACTION"
  KEEPER_STALL_ACTION="$act_keep"; KEEPER_RAIL_ALLOW_STALL_RESTART="$rail_keep"
  { [ "$forced_act" = audit ] && [ "$allowed_act" = restart ]; } \
    && _st_case "rail_stall_restart_needs_env" 1 "无 env ⇒ $forced_act（降级）；env 放行 ⇒ $allowed_act" \
    || _st_case "rail_stall_restart_needs_env" 0 "无 env ⇒ $forced_act；env ⇒ $allowed_act"

  printf -- '-- case 17: flock 单例锁——第二个 keeper 抢锁失败并出证持有者 --\n'
  local lk="$D/singleton.lock" hf="$D/singleton.holder"
  rm -f "$lk" "$hf"
  ( exec 9>>"$lk"; flock -n 9; printf 'pid=%s run_id=TEST holder\n' "$$" >"$hf"; sleep 3 ) & local lpid=$!
  sleep 0.5
  local hf_keep="$KEEPER_HOLDER_FILE"; KEEPER_HOLDER_FILE="$hf"
  singleton_acquire "$lk"; local rc_conflict=$?
  local holder_seen="$SINGLETON_HOLDER_PID"
  wait "$lpid" 2>/dev/null
  singleton_acquire "$lk"; local rc_ok=$?
  local held="$SINGLETON_HELD"
  singleton_release >/dev/null 2>&1
  KEEPER_HOLDER_FILE="$hf_keep"
  { [ "$rc_conflict" = 4 ] && [ "$rc_ok" = 0 ] && [ "$held" = 1 ]; } \
    && _st_case "singleton_flock_conflict" 1 "冲突 rc=$rc_conflict holder_pid=$holder_seen；持有者退出后 rc=$rc_ok SINGLETON_HELD=$held" \
    || _st_case "singleton_flock_conflict" 0 "冲突 rc=$rc_conflict holder_pid=$holder_seen；后续 rc=$rc_ok held=$held"

  printf -- '-- case 18-24: 心跳分类（退避 / 闸门冻结 / 预算降档 / 真卡死 / 完成 / 缺失 / 正常） --\n'
  local hbf="$D/hb.json"
  if command -v python3 >/dev/null 2>&1 && [ -r "$KEEPER_HEARTBEAT_PY" ]; then
    local mode want got fails=0 detail=""
    for pair in ok:ok done:hb_done gate:gate_frozen backoff:backoff budget:budget_downgrade stall:stall; do
      mode="${pair%%:*}"; want="${pair##*:}"
      _st_mk_hb "$hbf" "$mode" "$$"
      hb_classify_file "$hbf"; got="$HB_CLASS"
      detail+="$mode→$got "
      [ "$got" = "$want" ] || fails=$((fails+1))
    done
    rm -f "$hbf"; hb_classify_file "$hbf"; got="$HB_CLASS"
    detail+="missing→$got"
    [ "$got" = hb_missing ] || fails=$((fails+1))
    [ "$fails" = 0 ] && _st_case "heartbeat_classification_matrix" 1 "$detail（心跳读取器=$KEEPER_HEARTBEAT_PY）" \
                      || _st_case "heartbeat_classification_matrix" 0 "$detail（$fails 项不符）"
    # 不误杀合法退避：退避态下 heartbeat_cycle 不得产生 heartbeat_stall / restart
    local before_stall; before_stall="$(grep -c '"event":"heartbeat_stall"' "$KEEPER_AUDIT" 2>/dev/null)"; before_stall="${before_stall:-0}"
    _st_mk_hb "$hbf" backoff "$$"
    JOB_HB_FILE[0]="$hbf"
    st_set "hb_stall_streak_stj" 0
    heartbeat_cycle 0 "$$" legacy 2>/dev/null
    local after_stall; after_stall="$(grep -c '"event":"heartbeat_stall"' "$KEEPER_AUDIT" 2>/dev/null)"; after_stall="${after_stall:-0}"
    { [ "$after_stall" = "$before_stall" ] && [ "$HB_CLASS" = backoff ]; } \
      && _st_case "heartbeat_backoff_not_killed" 1 "class=$HB_CLASS，未产生 heartbeat_stall（$before_stall→$after_stall）：合法退避不被误杀" \
      || _st_case "heartbeat_backoff_not_killed" 0 "class=$HB_CLASS stall_events $before_stall→$after_stall"
    # 真卡死：连续 KEEPER_HB_CONFIRM 轮后必须出现 heartbeat_stall
    _st_mk_hb "$hbf" stall "$$"
    st_set "hb_stall_streak_stj" 0
    local i2; for ((i2=0; i2<KEEPER_HB_CONFIRM; i2++)); do heartbeat_cycle 0 "$$" legacy 2>/dev/null; done
    local after2; after2="$(grep -c '"event":"heartbeat_stall"' "$KEEPER_AUDIT" 2>/dev/null)"; after2="${after2:-0}"
    [ "$after2" -gt "$after_stall" ] \
      && _st_case "heartbeat_stall_confirmed_after_n_cycles" 1 "连续 $KEEPER_HB_CONFIRM 轮 stall ⇒ heartbeat_stall 事件 +$((after2-after_stall))（默认 action=audit，不杀进程）" \
      || _st_case "heartbeat_stall_confirmed_after_n_cycles" 0 "stall 事件未增加（$after_stall→$after2）"
    rm -f "$hbf"
  else
    _st_case "heartbeat_classification_matrix" 0 "环境缺 python3 或心跳读取器（$KEEPER_HEARTBEAT_PY）⇒ 无法验证分类矩阵"
  fi


  printf -- '-- case 25: 残留候选清理（自测不留残留进程） --\n'
  _st_kill_local "$dec"
  sleep 0.3
  local left=0
  for pp in $(scan_matches 0; printf '%s' "$SCAN_PIDS"); do [ -n "$pp" ] && left=$((left+1)); done
  [ "$left" = 0 ] && _st_case "selftest_leftover_processes_cleaned" 1 "无残留候选进程" \
                  || _st_case "selftest_leftover_processes_cleaned" 0 "仍有残留候选: $left"
  rm -rf "$D" 2>/dev/null
  local summary="{\"keeper\":\"$(jesc "$KEEPER_VERSION")\",\"ts\":\"$(now_iso)\",\"cases\":[$SELFTEST_RESULT],\"failed\":$SELFTEST_FAILED}"
  printf 'SELFTEST_JSON=%s\n' "$summary"
  local ncases; ncases="$(printf '%s' "$SELFTEST_RESULT" | grep -o '"case"' | wc -l)"
  audit selftest_result "" - "$(ev failed="$SELFTEST_FAILED" cases="$ncases" dir="$KEEPER_SELFTEST_DIR")"
  [ "$SELFTEST_FAILED" = 0 ] && { printf 'SELFTEST: PASS (%s cases)\n' "$ncases"; return 0; }
  printf 'SELFTEST: FAIL (%s failed / %s cases)\n' "$SELFTEST_FAILED" "$ncases"; return 1
}

# =========================== v3 [E] 单例锁 / 停机标志 =========================
# v3.1 [L] 加固：inode 校验（K-1 分裂脑）/ flock 失败原因区分 / fdinfo 交叉取证 /
#            starttime 核对（K-5 PID 复用）。退出码：0=持有 / 4=冲突 / 5=锁不可用 / 6=让位。
SINGLETON_HELD=0; SINGLETON_HOLDER_PID=""; SINGLETON_HOLDER_RAW=""; SINGLETON_HOLDER_ALIVE=""
SINGLETON_INODE=""; SINGLETON_LOCK_PATH=""; SINGLETON_HOLDER_VERIFIED=""
STOP_REQUESTED=0; STOP_SIGNAL=""
WAIT_CHILDREN_PENDING=0; WAIT_CHILDREN_WAITED=""
lock_inode_of() { stat -Lc '%d:%i' "$1" 2>/dev/null; }
# singleton_holders_scan LOCKFILE —— 用 /proc/<pid>/fdinfo 定位**真实**锁持有者
#   （不信任落盘 holder 记录：crit-adversarial C2b 读到 15/24 错指认）
#   输出每行：pid=<p> starttime=<st> fd=<n> cmd=<cmdline> lock=<fdinfo lock 行>
singleton_holders_scan() {
  local lf="$1" want d p fi fd fdino st n=0 lockline cmd kl
  want="$(stat -Lc '%d:%i' "$lf" 2>/dev/null)"; [ -n "$want" ] || return 1
  for d in /proc/[0-9]*; do
    p="${d#/proc/}"
    [ "$p" = "$$" ] && continue
    [ -d "$d/fdinfo" ] || continue
    for fi in "$d"/fdinfo/[0-9]*; do
      [ -r "$fi" ] || continue
      lockline="$(grep -m1 '^lock:' "$fi" 2>/dev/null)" || lockline=""
      [ -n "$lockline" ] || continue
      fd="${fi##*/}"
      fdino="$(stat -Lc '%d:%i' "$d/fd/$fd" 2>/dev/null)" || continue
      [ "$fdino" = "$want" ] || continue
      st="$(proc_starttime "$p" 2>/dev/null || printf '?')"
      cmd="$(proc_cmdline_text "$p" 2>/dev/null | cut -c1-60)"
      kl=no; case "$cmd" in *keeper-v3.sh*) kl=yes ;; esac
      printf 'pid=%s starttime=%s fd=%s keeper_like=%s cmd=%s lock=%s\n' "$p" "$st" "$fd" "$kl" "$cmd" \
        "$(printf '%s' "$lockline" | tr -d '\n' | cut -c1-40)"
      n=$((n+1))
    done
  done
  [ "$n" -gt 0 ]
}
# singleton_primary_holder LOCKFILE —— 从 fdinfo 扫描结果里挑"最可能的真持有者"
#   （优先 keeper_like=yes；纯 fd 继承的子进程排在后面 —— K-2 的 fd 泄漏会放大噪声）
singleton_primary_holder() {
  local lf="$1" lines best="" kl
  lines="$(singleton_holders_scan "$lf" 2>/dev/null)" || { printf ''; return 1; }
  best="$(printf '%s\n' "$lines" | grep -m1 'keeper_like=yes')"
  [ -n "$best" ] || best="$(printf '%s\n' "$lines" | head -1)"
  printf '%s' "$best"
}
singleton_holder_write() {
  local f="$KEEPER_HOLDER_FILE" tmp="$KEEPER_HOLDER_FILE.tmp.$$"
  mkdir -p "$(dirname "$f")" 2>/dev/null
  printf 'pid=%s starttime=%s run_id=%s keeper=%s script=%s lock_file=%s lock_inode=%s ts=%s\n' \
    "$$" "$(proc_starttime $$ 2>/dev/null || printf '?')" "$RUN_ID" "$KEEPER_VERSION" \
    "$KEEPER_SELF" "${SINGLETON_LOCK_PATH:-$KEEPER_LOCK_FILE}" "${SINGLETON_INODE:-?}" "$(now_iso)" \
    >"$tmp" 2>/dev/null && mv -f "$tmp" "$f" 2>/dev/null
}
# singleton_acquire [LOCKFILE] → 0=已持有 / 4=冲突（另有在位 keeper） / 5=锁不可用
singleton_acquire() {
  local lf="${1:-$KEEPER_LOCK_FILE}" hp="" hraw="" alive=unknown tries=0 err="" rc=0
  local errf="$KEEPER_STATE_DIR/.flock.err.$$" holders="" hst now_st
  mkdir -p "$(dirname "$lf")" 2>/dev/null
  # ---- (a) flock 二进制存在性：缺失 ⇒ 明确"锁不可用"，绝不误报"冲突" ----
  if ! command -v flock >/dev/null 2>&1; then
    audit keeper_lock_unavailable "" - \
      "$(ev reason=flock_binary_missing lock_file="$lf" self_pid="$$" rc=5 action=refuse_start note="flock 不存在：无法保证单例互斥，明确以 rc=5 拒绝启动（不再一律误报 singleton_conflict）")"
    hlog "FATAL flock 不存在：单例锁不可用 ⇒ 拒绝启动 (rc=5)"
    return 5
  fi
  # ---- (b) 打开锁文件：失败 ⇒ rc=5 + 错误文本取证 ----
  if ! exec 9>>"$lf"; then
    local err2; err2="$( bash -c 'exec 9>>"$1"' _ "$lf" 2>&1 >/dev/null )"
    audit keeper_lock_unavailable "" - \
      "$(ev reason=lock_open_failed lock_file="$lf" self_pid="$$" errno_text="$(printf '%s' "$err2" | head -c 160)" rc=5 action=refuse_start note="锁文件不可打开（权限/只读 fs/路径缺失）：区分于'冲突'")"
    hlog "FATAL 无法打开单例锁文件 $lf：$(printf '%s' "$err2" | head -c 120)（rc=5）"
    return 5
  fi
  # ---- (c) 抢锁（区分冲突 vs 不可用：errno 文本 + fdinfo 交叉取证）----
  flock -n 9 2>"$errf"; rc=$?
  err="$(head -c 200 "$errf" 2>/dev/null)"; rm -f "$errf" 2>/dev/null
  if [ "$rc" != 0 ]; then
    holders="$(singleton_holders_scan "$lf" 2>/dev/null | tr '\n' ';')"
    if [ -z "$holders" ] && [ -n "$err" ]; then
      # flock 报错且无任何进程持有该 inode ⇒ 判为"不可用"（ENOLCK/ENOTSUP/网络 fs 等）
      exec 9>&-
      audit keeper_lock_unavailable "" - \
        "$(ev reason=flock_syscall_error lock_file="$lf" self_pid="$$" flock_errno_text="$(printf '%s' "$err" | tr -d '\n' | head -c 160)" holders="${holders:-none}" rc=5 action=refuse_start note="flock 报错且 fdinfo 扫描无持有者 ⇒ 锁机制不可用（非冲突），明确拒绝启动")"
      hlog "FATAL flock 不可用：$(printf '%s' "$err" | head -c 120)（rc=5）"
      return 5
    fi
    if [ -z "$holders" ]; then
      # 无持有者但抢锁失败：疑竞态，短暂重试一次再定级
      sleep 0.2 9>&-; flock -n 9 2>/dev/null; rc=$?
      holders="$(singleton_holders_scan "$lf" 2>/dev/null | tr '\n' ';')"
      if [ "$rc" != 0 ] && [ -z "$holders" ]; then
        exec 9>&-
        audit keeper_lock_unavailable "" - \
          "$(ev reason=flock_failed_no_holder lock_file="$lf" self_pid="$$" flock_errno_text="$(printf '%s' "${err:-none}" | tr -d '\n' | head -c 160)" rc=5 action=refuse_start note="重试后仍抢锁失败且扫描不到持有者 ⇒ 判为锁不可用")"
        hlog "FATAL 抢锁失败且无持有者 ⇒ 锁不可用（rc=5）"
        return 5
      fi
    fi
    if [ "$rc" != 0 ]; then
      # 真冲突：持有者取证（fdinfo 优先，落盘记录仅作参考并交叉核对 starttime）
      while [ "$tries" -lt 6 ]; do
        hraw="$(head -c 256 "$KEEPER_HOLDER_FILE" 2>/dev/null | tr -d '\n')"
        hp="$(printf '%s' "$hraw" | sed -n 's/.*pid=\([0-9]*\).*/\1/p')"
        [ -n "$hp" ] && break
        sleep 0.1 9>&-; tries=$((tries+1))
      done
      SINGLETON_HOLDER_VERIFIED="no"
      if [ -n "$hp" ] && [ -d "/proc/$hp" ]; then
        alive=yes
        hst="$(printf '%s' "$hraw" | sed -n 's/.*starttime=\([0-9]*\).*/\1/p')"
        now_st="$(proc_starttime "$hp" 2>/dev/null || printf '?')"
        if [ -z "$hst" ]; then SINGLETON_HOLDER_VERIFIED="unverified_no_starttime"
        elif [ "$hst" = "$now_st" ]; then SINGLETON_HOLDER_VERIFIED="yes_starttime_ok"
        else SINGLETON_HOLDER_VERIFIED="no_starttime_mismatch(record=$hst proc=$now_st)"; alive=no_pid_reuse; fi
        case "$(proc_cmdline_text "$hp" 2>/dev/null)" in
          *keeper-v3.sh*) : ;;
          *) [ "$SINGLETON_HOLDER_VERIFIED" = yes_starttime_ok ] && SINGLETON_HOLDER_VERIFIED="no_cmdline_not_keeper" ;;
        esac
      elif [ -n "$hp" ]; then alive=no; SINGLETON_HOLDER_VERIFIED="no_record_dead"
      else SINGLETON_HOLDER_VERIFIED="no_record_missing"; fi
      # 落盘记录不可信时，用 fdinfo 实时读数指认持有者（C2b 修复：陈旧记录曾 15/24 错指认）
      local phline phpid
      phline="$(singleton_primary_holder "$lf" 2>/dev/null)"
      phpid="$(printf '%s' "$phline" | sed -n 's/^pid=\([0-9]*\).*/\1/p')"
      if [ "$SINGLETON_HOLDER_VERIFIED" != yes_starttime_ok ] && [ -n "$phpid" ]; then
        hp="$phpid"; alive=yes
        SINGLETON_HOLDER_VERIFIED="via_fdinfo_scan"
      fi
      SINGLETON_HOLDER_PID="${hp:-unknown}"; SINGLETON_HOLDER_RAW="$hraw"; SINGLETON_HOLDER_ALIVE="$alive"
      audit singleton_conflict "" - \
        "$(ev holder_pid="${hp:-unknown}" self_pid="$$" lock_file="$lf" lock_inode="$(lock_inode_of "$lf")" holder_alive="$alive" holder_verified="$SINGLETON_HOLDER_VERIFIED" holder_record_pid="$(printf '%s' "$hraw" | sed -n 's/.*pid=\([0-9]*\).*/\1/p')" primary_holder="${phline:-none}" holders_scan="${holders:-none}" holder_record="${hraw:0:200}" flock_errno_text="$(printf '%s' "${err:-none}" | tr -d '\n' | head -c 120)" action=exit exit_code=4 note="flock 单例锁被在位 keeper 持有：本实例拒绝启动，绝不与在位 keeper 互踩；持有者以 fdinfo 交叉取证为准（落盘记录仅参考）")" \
        process "$(ev holder_pid="${hp:-unknown}" self_pid="$$" holder_verified="$SINGLETON_HOLDER_VERIFIED")"
      exec 9>&-
      return 4
    fi
  fi
  # ---- (d) 持锁成功：记录 inode + holder 记录（含 starttime/inode，供 K-1/K-5 交叉核对）----
  SINGLETON_LOCK_PATH="$lf"
  SINGLETON_INODE="$(lock_inode_of "$lf")"
  printf 'pid=%s starttime=%s run_id=%s keeper=%s lock_inode=%s ts=%s\n' "$$" \
    "$(proc_starttime $$ 2>/dev/null || printf '?')" "$RUN_ID" "$KEEPER_VERSION" "$SINGLETON_INODE" "$(now_iso)" \
    >"$lf" 2>/dev/null
  singleton_holder_write
  SINGLETON_HELD=1
  audit singleton_acquired "" - "$(ev self_pid="$$" lock_file="$lf" lock_inode="$SINGLETON_INODE" holder_file="$KEEPER_HOLDER_FILE")"
  return 0
}
# singleton_check_inode —— K-1：每轮校验锁文件 inode，防「删锁重建 ⇒ 双持分裂脑」
#   0=仍安全持有（必要时已在新 inode 上续持）；1=已让位（调用方必须停机退出）
singleton_check_inode() {
  [ "$SINGLETON_HELD" = 1 ] || return 0
  [ "$KEEPER_LOCK_INODE_GUARD" = 1 ] || return 0
  local lf="${SINGLETON_LOCK_PATH:-$KEEPER_LOCK_FILE}" now holders
  now="$(lock_inode_of "$lf")"
  [ -n "$now" ] && [ "$now" = "$SINGLETON_INODE" ] && return 0
  audit keeper_lock_rotated "" - \
    "$(ev reason=lock_inode_changed lock_file="$lf" held_inode="${SINGLETON_INODE:-?}" now_inode="${now:-missing}" self_pid="$$" action=reacquire_probe note="锁文件被删除/重建（或消失）：fd9 指向旧 inode，已不再互斥 ⇒ 立即在新 inode 上重新抢锁，失败则让位")"
  exec 9>&- 2>/dev/null
  if exec 9>>"$lf" 2>/dev/null && flock -n 9 2>/dev/null; then
    SINGLETON_INODE="$(lock_inode_of "$lf")"
    printf 'pid=%s starttime=%s run_id=%s keeper=%s lock_inode=%s ts=%s\n' "$$" \
      "$(proc_starttime $$ 2>/dev/null || printf '?')" "$RUN_ID" "$KEEPER_VERSION" "$SINGLETON_INODE" "$(now_iso)" \
      >"$lf" 2>/dev/null
    singleton_holder_write
    audit keeper_lock_rotated "" - \
      "$(ev action=reacquired new_inode="$SINGLETON_INODE" lock_file="$lf" self_pid="$$" note="重新抢锁成功：仍为唯一持有者（分裂脑已避免）")"
    return 0
  fi
  holders="$(singleton_holders_scan "$lf" 2>/dev/null | tr '\n' ';')"
  audit singleton_conflict "" - \
    "$(ev reason=lock_replaced_held_elsewhere lock_file="$lf" held_inode="${SINGLETON_INODE:-?}" now_inode="${now:-missing}" self_pid="$$" holders_scan="${holders:-none}" action=yield exit_code=6 note="锁文件被删/重建且新 inode 已被他人持有 ⇒ 本实例让位（绝不双持）")" \
    process "$(ev self_pid="$$" reason=lock_replaced_held_elsewhere)"
  SINGLETON_HELD=0
  return 1
}
# singleton_probe [LOCKFILE] → 0=空闲 / 1=有持有者（打印持有者） / 2=不可判定（只读，不抢锁）
singleton_probe() {
  local lf="${1:-$KEEPER_LOCK_FILE}" hp hraw hst now_st holders rec_ok=stale
  [ -e "$lf" ] || { printf 'free (锁文件不存在)\n'; return 0; }
  if exec 8>>"$lf"; then
    if flock -n 8 2>/dev/null; then
      flock -u 8 2>/dev/null; exec 8>&-
      printf 'free (无活持有者)\n'; return 0
    fi
    exec 8>&-
  else
    printf 'unknown (锁文件不可打开)\n'; return 2
  fi
  # 已被持有：fdinfo 交叉取证（不信落盘记录）+ starttime 核对（K-5 防 PID 复用）
  holders="$(singleton_holders_scan "$lf" 2>/dev/null | tr '\n' ';')"
  local primary; primary="$(singleton_primary_holder "$lf" 2>/dev/null)"
  hraw="$(head -c 256 "$KEEPER_HOLDER_FILE" 2>/dev/null | tr -d '\n')"
  hp="$(printf '%s' "$hraw" | sed -n 's/.*pid=\([0-9]*\).*/\1/p')"
  hst="$(printf '%s' "$hraw" | sed -n 's/.*starttime=\([0-9]*\).*/\1/p')"
  if [ -n "$hp" ] && [ -d "/proc/$hp" ]; then
    now_st="$(proc_starttime "$hp" 2>/dev/null || printf '?')"
    if [ -z "$hst" ]; then rec_ok=unverified_no_starttime
    elif [ "$hst" = "$now_st" ]; then rec_ok=yes_starttime_ok
    else rec_ok="no_starttime_mismatch(record=$hst proc=$now_st)"; fi
  elif [ -n "$hp" ]; then rec_ok=no_record_dead
  else rec_ok=no_record_missing; fi
  if [ -n "$hp" ] && [ "$rec_ok" = yes_starttime_ok ]; then
    printf 'held pid=%s alive=yes record_verified=%s primary_holder=%s holders_scan=[%s] record=%s\n' \
      "$hp" "$rec_ok" "${primary:-none}" "${holders:-none}" "$hraw"; return 1
  fi
  # 落盘记录不可信（缺失/陈旧/starttime 不符）⇒ 用 fdinfo 实时读数指认持有者
  # （crit-adversarial C2b：陈旧 holder 记录曾造成 15/24 错指认）
  local pprim; pprim="$(printf '%s' "$primary" | sed -n 's/^pid=\([0-9]*\).*/\1/p')"
  printf 'held pid=%s alive=%s record_verified=%s primary_holder=%s holders_scan=[%s] record=%s\n' \
    "${pprim:-${hp:-unknown}}" "$([ -n "$pprim" ] && printf yes || printf unknown)" "$rec_ok" \
    "${primary:-none}" "${holders:-none}" "${hraw:-none}"
  return 1
}
singleton_release() {
  [ "$SINGLETON_HELD" = 1 ] || return 0
  audit singleton_released "" - "$(ev self_pid="$$" lock_file="${SINGLETON_LOCK_PATH:-$KEEPER_LOCK_FILE}" lock_inode="${SINGLETON_INODE:-?}")"
  rm -f "$KEEPER_HOLDER_FILE" 2>/dev/null
  exec 9>&-
  SINGLETON_HELD=0
  return 0
}
# ------------------------------------------------- v3 [I] rails 汇总（conf 之后） --
rails_apply() {
  local r
  if [ "${KEEPER_SINGLETON:-1}" != 1 ] && [ "${KEEPER_RAIL_ALLOW_SINGLETON_OFF:-0}" != 1 ]; then
    audit config_rail_refused "" - "$(ev rail=singleton requested="${KEEPER_SINGLETON}" forced=1 source=conf_or_env allow_env=KEEPER_ALLOW_SINGLETON_OFF note="单例锁是防双 keeper 互踩的挡板：部署模板关不掉，须由启动 env 显式放行")"
    KEEPER_SINGLETON=1
  fi
  if [ "$KEEPER_STALL_ACTION" = restart ]; then
    if [ "${KEEPER_RAIL_ALLOW_STALL_RESTART:-0}" != 1 ]; then
      audit config_rail_refused "" - "$(ev rail=stall_restart requested=restart forced=audit source=conf_or_env allow_env=KEEPER_ALLOW_STALL_RESTART note="卡死重启会终止活进程（高破坏动作）：须由启动 env 显式放行")"
      KEEPER_STALL_ACTION=audit
    else
      audit config_rail_override "" - "$(ev rail=stall_restart value=restart source=env allow_env=KEEPER_ALLOW_STALL_RESTART=1 note="env 已显式放行卡死重启；仅在 token/路径已验证的进程上执行")"
    fi
  fi
  for r in ${RAIL_TRIPPED[@]+"${RAIL_TRIPPED[@]}"}; do hlog "RAIL 拒绝：$r（部署模板不得关闭挡板）"; done
  for r in ${RAIL_OVERRIDES[@]+"${RAIL_OVERRIDES[@]}"}; do hlog "RAIL 放行：$r"; done
  return 0
}

# ==================== v3 [H] 心跳消费：判据表（见 test-report.md §2） ==========
# 分类优先级（先发生者胜）：
#   unavailable → 读侧不可用（heartbeat.py/python3 缺席）：不判卡死，只标注
#   hb_missing  → 心跳文件缺失/损坏：不判卡死（管线可能未启动），升级告警
#   hb_done     → state=done 或 verdict DONE：正常收敛终态，**永不重启**
#   gate_frozen → gate_open=false 且已关 ≥ freeze 阈值：被内存守卫冻住，**不重启**
#   gate_warn   → 闸门已关但未到冻结阈值：观察，**不重启**
#   backoff     → runtime.backoff_active（delay/strikes/cooldown）：合法退避，**不误杀**
#   budget_downgrade → mem.level≥PRESSURE 或 eff_workers<base_workers：预算降档，**不重启**
#   writer_gone → 写者不在且水位新鲜且非 done：交给进程级检测（absent ⇒ 重启）
#   stall       → stalled=true（水位停滞≥阈值）且以上都不能解释：真卡死（默认只告警）
#   ok          → 其余
HB_CLASS=""; HB_EV='{}'
HB_AVAILABLE=0; HB_PROBLEM=""; HB_VERDICT=""; HB_STALLED=0; HB_FROZEN=0; HB_GATE_OPEN=""
HB_GATE_CLOSED_S=""; HB_BACKOFF=0; HB_DELAY=""; HB_STRIKES=""; HB_WRITER_ALIVE=""
HB_WM_AGE=""; HB_HB_AGE=""; HB_MEM_LEVEL=""; HB_EFF_LEAF=""; HB_BASE_LEAF=""
HB_DONE=0; HB_STATE=""; HB_REASONS=""; HB_EXIT=""; HB_EXPLAINED=""; HB_SINCE_BOOT=""
hb_bool() { case "$1" in 1|true|True|yes) printf 1 ;; *) printf 0 ;; esac; }
hb_num() { case "$1" in ''|None|null|True|False) printf '' ;; *) printf '%s' "$1" ;; esac; }
# hb_read_verdict FILE → 扁平 key=value（借 heartbeat.py 的 is_stalled 作为唯一判据来源）
hb_read_verdict() {
  local f="$1"
  [ -n "$f" ] || { printf 'hb_available=0\nhb_problem=no_file_configured\n'; return 1; }
  [ "$KEEPER_HB_ENABLE" = 1 ] || { printf 'hb_available=0\nhb_problem=disabled\n'; return 1; }
  [ -r "$KEEPER_HEARTBEAT_PY" ] || { printf 'hb_available=0\nhb_problem=heartbeat_py_missing(%s)\n' "$KEEPER_HEARTBEAT_PY"; return 1; }
  command -v python3 >/dev/null 2>&1 || { printf 'hb_available=0\nhb_problem=python3_missing\n'; return 1; }
  timeout "$KEEPER_HB_TIMEOUT" python3 - "$KEEPER_HEARTBEAT_PY" "$f" "${KEEPER_HB_STALL_S:-}" <<'PY' 2>/dev/null 9>&-
import importlib.util, sys
hbpy, hbfile, thr = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    spec = importlib.util.spec_from_file_location("keeper_hb", hbpy)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    kw = {}
    if thr.strip():
        kw["threshold_s"] = float(thr)
    v = m.is_stalled(path=hbfile, **kw)
except Exception as e:                       # 读侧故障绝不拖垮 keeper
    print("hb_available=0")
    print("hb_problem=reader_error(%s)" % type(e).__name__)
    sys.exit(0)
out = {}
def flat(prefix, val):
    if isinstance(val, dict):
        for k, x in val.items():
            flat(prefix + k + ".", x)
    elif isinstance(val, list):
        out[prefix[:-1]] = ",".join(str(i) for i in val)
    else:
        out[prefix[:-1]] = val
flat("", v)
print("hb_available=1")
# 心跳文件本身不可用（missing/empty/corrupt/unreadable/not_heartbeat）：verdict 里
# heartbeat=None 且 reason=problem ⇒ 显式导出 problem，避免被误分类（如误判闸门关闭）
if v.get("heartbeat") is None:
    print("hb_problem=%s" % (v.get("reason") or "unknown"))
for k in ("verdict", "severity", "stalled", "frozen", "gate_frozen", "gate_open",
          "gate_closed_s", "gate_closed_s_effective", "backoff_active", "backoff_reason",
          "delay_s", "strikes", "writer_alive", "writer_gone", "watermark_age_s",
          "heartbeat_age_s", "startup_grace", "done", "state", "escalate", "reason",
          "reasons", "exit_code", "threshold_s", "watermark_clamped",
          "stall_explained_by", "since_boot_s", "detail"):
    if k in out:
        print("hb_%s=%s" % (k, out[k]))
for k in ("mem.level", "mem.level_num", "mem.pct", "mem.proc_pct", "mem.gate_open",
          "mem.gate_closed_s", "mem.eff_workers.leaf", "mem.eff_workers.probe",
          "mem.base_workers.leaf", "mem.base_workers.probe", "mem.why",
          "mem.abort_requested", "mem.in_safe_mode", "limiter.mode", "limiter.interval_s",
          "limiter.err_streak", "limiter.cooldown_remaining_s", "limiter.grow_events",
          "limiter.relax_events", "watermark.advance_count", "watermark.clamped"):
    if k in out:
        print("hbx_%s=%s" % (k.replace(".", "_"), out[k]))
PY
}
# 兼容前向字段：R2 移交 13 提到 W_MAX/预算档位/scale_events。memguard 的 heartbeat_state()
# 目前只给 base_workers/eff_workers（等价信息：eff<base 即"预算降档"），W_MAX 尚无读数。
# v3 的接口约定：若心跳里出现 runtime.mem.w_max / runtime.limiter.scale_events 等字段，
# 分类器自动采纳（R4 接入 scrape 侧后无需改 keeper）；缺失时以 eff/base 比值判档，
# 并在证据里标 budget_readout=derived（绝不伪造读数）。
hb_classify_file() { # FILE → 设置 HB_* / HB_CLASS / HB_EV
  local f="$1" line k v kv
  HB_CLASS=""; HB_EV='{}'
  HB_AVAILABLE=0; HB_PROBLEM=""; HB_VERDICT=""; HB_STALLED=0; HB_FROZEN=0; HB_GATE_OPEN=""
  HB_GATE_CLOSED_S=""; HB_BACKOFF=0; HB_DELAY=""; HB_STRIKES=""; HB_WRITER_ALIVE=""
  HB_WM_AGE=""; HB_HB_AGE=""; HB_MEM_LEVEL=""; HB_EFF_LEAF=""; HB_BASE_LEAF=""
  HB_DONE=0; HB_STATE=""; HB_REASONS=""; HB_EXIT=""; HB_EXPLAINED=""; HB_SINCE_BOOT=""
  kv="$(hb_read_verdict "$f")"
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    k="${line%%=*}"; v="${line#*=}"
    case "$k" in
      hb_available)  HB_AVAILABLE="$(hb_bool "$v")" ;;
      hb_problem)    HB_PROBLEM="$v" ;;
      hb_verdict)    HB_VERDICT="$v" ;;
      hb_stalled)    HB_STALLED="$(hb_bool "$v")" ;;
      hb_frozen)     HB_FROZEN="$(hb_bool "$v")" ;;
      hb_gate_open)  HB_GATE_OPEN="$(hb_num "$v")" ;;
      hb_gate_closed_s_effective) HB_GATE_CLOSED_S="$(hb_num "$v")" ;;
      hb_backoff_active) HB_BACKOFF="$(hb_bool "$v")" ;;
      hb_delay_s)    HB_DELAY="$(hb_num "$v")" ;;
      hb_strikes)    HB_STRIKES="$(hb_num "$v")" ;;
      hb_writer_alive) HB_WRITER_ALIVE="$v" ;;
      hb_watermark_age_s) HB_WM_AGE="$(hb_num "$v")" ;;
      hb_heartbeat_age_s) HB_HB_AGE="$(hb_num "$v")" ;;
      hb_mem_level|hbx_mem_level) HB_MEM_LEVEL="$v" ;;
      hbx_mem_eff_workers_leaf)   HB_EFF_LEAF="$(hb_num "$v")" ;;
      hbx_mem_base_workers_leaf)  HB_BASE_LEAF="$(hb_num "$v")" ;;
      hb_done)       HB_DONE="$(hb_bool "$v")" ;;
      hb_state)      HB_STATE="$v" ;;
      hb_reasons)    HB_REASONS="$v" ;;
      hb_exit_code)  HB_EXIT="$v" ;;
      hb_stall_explained_by) HB_EXPLAINED="$v" ;;
      hb_since_boot_s) HB_SINCE_BOOT="$(hb_num "$v")" ;;
    esac
  done <<<"$kv"
  local budget_derived=no budget_down=0
  [ -n "$HB_MEM_LEVEL" ] && budget_derived=yes
  case "$HB_MEM_LEVEL" in
    PRESSURE|CRITICAL|SAFE_MODE) budget_down=1 ;;
  esac
  if [ -n "$HB_EFF_LEAF" ] && [ -n "$HB_BASE_LEAF" ] && [ "$HB_EFF_LEAF" -lt "$HB_BASE_LEAF" ] 2>/dev/null; then
    budget_down=1; budget_derived=yes
  fi
  if [ "$HB_AVAILABLE" != 1 ]; then
    HB_CLASS="unavailable"
  elif [ "$HB_PROBLEM" = missing ]; then
    HB_CLASS="hb_missing"
  elif [ -n "$HB_PROBLEM" ] && [ "$HB_PROBLEM" != missing ]; then
    HB_CLASS="hb_unreadable"
  elif [ "$HB_DONE" = 1 ] || [ "$HB_VERDICT" = DONE ]; then
    HB_CLASS="hb_done"
  elif [ "$HB_FROZEN" = 1 ]; then
    HB_CLASS="gate_frozen"
  elif [ "$HB_GATE_OPEN" = 0 ]; then
    HB_CLASS="gate_warn"
  elif [ "$HB_BACKOFF" = 1 ]; then
    HB_CLASS="backoff"
  elif [ "$budget_down" = 1 ]; then
    HB_CLASS="budget_downgrade"
  elif [ "$HB_WRITER_ALIVE" = False ]; then
    HB_CLASS="writer_gone"
  elif [ "$HB_STALLED" = 1 ]; then
    HB_CLASS="stall"
  else
    HB_CLASS="ok"
  fi
  HB_EV="$(ev class="$HB_CLASS" file="${f:-none}" verdict="${HB_VERDICT:-none}" stalled="$HB_STALLED" wm_age_s="${HB_WM_AGE:--1}" hb_age_s="${HB_HB_AGE:--1}" gate_open="${HB_GATE_OPEN:-unknown}" gate_closed_s="${HB_GATE_CLOSED_S:--1}" frozen="$HB_FROZEN" backoff_active="$HB_BACKOFF" delay_s="${HB_DELAY:--1}" strikes="${HB_STRIKES:--1}" mem_level="${HB_MEM_LEVEL:-unknown}" eff_workers_leaf="${HB_EFF_LEAF:--1}" base_workers_leaf="${HB_BASE_LEAF:--1}" budget_readout="$budget_derived" writer_alive="${HB_WRITER_ALIVE:-unknown}" since_boot_s="${HB_SINCE_BOOT:--1}" reasons="${HB_REASONS:-none}" stall_explained_by="${HB_EXPLAINED:-none}" reader_problem="${HB_PROBLEM:-none}")"
}
hb_path_of() { printf '%s' "${JOB_HB_FILE[$1]:-}"; }
# heartbeat_cycle IDX PID TRUST —— 每轮对"活着的作业"做一次心跳分类与卡死确认
heartbeat_cycle() {
  local i="$1" pid="$2" trust="$3" f prev key_class key_streak streak age gate
  [ "$KEEPER_HB_ENABLE" = 1 ] || return 0
  f="$(hb_path_of "$i")"
  hb_classify_file "$f"
  key_class="hb_class_${JOB_NAME[$i]}"; key_streak="hb_stall_streak_${JOB_NAME[$i]}"
  prev="$(st_get "$key_class")"
  st_set "$key_class" "$HB_CLASS"
  case "$prev" in 0|"") prev=none ;; esac
  if [ "$HB_CLASS" != "$prev" ]; then
    audit heartbeat_class "$pid" "$pid" "$(evcat "$(ev prev_class="$prev" pid="$pid" trust="$trust")" "$HB_EV")"
  fi
  if [ "$HB_CLASS" = stall ]; then
    streak=$(( $(st_get "$key_streak") + 1 )); st_set "$key_streak" "$streak"
    age="${HB_WM_AGE:--1}"; gate="${HB_GATE_OPEN:-unknown}"
    if [ "$streak" -ge "$KEEPER_HB_CONFIRM" ] \
       && { [ "$streak" -eq "$KEEPER_HB_CONFIRM" ] \
            || [ $(( (streak - KEEPER_HB_CONFIRM) % (KEEPER_HB_REAUDIT > 0 ? KEEPER_HB_REAUDIT : 1) )) -eq 0 ]; }; then
      audit heartbeat_stall "$pid" "$pid" \
        "$(evcat "$(ev age_s="$age" gate_state="$gate" streak="$streak" pid="$pid" trust="$trust" action="$KEEPER_STALL_ACTION" writable_note="水位停滞且无闸门/退避/预算解释")" "$HB_EV")" \
        process "$(ev age_s="$age" gate_state="$gate" streak="$streak" pid="$pid")"
      if [ "$KEEPER_STALL_ACTION" = restart ]; then stall_restart_idx "$i" "$pid" "$trust" "$age"; fi
    fi
  else
    st_set "$key_streak" 0
  fi
  return 0
}
# stall_restart_idx IDX PID TRUST AGE —— 确认卡死后的受控重启（默认关闭，需 rail 放行）
stall_restart_idx() {
  local i="$1" pid="$2" trust="$3" age="$4" t0 t1 waited=0 gone=no
  case "$trust" in
    token|path_verified|relative_cwd_ok) : ;;
    *) audit stall_restart_refused "$pid" - \
         "$(ev reason=untrusted_process trust="$trust" pid="$pid" note="身份不可验证的进程绝不由 keeper 终止")"; return 1 ;;
  esac
  verify_pid_for_job "$i" "$pid" || {
    audit stall_restart_refused "$pid" - "$(ev reason=identity_recheck_failed why="$VERIFY_WHY" pid="$pid")"; return 1; }
  [ "$pid" != "$$" ] || return 1
  t0="$(now_ms)"
  kill -TERM "$pid" 2>/dev/null
  while [ "$waited" -lt $((KEEPER_STALL_KILL_GRACE * 4)) ]; do
    proc_exists "$pid" || break
    sleep 0.25; waited=$((waited+1))
  done
  if proc_exists "$pid"; then kill -9 "$pid" 2>/dev/null; sleep 0.3; fi
  t1="$(now_ms)"
  proc_exists "$pid" || gone=yes
  audit stall_kill "$pid" - \
    "$(ev reason=heartbeat_stall age_s="$age" trust="$trust" term_ms="$((t1-t0))" gone="$gone" note="卡死确认 + rail 放行：终止该作业进程，随后由正常重启路径拉起（≤${KEEPER_RECOVERY_DEADLINE}s）")"
  return 0
}

# =================== v3 [F] 优雅收尾 / checkpoint / 子进程排空 =================
JOBS_TODO=0; JOBS_DONE=0; JOBS_PENDING_NAMES=""; JOBS_DONE_NAMES=""
jobs_todo_done() {
  JOBS_TODO=0; JOBS_DONE=0; JOBS_PENDING_NAMES=""; JOBS_DONE_NAMES=""
  local i
  for ((i=0; i<${#JOB_NAME[@]}; i++)); do
    if job_settled "$i"; then
      JOBS_DONE=$((JOBS_DONE+1)); JOBS_DONE_NAMES+="${JOB_NAME[$i]}(${TERM_VERDICT:-completed}) "
    else
      JOBS_TODO=$((JOBS_TODO+1)); JOBS_PENDING_NAMES+="${JOB_NAME[$i]} "
    fi
  done
}
# wait_keeper_children GRACE —— 有界等待 keeper 自建的在飞子进程；**不等脱离的作业**
wait_keeper_children() {
  local grace="$1" w=0 t looks_job=0 i
  WAIT_CHILDREN_PENDING=0; WAIT_CHILDREN_WAITED=""
  [ -n "${SPAWN_CHILD_PID:-}" ] || return 0
  proc_exists "$SPAWN_CHILD_PID" || return 0
  while IFS= read -r t; do
    [ -n "$t" ] || continue
    for ((i=0; i<${#JOB_SCRIPT[@]}; i++)); do
      [ "$t" = "${JOB_SCRIPT[$i]}" ] && looks_job=1
    done
  done < <(proc_cmdline_tokens "$SPAWN_CHILD_PID")
  if [ "$looks_job" = 1 ]; then
    WAIT_CHILDREN_PENDING=0
    WAIT_CHILDREN_WAITED="skip:${SPAWN_CHILD_PID}(已经是作业本体，属脱离进程，不等待)"
    return 0
  fi
  WAIT_CHILDREN_PENDING=1
  while [ "$w" -lt $((grace * 4)) ] && proc_exists "$SPAWN_CHILD_PID"; do sleep 0.25 9>&-; w=$((w+1)); done
  WAIT_CHILDREN_WAITED="${SPAWN_CHILD_PID}($((w*250))ms)"
  return 0
}
keeper_checkpoint_write() { # SIG REASON
  local sig="$1" reason="$2" i jobs='{' sep='' cpl
  local f="$KEEPER_STATE_DIR/keeper-v3.checkpoint.json"
  local tmp="$f.tmp.$$"
  for ((i=0; i<${#JOB_NAME[@]}; i++)); do
    cpl=no; job_settled "$i" && cpl=yes
    jobs+="$sep\"$(jesc "${JOB_NAME[$i]}")\":$(ev state="$(st_get last_state_${JOB_NAME[$i]})" pid="$(st_get last_pid_${JOB_NAME[$i]})" trust="$(st_get last_trust_${JOB_NAME[$i]})" restarts="$(st_get restarts_${JOB_NAME[$i]})" hb_class="$(st_get hb_class_${JOB_NAME[$i]})" complete="$cpl" settled_verdict="${TERM_VERDICT:-none}" settled_source="${TERM_SOURCE:-none}" exit_code="$(st_get exit_code_${JOB_NAME[$i]})" halted="$(st_get halted_${JOB_NAME[$i]})" halt_reason="$(st_get halt_reason_${JOB_NAME[$i]})" incomplete_restarts="$(st_get incomplete_restarts_${JOB_NAME[$i]})" resume_restarts="$(st_get resume_restarts_${JOB_NAME[$i]})" fail_count="$(st_get fail_${JOB_NAME[$i]})")"
    sep=','
  done
  jobs+='}'
  mkdir -p "$KEEPER_STATE_DIR" 2>/dev/null
  printf '{"schema":"keeper-checkpoint/1","ts":"%s","ts_epoch":%s,"run_id":"%s","keeper_pid":%s,"signal":"%s","reason":"%s","run_count":%s,"jobs":%s}\n' \
    "$(now_iso)" "$(epoch_s)" "$RUN_ID" "$$" "$(jesc "$sig")" "$(jesc "$reason")" "$(st_get run_count)" "$jobs" \
    >"$tmp" 2>/dev/null && mv -f "$tmp" "$f" 2>/dev/null
}
# graceful_stop SIGNAL REASON —— 停重启循环 → 排空自建子进程 → checkpoint → 审计 → 退出
graceful_stop() {
  local sig="$1" reason="$2" t0 t1 waited=0 i detached=""
  CUR_JOB="-"
  t0="$(now_ms)"
  jobs_todo_done
  for ((i=0; i<${#JOB_NAME[@]}; i++)); do
    CUR_JOB="${JOB_NAME[$i]}"
    detect_job "$i"
    # checkpoint 必须反映"停机那一刻的事实"：即使 keeper 在首轮就被停（还没走过 running 分支），
    # 也要把本轮 detect_job 的结论写进状态（否则 checkpoint 里的 pid/trust 会是默认 0）
    st_set "last_state_${JOB_NAME[$i]}" "$DET_STATE"
    [ -n "$DET_PID" ] && st_set "last_pid_${JOB_NAME[$i]}" "$DET_PID"
    [ -n "$DET_TRUST" ] && st_set "last_trust_${JOB_NAME[$i]}" "$DET_TRUST"
    [ "$DET_STATE" = running ] && detached+="${JOB_NAME[$i]}=${DET_PID} "
  done
  CUR_JOB="-"
  wait_keeper_children "$KEEPER_STOP_GRACE"
  keeper_checkpoint_write "$sig" "$reason"
  t1="$(now_ms)"; waited=$((t1-t0))
  audit graceful_stop "" - \
    "$(ev signal="$sig" todo="$JOBS_TODO" done="$JOBS_DONE" pending_jobs="${JOBS_PENDING_NAMES:-none}" done_jobs="${JOBS_DONE_NAMES:-none}" detached_job_pids="${detached:-none}" waited_ms="$waited" children_pending="$WAIT_CHILDREN_PENDING" children_waited="${WAIT_CHILDREN_WAITED:-none}" run_count="$(st_get run_count)" run_id="$RUN_ID" stop_grace_s="$KEEPER_STOP_GRACE" action=stop_restart_loop_only note="keeper 只停自己：不向被看护作业发送任何信号，被看护作业继续运行（detached_job_pids）")" \
    process "$(ev signal="$sig" todo="$JOBS_TODO" done="$JOBS_DONE")"
  audit keeper_stop "" - \
    "$(ev reason="$reason" signal="$sig" uptime_ms="$(( t1 - $(st_get first_ms) ))" run_count="$(st_get run_count)" checkpoint="$KEEPER_STATE_DIR/keeper-v3.checkpoint.json" waited_ms="$waited")"
  singleton_release
  state_save
  return 0
}

usage() { sed -n '2,80p' "$KEEPER_SELF" | sed 's/^# \{0,1\}//'; }

# ---------------------------------------------------------------------- 主 --
STATE[first_ms]="$(now_ms)"
case "${1:-}" in
  start)       [ -r "$KEEPER_CONF" ] && . "$KEEPER_CONF"; rails_apply; cmd_start ;;
  daemon)      [ -r "$KEEPER_CONF" ] && . "$KEEPER_CONF"; rails_apply; cmd_daemon ;;
  once)        [ -r "$KEEPER_CONF" ] && . "$KEEPER_CONF"; rails_apply; cmd_once ;;
  status)      [ -r "$KEEPER_CONF" ] && . "$KEEPER_CONF"; cmd_status ;;
  fingerprint) [ -r "$KEEPER_CONF" ] && . "$KEEPER_CONF"; cmd_fingerprint ;;
  hbclass)     [ -r "$KEEPER_CONF" ] && . "$KEEPER_CONF"; shift; cmd_hbclass "${1:-}" ;;
  lock-probe)  cmd_lock_probe "${2:-}" ;;
  identity)    [ -r "$KEEPER_CONF" ] && . "$KEEPER_CONF"; shift; cmd_identity "${1:-}" "${2:-}" ;;
  spawn)       [ -r "$KEEPER_CONF" ] && . "$KEEPER_CONF"; rails_apply; shift; cmd_spawn "${1:-}" ;;
  resume)      [ -r "$KEEPER_CONF" ] && . "$KEEPER_CONF"; shift; cmd_resume "${1:-all}" ;;
  stop)        [ -r "$KEEPER_CONF" ] && . "$KEEPER_CONF"; cmd_stop ;;
  selftest)    cmd_selftest ;;
  ''|-h|--help|help) usage ;;
  *)           printf '未知子命令: %s\n' "$1" >&2; usage >&2; exit 2 ;;
esac
