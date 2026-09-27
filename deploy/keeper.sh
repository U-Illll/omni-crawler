#!/usr/bin/env bash
# keeper.sh — omni-crawler 通用长跑看护（融合 sm-recon keeper 与 R3 keeper 教训）
# 用法: bash keeper.sh {start|watch|stop|status}
# 环境变量:
#   OMNI_HOME     实例根（必须；如 /home/user/go/omni-crawler 或部署目录）
#   MAIN_CMD      主命令（必须；如: python3 -u omni/adapters/library/main.py managed --loop）
#   STALE_LIMIT   心跳停滞阈值秒（默认 1200；必须大于引擎休整 REST_BETWEEN_ROUNDS=900，防休整期误杀）
#   MATCH_PATTERN cmdline 匹配片段（默认 "main.py"；用于防 PID 复用）
#   HEARTBEAT     心跳文件（默认 $OMNI_HOME/runs/.heartbeat）
# 纪律（继承）:
#   - 停止用精确 PID（绝不 pgrep 输出喂 kill；防自匹配自杀）
#   - 单例 flock（防双 keeper 互踩——R3 keeper 教训）
#   - watch 周期 60s；连续拉起失败 5 次 → 冷却 600s
set -u

OMNI_HOME=${OMNI_HOME:-}
MAIN_CMD=${MAIN_CMD:-}
STALE_LIMIT=${STALE_LIMIT:-1200}
MATCH_PATTERN=${MATCH_PATTERN:-"main.py"}
HEARTBEAT=${HEARTBEAT:-}

if [ -z "$OMNI_HOME" ] || [ -z "$MAIN_CMD" ]; then
  echo "用法: OMNI_HOME=<实例根> MAIN_CMD='<主命令>' bash keeper.sh {start|watch|stop|status}" >&2
  exit 2
fi

LOGS=$OMNI_HOME/logs
PIDFILE=$OMNI_HOME/keeper-main.pid
KLOG=$LOGS/keeper.log
LOCKFILE=$OMNI_HOME/keeper.lock
[ -n "$HEARTBEAT" ] || HEARTBEAT=$OMNI_HOME/runs/.heartbeat
mkdir -p "$LOGS" "$OMNI_HOME/runs"

is_running() {
  [ -f "$PIDFILE" ] || return 1
  local pid
  pid=$(cat "$PIDFILE" 2>/dev/null || echo "")
  [ -n "$pid" ] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  grep -q "$MATCH_PATTERN" "/proc/$pid/cmdline" 2>/dev/null || return 1
  return 0
}

start_main() {
  cd "$OMNI_HOME" || exit 1
  nohup $MAIN_CMD > "$LOGS/main-$(date +%Y%m%d-%H%M%S).log" 2>&1 &
  echo $! > "$PIDFILE"
  echo "[keeper] $(date '+%F %T') 拉起主进程 PID $! (CMD: $MAIN_CMD)" | tee -a "$KLOG"
}

check_stale() {
  [ -f "$HEARTBEAT" ] || return 0
  local now mt pid
  now=$(date +%s)
  mt=$(stat -c %Y "$HEARTBEAT" 2>/dev/null || echo 0)
  [ "$mt" -gt 0 ] || return 0
  if [ $((now - mt)) -gt "$STALE_LIMIT" ]; then
    pid=$(cat "$PIDFILE" 2>/dev/null || echo "")
    echo "[keeper] $(date '+%F %T') 心跳停滞 $((now - mt))s → kill PID $pid" | tee -a "$KLOG"
    [ -n "$pid" ] && kill "$pid" 2>/dev/null
    sleep 5
  fi
}

do_watch() {
  # 单例：flock 非阻塞；拿不到锁说明已有 keeper
  exec 9>"$LOCKFILE"
  if ! flock -n 9; then
    echo "[keeper] 已有 keeper 在运行（$LOCKFILE 锁定），退出" | tee -a "$KLOG"
    exit 0
  fi
  echo "[keeper] watch 启动（周期 60s，停滞阈值 ${STALE_LIMIT}s）CMD: $MAIN_CMD" | tee -a "$KLOG"
  local fail=0
  while true; do
    if ! is_running; then
      start_main
      sleep 15
      if is_running; then
        fail=0
      else
        fail=$((fail + 1))
        echo "[keeper] 拉起失败第 $fail 次" >> "$KLOG"
        if [ "$fail" -ge 5 ]; then
          echo "[keeper] 连续失败 5 次 → 冷却 600s" >> "$KLOG"
          sleep 600
          fail=0
        fi
      fi
    else
      check_stale
    fi
    sleep 60
  done
}

case "${1:-}" in
  start)   start_main ;;
  watch)   do_watch ;;
  stop)
    if is_running; then
      pid=$(cat "$PIDFILE")
      kill "$pid" 2>/dev/null && echo "[keeper] 已停止 PID $pid"
    else
      echo "[keeper] 无运行中的主进程"
    fi
    ;;
  status)
    if is_running; then
      echo "RUNNING pid=$(cat "$PIDFILE")"
    else
      echo "NOT-RUNNING"
    fi
    ;;
  *)
    echo "用法: OMNI_HOME=<实例根> MAIN_CMD='<主命令>' bash keeper.sh {start|watch|stop|status}"
    ;;
esac
