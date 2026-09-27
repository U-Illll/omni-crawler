#!/usr/bin/env bash
# acceptance.sh — omni-crawler 验收汇总（框架自检 + adapter 冒烟 + 部署物检查）
# 用法: bash deploy/acceptance.sh
set -u
cd "$(dirname "$0")/.."
ROOT=$(pwd)
fail=0

echo "===== 1) 框架自检红队（tests/run_acceptance.sh）====="
if bash tests/run_acceptance.sh; then
  echo "[ok] 框架自检通过"
else
  echo "[FAIL] 框架自检未通过"
  fail=$((fail+1))
fi
echo

echo "===== 2) adapter 清单 ====="
for d in omni/adapters/*/; do
  name=$(basename "$d")
  [ "$name" = "__pycache__" ] && continue
  if [ -f "$d/adapter.py" ]; then
    echo "  [ok] adapter: $name"
  else
    echo "  [--] $name（无 adapter.py，跳过）"
  fi
done
echo

echo "===== 3) library adapter 关键文件 ====="
LIB=omni/adapters/library
for f in scrape.py keeper-v3.sh audit.py OMNI-PATCHES.md omni-inflight-snapshot.patch; do
  if [ -f "$LIB/$f" ]; then
    echo "  [ok] $f"
  else
    echo "  [FAIL] 缺 $f"
    fail=$((fail+1))
  fi
done
echo

echo "===== 4) social adapter 关键文件 ====="
SOC=omni/adapters/social
for f in sm_main.py sm_fetch.py adapter.py main.py; do
  if [ -f "$SOC/$f" ]; then
    echo "  [ok] $f"
  else
    echo "  [FAIL] 缺 $f"
    fail=$((fail+1))
  fi
done
echo

echo "=================================="
if [ $fail -eq 0 ]; then
  echo "OMNI-ACCEPTANCE: PASS"
else
  echo "OMNI-ACCEPTANCE: FAIL ($fail)"
fi
exit $fail
