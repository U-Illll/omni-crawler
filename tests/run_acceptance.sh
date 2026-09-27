#!/usr/bin/env bash
# omni-crawler 框架自检（红队套件汇总）
# 用法: bash tests/run_acceptance.sh
set -u
cd "$(dirname "$0")/.."
fail=0
total=0
for t in \
  tests/smoke_imports.py \
  tests/smoke_social.py \
  tests/smoke_library.py \
  tests/redteam_checkpoint.py \
  tests/redteam_retry.py \
  tests/redteam_limiter.py \
  tests/redteam_circuit.py \
  tests/redteam_store.py \
  tests/redteam_channel.py \
  tests/redteam_fetch.py \
  tests/redteam_escalate.py \
; do
  total=$((total+1))
  echo "===== $t ====="
  if ! python3 "$t"; then
    fail=$((fail+1))
  fi
  echo
done
echo "=================================="
if [ $fail -eq 0 ]; then
  echo "FRAMEWORK-ACCEPTANCE: PASS ($total/$total suites)"
else
  echo "FRAMEWORK-ACCEPTANCE: FAIL ($fail/$total suites failed)"
fi
exit $fail
