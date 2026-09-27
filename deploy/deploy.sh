#!/usr/bin/env bash
# deploy.sh — omni-crawler 双端同步（本机 → 云）
# 用法: OMNI_DEPLOY_KEY=~/.ssh/id_ed25519 OMNI_DEPLOY_DEST=user@host:/root/omni-crawler/ bash deploy/deploy.sh [目标]
#   目标: aliyun（需先用环境变量提供凭据）| 本地路径（如 /tmp/omni-deploy-test）
# 纪律（继承 sm-recon 教训）：
#   - 排除: runs/ logs/ site/ *.pid venv/ __pycache__/ .git/ test-results/ quarantine/
#   - 排除 *.pid（避免把看护 PID 同步过去造成误判）
#   - rsync 后校验关键文件存在
set -u
cd "$(dirname "$0")/.."
SRC=$(pwd)
TARGET=${1:-aliyun}

# 部署凭据与目标全部从环境变量读取（不硬编码主机/密钥路径）
SSH_KEY="${OMNI_DEPLOY_KEY:?请设置 OMNI_DEPLOY_KEY（SSH 私钥路径）}"
SSH_OPTS="-F /dev/null -i $SSH_KEY -o IdentitiesOnly=yes -o ConnectTimeout=15"
DEST="${OMNI_DEPLOY_DEST:?请设置 OMNI_DEPLOY_DEST（user@host:/path，如 user@your-server:/root/omni-crawler/）}"
DEST_HOST="${DEST%%:*}"
DEST_DIR="${DEST#*:}"

EXCLUDES=(
  --exclude 'runs/' --exclude 'logs/' --exclude 'site/'
  --exclude '__pycache__/' --exclude '.git/' --exclude 'venv/' --exclude '.venv/'
  --exclude '*.pid' --exclude 'test-results/' --exclude 'quarantine/'
  --exclude 'results/' --exclude '*.pyc' --exclude 'keeper-main.pid'
)

case "$TARGET" in
  aliyun)
    echo "[deploy] 同步 → $DEST"
    echo "[deploy] 说明：远端需已建目录 /root/omni-crawler（rsync 自动建）。"
    rsync -az "${EXCLUDES[@]}" -e "ssh $SSH_OPTS" ./ "$DEST"
    echo "[deploy] 校验远端关键文件："
    ssh $SSH_OPTS "$DEST_HOST" "cd $DEST_DIR && for f in omni/core/engine.py omni/adapters/social/sm_main.py omni/adapters/library/scrape.py deploy/keeper.sh; do [ -f \"\$f\" ] && echo \"  ok \$f\" || echo \"  MISS \$f\"; done; python3 tests/smoke_imports.py | tail -1"
    ;;
  *)
    echo "[deploy] 本地目标：$TARGET"
    mkdir -p "$TARGET"
    rsync -a "${EXCLUDES[@]}" ./ "$TARGET/"
    echo "[deploy] 校验："
    for f in omni/core/engine.py omni/adapters/social/sm_main.py omni/adapters/library/scrape.py deploy/keeper.sh; do
      [ -f "$TARGET/$f" ] && echo "  ok $f" || echo "  MISS $f"
    done
    ;;
esac
