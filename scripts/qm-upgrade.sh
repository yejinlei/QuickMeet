#!/usr/bin/env bash
# QuickMeet 一键升级（QM-015 交付要求：升级脚本，适配 MobaXterm SSH 操作）
#
# 用法：
#   bash scripts/qm-upgrade.sh               # 用当前工作树重建并逐个重启
#   bash scripts/qm-upgrade.sh --branch main # 先切分支再重建
#
# 升级语义：
#   * 只 `up -d --build`，**不** `down` —— 数据卷 meeting-data* / meeting-logs
#     完全不动，会议数据不丢（全局约束 5：数据本地留存）。
#   * 逐个 `restart` 而不是整体 `down -v && up`，避免同时断掉全部节点
#     导致 QM-006 的故障迁移在升级窗口内反复触发。
#   * 配置改动不需要升级镜像：改 config/container.env 后
#     `docker-compose up -d` 即可（环境变量在服务启动时读入）。

set -uo pipefail

START=$(date +%s)
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT" || exit 1

say() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
have() { command -v "$1" >/dev/null 2>&1; }
elapsed() { printf '%d' $(( $(date +%s) - START )); }

BRANCH=""
while [ $# -gt 0 ]; do
  case "$1" in
    --branch) shift; BRANCH="${1:-}" ;;
    --branch=*) BRANCH="${1#--branch=}" ;;
    -h|--help) grep -E '^#|^[a-z]+\.' "$0" | head -20; exit 0 ;;
  esac
  shift
done

have docker-compose || { echo "缺少 docker-compose 1.29.2 的 CLI" >&2; exit 2; }
have docker || { echo "缺少 docker" >&2; exit 2; }

say "升级前状态"
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' || true

# ── 可选切分支 ────────────────────────────────────────────────────
if [ -n "$BRANCH" ]; then
  say "切换分支到 $BRANCH"
  git fetch --quiet || echo "   git fetch 失败（远端可能暂时不可达），用本地已有引用"
  git checkout "$BRANCH" || exit 1
fi

# ── 重建并逐个滚动 ────────────────────────────────────────────────
say "重建镜像并滚动更新（数据卷保留）"
docker-compose up -d --build || { echo "   构建失败，未做任何变更" >&2; exit 1; }

SERVICES=(qm-media-3 qm-media-2 qm-media qm-signaling)
say "逐个重启媒体/信令节点（NATS 不动，保持协调点不中断）"
for svc in "${SERVICES[@]}"; do
  echo "   \$ docker restart $svc"
  docker restart "$svc" >/dev/null 2>&1 || { echo "   ⚠ $svc 重启失败"; continue; }
  # 等它回到 healthy；start_period 30s + interval 5s × retries 2，给 90s 余量。
  for i in $(seq 1 18); do
    st=$(docker inspect --format '{{.State.Health.Status}}' "$svc" 2>/dev/null)
    [ "$st" = "healthy" ] && break
    sleep 5
  done
  st=$(docker inspect --format '{{.State.Health.Status}}' "$svc" 2>/dev/null)
  printf '   %-14s -> %s\n' "$svc" "${st:-no-healthcheck}"
done

say "升级后状态"
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'

# 回收旧镜像：同一 tag 的多层历史会让磁盘持续膨胀，私有化服务器磁盘通常不大。
say "回收悬空镜像（不影响在跑容器）"
docker image prune -f >/dev/null 2>&1 || true

echo
echo "🟢 升级完成，耗时 $(elapsed)s"
exit 0
