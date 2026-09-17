#!/usr/bin/env bash
# QuickMeet 一键启动（QM-015 交付要求：一键启动脚本，适配 MobaXterm SSH 操作）
#
# 用法：
#   bash scripts/qm-up.sh             # 构建 + 拉起全套 + 等五容器 healthy
#   bash scripts/qm-up.sh --no-build  # 不重新构建镜像（复用本地已有镜像）
#
# 用 docker-compose 1.29.2 的 CLI（docker-compose，不是 docker compose）。
# 本机若装的是 v2，本脚本仍然能跑，但 1.29.2 兼容性由 CI 的 compose-legacy
# job 保证，本脚本不做版本判断。

set -uo pipefail

START=$(date +%s)
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT" || exit 1

NO_BUILD=0
for a in "$@"; do
  case "$a" in
    --no-build) NO_BUILD=1 ;;
    -h|--help) grep -E '^#|^[a-z]+\.' "$0" | head -20; exit 0 ;;
  esac
done

say() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
have() { command -v "$1" >/dev/null 2>&1; }
elapsed() { printf '%d' $(( $(date +%s) - START )); }

# ── 前置检查 ──────────────────────────────────────────────────────
say "前置检查"
if ! have docker-compose; then
  echo "   缺少 docker-compose 1.29.2 的 CLI（不是 docker compose）" >&2
  echo "   安装：curl -fsSL https://github.com/docker/compose/releases/download/1.29.2/\\" >&2
  echo "         docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose && chmod +x ..." >&2
  exit 2
fi
docker-compose version | head -1
have docker || { echo "   缺少 docker" >&2; exit 2; }
[ -f docker-compose.yml ] || { echo "   缺少 docker-compose.yml" >&2; exit 2; }
[ -f config/container.env ] || { echo "   缺少 config/container.env" >&2; exit 2; }

# ── 启动 ──────────────────────────────────────────────────────────
say "启动全套服务"
if [ "$NO_BUILD" = 1 ]; then
  echo "   \$ docker-compose up -d"
  docker-compose up -d || exit 1
else
  echo "   \$ docker-compose up -d --build"
  docker-compose up -d --build || exit 1
fi

# ── 等五容器 healthy（验收标准 1：5 分钟内全部启动成功）──────────
say "等待五容器全部 healthy（预算 300s = 验收标准的 5 分钟）"
ALL_OK=0
SERVICES=(qm-nats qm-media qm-media-2 qm-media-3 qm-signaling)
for i in $(seq 1 75); do
  # 五服务必须都在、且没有一个是 unhealthy / restarting。
  # healthy 数量只说明「有多少起来」，不说明「有没有挂掉」，所以要两头查。
  GOOD=$(docker ps --filter "health=healthy" --format '{{.Names}}' | sed '/^$/d' | tr '\n' ' ')
  GOODN=$(echo "$GOOD" | sed '/^$/d' | wc -w)
  BAD=$(docker ps --filter "health=unhealthy" --format '{{.Names}}' | sed '/^$/d' | tr '\n' ' ')
  DOWN=""
  for s in "${SERVICES[@]}"; do
    st=$(docker inspect --format '{{.State.Status}}' "$s" 2>/dev/null)
    [ "$st" = "running" ] || DOWN="${DOWN}${s}(${st:-missing}) "
  done
  echo "   [${i}/75] healthy(${GOODN}): ${GOOD:-none} | unhealthy: ${BAD:-none} | 非 running: ${DOWN:-none}"
  if [ "$BAD" = "" ] && [ -z "${DOWN// /}" ]; then
    [ "$GOODN" -ge 5 ] && { ALL_OK=1; break; }
  fi
  # 循环重启判据（验收标准 2）：任一容器重启次数 > 6 就停在这里等人看。
  MAXR=$(docker ps --format '{{.RestartCount}}' 2>/dev/null | sort -nr | head -1)
  [ "${MAXR:-0}" -gt 6 ] && { echo "   重启次数 ${MAXR} 超限，判定循环重启"; break; }
  sleep 4
done

# ── 探活（含集群硬判据）──────────────────────────────────────────
CL_OK=1
if have curl; then
  say "探活端点 + 集群状态"
  for p in 8081 8091 8092 8093; do
    code=$(curl -s -o /dev/null -w '%{http_code}' -m 3 "http://127.0.0.1:${p}/healthz" || echo 000)
    printf '   GET http://127.0.0.1:%s/healthz -> %s\n' "$p" "$code"
  done
  # 容器 healthy 只证明进程活着；NATS 连不上时三节点全退化单机、探针返 503。
  # 这里逐个节点断言 JSON 里的 nats_connected 与 cluster_nodes，避免「五容器
  # 全绿但集群能力为 0」的假通过。不依赖 jq，用 sed 抽取。
  for p in 8091 8092 8093; do
    body=$(curl -s -m 3 "http://127.0.0.1:${p}/healthz" || echo "")
    nc=$(printf '%s' "$body" | sed -n 's/.*"nats_connected"[: ]*\(true\|false\).*/\1/p')
    nodes=$(printf '%s' "$body" | sed -n 's/.*"cluster_nodes"[: ]*\([0-9][0-9]*\).*/\1/p')
    printf '   节点 :%s -> nats_connected=%s cluster_nodes=%s\n' \
      "$p" "${nc:-none}" "${nodes:-none}"
    [ "$nc" = "true" ] || CL_OK=0
    [ "${nodes:-0}" -ge 3 ] 2>/dev/null || CL_OK=0
  done
fi

say "状态"
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

if [ "$ALL_OK" = 1 ] && [ "$CL_OK" = 1 ]; then
  echo
  echo "🟢 五容器全部 healthy，三节点已入集群（nats_connected 且 cluster_nodes >= 3），耗时 $(elapsed)s"
  echo "   信令入口：http://127.0.0.1:8081/healthz"
  exit 0
else
  echo
  echo "🔴 未达到验收标准（healthy=${ALL_OK} 集群=${CL_OK}），耗时 $(elapsed)s"
  [ "$ALL_OK" = 1 ] \
    && echo "   五容器都 healthy 但集群没形成：查 QM_CLUSTER_SERVER（应为 qm-nats，" \
    "不是 127.0.0.1）与 qm-nats 容器日志"
  echo "   排查：docker-compose logs --tail 80 qm-media"
  exit 1
fi
