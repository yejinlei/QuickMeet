#!/usr/bin/env bash
# QuickMeet 一键停止（QM-015 交付要求：停止脚本，适配 MobaXterm SSH 操作）
#
# 用法：
#   bash scripts/qm-down.sh         # 停止并移除容器，**保留**数据卷
#   bash scripts/qm-down.sh --purge # 连数据卷一起删（会议数据会丢，慎用）
#
# 默认保留数据卷是刻意的：会议数据本地留存是 Epic 全局约束 5，
# 一次误操作 `down -v` 就全没了。要清数据必须显式 --purge。

set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT" || exit 1

command -v docker-compose >/dev/null 2>&1 || { echo "缺少 docker-compose CLI" >&2; exit 2; }

PURGE=0
for a in "$@"; do
  case "$a" in
    --purge) PURGE=1 ;;
    -h|--help) grep -E '^#|^[a-z]+\.' "$0" | head -16; exit 0 ;;
  esac
done

if [ "$PURGE" = 1 ]; then
  echo "⚠ 将删除全部数据卷（meeting-data / meeting-data-2 / meeting-data-3 / meeting-logs）"
  read -r -p "   输入 PURGE 确认：" ans
  [ "$ans" = "PURGE" ] || { echo "   已取消"; exit 0; }
  docker-compose down -v --remove-orphans
else
  docker-compose down --remove-orphans
  echo "   数据卷已保留：$(docker volume ls --format '{{.Name}}' | grep -c meeting- || true) 个 meeting-* 卷"
fi
echo "🟢 已停止"
