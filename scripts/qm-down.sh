#!/usr/bin/env bash
# QuickMeet 一键停止（QM-015 交付要求：停止脚本，适配 MobaXterm SSH 操作）
#
# 用法：
#   bash scripts/qm-down.sh         # 停止并移除容器，**保留**数据卷
#   bash scripts/qm-down.sh --purge     # 连数据卷一起删（会议数据会丢，慎用）
#   bash scripts/qm-down.sh --purge --yes   # 跳过确认提示（非交互/管道场景必须带）
#
# 默认保留数据卷是刻意的：会议数据本地留存是 Epic 全局约束 5，
# 一次误操作 `down -v` 就全没了。要清数据必须显式 --purge。
#
# --purge 走交互确认；在非交互环境（管道、CI、`bash < script`）里必须加 --yes。
# 没加 --yes 又不是 tty 时**返回非 0 退出**，不会静默 exit 0 —— 静默成功会让
# 自动化脚本以为数据已清理，实际什么都没删。

set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT" || exit 1

command -v docker-compose >/dev/null 2>&1 || { echo "缺少 docker-compose CLI" >&2; exit 2; }

PURGE=0
ASSUME_YES=0
for a in "$@"; do
  case "$a" in
    --purge) PURGE=1 ;;
    --yes|-y) ASSUME_YES=1 ;;
    -h|--help) grep -E '^#|^[a-z]+\.' "$0" | head -18; exit 0 ;;
  esac
done

if [ "$PURGE" = 1 ]; then
  echo "⚠ 将删除全部数据卷（meeting-data / meeting-data-2 / meeting-data-3 / meeting-logs）"
  if [ "$ASSUME_YES" = 1 ]; then
    echo "   --yes：跳过交互确认"
  elif [ -t 0 ]; then
    read -r -p "   输入 PURGE 确认：" ans
    [ "$ans" = "PURGE" ] || { echo "   已取消"; exit 0; }
  else
    # 非交互环境（管道 / CI / `bash < script`）：read 拿不到输入，这里绝不能
    # 顺着走 exit 0 —— 那等于「什么都没删还报成功」。改成明确报错退出。
    echo "   ✗ 非交互环境不能执行 --purge：请加 --yes 显式确认，或改成本机交互式执行" >&2
    exit 1
  fi
  docker-compose down -v --remove-orphans
else
  docker-compose down --remove-orphans
  echo "   数据卷已保留：$(docker volume ls --format '{{.Name}}' | grep -c meeting- || true) 个 meeting-* 卷"
fi
echo "🟢 已停止"
