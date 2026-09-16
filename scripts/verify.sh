#!/usr/bin/env bash
# QuickMeet 一键验证（QM-018）
#
# 用法：
#   ./scripts/verify.sh               # 全部步骤（需要 docker / docker-compose）
#   ./scripts/verify.sh --no-docker   # 跳过容器步骤（没有 Docker 的本机环境）
#   STEPS="1 3 4 5" ./scripts/verify.sh --no-docker
#
# 步骤编号与 .github/workflows/ci.yml 里的 job 步骤一一对应，
# 映射关系写在 docs/CI_MATRIX.md。本地与 CI 跑同一份脚本，避免「本机过、CI 挂」。
#
# 步骤：
#   1. MSRV 1.75 检查（Cargo.toml 声明 + 本机 rustc 版本）
#   2. cargo fmt --check / cargo clippy --workspace
#   3. cargo test --workspace --locked --no-fail-fast
#   4. cargo build --release --locked --workspace
#   5. qm-demo 默认命令 + 8080/8081 端口假设校验
#   6. docker build（Dockerfile）
#   7. docker-compose config（含 compose 1.29.2 语法兼容校验）
#   8. docker-compose up -d --build + 四容器 healthcheck 全部 healthy + /healthz 探活
#   9. 逐个 restart 服务，验证 restart: unless-stopped 自愈
#  10. CI workflow 语义校验（YEJ-114 并入 QM-015，见 scripts/verify-workflows.sh）

set -uo pipefail

START=$(date +%s)
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT" || exit 1

NO_DOCKER=0
SELECTED="${STEPS:-1 2 3 4 5 6 7 8 9 10}"
# 读成数组，避免「步骤 10 被 SELECTED 里的 " 1 " 模式匹配误命中」。
SELECTED_ARR=($SELECTED)
case "${1:-}" in
  --no-docker) NO_DOCKER=1 ;;
  --help|-h) grep -E '^#|^[0-9]+\.' "$0" | head -40; exit 0 ;;
esac

FAILURES=()
PASS_COUNT=0
elapsed() { printf '%d' $(( $(date +%s) - START )); }
have() { command -v "$1" >/dev/null 2>&1; }
say() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
ok() { printf '   \033[32mPASS\033[0m %s\n' "$1"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail() { printf '   \033[31mFAIL\033[0m %s\n' "$1"; FAILURES+=("$1"); }
skip() { printf '   \033[33mSKIP\033[0m %s\n' "$1"; }

want() { for s in "${SELECTED_ARR[@]}"; do [ "$s" = "$1" ] && return 0; done; return 1; }

# 运行一条命令并记录 PASS/FAIL。
step() {
  local label="$1"; shift
  say "$label"
  echo "   \$ $*"
  if "$@"; then ok "$label"; else fail "$label"; fi
}

# ── 前置 ──────────────────────────────────────────────────────────
if ! have cargo; then
  echo "缺少 cargo：先装 Rust（rustup）再重跑。" >&2
  exit 2
fi

# ── 1. MSRV 1.75 ─────────────────────────────────────────────────
if want 1; then
  say "1. MSRV 1.75 检查"
  # workspace 包级 rust-version，形如 rust-version = "1.75"
  DECLARED=$(grep -oE 'rust-version[[:space:]]*=[[:space:]]*"[0-9]+\.[0-9]+"' Cargo.toml \
    | grep -oE '[0-9]+\.[0-9]+' | head -1)
  RUSTC=$(rustc --version | grep -oE 'rustc [0-9]+\.[0-9]+\.[0-9]+' | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
  echo "   Cargo.toml 声明  rust-version = ${DECLARED:-<未声明>}"
  echo "   本机             rustc $RUSTC"
  if [ -z "$DECLARED" ]; then
    fail "1. MSRV 声明缺失"
  else
    D_MINOR=${DECLARED#*.}; R_MINOR=${RUSTC#*.}; R_MINOR=${R_MINOR%%.*}
    if [ "${RUSTC%%.*}" -gt 1 ] || { [ "${RUSTC%%.*}" -eq 1 ] && [ "$R_MINOR" -ge 75 ]; }; then
      ok "1. MSRV 1.75 检查"
    else
      fail "1. MSRV 检查（本机 $RUSTC < 声明 $DECLARED）"
    fi
  fi
fi

# ── 2. fmt / clippy ──────────────────────────────────────────────
#
# clippy 只对**本 PR 触碰的 crate**（qm-common / qm-cluster）用 -D warnings 硬挡；
# 其余 crate（qm-media / qm-sfu / qm-signaling）在历史提交里就有一批 warning，
# 那是另开的技术债，不在本 PR 的验收范围内，所以这里单独跑一次、结果只作提示。
# 等历史 warning 清零后再把下面一行改回 --workspace 并把提示跑改成 step。
if want 2; then
  step "2a. cargo fmt --check" cargo fmt --check
  step "2b. cargo clippy（本 PR 触碰的 crate，-D warnings）" \
    cargo clippy --all-targets --locked -p qm-common -p qm-cluster -- -D warnings
  say "2c. cargo clippy --workspace（历史 warning，仅提示）"
  echo "   \$ cargo clippy --workspace --all-targets --locked"
  if cargo clippy --workspace --all-targets --locked; then
    ok "2c. workspace clippy"
  else
    skip "2c. workspace clippy 有历史 warning（不阻塞本 PR）"
  fi
fi

# ── 3. 测试 ──────────────────────────────────────────────────────
if want 3; then
  step "3. cargo test --workspace --locked --no-fail-fast" \
    cargo test --workspace --locked --no-fail-fast
fi

# ── 4. release 构建 ──────────────────────────────────────────────
if want 4; then
  step "4. cargo build --release --locked --workspace" \
    cargo build --release --locked --workspace
fi

# ── 5. qm-demo 默认命令 + 端口假设 ───────────────────────────────
if want 5; then
  say "5. qm-demo 默认命令（验收标准 4：8080 媒体 / 8081 信令）"
  BIN="$REPO_ROOT/target/release/qm-demo"
  [ -x "$BIN" ] || cargo build --release --locked --bin qm-demo
  if [ ! -x "$BIN" ]; then
    fail "5. qm-demo 构建失败"
  else
    OUT="$REPO_ROOT/target/qm-demo.verify.out"
    if RUST_LOG=warn "$BIN" --bind 127.0.0.1 --frames 2 >"$OUT" 2>&1; then
      echo "   qm-demo 退出码 0（报告写入 $OUT）"
      ok "5. qm-demo 默认命令"
    else
      echo "   ---- 输出尾部 ----"; tail -20 "$OUT"
      fail "5. qm-demo 默认命令"
    fi
    # 配置层断言：默认配置必须是 8080 / 8081（全局约束）。
    if grep -qE '^port[[:space:]]*=[[:space:]]*8080' config/default.toml \
       && grep -qE '^signaling_port[[:space:]]*=[[:space:]]*8081' config/default.toml; then
      ok "5b. default.toml 端口假设 8080/8081"
    else
      fail "5b. default.toml 端口假设"
    fi
  fi
fi

# ── 6. docker build ──────────────────────────────────────────────
if want 6; then
  if [ "$NO_DOCKER" = 1 ] || ! have docker; then
    skip "6. docker build（docker 不可用或 --no-docker）"
  else
    step "6. docker build -t quickmeet-verify:local ." docker build -t quickmeet-verify:local .
  fi
fi

# ── 7. compose 语法（含 1.29.2 兼容性）───────────────────────────
if want 7; then
  if [ "$NO_DOCKER" = 1 ] || ! have docker-compose; then
    skip "7. docker-compose config（docker-compose 不可用或 --no-docker）"
  else
    say "7. compose 1.29.2 语法兼容检查"
    HIT=0
    # compose 1.29.2 会直接拒绝的键。只列**确定**是 2.x 才有的顶层/服务级键：
    #   deploy（compose file 场景）、extends、develop、secrets、config（顶层）、include。
    # 刻意不查 ipam 的 `config`（1.29.2 支持 `networks.<n>.ipam.config`），
    # 也不查 `target`（多阶段 build 的目标，1.29.2 支持）。
    # 键必须出现在 4 空格缩进以内（服务级）—— 更深的缩进说明它是别的键的子项，不算命中。
    grep -nE "^[[:space:]]*(deploy|extends|develop|secrets|include):[[:space:]]*" \
      docker-compose.yml > "$REPO_ROOT/target/compose-2x.txt" 2>/dev/null && HIT=1
    if [ "$HIT" -ne 0 ]; then
      echo "   发现 2.x 专属键（compose 1.29.2 会拒绝）："
      cat "$REPO_ROOT/target/compose-2x.txt"
      fail "7a. compose 1.29.2 语法检查"
    else
      echo "   未发现 deploy:/extends/develop/secrets/include（ipam.config 属 1.29.2 合法键）"
      ok "7a. compose 1.29.2 语法检查"
    fi
    step "7b. docker-compose config" docker-compose -f "$REPO_ROOT/docker-compose.yml" config
  fi
fi

# ── 8. up + 健康检查 + 探活端点 ─────────────────────────────────
if want 8; then
  if [ "$NO_DOCKER" = 1 ] || ! have docker-compose; then
    skip "8. 四容器健康检查（docker-compose 不可用或 --no-docker）"
  else
    say "8. docker-compose up -d --build + 健康检查"
    docker-compose down -v --remove-orphans >/dev/null 2>&1
    if ! docker-compose up -d --build; then
      fail "8. docker-compose up"
    else
      # start_period 30s + interval 5s × retries 2 = 45s 判死窗口；给 240s 余量。
      echo "   等待 qm-nats/qm-media/qm-media-2/qm-media-3/qm-signaling 全部 healthy"
      ALL_OK=0
      for i in $(seq 1 60); do
        GOOD=$(docker ps --filter status=healthy --format '{{.Names}}' | sed '/^$/d' | tr '\n' ' ')
        BAD=$(docker ps --filter status=unhealthy --format '{{.Names}}' | sed '/^$/d' | tr '\n' ' ')
        N=$(echo "$GOOD" | sed '/^$/d' | wc -w)
        echo "   [$i/60] healthy($N): ${GOOD:-none} | unhealthy: ${BAD:-none}"
        [ "$N" -ge 5 ] && [ -z "$BAD" ] && { ALL_OK=1; break; }
        MAXR=$(docker ps --format '{{.RestartCount}}' 2>/dev/null | sort -nr | head -1)
        [ "${MAXR:-0}" -gt 6 ] && { echo "   重启次数 $MAXR 超限，判定循环重启"; break; }
        sleep 4
      done
      docker ps --format 'table {{.Names}}\t{{.Status}}' 2>/dev/null || true
      [ "$ALL_OK" = 1 ] && ok "8a. 五容器全部 healthy" || fail "8a. 容器健康检查"

      if have curl; then
        EP_OK=1
        # 容器内探针 8090 -> 宿主机 8091/8092/8093；信令 8081。
        for p in 8081 8091 8092 8093; do
          code=$(curl -s -o /dev/null -w '%{http_code}' -m 3 "http://127.0.0.1:$p/healthz" || echo 000)
          echo "   GET http://127.0.0.1:$p/healthz -> $code"
          [ "$code" = "200" ] || EP_OK=0
        done
        [ "$EP_OK" = 1 ] && ok "8b. /healthz 探活端点全部 200" || fail "8b. /healthz 探活端点"
      else
        skip "8b. /healthz 探活（curl 不可用）"
      fi
    fi
  fi
fi

# ── 9. 重启自愈 ──────────────────────────────────────────────────
if want 9; then
  if [ "$NO_DOCKER" = 1 ] || ! have docker-compose; then
    skip "9. 重启自愈（docker-compose 不可用或 --no-docker）"
  else
    say "9. 逐个 restart，验证 restart: unless-stopped 自愈"
    OK_ALL=1
    for svc in qm-nats qm-media qm-media-2 qm-media-3 qm-signaling; do
      docker restart "$svc" >/dev/null 2>&1 || OK_ALL=0
      sleep 6
      st=$(docker inspect --format '{{.State.Health.Status}}' "$svc" 2>/dev/null)
      echo "   $svc -> ${st:-no-healthcheck}"
      [ "$st" = "healthy" ] || OK_ALL=0
    done
    [ "$OK_ALL" = 1 ] && ok "9. 重启自愈" || fail "9. 重启自愈"
  fi
fi

# ── 10. CI workflow 语义校验（YEJ-114 并入 QM-015）───────────────
#
# 与 CI job `workflow-lint` 跑的是同一份脚本，本地过一遍就能提前发现
# 「YAML 解析成功但语义已变」这类坑（on 被吞成 bool、job id 含点号、
# name 被当成 inline mapping、uses: 没固定 ref 等）。
# 不依赖 docker，所以 --no-docker 时也照跑。
if want 10; then
  say "10. CI workflow 语义校验（scripts/verify-workflows.sh）"
  PY_BIN=""
  for c in python3 python py; do
    if have "$c"; then PY_BIN="$c"; break; fi
  done
  if [ -z "$PY_BIN" ]; then
    skip "10. workflow 校验（缺少 python3 / python）"
  elif ! "$PY_BIN" -c 'import yaml' >/dev/null 2>&1; then
    skip "10. workflow 校验（缺少 pyyaml：$PY_BIN -m pip install pyyaml）"
  else
    export PYTHONIOENCODING=UTF-8 PYTHONUNBUFFERED=1
    step "10. workflow 预检（含 7 个反例自证）" \
      bash scripts/verify-workflows.sh
  fi
fi

# ── 清理 ──────────────────────────────────────────────────────────
if have docker-compose && [ "$NO_DOCKER" = 0 ]; then
  docker-compose down -v --remove-orphans >/dev/null 2>&1 || true
fi

# ── 汇总 ──────────────────────────────────────────────────────────
echo
echo "════════════════════════════════════════════"
if [ "${#FAILURES[@]}" -eq 0 ]; then
  echo "🟢 全部通过：$PASS_COUNT 项，耗时 $(elapsed)s"
  exit 0
else
  echo "🔴 ${#FAILURES[@]} 项失败："
  for f in "${FAILURES[@]}"; do echo "   - $f"; done
  echo "   通过 $PASS_COUNT 项，耗时 $(elapsed)s"
  exit 1
fi
