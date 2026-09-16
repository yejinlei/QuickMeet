#!/usr/bin/env bash
# QuickMeet CI workflow 本地预检（YEJ-114 并入 QM-015）
#
# 用途：改 .github/workflows/*.yml 之前先跑一遍，把「YAML 解析成功但语义已变」
# 这类坑挡在提交前，不要留到 CI 上才暴露。CI job `workflow-lint` 跑的是
# **同一个脚本**（bash 直跑，不依赖任何第三方 action）。
#
# 用法：
#   bash scripts/verify-workflows.sh              # 扫 .github/workflows/*.yml*
#   bash scripts/verify-workflows.sh <file>...    # 只扫指定文件
#
# 退出码：0 全过 / 1 有失败 / 2 环境缺依赖或没有可校验的文件。

set -uo pipefail

# Windows 的 GBK 控制台编不出 ⚠/✓/✗；CI 的 ubuntu 是 C.UTF-8。
# 统一 UTF-8，保证两种环境输出一致，脚本不因编码崩掉。
export PYTHONIOENCODING=UTF-8
export PYTHONUNBUFFERED=1

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PY="scripts/verify-workflows.py"

# 把仓库里的 workflow 展开成显式列表；一个都没有就直接报错。
FILES=(.github/workflows/*.yml .github/workflows/*.yaml)
FILES=( $(printf '%s\n' "${FILES[@]}" | grep -v 'workflows/\*' ) )
if [ "${#FILES[@]}" -eq 0 ]; then
  echo "🔴 没有找到 .github/workflows/*.yml，无法校验" >&2
  exit 2
fi

echo "▶ CI workflow 本地预检（与 CI job workflow-lint 同一份脚本）"
echo "  目标：${FILES[*]}"
echo

# CI 的 ubuntu 是 `python3`；Windows（MobaXterm 现场）常只有 `python` 或 `py`。
# 逐个探测，避免「本地跑不了、CI 能跑」这种假失败。
PY_BIN=""
for c in python3 python py; do
  command -v "$c" >/dev/null 2>&1 && PY_BIN="$c" && break
done
if [ -z "$PY_BIN" ]; then
  echo "🔴 缺少 python3 / python（任一即可）" >&2
  exit 2
fi
if ! "$PY_BIN" -c 'import yaml' >/dev/null 2>&1; then
  echo "🔴 缺少 pyyaml：$PY_BIN -m pip install pyyaml" >&2
  exit 2
fi

# ── 1. 真实仓库的 workflow 必须过 ─────────────────────────────────
if ! "$PY_BIN" "$PY" "${FILES[@]}" "$@"; then
  echo >&2
  echo "🔴 仓库里的 workflow 未通过预检（见上面的失败项）" >&2
  exit 1
fi

# ── 2. 反例自证：校验器必须能把踩过的坑拦下来 ────────────────────
# 只跑这一份脚本还不够 —— 下面 7 个反例如果被放过去，说明校验器本身失效了。
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

fail_case() {
  local desc="$1" body="$2"
  printf '%s' "$body" > "$TMP/case.yml"
  if "$PY_BIN" "$PY" "$TMP/case.yml" >/dev/null 2>&1; then
    echo "   ✗ $desc：校验器**没有**拦住（校验器失效）"
    return 1
  fi
  echo "   ✓ $desc：已被拦住"
  return 0
}

echo "▶ 反例自证（校验器必须能把它们拦下来）"
CASES_OK=1

# 坑 3：job name 含「冒号+空格」且没加引号 → YAML 当成 inline mapping。
fail_case "坑 3（name 冒号+空格未加引号）" '
name: CI
"on":
  push:
    branches: [main]
permissions:
  contents: read
jobs:
  pr-title:
    name: PR 标题 QM-00x: xxx 约定
    runs-on: ubuntu-22.04
    timeout-minutes: 5
    steps:
      - run: echo hi
' || CASES_OK=0

# 坑 2：job id 带点号 → Actions 当 JSONPath 解析，整个 workflow 静默不执行。
fail_case "坑 2（job id 含点号）" '
name: CI
"on":
  push:
    branches: [main]
permissions:
  contents: read
jobs:
  "rust-1.75":
    name: msrv
    runs-on: ubuntu-22.04
    timeout-minutes: 10
    steps:
      - run: echo hi
' || CASES_OK=0

# 依赖漂移：ref 写成分支名（会解析到默认分支 HEAD）。
fail_case "依赖漂移（ref 写成分支名）" '
name: CI
"on":
  pull_request:
permissions:
  contents: read
jobs:
  lint:
    name: lint
    runs-on: ubuntu-22.04
    timeout-minutes: 5
    steps:
      - uses: dtolnay/rust-toolchain@master
        with:
          profile: minimal
' || CASES_OK=0

# 依赖漂移：没有 @<ref>。
fail_case "依赖漂移（缺 @<ref>）" '
name: CI
"on":
  pull_request:
permissions:
  contents: read
jobs:
  lint:
    name: lint
    runs-on: ubuntu-22.04
    timeout-minutes: 5
    steps:
      - uses: actions/checkout
' || CASES_OK=0

# 依赖来源：不在 owner 白名单（等于新增一条供应链）。
fail_case "依赖来源（owner 不在白名单）" '
name: CI
"on":
  pull_request:
permissions:
  contents: read
jobs:
  lint:
    name: lint
    runs-on: ubuntu-22.04
    timeout-minutes: 5
    steps:
      - uses: some-random-user/format@v1.0.0
' || CASES_OK=0

# 超时超预算。
fail_case "超时超预算（timeout-minutes 30）" '
name: CI
"on":
  push:
permissions:
  contents: read
jobs:
  slow:
    name: slow
    runs-on: ubuntu-22.04
    timeout-minutes: 30
    steps:
      - run: echo hi
' || CASES_OK=0

# 结构缺失：没有 steps。
fail_case "结构缺失（无 steps）" '
name: CI
"on":
  push:
permissions:
  contents: read
jobs:
  empty:
    name: empty
    runs-on: ubuntu-22.04
    timeout-minutes: 5
' || CASES_OK=0

if [ "$CASES_OK" = 1 ]; then
  echo
  echo "🟢 workflow 预检全部通过：仓库 workflow 合规，校验器对 7 类反例都能拦住"
  exit 0
fi
echo
echo "🔴 有反例没被拦住 —— 校验器本身失效了，请修 scripts/verify-workflows.py" >&2
exit 1
