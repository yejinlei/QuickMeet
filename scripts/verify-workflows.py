#!/usr/bin/env python3
"""校验 .github/workflows/*.yml（YEJ-114 并入 QM-015 / CI workflow 校验）。

本地预检脚本：改 workflow 前先跑它，别把 YAML 解析坑留到 CI 才暴露。
CI job `workflow-lint` 跑的是同一个文件。

用法：
  python3 scripts/verify-workflows.py             # 扫 .github/workflows/*.yml*
  python3 scripts/verify-workflows.py <file>...   # 只扫指定文件
退出码：0 全过 / 1 有失败 / 2 环境缺依赖（pyyaml）或没有可校验的文件。

── 三个真实踩过的坑 ──────────────────────────────────────────────

坑 1：`on:` 被 YAML 1.1 吞成布尔 True。
  pyyaml 按 YAML 1.1 解析，`on` 是 bool 字面量，于是顶层键变成 True。
  **Actions 自己有解析器，它认 "on"，所以 workflow 本身照常跑** —— 被坑的
  是任何消费 pyyaml 输出的工具（本脚本、yq、部署脚本）。因此这里不做
  「解析结果必须有 on 键」的断言（那样每个正常的 Actions workflow 都会误报），
  而是做**文本层 vs 解析层对账**：把文本里的顶层键和解析后的键集合比一下，
  差集必须全部落在 YAML 1.1 的 bool 字面量集合里。出现了不在集合内的差集键，
  说明有别的东西被静默吞掉了 —— 那才是真正的事故。

坑 2：job id 里带点号（`rust-1.75`、`compose-1.29.2`）。
  Actions 把 job id 放进表达式上下文按 JSONPath 解析，点号被当成取字段，
  直接报 `Can get expression value only for Object or Array, got: 'Number'`，
  整个 workflow 静默不执行 —— 表象是「CI 没跑」，不是语法错误。
  → 断言 job id 只含字母/数字/_/-。

坑 3：`name:` 里含「冒号+空格」没加引号（`name: PR 标题 QM-00x: xxx 约定`）。
  YAML 把它当成 inline mapping，要么解析报错，要么 name 变成 dict。
  → 断言 job 级和 step 级的 `name` 解析出来都是字符串。

── 结构校验 ──────────────────────────────────────────────────────
每个 job 必须有 `runs-on` / `timeout-minutes`（且 ≤15 分钟，CI 预算上限）/
非空 `steps`，每个 step 必须有 `uses` 或 `run`。

── 依赖校验（「不引入私有依赖」的机器判据）──────────────────────
`uses:` 只允许公开仓库 + 固定 ref：不允许本地路径（`./`）、不允许无 ref
（无 ref 会解析到默认分支 HEAD，会漂移）、不允许分支名（`main`/`master`）。
owner 走白名单 —— 白名单里的都是社区公开 action，加一项要在 PR 里说明理由。
"""

from __future__ import annotations

import pathlib
import re
import sys

try:
    import yaml
except ImportError:
    print("需要 pyyaml：python -m pip install pyyaml", file=sys.stderr)
    sys.exit(2)

# Windows 的 GBK 控制台编不出 ✓/⚠/✗，一打印就 UnicodeEncodeError。
# 强制 UTF-8 输出，编不出的字符降级成 ?，脚本本身不因编码崩掉。
for _stream in (sys.stdout, sys.stderr):
    _reconfigure = getattr(_stream, "reconfigure", None)
    if _reconfigure is not None:
        try:
            _reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError):  # 已被替换 / 已 detach 的流
            pass

# ── 常量 ──────────────────────────────────────────────────────────

# YAML 1.1 的 bool 字面量。pyyaml 会把它们统统解析成 True/False。
YAML11_BOOLS = {"on", "off", "yes", "no", "true", "false", "y", "n"}

# CI 单次运行预算（Issue 约束：≤15 分钟）。
MAX_TIMEOUT_MIN = 15

# 允许在 `uses:` 里引用的 action owner。全部是公开仓库，无凭据、无私有网络访问。
# 加新 owner 等于引入新的供应链依赖，必须在 PR 里写理由。
ALLOWED_ACTION_OWNERS = {
    "actions",       # GitHub 官方（checkout / upload-artifact）
    "actions-rs",    # actions-rs 官方镜像
    "dtolnay",       # rust-toolchain：pin MSRV 1.75 的唯一可靠方式
    "swatinem",      # rust-cache
    "ilammy",        # msvc-dev-cmd：windows MSVC 环境
    # 复审意见要求 workflow-lint 追加一个 action-validator 做交叉校验。注意
    # `actions/action-validator` 在 GitHub 上**不存在**（API 404）——这个名字的
    # 真实归属是 mpalmer/action-validator，所以白名单里加的是 mpalmer，不是
    # 往 actions/* 里凑一个假 ref。该步骤是 continue-on-error 的非阻塞校验。
    "mpalmer",       # action-validator（第三方，仅 workflow-lint 非阻塞交叉校验）
}

JOB_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
# ref 必须是 tag（v4 / 1.75.0 / v0.12.3）或提交 SHA（7~40 位十六进制）。
REF_RE = re.compile(r"^(v?\d[0-9A-Za-z._-]*|\d{7,40})$")
USES_RE = re.compile(r"^\s*(?:-\s*)?uses:\s*['\"]?([^'\"#\s]+)\s*['\"]?\s*(#.*)?$")

MAX_TIMEOUT_MSG = f"CI 单次运行预算 {MAX_TIMEOUT_MIN} 分钟"

FAILS: list[str] = []
WARNS: list[str] = []


def bad(msg: str) -> None:
    FAILS.append(msg)
    print(f"  ✗ {msg}")


def warn(msg: str) -> None:
    WARNS.append(msg)
    print(f"  ⚠ {msg}")


def good(msg: str) -> None:
    print(f"  ✓ {msg}")


def text_top_keys(text: str) -> list[str]:
    """文本层的顶层键（缩进为 0 的 `key:`），保留顺序、去重。"""
    seen: dict[str, None] = {}
    for line in text.splitlines():
        if not line or line.lstrip().startswith("#"):
            continue
        if line[0] in " \t":
            continue
        m = re.match(r"^([A-Za-z0-9_.-]+)\s*:", line)
        if m:
            seen.setdefault(m.group(1))
    return list(seen)


def check_workflow(path: pathlib.Path) -> None:
    print(f"\n[{path.name}]")
    text = path.read_text(encoding="utf-8")

    # ── 坑 3 前置：解析得动 ────────────────────────────────────────
    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        bad(f"YAML 解析失败（坑 3 形态：多半是 name 里冒号+空格没加引号）：{exc}")
        return
    if not isinstance(doc, dict):
        bad(f"顶层不是映射（got {type(doc).__name__}）")
        return
    good("YAML 解析成功")

    # ── 坑 1：文本层 vs 解析层对账 ─────────────────────────────────
    on_key = True if True in doc else ("on" if "on" in doc else None)
    parsed_keys = {str(k) for k in doc}
    raw_keys = text_top_keys(text)
    swallowed = [k for k in raw_keys if k not in parsed_keys]
    unexpected = [k for k in swallowed if k not in YAML11_BOOLS]
    if unexpected:
        bad(
            f"顶层键 {unexpected} 在文本里存在但解析后丢失，且不是 YAML 1.1 bool"
            f"字面量 —— 有东西被静默吞掉了"
        )
    if swallowed:
        warn(
            f"顶层键 {swallowed} 被 YAML 1.1 解析成 bool（预期行为，Actions 有自己的"
            f"解析器不受影响；只有 pyyaml/yq 这类工具会中招）"
        )
    if on_key is None:
        bad("缺少触发器 `on:`（文本层与解析层都找不到）")
        return
    if not isinstance(doc.get(on_key), dict) or not doc[on_key]:
        bad("顶层 `on` 的值不是非空映射（没有触发器？）")
    else:
        good("顶层 `on` 存在且触发器列表非空")

    # ── 显式确认 concurrency / permissions 没被吞成 bool/int ────────
    for key in ("concurrency", "permissions"):
        if key not in parsed_keys:
            if key == "permissions":
                bad(f"缺少顶层 `{key}:`（应显式声明最小权限，如 contents: read）")
            else:
                warn(f"缺少顶层 `{key}:`（同一分支并发会互相排队）")
            continue
        got = doc[key]
        if isinstance(got, (bool, int, str)) or not isinstance(got, dict):
            bad(f"顶层 `{key}` 解析成 {type(got).__name__}，不是映射")
        else:
            good(f"顶层 `{key}` 是映射（未被吞成 bool/int）")

    # ── jobs 结构 ──────────────────────────────────────────────────
    jobs = doc.get("jobs")
    if not isinstance(jobs, dict) or not jobs:
        bad("`jobs` 缺失或不是非空映射")
        return

    for jid, job in jobs.items():
        if not isinstance(jid, str):
            bad(f"job id {jid!r} 解析成 {type(jid).__name__}")
        elif not JOB_ID_RE.match(jid):
            bad(
                f"job id `{jid}` 含非法字符（坑 2）：只允许字母/数字/_/-。点号会被"
                f" Actions 当 JSONPath 解析，整个 workflow 静默不执行"
            )
        if not isinstance(job, dict):
            bad(f"job `{jid}` 不是映射（got {type(job).__name__}）")
            continue

        runs_on = job.get("runs-on")
        if not isinstance(runs_on, (str, list)) or not runs_on:
            bad(f"job `{jid}` 缺 `runs-on`（或类型不对）")

        timeout = job.get("timeout-minutes")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            bad(f"job `{jid}` 缺 `timeout-minutes`（或不是数字）")
        elif timeout > MAX_TIMEOUT_MIN:
            bad(f"job `{jid}` 的 timeout-minutes={timeout} 超预算（{MAX_TIMEOUT_MSG}）")

        steps = job.get("steps")
        if not isinstance(steps, list) or not steps:
            bad(f"job `{jid}` 缺非空 `steps`")

        for i, step in enumerate(steps if isinstance(steps, list) else [], start=1):
            if not isinstance(step, dict):
                bad(f"job `{jid}` step {i} 不是映射")
                continue
            if not ("uses" in step or "run" in step):
                bad(f"job `{jid}` step {i} 既无 `uses` 也无 `run`")
            if "name" in step and not isinstance(step["name"], str):
                bad(
                    f"job `{jid}` step {i} 的 `name` 解析成 {type(step['name']).__name__}"
                    f"（坑 3：含「冒号+空格」必须加引号）"
                )

        if "name" in job and not isinstance(job["name"], str):
            bad(
                f"job `{jid}` 的 `name` 解析成 {type(job['name']).__name__}"
                f"（坑 3：含「冒号+空格」必须加引号）"
            )

    good(f"{len(jobs)} 个 job 的 runs-on / timeout-minutes / steps 结构合规")

    # ── 依赖：uses: 必须是公开仓库 + 固定 ref ───────────────────────
    for i, line in enumerate(text.splitlines(), start=1):
        m = USES_RE.match(line)
        if not m:
            continue
        ref = m.group(1)
        if ref.startswith((".", "/", "docker://")):
            bad(
                f"第 {i} 行 `uses: {ref}` 引用本地路径/容器镜像 —— 必须用公开仓库"
                f"（私有路径会让断网或换机器后直接失败）"
            )
            continue
        if "/" not in ref or "@" not in ref:
            bad(
                f"第 {i} 行 `uses: {ref}` 缺少 `@<tag-or-sha>` 固定 ref"
                f"（无 ref 会漂移到默认分支 HEAD）"
            )
            continue
        owner_repo, version = ref.rsplit("@", 1)
        owner, repo = owner_repo.split("/", 1)
        if owner.lower() not in ALLOWED_ACTION_OWNERS:
            bad(
                f"第 {i} 行 `{owner}/{repo}` 不在 owner 白名单"
                f" {sorted(ALLOWED_ACTION_OWNERS)} 内 —— 新增 owner 等于新增"
                f" 供应链依赖，请在 PR 里说明理由"
            )
        if not REF_RE.match(version):
            bad(
                f"第 {i} 行 `{ref}` 的 ref `{version}` 不是 tag 或提交 SHA"
                f"（不能是分支名，分支会漂移）"
            )

    good("uses: 全部来自白名单 owner 且固定 ref（无私有依赖）")


def main() -> int:
    targets = sys.argv[1:]
    if targets:
        paths = [pathlib.Path(t) for t in targets]
    else:
        root = pathlib.Path(__file__).resolve().parent.parent
        wf = root / ".github" / "workflows"
        paths = sorted(wf.glob("*.yml")) + sorted(wf.glob("*.yaml"))

    missing = [p for p in paths if not p.is_file()]
    paths = [p for p in paths if p.is_file()]
    for p in missing:
        bad(f"{p} 不存在")

    if not paths and not targets:
        print("没有找到 .github/workflows/*.yml，无法校验", file=sys.stderr)
        return 2

    for p in paths:
        check_workflow(p)

    print()
    for w in WARNS:
        print(f"⚠ 提示：{w}")
    if FAILS:
        print(f"❌ workflow 校验失败，{len(FAILS)} 项：")
        for f in FAILS:
            print(f"   - {f}")
        return 1
    print(f"✅ workflow 校验通过（{len(paths)} 个文件）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
