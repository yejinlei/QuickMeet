<!--
PR 标题约定（强制）：`QM-00x: 简短中文说明`
  例：QM-006: 修复异构集群调度权重与旁听加减冲掉房间归属

标题必须带 Issue 编号，否则：
  * 无法与 Issue 自动关联（QEJ-109 / QM-018 的验收标准 1）
  * CI 的 `pr-title` job 会直接失败并打回
分支命名：`qm-<编号>-<短描述>`，例 `qm-006-cluster`
-->

## 关联

- Issue：QM-00x（`QEJ-nn`）
- 所属 Epic：YEJ-89 QuickMeet 全量需求总览
- 架构方案：（贴系统架构师审核通过的方案链接 / Issue 编号；未审核不得开 PR）

## 变更摘要

<!-- 3-5 句：改了什么、为什么改、影响哪个模块。不要贴代码 diff 摘要。 -->

## 覆盖的验收标准

<!-- 对照 Issue 正文的「验收标准」逐条勾选，写不出的条目要说明原因。 -->

- [ ] 验收标准 1：<对应项>
- [ ] 验收标准 2：<对应项>
- [ ] 验收标准 3：<对应项>

## 自检结果

本地跑 `bash scripts/verify.sh --no-docker`（有 Docker 环境跑全量，不带 `--no-docker`），
粘贴结尾的汇总行（通过项数 / 失败项数）。CI 跑同一份脚本，避免「本机过、CI 挂」。

```text
🟢 全部通过：N 项，耗时 Xs
```

补充：哪些步骤因为本机环境缺依赖被 SKIP（例如未装 docker / docker-compose），
以及 CI 上对应 job 名：

- 本机构建与测试：`rust-msrv`
- 容器构建与四容器健康检查：`compose-legacy`
- MSVC + webrtc 特性：`windows-webrtc`
- PR 标题约定：`pr-title`

## 跨文件依赖 / 兼容性

- 改了公共接口或配置项：（列出被影响的 crate / 文件；改了配置字段要同步
  `crates/qm-common/src/config.rs` 的 `known_section` 白名单，否则
  `QM_*` 环境变量会直接报「指向未知的配置项」）
- 文档是否同步：（`docs/` 与 `config/` 是否一起改，PR 里点明）
- 是否引入新的 crates.io 依赖：（新增依赖必须声明 MSRV 是否仍为 1.75）

## 回滚方式

<!-- 出问题时怎么撤回：单个 commit revert / 关 feature flag / 回退镜像 tag。 -->
