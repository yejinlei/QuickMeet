# -*- coding: utf-8 -*-
"""Generate the 17 QuickMeet sub-issue description files + a dispatch manifest."""
import json
import os

ROOT = os.path.dirname(os.path.abspath(__file__))

CONSTRAINTS = """## 全局强制约束（本 Issue 必须遵守）

1. 所有 Rust 代码兼容 Rust 1.75+ 稳定版，禁止引入私有不可访问依赖
2. 所有容器化配置必须兼容 docker-compose 1.29.2，禁止依赖 docker-compose-plugin
3. 媒体服务默认监听 8080 端口，适配内网 192.168.0.0/24 网段
4. AI 能力只对接本地部署的硅基流动大模型接口，禁止调用公网第三方 API
5. 所有音视频、会议数据必须本地存储，不得上传公网，满足私有化合规要求

## 交付与汇报

- 代码落在本仓库工作目录（`F:\\src\\QuickMeet`），PR 标题包含本 Issue 编号（如 `QM-007: xxx`）以便自动关联
- 完成后把 Issue 状态置为 `in_review`，并在评论区提交：交付清单、验收标准逐项自检结果、遗留风险与未覆盖项
- 验收复核人是 **资深测试工程师**（agent id `0aed226d-529b-44ef-b6cb-2d504105e3d0`）：在提交评论中 @他请求对照上方验收标准逐条校验，不达标直接打回修改；**不要** @ 总调度或 Epic 智能体复核（总调度只做派单与推进，不做编码/测试验收）
- 卡点问题（依赖缺失、需求冲突、环境不通）不要静默跳过，在本 Issue 评论区说明并同步 Epic
"""

AGENTS = {
    "claude": ("代码高手 Claude（高阶攻坚专属）", "1ef2db42-c45f-48f5-b061-c3c84140b467"),
    "codex1": ("代码高手Codex-01", "4fa7b7fa-8d24-4e54-ab1a-da2030eb9093"),
    "codex2": ("代码高手Codex-02", "f68c8312-25a7-4954-b387-c061da0363c8"),
    "codex3": ("代码高手Codex-03", "f480ef83-4c8a-4a85-b94a-0ce8597cb24a"),
    "qa": ("资深测试工程师", "c604557f-3d56-4baa-9ec5-2d8a6bfe834a"),
}

ISSUES = [
    dict(no="QM-001", stage=1, agent="claude",
         title="基础 Rust 异步框架搭建与 webrtc-rs 编解码验证",
         deps="无（第一阶段基线任务，可立即开始）",
         scope=["搭建 Cargo workspace + tokio 异步框架骨架，统一错误处理、日志、配置加载（媒体端口 8080、内网 192.168.0.0/24）",
                "以 webrtc-rs v0.17.1 跑通原生示例：PeerConnection 建立、收发媒体轨道、ICE/DTLS 握手",
                "完成 VP8、H.264、Opus 三种编解码的收发兼容验证，输出可复用的 codec 封装模块"],
         accept=["`cargo build --release` 在 Rust 1.75+ 稳定版通过，无私有依赖",
                 "webrtc-rs v0.17.1 双端 PeerConnection 示例可稳定互连，媒体可往返",
                 "VP8/H.264 视频与 Opus 音频编解码均有可复现的收发验证结果（单元测试或验证脚本）",
                 "提供本地运行的可执行 demo 与运行说明"]),
    dict(no="QM-002", stage=1, agent="codex2",
         title="SFU 选择性转发架构与音视频轨道解耦管理",
         deps="依赖 QM-001（webrtc-rs 框架与 codec 封装）",
         scope=["实现 SFU 选择性转发（Selective Forwarding）架构，按订阅者需求转发而非混流",
                "音视频轨道解耦管理：独立的生命周期、订阅关系与发布者/接收者映射",
                "STUN/TURN 穿透能力：STUN 服务器 + TURN 中继兜底，覆盖内网 192.168.0.0/24 与 NAT 场景"],
         accept=["SFU 可在多订阅者场景下按需转发，无全量混流带宽浪费（有转发路径测试证明）",
                 "音视频轨道可独立订阅/退订，断开单一订阅者不影响其他订阅者",
                 "STUN 打洞与 TURN 中继均在测试环境跑通，含回环与 NAT 模拟用例",
                 "接口设计文档说明转发拓扑与轨道管理模型"]),
    dict(no="QM-003", stage=1, agent="codex3",
         title="NACK/FEC 弱网优化与硬件加速（单服 200+ 路承载）",
         deps="依赖 QM-001、QM-002（转发架构与轨道管理）",
         scope=["实现 NACK 丢包重传与 FEC 前向纠错，弱网下音视频可恢复",
                "硬件加速适配：编码器/解码器优先走 GPU/硬编（不可用时优雅降级到软编）",
                "容量压测并调优，目标单服务器承载 ≥200 路 1080p@30fps 媒体流，端到端延迟 ≤200ms"],
         accept=["30% 丢包场景下音频无中断、视频无长时间卡顿（有压测/抓包数据）",
                 "硬件加速路径与软编降级路径均有验证结果",
                 "≥200 路流的压测报告：吞吐、延迟、CPU/内存占用与瓶颈结论",
                 "压测方法与复现脚本随代码提交"]),
    dict(no="QM-004", stage=2, agent="codex2",
         title="WebSocket 信令协议、SDP/ICE 协商与 JWT 鉴权",
         deps="依赖 QM-002（媒体转发链路）；可与 QM-001/QM-003 并行设计",
         scope=["WebSocket 轻量信令协议：消息类型、时序、错误码、重连与心跳",
                "SDP offer/answer 与 ICE candidate 协商适配，打通与 SFU 的媒体协商",
                "JWT 鉴权：令牌签发、校验、过期与刷新，未鉴权连接拒绝"],
         accept=["信令协议文档覆盖全部消息类型与失败路径",
                 "客户端经信令完成 SDP/ICE 协商并成功建联媒体（与 SFU 端到端验证）",
                 "JWT 无效/过期/缺失三种情况均被拒绝且有明确错误码",
                 "断线重连后信令状态可恢复"]),
    dict(no="QM-005", stage=2, agent="codex3",
         title="房间生命周期管理与参会者权限、主持人控制",
         deps="依赖 QM-004（信令与鉴权链路）",
         scope=["房间生命周期：创建、加入、离开、销毁，房间唯一标识与元数据管理",
                "参会者权限模型：观察者/普通参会者/主持人，操作权限矩阵",
                "主持人控制能力：静音、移出会议、锁定房间、举手应答、广播消息"],
         accept=["房间从创建到销毁全生命周期有状态机说明与测试覆盖",
                 "权限矩阵中每一条操作均有允许/拒绝的自动化验证",
                 "主持人控制能力全部可用且有 UI 层可调用接口",
                 "异常退出（掉线、崩溃）的参会者能被正确清理"]),
    dict(no="QM-006", stage=2, agent="claude",
         title="NATS 分布式状态同步与集群路由调度（万人级旁听扩容）",
         deps="依赖 QM-005（房间模型与参会者状态）",
         scope=["引入 NATS 做跨节点房间状态同步，多节点对同一房间视图一致",
                "集群路由调度：入会节点选择、负载分发、节点故障转移",
                "面向万人级旁听（只读观众）的扩容路径：旁听流量与互动流量的分级承载"],
         accept=["多节点部署下房间状态一致（有并发入会/离会的同步验证）",
                 "节点宕机后服务可自动转移到其他节点，会议不中断",
                 "旁听场景压测数据与扩容方案文档（说明达到万人级的前提条件与瓶颈）",
                 "NATS 配置与容器编排片段符合 compose 1.29.2 兼容要求"]),
    dict(no="QM-007", stage=3, agent="codex1",
         title="对接硅基流动 ASR 实现实时语音转写与字幕推送",
         deps="依赖 QM-004（信令通道用于字幕推送）；媒体流来自第一阶段",
         scope=["对接本地部署的硅基流动 ASR 接口，实现实时流式语音转写",
                "转写结果按说话人/时间戳结构化，通过信令实时推送字幕",
                "字幕延迟与准确率指标采集（转写准确率 ≥95%、延迟 ≤1s）"],
         accept=["ASR 仅访问本地部署接口，无任何公网 API 调用（可审查代码与配置）",
                 "转写准确率 ≥95%，端到端字幕延迟 ≤1s（有测试语料与统计结果）",
                 "字幕流式推送可被客户端实时渲染",
                 "转写数据本地存储，不上传公网"]),
    dict(no="QM-008", stage=3, agent="codex3",
         title="大模型驱动的智能会议纪要自动生成",
         deps="依赖 QM-007（转写结果）",
         scope=["会议结束后基于转写文本调用本地部署的硅基流动大模型生成结构化纪要",
                "纪要结构：议题、结论、行动项（责任人 + 截止时间）、待确认问题",
                "纪要本地持久化与可导出（Markdown/文本），支持重新生成"],
         accept=["会后自动生成结构化会议纪要，字段完整可审查",
                 "仅调用本地硅基流动大模型，无公网请求",
                 "纪要原文与转写全文均本地留存，不上传公网",
                 "生成失败有明确错误反馈，不产生半成品纪要"]),
    dict(no="QM-009", stage=3, agent="claude",
         title="AI 音视频增强：虚拟背景与智能降噪",
         deps="依赖第一阶段媒体管线；模型调用走本地部署接口",
         scope=["虚拟背景：实时抠图 + 背景替换/模糊，提供多种可选背景",
                "智能降噪：人声增强与背景噪声抑制，低算力设备可降级",
                "增强效果默认关闭，参会者按需开启，并上报算力开销指标"],
         accept=["虚拟背景与智能降噪均可用，开关实时生效",
                 "仅使用本地部署的硅基流动模型能力，无公网调用",
                 "增强开启时的 CPU/GPU 占用与帧率影响有实测数据",
                 "低配环境自动降级到关闭或低质量模式"]),
    dict(no="QM-010", stage=3, agent="codex2",
         title="AI 弱网自适应码率",
         deps="依赖 QM-003（NACK/FEC 与网络质量度量）",
         scope=["基于网络质量信号（丢包、RTT、抖动）驱动码率/分辨率自适应决策",
                "弱网下优先保音频：视频自动降分辨率/降帧率，网络恢复后回升",
                "策略可观测：每次降级/回升输出决策日志，便于压测复盘"],
         accept=["30% 弱网丢包下音频无中断、视频无长时间卡顿",
                 "网络恢复后码率与分辨率可自动回升，无明显震荡",
                 "自适应决策日志完整，压测数据可复现",
                 "策略参数可配置，不硬编码"]),
    dict(no="QM-011", stage=4, agent="codex1",
         title="React 网页客户端（多布局、屏幕共享、字幕）",
         deps="依赖 QM-004（信令）、QM-005（房间与权限）、QM-007（字幕）",
         scope=["React 网页端：兼容 Chrome/Edge/Firefox/Safari 全主流浏览器",
                "多布局：画廊、演讲者、缩略图；屏幕共享与摄像头/麦克风控制",
                "字幕实时渲染、举手、聊天、成员列表，体验对齐主流商用会议软件"],
         accept=["三端浏览器入会正常，4 人会议音视频流畅",
                 "屏幕共享、字幕、举手、聊天功能全部可用",
                 "无阻断级控制台报错，加载性能可接受（说明实测指标）",
                 "页面访问仅限内网部署地址，无公网资源依赖"]),
    dict(no="QM-012", stage=4, agent="codex3",
         title="Tauri 桌面客户端（系统级共享、本地录制）",
         deps="依赖 QM-004/QM-005；网页端 QM-011 的交互可复用",
         scope=["Tauri（Rust + Webview）桌面客户端，Windows/macOS 双平台",
                "系统级屏幕共享（含其他应用窗口/整个屏幕）、本地录制（本地磁盘保存）",
                "系统托盘、后台会议提醒、自动更新通道预留"],
         accept=["Windows 与 macOS 构建产物均可安装运行并完整入会",
                 "系统级共享与本地录制可用，录制文件保存在本地且可回放",
                 "麦克风/摄像头权限申请流程正常，无崩溃路径",
                 "包体与启动耗时数据可报告"]),
    dict(no="QM-013", stage=4, agent="codex2",
         title="Flutter + Rust FFI 移动客户端（移动网络与低功耗）",
         deps="依赖 QM-004/QM-005；需与第一阶段 webrtc 能力对齐",
         scope=["Flutter 客户端 + Rust FFI 封装底层媒体能力，Android/iOS 双端",
                "适配移动网络：弱网自适应、蜂窝/WiFi 切换、后台保活",
                "低功耗优化：后台降帧、屏幕常亮控制、通知与来电式提醒"],
         accept=["Android 与 iOS 真机可入会并完成 4 人会议通话",
                 "移动网络切换场景下会议不中断",
                 "后台运行 30 分钟以上的功耗/流量实测数据",
                 "Rust FFI 边界错误处理完备，无悬垂指针/崩溃风险"]),
    dict(no="QM-014", stage=4, agent="qa",
         title="三端全量联调与性能验收测试",
         deps="依赖 QM-011、QM-012、QM-013（三端客户端）及第三阶段 AI 能力",
         scope=["网页 / 桌面 / 移动三端混合入会的全量联调测试",
                "对齐总验收标准：单命令部署、三端入会、4 人会议流畅、屏幕共享与字幕、纪要自动生成（准确率 ≥95%、延迟 ≤1s）、30% 丢包弱网表现",
                "输出缺陷清单、性能基线与验收报告"],
         accept=["四项总验收标准逐项给出 PASS/FAIL 结论与实测数据",
                 "三端混合会议（网页+桌面+移动同会）音视频流畅、屏幕共享与字幕可用",
                 "缺陷按严重度分级并回写到对应 Issue",
                 "提交可复现的测试脚本/用例清单，供回归使用"]),
    dict(no="QM-015", stage=5, agent="claude",
         title="DTLS-SRTP 全链路加密与安全合规（审计、水印）",
         deps="依赖 QM-005（权限模型）、QM-006（多节点状态）",
         scope=["DTLS-SRTP 全链路加密核查与加固：证书、密钥轮换、传输层配置审计",
                "操作审计：关键操作（入会、共享、录制、纪要导出、踢出）全量留痕",
                "合规能力：屏幕水印（含参会者标识）、数据留存策略与本地化存储校验"],
         accept=["全链路加密启用可验证（抓包或配置审计证据），无明文媒体外泄",
                 "审计日志覆盖全部关键操作，可查询、可导出、本地留存",
                 "水印功能在屏幕共享与录制中生效",
                 "全量数据留存于本地，无私网外发路径"]),
    dict(no="QM-016", stage=5, agent="codex1",
         title="Docker 编排一键私有化部署（兼容 docker-compose 1.29.2）",
         deps="依赖全部服务端阶段（第一~三阶段）；AI 依赖走本地硅基流动接口",
         scope=["全服务 Dockerfile 与 docker-compose.yml 编排：媒体服务、信令、NATS、AI 网关、存储",
                "严格兼容 docker-compose 1.29.2，禁止 docker-compose-plugin 特性",
                "单条 `docker-compose up -d` 拉起全套服务，端口 8080，适配内网 192.168.0.0/24"],
         accept=["`docker-compose up -d` 单命令成功拉起全部服务，无配置报错",
                 "compose 文件在 docker-compose 1.29.2 下 `config` 校验通过",
                 "服务健康检查与重启策略完备，容器日志可查",
                 "提供部署文档：前置条件、步骤、回滚方式"]),
    dict(no="QM-017", stage=5, agent="codex1",
         title="可视化监控面板、异常告警与单命令运维脚本",
         deps="依赖 QM-016（服务编排）",
         scope=["可视化监控面板：会议数、并发路数、媒体吞吐、延迟、丢包、节点健康",
                "异常告警：服务宕机、节点失联、延迟/丢包超阈值、磁盘与内存水位",
                "单命令运维脚本：状态巡检、日志收集、故障恢复一键执行"],
         accept=["监控面板展示全部约定指标且数据实时（≤30s 刷新）",
                 "告警在阈值触发时可靠送达并去重，恢复后自动解除",
                 "运维脚本单命令执行，覆盖巡检/日志/恢复三类操作",
                 "监控数据本地存储，不外发公网"]),
]

os.makedirs(os.path.join(ROOT, "desc"), exist_ok=True)

manifest = []
for it in ISSUES:
    name, agent_id = AGENTS[it["agent"]]
    body = [
        "## 上游依赖\n\n" + it["deps"],
        "## 交付内容\n\n" + "\n".join("- " + s for s in it["scope"]),
        "## 本 Issue 验收标准\n\n" + "\n".join("%d. %s" % (i + 1, s) for i, s in enumerate(it["accept"])),
        CONSTRAINTS.rstrip(),
    ]
    path = "desc/%s.md" % it["no"].lower()
    with open(os.path.join(ROOT, "desc", os.path.basename(path)), "w", encoding="utf-8", newline="\n") as f:
        f.write("\n\n".join(body) + "\n")
    manifest.append(dict(no=it["no"], title=it["title"], stage=it["stage"],
                         agent=name, agent_id=agent_id, file=path))

with open(os.path.join(ROOT, "desc", "manifest.json"), "w", encoding="utf-8", newline="\n") as f:
    json.dump(manifest, f, ensure_ascii=False, indent=1)

print("wrote %d descriptions" % len(manifest))
for m in manifest:
    print(m["no"], "stage", m["stage"], "|", m["agent"], "|", m["title"])
