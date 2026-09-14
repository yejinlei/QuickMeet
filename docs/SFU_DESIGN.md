# SFU 接口设计文档（YEJ-91）

## 转发拓扑

QuickMeet 采用 SFU（Selective Forwarding Unit）架构，与 MCU 混流模式对比：

| 维度 | SFU（QuickMeet） | MCU |
|------|------------------|-----|
| 转发方式 | 按订阅者需求直接转发原始 RTP | 解码 -> 混流 -> 重编码 |
| CPU 开销 | 低（不解码不重编码） | 高 |
| 带宽 | O(发布者 x 订阅者) | O(订阅者) |
| 媒体质量 | 原始流无损 | 混流后降质 |
| 适配场景 | 私有化部署、内网低延迟 | 大量订阅者、带宽受限 |

### 转发路径

```
发布者 Alice --> [轨道 audio-1] --+
                                  +--> SFU 路由核心 --> 按订阅表选择性转发
发布者 Alice --> [轨道 video-1] --+
                                      |
                          +-----------+-----------+
                          |           |           |
                      订阅者 Bob   订阅者 Carol  (未订阅->跳过)
                      (audio-1)   (video-1)
```

核心原则：一个 RTP 包只转发给真正订阅了该轨道的 peer，不发给未订阅者，
不进行全量混流。

## 轨道管理模型

### 轨道生命周期

```
[创建 publish] -> [Live] -- subscribe/unsubscribe（可多次）--> [Live]
                         |
                         +-- publisher left / end --> [Ended] -> 清理
```

- 创建：peer 调用 publish，生成 UUID track_id，状态为 Live
- 订阅：其他 peer 调用 subscribe，加入该轨道的 subscribers 集合
- 退订：peer 调用 unsubscribe，从 subscribers 移除
- 结束：发布者离开时调用 peer_left，该 peer 发布的轨道标记为 Ended
- 清理：已结束且无订阅者的轨道被自动移除

### 独立性保证（验收标准 2）

- 每条轨道有独立的 subscribers 集合，互不影响
- 退订一条轨道不影响其他轨道的订阅关系
- 订阅者离开只退订其参与的所有轨道，不结束其他 peer 的轨道
- 发布者离开只结束其发布的轨道，不影响其他发布者的轨道

### 发布者/接收者映射

```
TrackRegistry
  +-- rooms: HashMap<room_id, HashMap<TrackId, Track>>
       Track {
         id, room_id, publisher, kind, state,
         subscribers: HashSet<peer_id>,
         packets_forwarded: u64,
       }
```

## ICE/STUN/TURN 穿透模型

### NAT 类型与 ICE server 选择

| NAT 类型 | STUN 打洞 | TURN 中继 | 选择策略 |
|----------|-----------|-----------|----------|
| None（同网段） | 不需要 | 不使用 | 空（host candidate 直连） |
| Cone（锥形） | 成功 | 备用 | STUN + TURN |
| Symmetric（对称） | 失败 | 必须 | STUN + TURN |

### 私有化约束

- STUN/TURN 服务器地址必须在配置的内网网段内（192.168.0.0/24）
- 不向公网 STUN/TURN 暴露地址
- 默认配置：STUN 和 TURN 均部署在 192.168.0.5:3478

## SFU 路由接口（全部 JSON）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /healthz | 存活探针 |
| GET | /room/{id}/tracks | 房间内轨道列表 |
| POST | /room/{id}/publish | 发布轨道（返回 track_id） |
| POST | /room/{id}/subscribe | 订阅轨道 |
| POST | /room/{id}/unsubscribe | 退订轨道 |
| POST | /room/{id}/forward | 模拟转发一批包（返回决策+统计） |
| GET | /room/{id}/ice | ICE server 配置 + NAT 模拟 |
| POST | /room/{id}/leave | peer 离开（结束其轨道+退订其订阅） |

## 模块结构

```
crates/qm-sfu/
  src/
    lib.rs          -- 模块入口与公共导出
    track.rs        -- 音视频轨道解耦管理（TrackRegistry）
    forwarding.rs   -- 选择性转发逻辑（decide / simulate_forward_batch）
    ice.rs          -- STUN/TURN 配置与 NAT 模拟
    router.rs       -- SFU 路由核心（纯函数分发，HTTP 无关）
```

设计原则与 qm-signaling 一致：核心路由逻辑是纯 Rust 函数，不依赖 webrtc-rs
或任何 HTTP 库，可在 cargo test 离线逐条断言转发路径、订阅隔离、NAT 模拟。
