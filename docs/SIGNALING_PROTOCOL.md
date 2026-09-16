# QuickMeet 信令协议 API 文档（QM-004）

信令服务：`crates/qm-signaling`（`src/server.rs` 传输层 + `src/ws.rs` 协议层 + `src/auth.rs` 鉴权）。

设计目标（Issue YEJ-93 / QM-004 的硬约束，逐条对应到实现）：

| 约束 | 实现位置 |
| --- | --- |
| 独立部署、与 SFU 解耦 | 独占 `media.signaling_ws_port`（默认 `8082`），不接触媒体端口 `8080`，不依赖 `qm-media` |
| 未携带有效 JWT 的连接直接拒绝 | `auth_callback` 在握手阶段返回 HTTP `401`，`101 Switching Protocols` 永不发出 |
| 信令通道强制 WSS | `run_server` 在 `auth.tls.enabled = false` 时直接拒绝启动；端口上只有 TLS 监听器 |
| 信令消息延迟 ≤ 50 ms | 单帧处理路径无锁外 I/O：一次 `dispatch` 只取一次 `parking_lot::Mutex`，转发是内存广播；见下文「延迟预算」 |
| 适配 webrtc-rs 异步时序，消除黑屏 | ICE candidate 暂存 + 重排（见「ICE 乱序」一节） |

---

## 1. 连接

```
wss://<host>:8082/?peer=<你的 peer 名>[&token=<JWT>][&gen=<重连代数>]
```

- **只接受 `wss://`。** 明文 `ws://` 客户端在 TCP 之后立即被拒（TLS 握手失败），拿不到任何会议信息。
- `peer` **必填**：房间内的显示标识。服务端在握手阶段就把它取出来做鉴权上下文，缺 `peer` 直接 `401`。
- `gen` 可选，默认 `0`。同一个 `peer` 重连时递增它，服务端据此丢弃旧连接滞留的帧（防止半死连接把过期 SDP 塞进新会话）。
- 帧大小上限 `media.signaling_max_frame_bytes`（默认 `1 MiB`），超限即关闭连接（关闭码 `1009`）。

### 1.1 Token 的两条通道

两条通道**等价**，服务端优先取 query（浏览器 `WebSocket` API 无法自定义握手头），再取 `Authorization`：

| 通道 | 写法 | 适用客户端 |
| --- | --- | --- |
| URL query | `wss://host:8082/?peer=alice&token=eyJhbG…` | 浏览器 `new WebSocket(url)` |
| HTTP 头 | `GET /?peer=alice HTTP/1.1` + `Authorization: Bearer eyJhbG…` | `curl` / 自写客户端 |

拒绝时的行为（验收标准 3 的可观测证据）：

```
$ curl -vk ws://host:8082/?peer=alice
* TCP connection refused            # 明文：端口上只有 TLS 监听器

$ curl -vk --http1.1 wss://host:8082/?peer=alice
< HTTP/1.1 401 Unauthorized         # 握手阶段被拒，没有 101
< connection: close
< content-type: text/plain
missing token
```

---

## 2. JWT

HS256 对称密钥，claims：

| claim | 必填 | 含义 |
| --- | --- | --- |
| `sub` | 是 | 稳定的参会者标识（作为房间身份的 `subject`）。配了 `auth.jwt_required_domain` 时做**域级准入**：`sub` 不含该后缀则拒绝 |
| `name` | 否 | 显示名（房间里的 `peer` 展示用）；缺失时回落为 `sub` |
| `exp` | 是（除非 `jwt_verify_exp = false`） | 过期时间。允许 `auth.jwt_clock_skew_secs`（默认 `60 s`）的时钟偏移 |
| `iss` | 否 | 签发方；配了 `auth.jwt_issuer` 才校验 |
| `aud` | 否 | 受众；配了 `auth.jwt_audience` 才校验 |

示例（本地联调，`--features dev-sign` 可签）：

```json
{
  "sub": "alice@corp.example",
  "name": "Alice",
  "iss": "quickmeet",
  "aud": "quickmeet-sfu",
  "exp": 1789670400
}
```

对应配置：

```toml
[auth]
enabled = true                # 生产必须 true
jwt_secret = "…"              # ≥ 32 字节；空 + enabled = true → 拒绝启动
jwt_algorithm = "HS256"       # 其他值拒绝启动
jwt_exp_secs = 86400
jwt_clock_skew_secs = 60
jwt_verify_exp = true
jwt_issuer = ""
jwt_audience = ""
jwt_required_domain = ""      # 例如 "corp.example"

[auth.tls]
enabled = true                # 必须 true，否则拒绝启动
cert_file = ""
key_file = ""
cert_pem = ""
key_pem = ""
self_signed_cn = "QuickMeet Signaling"   # 上面四组都留空时自动生成 90 天自签证书
self_signed_days = 90
bind_host = ""                # 留空用 network.bind_host
min_tls_version = "1.2"       # 只接受 1.2 / 1.3
```

### 2.1 SSO / OAuth 对接入口

`sub` 是唯一身份入口，替换签名方不需要动协议层：

1. IdP 侧把登录用户映射成 `sub`（建议 `<域>.<用户>` 或稳定 email）；
2. IdP 用共享密钥签 HS256，或改为 RS256；
3. `auth.jwt_required_domain` 配企业域后缀，`sub` 域级准入生效；
4. `iss` / `aud` 配 IdP 标识，完成签发方与受众校验。

---

## 3. 消息模型

**一条 JSON 文本帧 = 一个动作**。文本帧走协议；**二进制帧一律拒绝**并回 `BAD_REQUEST`（信令全部是 JSON，二进制帧说明客户端串了流）。

### 3.1 上行（客户端 → 服务端）

| `t` | 必填字段 | 可选字段 | 作用 |
| --- | --- | --- | --- |
| `join` | `room`, `peer` | `seq` | 加入房间；服务端回 `join` 应答，并向房间内其他成员广播 `peer_joined` |
| `offer` | `room`, `peer`, `to`, `sdp` | `seq` | 转发起一个 SDP offer |
| `answer` | `room`, `peer`, `to`, `sdp` | `seq` | 转发对端 SDP answer |
| `candidate`（别名 `ice`） | `room`, `peer`, `to`, `candidate` | `sdpMid`, `sdpMLineIndex`, `seq` | 转发一条 ICE candidate |
| `leave` | `room`, `peer` | `seq` | 主动离开；服务端广播 `peer_left`，空房间自动回收 |

### 3.2 下行（服务端 → 客户端）

| `type` | 触发 | 关键字段 |
| --- | --- | --- |
| `join` | 收到 `join` | `room`, `ok` |
| `sdp` | 收到 `offer` / `answer` | `to`, `from`, `sdp`, `kind` |
| `ice` | 收到 `candidate` | `to`, `from`, `candidate`, `sdpMid`, `sdpMLineIndex` |
| `peer_joined` | 有人 join 了你在的房间 | `peer`, `room` |
| `peer_left` | 有人 leave 或断连 | `peer`, `room` |
| —— | 错误 | `ok:false`, `code`, `error` |

所有字段 `camelCase`（`sdpMid` / `sdpMLineIndex`），与浏览器 `RTCPeerConnection` 的 onicecandidate 事件字段同名，客户端可以几乎零转换地转储。

---

## 4. 完整报文示例

### 4.1 两方会议：A ↔ B

```jsonc
// A 建立连接
wss://sfu:8082/?peer=client-a&token=<JWT>

// A 加入房间
→ {"t":"join","room":"qm-1024","peer":"client-a","seq":1}
← {"ok":true,"room":"qm-1024","type":"join"}

// A 发起 offer
→ {"t":"offer","room":"qm-1024","peer":"client-a","to":"client-b",
   "sdp":"v=0\r\no=- 4611739436285554058 2 IN IP4 127.0.0.1\r\n...","seq":2}
← {"ok":true,"room":"qm-1024","to":"client-b","from":"client-a","type":"offer","sdp":"...","kind":"offer"}

// B 回 answer
→ {"t":"answer","room":"qm-1024","peer":"client-b","to":"client-a",
   "sdp":"v=0\r\no=- 9161662970515242836 1 IN IP4 127.0.0.1\r\n...","seq":3}
← {"ok":true,"room":"qm-1024","to":"client-a","from":"client-b","type":"answer","sdp":"...","kind":"answer"}

// B 推 candidate（Chrome 默认开启 mDNS 遮蔽，服务端会自动还原）
→ {"t":"candidate","room":"qm-1024","peer":"client-b","to":"client-a",
   "candidate":"candidate:8174850770 1 udp 1677729535 7a9f.local 55207 typ host generation 0",
   "sdpMid":"0","sdpMLineIndex":0,"seq":4}
← {"ok":true,"room":"qm-1024","to":"client-a","from":"client-b","type":"ice",
   "candidate":"candidate:... 192.168.0.12 55207 typ host ...","sdpMid":"0","sdpMLineIndex":0}

// A 断连或 leave
→ {"t":"leave","room":"qm-1024","peer":"client-a","seq":5}
← {"ok":true,"room":"qm-1024","type":"leave"}
← {"ok":true,"room":"qm-1024","to":"client-b","type":"peer_left","peer":"client-a"}
```

### 4.2 多人房间的事件流

```jsonc
// 房间里已有 client-a / client-b，client-c join
← {"ok":true,"room":"qm-1024","type":"join"}            // 给 client-c
← {"ok":true,"room":"qm-1024","to":"client-a","type":"peer_joined","peer":"client-c"}
← {"ok":true,"room":"qm-1024","to":"client-b","type":"peer_joined","peer":"client-c"}
```

---

## 5. 错误码

| `code` | 触发条件 | 客户端动作 |
| --- | --- | --- |
| `BAD_REQUEST` | 帧不是 JSON；缺 `room` / `peer` / `to` / `sdp` / `candidate`；`sdp` 为空；candidate 格式非法；缺 `sdpMid` / `sdpMLineIndex`；未知 `t`；收到二进制帧 | 修数据后重发；不要重试 |
| `FORBIDDEN` | `peer` 或 `to` 不在内网网段（`network.cidrs`，默认 `192.168.0.0/24`） | 换地址；这是硬边界，重试无效 |
| `UNAUTHORIZED` | 已建连但还没 `join` 就发了 `offer`/`answer`/`candidate`/`leave`；帧里声明了一个不是房间成员的 `peer` | 先发 `join` |
| `ICE_PENDING` | 本端 answer 未就位（协商未完成），candidate 已暂存 | 继续，无需重试 —— candidate 会在收到 SDP 后自动下发 |
| `ROOM_FULL` | 房间人数达到 `media.signaling_max_per_room` | 换房间或联系主持人 |

> 重复 `join` 同一房间同一 peer 是**幂等**的：覆盖本地会话并把 `generation` 递增，其余成员只会收到一次 `peer_joined` 广播差异，不需要客户端做去重。

错误应答示例：

```json
{"ok":false,"type":"candidate","code":"BAD_REQUEST",
 "error":"ICE candidate 缺少 sdpMid 或 sdpMLineIndex，无法挂载到 m-line"}
```

握手阶段的拒绝不是消息，而是 HTTP 状态码：`401 Unauthorized`（无 token / token 无效 / 缺 `peer`），`101` 表示握手成功。

---

## 6. WebSocket 关闭码

| 关闭码 | 含义 | 由谁发出 |
| --- | --- | --- |
| `1000` | 正常关闭（`leave` 后、收到客户端 close） | 双方 |
| `1002` | 握手状态异常 / 协议错误 | 服务端 |
| `1003` | 连接数达到 `max_connections` 容量上限 | 服务端 |
| `1005` | 连接对端未发 close 帧即断开 | —— |
| `1008` | 读错误或读端异常（含对端崩溃） | 服务端 |
| `1009` | 单帧超过 `signaling_max_frame_bytes` | 服务端 |

心跳：服务端每 `20 s` 发一次 `PING`（小于浏览器默认 30 s），收到 `PING` 立即回 `PONG`。任一方向的发送都带 `15 s` 超时 —— 一个已死但还没探测出来的连接不会挂住广播出口。

---

## 7. ICE 乱序与首次连接黑屏

浏览器 `onicecandidate` 的到达顺序**不保证**先于 SDP 协商完成：本地 host candidate 常常在收集还在进行时就被发出。若此时直接丢弃，对方永远收不到这条 candidate，就会表现为**首次连接黑屏 / ICE 超时**（重试恰好赶在收集完成之后，所以"重连就好了"）。

处理是**确定性重排，不是重试**：

1. 本端 answer 未就位时，candidate 进入 `ice_pending` 暂存队列（上限 64，`ICE_PENDING`）；
2. 收到 SDP 时把暂存的 candidate 一并下发（`drain_pending_ice`）；
3. 缺 `sdpMid` / `sdpMLineIndex` 的 candidate 一律**拒绝**而不是猜 —— 无法挂载到 m-line 的 candidate 只会制造假失败；
4. **mDNS 还原**：Chrome 默认把 candidate 地址遮蔽成 `<uuid>.local`，服务端还原成字面 IP 再转发；不还原就连不通，这是黑屏的第二大来源。

客户端侧唯一要求：**不要**在 `onicecandidate` 里自己加队列或重试，把 candidate 原样发上来，服务端负责顺序。

---

## 8. 限制与延迟预算

| 项 | 默认值 | 配置 |
| --- | --- | --- |
| 信令端口 | `8082` | `media.signaling_ws_port` |
| 单帧字节上限 | `1 MiB` | `media.signaling_max_frame_bytes`（允许范围 `1 KiB` ~ `8 MiB`） |
| 单房间人数上限 | `100` | `media.signaling_max_per_room`（`0` = 不限，上限 `512`） |
| 心跳间隔 | `20 s` | 常量 |
| 单帧发送超时 | `15 s` | 常量 |
| 握手超时 | `15 s` | 常量 |
| ICE 就绪提示窗口 | `15 s` | `ICE_READY_TIMEOUT_SECS` |

延迟预算（≤ 50 ms，进程内单帧路径）：

- 帧读：`tokio` 异步读，无轮询；
- 解析 + 准入：一次 `serde_json` 反序列化 + 一次 `Ipv4Addr` 网段判定；
- 房间锁：`parking_lot::Mutex` 单次取放，房间表操作是 `HashMap` 查找（O(1)）；
- 转发：向目标 peer 的 `broadcast::Sender` 入队，容量 64，**无网络往返**；
- 全程无阻塞 I/O、无跨进程调用。

并发口径（验收标准 4）：连接注册表按 `(peer, generation)` 索引，断连即摘除；关闭路径统一走 `forget_peer` + `peer_left` 广播，不遗留半死连接；`max_connections` 超限时握手成功但立即以 `1003` 关闭。

---

## 9. 浏览器侧最小实现

```js
const ws = new WebSocket("wss://sfu:8082/?peer=" + PEER + "&token=" + JWT);
const pc = new RTCPeerConnection({ iceServers: [] });   // 纯内网，不用 STUN/TURN

ws.onmessage = (e) => {
  const m = JSON.parse(e.data);
  if (m.ok === false) { console.warn(m.code, m.error); return; }
  if (m.type === "sdp")   pc.setRemoteDescription({ type: m.kind, sdp: m.sdp });
  if (m.type === "ice")   pc.addIceCandidate({ candidate: m.candidate, sdpMid: m.sdpMid, sdpMLineIndex: m.sdpMLineIndex });
  if (m.type === "peer_joined") createTrackFor(m.peer);
  if (m.type === "peer_left")   dropTrack(m.peer);
};

ws.addEventListener("open", async () => {
  ws.send(JSON.stringify({ t: "join", room: ROOM, peer: PEER, seq: 1 }));
  if (IS_HOST) {
    await pc.setLocalDescription(await pc.createOffer());
    ws.send(JSON.stringify({ t: "offer", room: ROOM, peer: PEER, to: OTHER, sdp: pc.localDescription.sdp, seq: 2 }));
  }
});

pc.onicecandidate = (ev) => {
  if (!ev.candidate) return;
  ws.send(JSON.stringify({
    t: "candidate", room: ROOM, peer: PEER, to: OTHER,
    candidate: ev.candidate.candidate,
    sdpMid: ev.candidate.sdpMid, sdpMLineIndex: ev.candidate.sdpMLineIndex,
  }));
};

pc.onnegotiationneeded = async () => { /* 同上发 offer/answer */ };
```

---

## 10. 部署

```bash
# 只起 WSS 信令（与 SFU 解耦，独立进程 / 容器）
QM_NETWORK_BIND_HOST=127.0.0.1 \
QM_AUTH_JWT_SECRET="$(openssl rand -base64 48)" \
  cargo run --release -- -p qm-demo -- --wss

# 兼容旧的 REST 信令面（8081），两个面互不影响
cargo run --release -- -p qm-demo -- --signal
```

私有化交付时必须替换自签证书（`auth.tls.cert_file` / `key_file`），否则浏览器会提示证书不受信任。

## 11. 健康检查

`media.signaling_port`（REST 面）提供 `/healthz`；WSS 面没有独立的 HTTP 探针，用 `wss://host:8082/?peer=probe&token=<JWT>` 能否拿到 `101` 作为存活判据（无效 token 返回 `401` 即说明服务在监听）。
