//! # qm-signaling::rooms — 会议房间生命周期与参会者权限管控（QM-005）
//!
//! 本模块是房间层的**唯一状态源**，刻意不依赖任何 HTTP crate：[`RoomManager`]
//! 是纯状态机（`Arc<Mutex<State>>`，`Clone` 便宜），`cargo test --workspace` 可以
//! 离线逐条断言每条规则；HTTP 适配是薄壳（见 [`crate::route`]），只负责解析请求体、
//! 校验内网准入、把 [`RoomError`] 映射成状态码。
//!
//! ## 需求点落点
//! * **创建 / 销毁** —— [`RoomManager::create_room`] / [`RoomManager::destroy_room`]，
//!   支持会议密码与参会人数上限；房间 ID 全局唯一（重复创建报 [`RoomError::Duplicate`]）。
//! * **等候室** —— 密码错误 / 房间满员 → [`PeerState::Waiting`]；主持人
//!   [`RoomManager::approve_peer`] 批准或 [`RoomManager::deny_peer`] 拒绝。
//! * **主持人权限** —— [`RoomManager::mute_peer`] / [`RoomManager::mute_all`] /
//!   [`RoomManager::kick_peer`] / [`RoomManager::set_role`]。权限按层级收紧：
//!   联席主持人只能管控普通参会者，**只有主持人**能授予 / 撤销联席主持人（防权限自我膨胀），
//!   任何人都改不了主持人。非成员一律 [`RoomError::Forbidden`]。
//! * **状态实时同步** —— 每次操作返回 [`OpView`]：`events` 是本操作产生的强顺序事件流
//!   （锁内分配单调 `seq`），`peers` 是操作完成后的完整快照（麦克风 / 摄像头 / 屏幕共享 /
//!   被主持人静音 + 生效态 `mic_effective`）。所有端按 `seq` 排序即可得到完全一致的状态；
//!   状态在进程内存里读写，操作到快照返回是微秒级，[`sync_roundtrip_is_fast`] 把它断言成 ≤100ms。
//! * **资源自动回收** —— 所有人离开后 [`RoomConfig::grace_secs`]（默认 300s）宽限期内房间**保留**
//!   （支持短时间重连），到期由 [`RoomManager::reap_expired_at`] / [`RoomManager::tick_at`]
//!   整体释放（房间对象 + 成员表 + 事件环形队列一起丢弃）。[`RoomManager::memory_facts`]
//!   可断言房间 / 参会者 / 事件 / 预约计数全部归零（无内存泄漏）。
//! * **会议预约与日程**（原 YEJ-111 并入）—— [`RoomManager::create_appt`] /
//!   [`RoomManager::update_appt`] / [`RoomManager::cancel_appt`] / [`RoomManager::list_appts`]，
//!   含会议室号复用与冲突规则（同会议室号或同主持人时间重叠即冲突）、定时自动建房 / 销毁
//!   （与 5 分钟宽限回收对齐）、可配置会前提醒 + 触发去重、受邀名单（昵称水印身份字段，对接 QM-016）。
//!
//! ## 错误与 HTTP 状态映射（[`RoomError::status_code`]）
//!
//! | [`RoomError`] | HTTP |
//! |---|---|
//! | `NotFound` | 404 |
//! | `Duplicate` / `Conflict` | 409 |
//! | `Unauthenticated` / `WrongPassword` / `NotInRoom` / `NotParticipant` / `Forbidden` | 403 |
//! | `Invalid` | 400 |
//!
//! ## 实现约定：变更与事件发射分块
//!
//! `MutexGuard` 解引用后的字段级不相交借用，在「`s.rooms.get_mut()` 拿到的 `&mut Room`
//! 还活着时再借 `s.pending`」这个组合上并不成立（E0499）。所以统一走 [`St`]：
//! 持锁后立刻把 `State` 的字段解构成真实的不相交引用；所有变更放在
//! `{ let r = st.rooms.get_mut(id); ... }` 的内层块里完成，**块结束后**才调
//! [`St::emit`]。此时 `r` 已经消亡，`emit` 再借 `st.rooms` 就没有冲突。

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

// ── 参会者 ────────────────────────────────────────────────────────

/// 参会者角色。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Role {
    /// 房间创建者。
    Host,
    /// 联席主持人：可管控普通参会者，不能授权 / 撤销联席主持人、不能移除主持人。
    CoHost,
    /// 普通参会者。
    Participant,
}

impl Role {
    /// 权限层级（越大越高）。
    pub fn rank(self) -> u8 {
        match self {
            Role::Host => 2,
            Role::CoHost => 1,
            Role::Participant => 0,
        }
    }

    /// 能否执行常规管控（静音 / 移除 / 批准 / 拒绝 / 销毁）。
    pub fn can_control(self) -> bool {
        self != Role::Participant
    }

    /// 能否变更他人角色（只有主持人，避免权限自我膨胀）。
    pub fn can_manage_roles(self) -> bool {
        matches!(self, Role::Host)
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Role::Host => "host",
            Role::CoHost => "cohost",
            Role::Participant => "participant",
        })
    }
}

/// 参会者状态：已入会 / 在等候室。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerState {
    Admitted,
    Waiting,
}

impl PeerState {
    pub fn as_str(self) -> &'static str {
        match self {
            PeerState::Admitted => "admitted",
            PeerState::Waiting => "waiting",
        }
    }
}

impl std::fmt::Display for PeerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 参会者（内部表示；`display_name` 是昵称水印身份字段，对接 QM-016）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    pub id: String,
    /// 来源地址 `ip:port`（准入校验由 HTTP 层做，这里只存已验证的值）。
    pub address: String,
    pub display_name: String,
    pub role: Role,
    pub mic_on: bool,
    pub cam_on: bool,
    pub screen_share: bool,
    /// 被主持人强制静音（优先级高于 `mic_on`）。
    pub muted_by_host: bool,
    pub state: PeerState,
}

impl Peer {
    fn new(id: String, address: String, display_name: String, role: Role, state: PeerState) -> Self {
        Self {
            id,
            address,
            display_name,
            role,
            mic_on: false,
            cam_on: false,
            screen_share: false,
            muted_by_host: false,
            state,
        }
    }

    /// 实际生效的麦克风状态：被主持人静音时本人开关不起作用。
    ///
    /// 快照里同时给 `mic_on`（本人开关）与 `mic_effective`（生效态），
    /// 所有端只按 `mic_effective` 渲染，状态才能完全一致。
    pub fn mic_effective(&self) -> bool {
        self.mic_on && !self.muted_by_host
    }
}

/// 参会者对外快照。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerView {
    pub id: String,
    pub address: String,
    pub display_name: String,
    pub role: Role,
    pub mic_on: bool,
    pub mic_effective: bool,
    pub cam_on: bool,
    pub screen_share: bool,
    pub muted_by_host: bool,
    pub state: PeerState,
}

impl From<&Peer> for PeerView {
    fn from(p: &Peer) -> Self {
        Self {
            id: p.id.clone(),
            address: p.address.clone(),
            display_name: p.display_name.clone(),
            role: p.role,
            mic_on: p.mic_on,
            mic_effective: p.mic_effective(),
            cam_on: p.cam_on,
            screen_share: p.screen_share,
            muted_by_host: p.muted_by_host,
            state: p.state,
        }
    }
}

// ── 事件 ─────────────────────────────────────────────────────────

/// 房间事件类型（状态同步的最小事件集）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    Created,
    Joined,
    Waiting,
    Admitted,
    Declined,
    Left,
    Muted,
    Unmuted,
    RoleChanged,
    MediaChanged,
    Destroyed,
}

impl EventKind {
    fn as_str(self) -> &'static str {
        match self {
            EventKind::Created => "created",
            EventKind::Joined => "joined",
            EventKind::Waiting => "waiting",
            EventKind::Admitted => "admitted",
            EventKind::Declined => "declined",
            EventKind::Left => "left",
            EventKind::Muted => "muted",
            EventKind::Unmuted => "unmuted",
            EventKind::RoleChanged => "role_changed",
            EventKind::MediaChanged => "media_changed",
            EventKind::Destroyed => "destroyed",
        }
    }
}

fn ser_kind<S: serde::Serializer>(k: &EventKind, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(k.as_str())
}

/// 一条状态同步事件。客户端按 `seq` 排序即可得到完全一致的状态。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomEvent {
    /// Unix 秒。
    pub ts_unix: i64,
    /// 进程内全局单调序号（跨房间递增，锁内分配，不会撞号）。
    pub seq: u64,
    /// 事件类型（JSON 序列化为小写字符串）。
    #[serde(serialize_with = "ser_kind")]
    pub kind: EventKind,
    /// 事件作用对象（房间级事件为空串）。
    pub peer_id: String,
    /// 触发者（系统事件为 `"system"`）。
    pub by: String,
    /// 人可读详情（如 `role=cohost`、`reason=password_wrong`）。
    pub detail: String,
}

// ── 预约 ─────────────────────────────────────────────────────────

/// 预约状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApptStatus {
    Scheduled,
    Cancelled,
}

impl ApptStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ApptStatus::Scheduled => "scheduled",
            ApptStatus::Cancelled => "cancelled",
        }
    }
}

impl std::fmt::Display for ApptStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 受邀人（昵称水印身份字段对接 QM-016）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Invitee {
    /// 受邀人昵称（入会后作为水印身份）。
    pub display_name: String,
    /// 联系方式（邮箱 / 手机号），仅本地存储，不出域。
    pub contact: String,
}

/// 会议预约。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Appointment {
    pub id: String,
    pub title: String,
    /// 会议室号（可复用：不同时间段同一会议室号允许多场预约）。
    pub room_id: String,
    pub host_id: String,
    pub host_name: String,
    /// 开始时间（Unix 秒）。
    pub starts_at: i64,
    /// 时长（分钟）。
    pub duration_mins: i64,
    pub password: Option<String>,
    pub max_participants: Option<usize>,
    pub invitees: Vec<Invitee>,
    /// 会前提醒提前量（分钟，0 = 不提醒）。
    pub reminder_mins: i64,
    pub status: ApptStatus,
    /// 取消原因（`status == Cancelled` 时有效）。
    pub cancel_reason: String,
    /// 房间是否已由本次预约自动创建过（幂等标记）。
    pub room_created: bool,
    /// 提醒是否已触发过（触发去重标记）。
    pub reminder_fired: bool,
}

impl Appointment {
    /// 结束时间（Unix 秒）。
    pub fn ends_at(&self) -> i64 {
        self.starts_at + self.duration_mins * 60
    }

    fn view(&self, room_exists: bool) -> ApptView {
        ApptView {
            id: self.id.clone(),
            title: self.title.clone(),
            room_id: self.room_id.clone(),
            host_id: self.host_id.clone(),
            host_name: self.host_name.clone(),
            starts_at: self.starts_at,
            duration_mins: self.duration_mins,
            ends_at: self.ends_at(),
            password_set: self.password.is_some(),
            max_participants: self.max_participants,
            invitees: self.invitees.clone(),
            reminder_mins: self.reminder_mins,
            reminder_fired: self.reminder_fired,
            status: self.status,
            cancel_reason: self.cancel_reason.clone(),
            room_exists,
        }
    }
}

/// 预约对外快照。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApptView {
    pub id: String,
    pub title: String,
    pub room_id: String,
    pub host_id: String,
    pub host_name: String,
    pub starts_at: i64,
    pub duration_mins: i64,
    pub ends_at: i64,
    /// 是否设置了密码（不返回明文，避免泄漏）。
    pub password_set: bool,
    pub max_participants: Option<usize>,
    pub invitees: Vec<Invitee>,
    pub reminder_mins: i64,
    /// 提醒是否已触发过。
    pub reminder_fired: bool,
    pub status: ApptStatus,
    pub cancel_reason: String,
    /// 对应房间当前是否已存在。
    pub room_exists: bool,
}

// ── 房间 ─────────────────────────────────────────────────────────

/// 房间（不存媒体，只存成员名单 / 事件 / 配置）。
#[derive(Debug)]
pub struct Room {
    pub id: String,
    pub password: Option<String>,
    /// 参会人数上限（仅统计已入会 peer；`None` = 不限制）。
    pub max_participants: Option<usize>,
    pub created_at_unix: i64,
    /// 最后一次活动时刻（有人离开也会刷新，宽限期从此刻起算）。
    pub last_activity_unix: i64,
    /// 预约自动创建的房间才有（到期自动销毁）。
    pub ends_at_unix: Option<i64>,
    peers: Vec<Peer>,
    /// 事件环形队列（上限 `room.max_events`）。
    events: VecDeque<RoomEvent>,
}

impl Room {
    fn new(id: String, now_unix: i64) -> Self {
        Self {
            id,
            password: None,
            max_participants: None,
            created_at_unix: now_unix,
            last_activity_unix: now_unix,
            ends_at_unix: None,
            peers: Vec::new(),
            events: VecDeque::new(),
        }
    }

    fn push_event(&mut self, ev: RoomEvent, cap: usize) {
        self.events.push_back(ev);
        while self.events.len() > cap {
            self.events.pop_front();
        }
    }

    /// 按 peer id 查找（不限状态）。
    fn find(&self, peer_id: &str) -> Option<&Peer> {
        self.peers.iter().find(|p| p.id == peer_id)
    }

    fn find_mut(&mut self, peer_id: &str) -> Option<&mut Peer> {
        self.peers.iter_mut().find(|p| p.id == peer_id)
    }

    /// 已入会人数（等候室不计入容量）。
    pub fn admitted_count(&self) -> usize {
        self.peers.iter().filter(|p| p.state == PeerState::Admitted).count()
    }

    pub fn waiting_count(&self) -> usize {
        self.peers.iter().filter(|p| p.state == PeerState::Waiting).count()
    }

    /// 是否已满员（`max_participants == None` 表示不限制）。
    pub fn is_full(&self) -> bool {
        self.max_participants.is_some_and(|n| self.admitted_count() >= n)
    }

    pub fn view(&self) -> RoomView {
        let admitted = self.admitted_count();
        RoomView {
            id: self.id.clone(),
            password_set: self.password.is_some(),
            max_participants: self.max_participants,
            created_at_unix: self.created_at_unix,
            last_activity_unix: self.last_activity_unix,
            ends_at_unix: self.ends_at_unix,
            admitted_count: admitted,
            waiting_count: self.waiting_count(),
            reclaim_started: admitted == 0,
        }
    }
}

/// 房间对外快照。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomView {
    pub id: String,
    /// 是否设置了密码（不返回明文密码，避免泄漏）。
    pub password_set: bool,
    pub max_participants: Option<usize>,
    pub created_at_unix: i64,
    pub last_activity_unix: i64,
    pub ends_at_unix: Option<i64>,
    pub admitted_count: usize,
    pub waiting_count: usize,
    /// 宽限期是否已开始（已空房）。
    pub reclaim_started: bool,
}

// ── 错误 ─────────────────────────────────────────────────────────

/// 房间层错误。HTTP 层按 [`RoomError::status_code`] 映射状态码。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomError {
    /// 房间 / 参会者 / 预约不存在。
    NotFound(String),
    /// 房间号重复（房间 ID 全局唯一）。
    Duplicate(String),
    /// 预约冲突（同会议室号或同主持人时间重叠）。
    Conflict(String),
    /// 未提供会议密码。
    Unauthenticated,
    /// 密码错误。
    WrongPassword,
    /// 参会者不在房间内（含已离开、从未加入）。
    NotInRoom,
    /// 目标参会者不存在或状态不允许该操作。
    NotParticipant,
    /// 权限不足（非主持人 / 联席主持人，或试图管控同级及以上）。
    Forbidden,
    /// 参数非法（房间号格式、名字长度、时间范围等）。
    Invalid(String),
}

impl std::fmt::Display for RoomError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            RoomError::NotFound(x) => format!("不存在：{x}"),
            RoomError::Duplicate(x) => format!("已存在（唯一性冲突）：{x}"),
            RoomError::Conflict(c) => format!("冲突：{c}"),
            RoomError::Unauthenticated => "房间设置了会议密码，请先提供密码".to_string(),
            RoomError::WrongPassword => "会议密码错误".to_string(),
            RoomError::NotInRoom => "该参会者不在房间内".to_string(),
            RoomError::NotParticipant => "目标参会者不存在或状态不允许".to_string(),
            RoomError::Forbidden => "权限不足：只有主持人或联席主持人可以执行该操作".to_string(),
            RoomError::Invalid(v) => format!("参数非法：{v}"),
        };
        f.write_str(&s)
    }
}

impl RoomError {
    /// HTTP 状态码映射。
    pub fn status_code(&self) -> u16 {
        match self {
            RoomError::NotFound(_) => 404,
            RoomError::Duplicate(_) | RoomError::Conflict(_) => 409,
            RoomError::Unauthenticated
            | RoomError::WrongPassword
            | RoomError::NotInRoom
            | RoomError::NotParticipant
            | RoomError::Forbidden => 403,
            RoomError::Invalid(_) => 400,
        }
    }

    /// 映射到统一错误模型（日志 / 指标用）。
    pub fn to_qm_error(&self) -> qm_common::error::Error {
        qm_common::error::Error::signaling(self.to_string())
    }
}

pub type RResult<T> = std::result::Result<T, RoomError>;

// ── 请求 / 响应 ──────────────────────────────────────────────────

/// 房间操作结果：事件流 + 状态快照，一次响应即可让所有端对齐。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpView {
    pub room_id: String,
    /// 操作者（系统操作为 `"system"`）。
    pub peer: String,
    /// 操作者本人状态（房间级操作为 `None`）。
    pub state: Option<PeerState>,
    /// 人可读结果说明。
    pub note: String,
    /// 本次操作产生的事件（按 `seq` 升序）。
    pub events: Vec<RoomEvent>,
    /// 操作完成后的参会者快照（已入会在前，等候室在后）。
    pub peers: Vec<PeerView>,
    /// 操作完成后的房间快照（房间已销毁时为 `None`）。
    pub room: Option<RoomView>,
}

/// 操作者引用（HTTP 层负责装配，核心层不解析 JSON）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerRef {
    /// peer 标识；为空时由管理器分配 uuid。
    pub id: String,
    /// 来源地址（已通过内网准入校验）。
    pub address: String,
    /// 会议密码。
    pub password: Option<String>,
    /// 昵称（水印身份字段）；为空时用 peer id。
    pub display_name: Option<String>,
}

/// 创建房间参数。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateRoom {
    pub room_id: String,
    pub password: Option<String>,
    /// 参会人数上限（`None` 用配置默认值；`Some(0)` = 不限制）。
    pub max_participants: Option<usize>,
    /// 创建者（成为主持人并直接入会）。
    pub host: Option<PeerRef>,
}

/// 预约创建参数。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApptReq {
    pub title: String,
    pub room_id: String,
    pub host_id: String,
    pub host_name: String,
    pub starts_at: i64,
    pub duration_mins: i64,
    pub password: Option<String>,
    pub max_participants: Option<usize>,
    pub invitees: Vec<Invitee>,
    pub reminder_mins: i64,
}

/// 预约修改参数（字段为空 / 0 表示保持原值；密码传空串表示清除密码）。
#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApptPatch {
    pub title: String,
    pub room_id: String,
    pub host_id: String,
    pub host_name: String,
    pub starts_at: Option<i64>,
    pub duration_mins: Option<i64>,
    pub password: Option<String>,
    pub max_participants: Option<usize>,
    pub invitees: Vec<Invitee>,
    pub reminder_mins: Option<i64>,
}

/// 一条会前提醒。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reminder {
    pub appt_id: String,
    pub room_id: String,
    pub title: String,
    /// 提醒目标（主持人 + 受邀人联系方式，已去重）。
    pub targets: Vec<String>,
    pub lead_mins: i64,
    pub starts_at: i64,
}

/// 一次调度循环的结果。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickReport {
    pub rooms_created: Vec<String>,
    pub rooms_destroyed: Vec<String>,
    pub rooms_reclaimed: Vec<String>,
    pub reminders: Vec<Reminder>,
}

/// 资源占用事实（无内存泄漏的断言面）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryFacts {
    pub rooms: usize,
    pub peers: usize,
    pub events: usize,
    pub appointments: usize,
}

// ── 状态与时钟 ───────────────────────────────────────────────────

/// 时钟抽象：固定时钟让 5 分钟宽限回收与预约调度可以确定性测试。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Clock {
    /// 墙钟（Unix 秒）。
    Wall,
    /// 固定时刻。
    Fixed(i64),
}

impl Clock {
    fn unix(self) -> i64 {
        match self {
            Clock::Wall => now_unix(),
            Clock::Fixed(t) => t,
        }
    }
}

/// 房间层内存状态。
#[derive(Debug)]
struct State {
    rooms: HashMap<String, Room>,
    appts: HashMap<String, Appointment>,
    seq: u64,
    appt_seq: u64,
    /// 待下发事件（每个响应取出并清空，客户端据此追平状态）。
    pending: VecDeque<RoomEvent>,
    clock: Clock,
}

impl Default for State {
    fn default() -> Self {
        Self {
            rooms: HashMap::new(),
            appts: HashMap::new(),
            seq: 0,
            appt_seq: 0,
            pending: VecDeque::new(),
            clock: Clock::Wall,
        }
    }
}

impl State {
    /// 参会者快照排序：已入会在前，等候室在后；各组内按 id 排序（稳定可回放）。
    fn peers_of(r: &Room) -> Vec<PeerView> {
        let mut v: Vec<PeerView> = r.peers.iter().map(PeerView::from).collect();
        v.sort_by(|a, b| a.state.as_str().cmp(b.state.as_str()).then(a.id.cmp(&b.id)));
        v
    }
}

/// 持锁后的字段级视图：把 `State` 解构成真实的不相交引用，
/// 这样 `st.rooms.get_mut()` 与 `st.pending` / `st.seq` 可以同时存在。
struct St<'a> {
    rooms: &'a mut HashMap<String, Room>,
    appts: &'a mut HashMap<String, Appointment>,
    seq: &'a mut u64,
    pending: &'a mut VecDeque<RoomEvent>,
    clock: &'a Clock,
}

impl<'a> St<'a> {
    fn new(s: &'a mut State) -> Self {
        St {
            rooms: &mut s.rooms,
            appts: &mut s.appts,
            seq: &mut s.seq,
            pending: &mut s.pending,
            clock: &s.clock,
        }
    }

    fn now(&mut self) -> i64 {
        self.clock.unix()
    }

    /// 产生一条状态同步事件（房间级事件 room_id 为 None）。
    fn emit(
        &mut self,
        room_id: Option<&str>,
        kind: EventKind,
        peer_id: String,
        by: String,
        detail: String,
        cap: usize,
    ) {
        *self.seq += 1;
        let ev = RoomEvent {
            ts_unix: self.now(),
            seq: *self.seq,
            kind,
            peer_id,
            by,
            detail,
        };
        if let Some(id) = room_id {
            if let Some(r) = self.rooms.get_mut(id) {
                r.push_event(ev.clone(), cap);
            }
        }
        self.pending.push_back(ev);
    }
}

// ── 管理器 ───────────────────────────────────────────────────────

/// 房间 / 预约管理器。状态在 `Arc<Mutex<_>>` 里，`Clone` 是廉价的。
#[derive(Clone)]
pub struct RoomManager {
    cfg: Arc<qm_common::AppConfig>,
    st: Arc<Mutex<State>>,
}

impl RoomManager {
    /// 用墙钟构造。
    pub fn new(cfg: Arc<qm_common::AppConfig>) -> Self {
        Self {
            cfg,
            st: Arc::new(Mutex::new(State::default())),
        }
    }

    /// 用固定时钟构造（测试用）。
    pub fn with_fixed_time(cfg: Arc<qm_common::AppConfig>, t: i64) -> Self {
        let mut s = State::default();
        s.clock = Clock::Fixed(t);
        Self {
            cfg,
            st: Arc::new(Mutex::new(s)),
        }
    }

    /// 推进固定时钟（测试用）。
    pub fn set_fixed_time(&self, t: i64) {
        self.st.lock().clock = Clock::Fixed(t);
    }

    pub fn grace_secs(&self) -> i64 {
        self.cfg.room.grace_secs as i64
    }

    fn max_events(&self) -> usize {
        self.cfg.room.max_events.max(1)
    }

    /// 解析参会人数上限：`None` → 配置默认值；`Some(0)` → 不限制。
    fn cap_of(&self, cap: Option<usize>) -> Option<usize> {
        let n = cap.unwrap_or(self.cfg.room.default_max_participants);
        if n == 0 {
            None
        } else {
            Some(n)
        }
    }

    // ── 查询 ──────────────────────────────────────────────────────

    /// 全部房间快照（按房间号排序，稳定可回放）。
    pub fn rooms(&self) -> Vec<RoomView> {
        let s = self.st.lock();
        let mut v: Vec<RoomView> = s.rooms.values().map(Room::view).collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        v
    }

    pub fn room_view(&self, room: &str) -> RResult<RoomView> {
        let s = self.st.lock();
        s.rooms
            .get(room)
            .map(Room::view)
            .ok_or_else(|| RoomError::NotFound(room.to_string()))
    }

    /// 参会者快照（已入会在前，等候室在后；各组内按 id 排序）。
    pub fn peers_of(&self, room: &str) -> RResult<Vec<PeerView>> {
        let s = self.st.lock();
        let r = s
            .rooms
            .get(room)
            .ok_or_else(|| RoomError::NotFound(room.to_string()))?;
        Ok(State::peers_of(r))
    }

    /// 等候室名单。
    pub fn waitlist_of(&self, room: &str) -> RResult<Vec<PeerView>> {
        let s = self.st.lock();
        let r = s
            .rooms
            .get(room)
            .ok_or_else(|| RoomError::NotFound(room.to_string()))?;
        Ok(State::peers_of(r)
            .into_iter()
            .filter(|p| p.state == PeerState::Waiting)
            .collect())
    }

    /// 房间事件日志（按 `seq` 升序）。
    pub fn events_of(&self, room: &str) -> RResult<Vec<RoomEvent>> {
        let s = self.st.lock();
        let r = s
            .rooms
            .get(room)
            .ok_or_else(|| RoomError::NotFound(room.to_string()))?;
        let mut v: Vec<RoomEvent> = r.events.clone().into();
        v.sort_by_key(|e| e.seq);
        Ok(v)
    }

    /// 取出全部待下发事件（取出即清空）。
    pub fn take_pending_events(&self) -> Vec<RoomEvent> {
        let mut s = self.st.lock();
        s.pending.drain(..).collect()
    }

    /// 按时间范围查预约（`starts_at >= from` 且 `ends_at <= to`）。
    pub fn list_appts(&self, from: Option<i64>, to: Option<i64>) -> Vec<ApptView> {
        let s = self.st.lock();
        let mut v: Vec<ApptView> = s
            .appts
            .values()
            .filter(|a| {
                (from.is_none() || a.starts_at >= from.unwrap_or(i64::MIN))
                    && (to.is_none() || a.ends_at() <= to.unwrap_or(i64::MAX))
            })
            .map(|a| a.view(s.rooms.contains_key(&a.room_id)))
            .collect();
        v.sort_by(|a, b| a.starts_at.cmp(&b.starts_at).then(a.id.cmp(&b.id)));
        v
    }

    pub fn appt(&self, id: &str) -> RResult<ApptView> {
        let s = self.st.lock();
        s.appts
            .get(id)
            .map(|a| a.view(s.rooms.contains_key(&a.room_id)))
            .ok_or_else(|| RoomError::NotFound(id.to_string()))
    }

    /// 某会议室号的全部预约。
    pub fn appts_of_room(&self, room_id: &str) -> Vec<ApptView> {
        let s = self.st.lock();
        let mut v: Vec<ApptView> = s
            .appts
            .values()
            .filter(|a| a.room_id == room_id)
            .map(|a| a.view(s.rooms.contains_key(&a.room_id)))
            .collect();
        v.sort_by(|a, b| a.starts_at.cmp(&b.starts_at).then(a.id.cmp(&b.id)));
        v
    }

    /// 资源占用事实（无内存泄漏断言用）。
    pub fn memory_facts(&self) -> MemoryFacts {
        let s = self.st.lock();
        MemoryFacts {
            rooms: s.rooms.len(),
            peers: s.rooms.values().map(|r| r.peers.len()).sum(),
            events: s.rooms.values().map(|r| r.events.len()).sum(),
            appointments: s.appts.len(),
        }
    }

    /// 宽限期剩余截止时刻（真实 `Instant`，可直接与 `Instant::now()` 比较）。
    ///
    /// 仍有已入会成员时返回 `None`（不进入回收流程）。
    pub fn grace_until(&self, room: &str) -> RResult<Option<Instant>> {
        let s = self.st.lock();
        let r = s
            .rooms
            .get(room)
            .ok_or_else(|| RoomError::NotFound(room.to_string()))?;
        if r.admitted_count() > 0 {
            return Ok(None);
        }
        let deadline_unix = r.last_activity_unix + self.cfg.room.grace_secs as i64;
        let now = now_unix();
        if deadline_unix <= now {
            return Ok(None);
        }
        Ok(Some(Instant::now() + Duration::from_secs((deadline_unix - now) as u64)))
    }

    // ── 房间生命周期 ──────────────────────────────────────────────

    /// 创建房间：房间 ID 全局唯一，创建者成为主持人并直接入会。
    pub fn create_room(&self, req: &CreateRoom) -> RResult<OpView> {
        let id = validate_room_id(&req.room_id)?;
        validate_password(req.password.as_deref())?;
        if let Some(c) = req.max_participants {
            validate_cap(c)?;
        }
        let host_id = req.host.as_ref().map(peer_id_of);
        if let Some(ref h) = req.host {
            validate_name(&display_name_of(h))?;
        }
        let host_obj = req.host.as_ref().map(peer_ref_to_peer).transpose()?;

        let max_events = self.max_events();
        {
            let mut g = self.st.lock();
            let mut st = St::new(&mut *g);
            if st.rooms.contains_key(&id) {
                return Err(RoomError::Duplicate(id));
            }
            let mut room = Room::new(id.clone(), st.now());
            room.password = req.password.clone();
            room.max_participants = self.cap_of(req.max_participants);
            if let Some(ref h) = host_obj {
                room.peers.push(Peer::new(
                    h.id.clone(),
                    h.address.clone(),
                    h.display_name.clone(),
                    Role::Host,
                    PeerState::Admitted,
                ));
            }
            st.rooms.insert(id.clone(), room);
            st.emit(
                Some(&id),
                EventKind::Created,
                String::new(),
                String::new(),
                "room_created".to_string(),
                max_events,
            );
            if let Some(h) = host_obj.as_ref() {
                st.emit(
                    Some(&id),
                    EventKind::Joined,
                    h.id.clone(),
                    h.id.clone(),
                    "admitted=1".to_string(),
                    max_events,
                );
            }
        }

        let cap_note = self
            .cap_of(req.max_participants)
            .map(|n| n.to_string())
            .unwrap_or_else(|| "不限制".to_string());
        Ok(OpView {
            room_id: id.clone(),
            peer: host_id.unwrap_or_else(|| "system".to_string()),
            state: Some(PeerState::Admitted),
            note: format!("房间已创建（上限 {cap_note} 人）"),
            events: self.take_pending_events(),
            peers: self.peers_of(&id).unwrap_or_default(),
            room: self.room_view(&id).ok(),
        })
    }

    /// 加入房间：密码校验 → 人数上限 → 等候室判定。
    ///
    /// * 密码正确且未超员 → [`PeerState::Admitted`]，事件 [`EventKind::Joined`]
    /// * 密码错误 / 房间满员 → [`PeerState::Waiting`]，事件 [`EventKind::Waiting`]
    /// * 已在等候室且此次密码正确 → 直接放行（事件 [`EventKind::Admitted`]）
    /// * 已入会重复 join → [`RoomError::Duplicate`]
    pub fn join(&self, room: &str, ref_: &PeerRef) -> RResult<OpView> {
        let peer_id = peer_id_of(ref_);
        let password = ref_.password.clone();
        let address = ref_.address.clone();
        let name = display_name_of(ref_);
        validate_name(&name)?;

        let max_events = self.max_events();
        // 阶段 1：只读判定（不改动状态）
        let plan = {
            let s = self.st.lock();
            let r = s
                .rooms
                .get(room)
                .ok_or_else(|| RoomError::NotFound(room.to_string()))?;
            match r.find(&peer_id).map(|p| p.state) {
                Some(PeerState::Admitted) => return Err(RoomError::Duplicate(peer_id)),
                Some(PeerState::Waiting) if r.is_full() => Plan::StayWaiting(JoinReason::Full),
                Some(PeerState::Waiting) if r.password.as_deref() != password.as_deref() => {
                    Plan::StayWaiting(JoinReason::PasswordWrong)
                }
                Some(PeerState::Waiting) => Plan::Promote,
                None => decide_join(r, password.as_deref()),
            }
        };

        // 阶段 2：写入
        let (self_state, note) = {
            let mut g = self.st.lock();
            let mut st = St::new(&mut *g);
            if !st.rooms.contains_key(room) {
                return Err(RoomError::NotFound(room.to_string()));
            }
            let now = st.now();
            match plan {
                Plan::Admit => {
                    let admitted = {
                        let r = st.rooms.get_mut(room).expect("上面已确认房间存在");
                        r.peers.push(Peer::new(
                            peer_id.clone(),
                            address,
                            name,
                            Role::Participant,
                            PeerState::Admitted,
                        ));
                        r.last_activity_unix = now;
                        r.admitted_count()
                    };
                    st.emit(
                        Some(room),
                        EventKind::Joined,
                        peer_id.clone(),
                        peer_id.clone(),
                        format!("admitted={admitted}"),
                        max_events,
                    );
                    (PeerState::Admitted, format!("已加入房间（当前 {admitted} 人）"))
                }
                Plan::Promote => {
                    let admitted = {
                        let r = st.rooms.get_mut(room).expect("上面已确认房间存在");
                        if let Some(p) = r.find_mut(&peer_id) {
                            p.state = PeerState::Admitted;
                        }
                        r.last_activity_unix = now;
                        r.admitted_count()
                    };
                    st.emit(
                        Some(room),
                        EventKind::Admitted,
                        peer_id.clone(),
                        "system".to_string(),
                        "reason=password_ok".to_string(),
                        max_events,
                    );
                    (
                        PeerState::Admitted,
                        format!("密码校验通过，已进入会议（当前 {admitted} 人）"),
                    )
                }
                Plan::StayWaiting(why) => (
                    PeerState::Waiting,
                    format!("仍在等候室（{why}），等待主持人审核"),
                ),
                Plan::NewWaiting(why) => {
                    {
                        let r = st.rooms.get_mut(room).expect("上面已确认房间存在");
                        r.peers.push(Peer::new(
                            peer_id.clone(),
                            address,
                            name,
                            Role::Participant,
                            PeerState::Waiting,
                        ));
                    }
                    st.emit(
                        Some(room),
                        EventKind::Waiting,
                        peer_id.clone(),
                        peer_id.clone(),
                        format!("reason={why}"),
                        max_events,
                    );
                    (PeerState::Waiting, format!("已进入等候室（{why}）"))
                }
            }
        };

        Ok(OpView {
            room_id: room.to_string(),
            peer: peer_id,
            state: Some(self_state),
            note,
            events: self.take_pending_events(),
            peers: self.peers_of(room).unwrap_or_default(),
            room: self.room_view(room).ok(),
        })
    }

    /// 离开房间。主持人离开不转移主持人身份（房间随宽限期回收）。
    pub fn leave(&self, room: &str, ref_: &PeerRef) -> RResult<OpView> {
        let peer_id = peer_id_of(ref_);
        let max_events = self.max_events();
        {
            let mut g = self.st.lock();
            let mut st = St::new(&mut *g);
            if !st.rooms.contains_key(room) {
                return Err(RoomError::NotFound(room.to_string()));
            }
            let now = st.now();
            let (had, remaining) = {
                let r = st.rooms.get_mut(room).expect("上面已确认房间存在");
                let had = r.peers.iter().any(|p| p.id == peer_id);
                if had {
                    r.peers.retain(|p| p.id != peer_id);
                    r.last_activity_unix = now;
                }
                (had, r.peers.len())
            };
            if !had {
                return Err(RoomError::NotInRoom);
            }
            st.emit(
                Some(room),
                EventKind::Left,
                peer_id.clone(),
                peer_id.clone(),
                format!("remaining={remaining}"),
                max_events,
            );
        }
        let remaining = self
            .room_view(room)
            .map(|v| v.admitted_count + v.waiting_count)
            .unwrap_or(0);
        Ok(OpView {
            room_id: room.to_string(),
            peer: peer_id,
            state: None,
            note: format!("已离开房间（剩余 {remaining} 人）"),
            events: self.take_pending_events(),
            peers: self.peers_of(room).unwrap_or_default(),
            room: self.room_view(room).ok(),
        })
    }

    /// 主持人销毁房间（立即释放，不走宽限期）。
    pub fn destroy_room(&self, room: &str, actor: &PeerRef) -> RResult<OpView> {
        let actor_id = peer_id_of(actor);
        let max_events = self.max_events();
        {
            let mut g = self.st.lock();
            let mut st = St::new(&mut *g);
            if !st.rooms.contains_key(room) {
                return Err(RoomError::NotFound(room.to_string()));
            }
            if !can_control_in(st.rooms.get(room).expect("上面已确认房间存在"), &actor_id) {
                return Err(RoomError::Forbidden);
            }
            st.emit(
                Some(room),
                EventKind::Destroyed,
                String::new(),
                actor_id.clone(),
                "reason=host_destroy".to_string(),
                max_events,
            );
            st.rooms.remove(room);
        }
        Ok(OpView {
            room_id: room.to_string(),
            peer: actor_id,
            state: None,
            note: "房间已销毁（资源已释放）".to_string(),
            events: self.take_pending_events(),
            peers: Vec::new(),
            room: None,
        })
    }

    // ── 主持人权限 ────────────────────────────────────────────────

    /// 静音 / 取消静音某个参会者。
    pub fn mute_peer(&self, room: &str, actor: &PeerRef, target: &str, mute: bool) -> RResult<OpView> {
        let actor_id = peer_id_of(actor);
        let max_events = self.max_events();
        {
            let mut g = self.st.lock();
            let mut st = St::new(&mut *g);
            if !st.rooms.contains_key(room) {
                return Err(RoomError::NotFound(room.to_string()));
            }
            if !can_control_in(st.rooms.get(room).expect("上面已确认房间存在"), &actor_id) {
                return Err(RoomError::Forbidden);
            }
            {
                let r = st.rooms.get_mut(room).expect("上面已确认房间存在");
                // 先把操作者角色拷出来，避免持有 `&mut Peer` 时再借 `Room`
                let actor_role = r
                    .find(&actor_id)
                    .map(|p| p.role)
                    .unwrap_or(Role::Participant);
                let t = r.find_mut(target).ok_or(RoomError::NotParticipant)?;
                if t.id == actor_id {
                    return Err(RoomError::Invalid("不能静音自己".to_string()));
                }
                if t.role.rank() >= actor_role.rank() {
                    return Err(RoomError::Forbidden);
                }
                if t.state != PeerState::Admitted {
                    return Err(RoomError::NotParticipant);
                }
                if t.muted_by_host == mute {
                    return Err(RoomError::Invalid("目标已是该静音状态".to_string()));
                }
                t.muted_by_host = mute;
            }
            st.emit(
                Some(room),
                if mute { EventKind::Muted } else { EventKind::Unmuted },
                target.to_string(),
                actor_id.clone(),
                "by_host".to_string(),
                max_events,
            );
        }
        let note = if mute {
            format!("已静音 {target}")
        } else {
            format!("已取消静音 {target}")
        };
        Ok(OpView {
            room_id: room.to_string(),
            peer: actor_id.clone(),
            state: self.state_of(room, &actor_id),
            note,
            events: self.take_pending_events(),
            peers: self.peers_of(room).unwrap_or_default(),
            room: self.room_view(room).ok(),
        })
    }

    /// 全局静音（主持人自己除外）。
    pub fn mute_all(&self, room: &str, actor: &PeerRef) -> RResult<OpView> {
        let actor_id = peer_id_of(actor);
        let max_events = self.max_events();
        let muted: Vec<String>;
        {
            let mut g = self.st.lock();
            let mut st = St::new(&mut *g);
            if !st.rooms.contains_key(room) {
                return Err(RoomError::NotFound(room.to_string()));
            }
            if !can_control_in(st.rooms.get(room).expect("上面已确认房间存在"), &actor_id) {
                return Err(RoomError::Forbidden);
            }
            // 先算出要静音的人（收集成 Vec 后即释放 `&mut Room` 借用），
            // 再统一改状态、发事件 —— 否则 `r` 仍活着时调 `st.emit` 会撞 E0499。
            {
                let r = st.rooms.get_mut(room).expect("上面已确认房间存在");
                muted = r
                    .peers
                    .iter()
                    .filter(|p| p.state == PeerState::Admitted && p.id != actor_id && !p.muted_by_host)
                    .map(|p| p.id.clone())
                    .collect();
                for p in r.peers.iter_mut() {
                    if muted.iter().any(|m| *m == p.id) {
                        p.muted_by_host = true;
                    }
                }
            }
            for id in &muted {
                st.emit(
                    Some(room),
                    EventKind::Muted,
                    id.clone(),
                    actor_id.clone(),
                    "by_host".to_string(),
                    max_events,
                );
            }
        }
        Ok(OpView {
            room_id: room.to_string(),
            peer: actor_id.clone(),
            state: self.state_of(room, &actor_id),
            note: format!("已静音全部参会者（{} 人）", muted.len()),
            events: self.take_pending_events(),
            peers: self.peers_of(room).unwrap_or_default(),
            room: self.room_view(room).ok(),
        })
    }

    /// 移除参会者（不能移除自己，不能移除同级及以上角色）。
    pub fn kick_peer(&self, room: &str, actor: &PeerRef, target: &str) -> RResult<OpView> {
        let actor_id = peer_id_of(actor);
        let max_events = self.max_events();
        {
            let mut g = self.st.lock();
            let mut st = St::new(&mut *g);
            if !st.rooms.contains_key(room) {
                return Err(RoomError::NotFound(room.to_string()));
            }
            if !can_control_in(st.rooms.get(room).expect("上面已确认房间存在"), &actor_id) {
                return Err(RoomError::Forbidden);
            }
            let now = st.now();
            {
                let r = st.rooms.get_mut(room).expect("上面已确认房间存在");
                let actor_role = r
                    .find(&actor_id)
                    .map(|p| p.role)
                    .unwrap_or(Role::Participant);
                let t = r.find(target).ok_or(RoomError::NotParticipant)?;
                if t.id == actor_id {
                    return Err(RoomError::Invalid("不能移除自己".to_string()));
                }
                if t.state != PeerState::Admitted {
                    return Err(RoomError::NotParticipant);
                }
                if t.role.rank() >= actor_role.rank() {
                    return Err(RoomError::Forbidden);
                }
                r.peers.retain(|p| p.id != target);
                r.last_activity_unix = now;
            }
            st.emit(
                Some(room),
                EventKind::Left,
                target.to_string(),
                actor_id.clone(),
                "reason=kicked".to_string(),
                max_events,
            );
        }
        Ok(OpView {
            room_id: room.to_string(),
            peer: actor_id.clone(),
            state: self.state_of(room, &actor_id),
            note: format!("已移除 {target}"),
            events: self.take_pending_events(),
            peers: self.peers_of(room).unwrap_or_default(),
            room: self.room_view(room).ok(),
        })
    }

    /// 变更他人角色。只有主持人可以，且不能改主持人自己。
    pub fn set_role(&self, room: &str, actor: &PeerRef, target: &str, role: Role) -> RResult<OpView> {
        let actor_id = peer_id_of(actor);
        let max_events = self.max_events();
        {
            let mut g = self.st.lock();
            let mut st = St::new(&mut *g);
            if !st.rooms.contains_key(room) {
                return Err(RoomError::NotFound(room.to_string()));
            }
            let manage = st
                .rooms
                .get(room)
                .and_then(|r| r.find(&actor_id))
                .map(|p| p.role.can_manage_roles());
            if manage != Some(true) {
                return Err(RoomError::Forbidden);
            }
            if role == Role::Host {
                return Err(RoomError::Invalid("主持人由创建者担任，不能转授".to_string()));
            }
            {
                let r = st.rooms.get_mut(room).expect("上面已确认房间存在");
                let t = r.find(target).ok_or(RoomError::NotParticipant)?;
                if t.id == actor_id {
                    return Err(RoomError::Invalid("不能修改自己的角色".to_string()));
                }
                if t.state != PeerState::Admitted {
                    return Err(RoomError::NotParticipant);
                }
                if t.role == role {
                    return Err(RoomError::Invalid("目标已是该角色".to_string()));
                }
                if let Some(t) = r.find_mut(target) {
                    t.role = role;
                }
            }
            st.emit(
                Some(room),
                EventKind::RoleChanged,
                target.to_string(),
                actor_id.clone(),
                format!("role={role}"),
                max_events,
            );
        }
        Ok(OpView {
            room_id: room.to_string(),
            peer: actor_id.clone(),
            state: self.state_of(room, &actor_id),
            note: format!("已将 {target} 设为 {role}"),
            events: self.take_pending_events(),
            peers: self.peers_of(room).unwrap_or_default(),
            room: self.room_view(room).ok(),
        })
    }

    /// 从等候室批准某个参会者（房间已满时拒绝）。
    pub fn approve_peer(&self, room: &str, actor: &PeerRef, target: &str) -> RResult<OpView> {
        let actor_id = peer_id_of(actor);
        let max_events = self.max_events();
        let admitted: usize;
        {
            let mut g = self.st.lock();
            let mut st = St::new(&mut *g);
            if !st.rooms.contains_key(room) {
                return Err(RoomError::NotFound(room.to_string()));
            }
            if !can_control_in(st.rooms.get(room).expect("上面已确认房间存在"), &actor_id) {
                return Err(RoomError::Forbidden);
            }
            if st.rooms.get(room).expect("上面已确认房间存在").is_full() {
                return Err(RoomError::Invalid("房间已满，无法批准".to_string()));
            }
            let now = st.now();
            {
                let r = st.rooms.get_mut(room).expect("上面已确认房间存在");
                let t = r.find(target).ok_or(RoomError::NotParticipant)?;
                if t.state != PeerState::Waiting {
                    return Err(RoomError::NotParticipant);
                }
                if let Some(t) = r.find_mut(target) {
                    t.state = PeerState::Admitted;
                }
                r.last_activity_unix = now;
                admitted = r.admitted_count();
            }
            st.emit(
                Some(room),
                EventKind::Admitted,
                target.to_string(),
                actor_id.clone(),
                format!("admitted={admitted}"),
                max_events,
            );
        }
        Ok(OpView {
            room_id: room.to_string(),
            peer: actor_id.clone(),
            state: self.state_of(room, &actor_id),
            note: format!("已批准 {target} 入会（当前 {admitted} 人）"),
            events: self.take_pending_events(),
            peers: self.peers_of(room).unwrap_or_default(),
            room: self.room_view(room).ok(),
        })
    }

    /// 从等候室拒绝某个参会者（直接移出，不占席位）。
    pub fn deny_peer(&self, room: &str, actor: &PeerRef, target: &str) -> RResult<OpView> {
        let actor_id = peer_id_of(actor);
        let max_events = self.max_events();
        {
            let mut g = self.st.lock();
            let mut st = St::new(&mut *g);
            if !st.rooms.contains_key(room) {
                return Err(RoomError::NotFound(room.to_string()));
            }
            if !can_control_in(st.rooms.get(room).expect("上面已确认房间存在"), &actor_id) {
                return Err(RoomError::Forbidden);
            }
            {
                let r = st.rooms.get_mut(room).expect("上面已确认房间存在");
                let t = r.find(target).ok_or(RoomError::NotParticipant)?;
                if t.state != PeerState::Waiting {
                    return Err(RoomError::NotParticipant);
                }
                r.peers.retain(|p| p.id != target);
            }
            st.emit(
                Some(room),
                EventKind::Declined,
                target.to_string(),
                actor_id.clone(),
                "reason=host_declined".to_string(),
                max_events,
            );
        }
        Ok(OpView {
            room_id: room.to_string(),
            peer: actor_id.clone(),
            state: self.state_of(room, &actor_id),
            note: format!("已拒绝 {target}"),
            events: self.take_pending_events(),
            peers: self.peers_of(room).unwrap_or_default(),
            room: self.room_view(room).ok(),
        })
    }

    /// 参会者切换媒体开关（麦克风 / 摄像头 / 屏幕共享）。
    ///
    /// 只在**有变化**的开关上发事件，`detail` 列出实际变化的项（`mic=on,cam=off`）。
    pub fn set_media(
        &self,
        room: &str,
        ref_: &PeerRef,
        mic: Option<bool>,
        cam: Option<bool>,
        screen_share: Option<bool>,
    ) -> RResult<OpView> {
        let peer_id = peer_id_of(ref_);
        let max_events = self.max_events();
        let mut changed = Vec::new();
        {
            let mut g = self.st.lock();
            let mut st = St::new(&mut *g);
            if !st.rooms.contains_key(room) {
                return Err(RoomError::NotFound(room.to_string()));
            }
            {
                let r = st.rooms.get_mut(room).expect("上面已确认房间存在");
                let p = r.find_mut(&peer_id).ok_or(RoomError::NotInRoom)?;
                if p.state != PeerState::Admitted {
                    return Err(RoomError::Invalid("等候室参会者不能切换媒体状态".to_string()));
                }
                if let Some(v) = mic {
                    if p.mic_on != v {
                        p.mic_on = v;
                        changed.push(format!("mic={}", if v { "on" } else { "off" }));
                    }
                }
                if let Some(v) = cam {
                    if p.cam_on != v {
                        p.cam_on = v;
                        changed.push(format!("cam={}", if v { "on" } else { "off" }));
                    }
                }
                if let Some(v) = screen_share {
                    if p.screen_share != v {
                        p.screen_share = v;
                        changed.push(format!("screen={}", if v { "on" } else { "off" }));
                    }
                }
            }
            if changed.is_empty() {
                return Err(RoomError::Invalid("媒体状态无变化".to_string()));
            }
            st.emit(
                Some(room),
                EventKind::MediaChanged,
                peer_id.clone(),
                peer_id.clone(),
                changed.join(","),
                max_events,
            );
        }
        Ok(OpView {
            room_id: room.to_string(),
            peer: peer_id,
            state: Some(PeerState::Admitted),
            note: "媒体状态已更新".to_string(),
            events: self.take_pending_events(),
            peers: self.peers_of(room).unwrap_or_default(),
            room: self.room_view(room).ok(),
        })
    }

    /// 操作者本人当前状态（`None` = 不在房间）。
    fn state_of(&self, room: &str, peer_id: &str) -> Option<PeerState> {
        let s = self.st.lock();
        s.rooms.get(room)?.find(peer_id).map(|p| p.state)
    }

    // ── 资源回收 ──────────────────────────────────────────────────

    /// 回收宽限期到期的空房：房间对象 + 成员表 + 事件队列整体释放。
    ///
    /// 只有 `admitted_count() == 0` 且距最后一次活动超过宽限期的房间才会被回收；
    /// 仍有成员（哪怕只有等候室人）的房间不受影响。
    pub fn reap_expired_at(&self, now_unix: i64) -> Vec<String> {
        let grace = self.grace_secs();
        let max_events = self.max_events();
        let mut gone = Vec::new();
        {
            let mut g = self.st.lock();
            let mut st = St::new(&mut *g);
            let expired: Vec<String> = st
                .rooms
                .iter()
                .filter(|(_, r)| r.admitted_count() == 0 && now_unix >= r.last_activity_unix + grace)
                .map(|(id, _)| id.clone())
                .collect();
            for id in expired {
                st.emit(
                    Some(&id),
                    EventKind::Destroyed,
                    String::new(),
                    "system".to_string(),
                    format!("reason=grace_expired({grace}s)"),
                    max_events,
                );
                if st.rooms.remove(&id).is_some() {
                    gone.push(id);
                }
            }
        }
        gone
    }

    /// 调度循环：会前提醒（去重）→ 自动建房 → 预约到期销毁 → 空房宽限期回收。
    ///
    /// 返回本轮动作清单；事件同时进入 `pending`，可用 [`RoomManager::take_pending_events`] 取走。
    pub fn tick_at(&self, now_unix: i64) -> TickReport {
        let mut rep = TickReport::default();
        let max_events = self.max_events();
        let default_cap = self.cfg.room.default_max_participants;
        let grace = self.grace_secs();
        let mut g = self.st.lock();
        let mut st = St::new(&mut *g);

        // 1) 会前提醒：提醒窗口内、未触发过才发一次（触发去重）
        let to_remind: Vec<Reminder> = st
            .appts
            .values()
            .filter(|a| {
                a.status == ApptStatus::Scheduled
                    && a.reminder_mins > 0
                    && !a.reminder_fired
                    && now_unix >= a.starts_at - a.reminder_mins * 60
                    && now_unix < a.starts_at
            })
            .map(|a| Reminder {
                appt_id: a.id.clone(),
                room_id: a.room_id.clone(),
                title: a.title.clone(),
                targets: reminder_targets(a),
                lead_mins: a.reminder_mins,
                starts_at: a.starts_at,
            })
            .collect();
        for rem in &to_remind {
            if let Some(a) = st.appts.get_mut(&rem.appt_id) {
                a.reminder_fired = true;
            }
        }
        rep.reminders = to_remind;

        // 2) 到期自动建房（幂等：room_created 或房间已存在都跳过）
        let to_create: Vec<CreatePlan> = st
            .appts
            .values()
            .filter(|a| {
                a.status == ApptStatus::Scheduled
                    && now_unix >= a.starts_at
                    && !a.room_created
                    && !st.rooms.contains_key(&a.room_id)
            })
            .map(|a| CreatePlan {
                appt_id: a.id.clone(),
                room_id: a.room_id.clone(),
                host_id: a.host_id.clone(),
                host_name: a.host_name.clone(),
                ends_at_unix: a.ends_at(),
                password: a.password.clone(),
                max_participants: a.max_participants,
            })
            .collect();
        for p in to_create {
            let mut room = Room::new(p.room_id.clone(), now_unix);
            room.password = p.password;
            let cap = p.max_participants.unwrap_or(default_cap);
            room.max_participants = if cap == 0 { None } else { Some(cap) };
            room.ends_at_unix = Some(p.ends_at_unix);
            room.peers.push(Peer::new(
                p.host_id.clone(),
                "system".to_string(),
                p.host_name,
                Role::Host,
                PeerState::Admitted,
            ));
            st.rooms.insert(p.room_id.clone(), room);
            st.emit(
                Some(&p.room_id),
                EventKind::Created,
                String::new(),
                p.host_id.clone(),
                format!("appt={}", p.appt_id),
                max_events,
            );
            if let Some(a) = st.appts.get_mut(&p.appt_id) {
                a.room_created = true;
            }
            rep.rooms_created.push(p.room_id);
        }

        // 3) 预约到期自动销毁房间（先收集 id，避免边遍历边改）
        let to_destroy: Vec<String> = st
            .rooms
            .iter()
            .filter(|(_, r)| r.ends_at_unix.is_some_and(|e| now_unix >= e))
            .map(|(id, _)| id.clone())
            .collect();
        for id in to_destroy {
            st.emit(
                Some(&id),
                EventKind::Destroyed,
                String::new(),
                "system".to_string(),
                "reason=appointment_ended".to_string(),
                max_events,
            );
            if st.rooms.remove(&id).is_some() {
                rep.rooms_destroyed.push(id);
            }
        }

        // 4) 空房宽限期回收
        let expired: Vec<String> = st
            .rooms
            .iter()
            .filter(|(_, r)| r.admitted_count() == 0 && now_unix >= r.last_activity_unix + grace)
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            st.emit(
                Some(&id),
                EventKind::Destroyed,
                String::new(),
                "system".to_string(),
                format!("reason=grace_expired({grace}s)"),
                max_events,
            );
            if st.rooms.remove(&id).is_some() {
                rep.rooms_reclaimed.push(id);
            }
        }

        rep
    }

    // ── 会议预约（原 YEJ-111 并入）────────────────────────────────

    /// 创建预约：校验参数 + 会议室号冲突规则（同会议室号或同主持人时间重叠即冲突）。
    pub fn create_appt(&self, req: &ApptReq) -> RResult<ApptView> {
        let mut a = Appointment {
            id: String::new(),
            title: req.title.trim().to_string(),
            room_id: req.room_id.trim().to_string(),
            host_id: req.host_id.trim().to_string(),
            host_name: req.host_name.trim().to_string(),
            starts_at: req.starts_at,
            duration_mins: req.duration_mins,
            password: req.password.clone(),
            max_participants: req.max_participants.filter(|n| *n > 0),
            invitees: req.invitees.clone(),
            reminder_mins: req.reminder_mins,
            status: ApptStatus::Scheduled,
            cancel_reason: String::new(),
            room_created: false,
            reminder_fired: false,
        };
        validate_appt(&a)?;
        let mut s = self.st.lock();
        a.id = format!("appt-{}", s.appt_seq + 1);
        let exists = s.rooms.contains_key(&a.room_id);
        check_conflict(&s.appts, &a, None)?;
        s.appt_seq += 1;
        let view = a.view(exists);
        s.appts.insert(a.id.clone(), a);
        Ok(view)
    }

    /// 修改预约（仅未取消、未开始的预约可改；改动后重跑冲突规则）。
    ///
    /// 先把条目取出再改：`check_conflict` 要读整个 `s.appts`、`ApptView` 要读
    /// `s.rooms`，`get_mut` 的借用还活着时会冲突。无论成败都会把条目放回。
    pub fn update_appt(&self, id: &str, patch: &ApptPatch) -> RResult<ApptView> {
        let mut s = self.st.lock();
        // 在副本上改，全部校验通过才写回 ——
        // 校验 / 冲突失败绝不得污染原预约（否则一次失败的修改就把预约弄坏）。
        let mut a = s
            .appts
            .get(id)
            .cloned()
            .ok_or_else(|| RoomError::NotFound(id.to_string()))?;
        let now = s.clock.unix();
        if a.status == ApptStatus::Cancelled {
            return Err(RoomError::Invalid("已取消的预约不能修改".to_string()));
        }
        if now >= a.starts_at {
            return Err(RoomError::Invalid("已开始的预约不能修改".to_string()));
        }
        if !patch.title.trim().is_empty() {
            a.title = patch.title.trim().to_string();
        }
        if !patch.room_id.trim().is_empty() {
            a.room_id = patch.room_id.trim().to_string();
        }
        if !patch.host_id.trim().is_empty() {
            a.host_id = patch.host_id.trim().to_string();
        }
        if !patch.host_name.trim().is_empty() {
            a.host_name = patch.host_name.trim().to_string();
        }
        if let Some(v) = patch.starts_at {
            a.starts_at = v;
        }
        if let Some(v) = patch.duration_mins {
            a.duration_mins = v;
        }
        if let Some(p) = &patch.password {
            a.password = if p.is_empty() { None } else { Some(p.clone()) };
        }
        if let Some(c) = patch.max_participants {
            a.max_participants = if c == 0 { None } else { Some(c) };
        }
        if !patch.invitees.is_empty() {
            a.invitees = patch.invitees.clone();
        }
        if let Some(v) = patch.reminder_mins {
            a.reminder_mins = v;
        }
        validate_appt(&a)?;
        check_conflict(&s.appts, &a, Some(id))?;
        let view = a.view(s.rooms.contains_key(&a.room_id));
        s.appts.insert(id.to_string(), a);
        Ok(view)
    }

    /// 取消预约（已取消 / 已开始的不能取消）。
    pub fn cancel_appt(&self, id: &str, reason: &str) -> RResult<ApptView> {
        let mut s = self.st.lock();
        let mut a = s
            .appts
            .get(id)
            .cloned()
            .ok_or_else(|| RoomError::NotFound(id.to_string()))?;
        let now = s.clock.unix();
        if a.status == ApptStatus::Cancelled {
            return Err(RoomError::Invalid("预约已取消".to_string()));
        }
        if now >= a.starts_at {
            return Err(RoomError::Invalid("已开始的预约不能取消".to_string()));
        }
        a.status = ApptStatus::Cancelled;
        a.cancel_reason = reason.to_string();
        let view = a.view(s.rooms.contains_key(&a.room_id));
        s.appts.insert(id.to_string(), a);
        Ok(view)
    }
}

/// 入会计划（阶段 1 的只读判定结果，阶段 2 据此写入）。
enum Plan {
    Admit,
    Promote,
    StayWaiting(JoinReason),
    NewWaiting(JoinReason),
}

/// 入会判定原因（写入事件 `detail`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinReason {
    PasswordWrong,
    Full,
}

impl std::fmt::Display for JoinReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            JoinReason::PasswordWrong => "password_wrong",
            JoinReason::Full => "room_full",
        })
    }
}

/// 入会判定：密码 + 人数上限。
fn decide_join(r: &Room, password: Option<&str>) -> Plan {
    if r.password.as_deref() != password {
        return Plan::NewWaiting(JoinReason::PasswordWrong);
    }
    if r.is_full() {
        return Plan::NewWaiting(JoinReason::Full);
    }
    Plan::Admit
}

/// 操作者是否为**已入会**的主持人 / 联席主持人（非成员一律按权限不足处理）。
fn can_control_in(r: &Room, actor_id: &str) -> bool {
    r.find(actor_id)
        .is_some_and(|p| p.state == PeerState::Admitted && p.role.can_control())
}

/// 自动建房计划（从预约快照出，避免在遍历 `s.appts` 时借用 `s.rooms`）。
struct CreatePlan {
    appt_id: String,
    room_id: String,
    host_id: String,
    host_name: String,
    ends_at_unix: i64,
    password: Option<String>,
    max_participants: Option<usize>,
}

// ── 校验与工具函数 ────────────────────────────────────────────────

const MAX_ROOM_ID: usize = 64;
const MAX_PASSWORD_LEN: usize = 64;
const MAX_NAME_LEN: usize = 64;
const MAX_PARTICIPANTS_CAP: usize = 512;
const MIN_APPT_MINS: i64 = 1;
const MAX_APPT_MINS: i64 = 24 * 60;
const MIN_REMINDER_MINS: i64 = 0;
const MAX_REMINDER_MINS: i64 = 12 * 60;

fn validate_room_id(id: &str) -> RResult<String> {
    let t = id.trim();
    if t.is_empty() {
        return Err(RoomError::Invalid("房间号不能为空".to_string()));
    }
    if t.len() > MAX_ROOM_ID {
        return Err(RoomError::Invalid(format!("房间号过长（上限 {MAX_ROOM_ID} 字符）")));
    }
    if t.chars().any(|c| c == '/' || c.is_whitespace() || c == '\0') {
        return Err(RoomError::Invalid("房间号不能含分隔符或空白".to_string()));
    }
    Ok(t.to_string())
}

fn validate_password(pw: Option<&str>) -> RResult<()> {
    if let Some(p) = pw {
        if p.is_empty() {
            return Err(RoomError::Invalid(
                "会议密码不能为空字符串（不设置请传 null）".to_string(),
            ));
        }
        if p.len() > MAX_PASSWORD_LEN {
            return Err(RoomError::Invalid(format!("会议密码过长（上限 {MAX_PASSWORD_LEN}）")));
        }
    }
    Ok(())
}

fn validate_cap(n: usize) -> RResult<()> {
    if n > MAX_PARTICIPANTS_CAP {
        return Err(RoomError::Invalid(format!(
            "参会人数上限过大（上限 {MAX_PARTICIPANTS_CAP}）: {n}"
        )));
    }
    Ok(())
}

fn validate_name(name: &str) -> RResult<()> {
    let t = name.trim();
    if t.is_empty() {
        return Err(RoomError::Invalid("显示名不能为空".to_string()));
    }
    if t.len() > MAX_NAME_LEN {
        return Err(RoomError::Invalid(format!("显示名过长（上限 {MAX_NAME_LEN}）")));
    }
    Ok(())
}

fn display_name_of(ref_: &PeerRef) -> String {
    ref_
        .display_name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(&ref_.id)
        .to_string()
}

fn peer_id_of(ref_: &PeerRef) -> String {
    let t = ref_.id.trim();
    if t.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        t.to_string()
    }
}

fn peer_ref_to_peer(ref_: &PeerRef) -> RResult<Peer> {
    let id = peer_id_of(ref_);
    if id.trim().is_empty() {
        return Err(RoomError::Invalid("peer_id 不能为空".to_string()));
    }
    Ok(Peer::new(
        id,
        ref_.address.clone(),
        display_name_of(ref_),
        Role::Participant,
        PeerState::Admitted,
    ))
}

fn validate_appt(a: &Appointment) -> RResult<()> {
    if a.title.trim().is_empty() {
        return Err(RoomError::Invalid("预约标题不能为空".to_string()));
    }
    validate_room_id(&a.room_id)?;
    if a.host_id.trim().is_empty() {
        return Err(RoomError::Invalid("主持人不能为空".to_string()));
    }
    if a.host_name.trim().is_empty() {
        return Err(RoomError::Invalid("主持人昵称不能为空".to_string()));
    }
    if a.starts_at <= 0 {
        return Err(RoomError::Invalid("开始时间非法".to_string()));
    }
    if a.duration_mins < MIN_APPT_MINS || a.duration_mins > MAX_APPT_MINS {
        return Err(RoomError::Invalid(format!(
            "会议时长必须在 {MIN_APPT_MINS}–{MAX_APPT_MINS} 分钟之间: {}",
            a.duration_mins
        )));
    }
    if a.reminder_mins < MIN_REMINDER_MINS || a.reminder_mins > MAX_REMINDER_MINS {
        return Err(RoomError::Invalid(format!(
            "提醒提前量必须在 {MIN_REMINDER_MINS}–{MAX_REMINDER_MINS} 分钟之间: {}",
            a.reminder_mins
        )));
    }
    if a.reminder_mins > 0 && a.reminder_mins * 60 >= a.duration_mins * 60 {
        return Err(RoomError::Invalid("提醒提前量必须早于会议开始".to_string()));
    }
    validate_password(a.password.as_deref())?;
    if let Some(c) = a.max_participants {
        validate_cap(c)?;
    }
    for inv in &a.invitees {
        if inv.display_name.trim().is_empty() {
            return Err(RoomError::Invalid("受邀人昵称不能为空".to_string()));
        }
        if inv.display_name.trim().len() > MAX_NAME_LEN {
            return Err(RoomError::Invalid(format!("受邀人昵称过长（上限 {MAX_NAME_LEN}）")));
        }
    }
    Ok(())
}

/// 会议预约冲突规则：
/// 半开区间 `[starts_at, ends_at)`，**同一会议室号或同一主持人**时间重叠即冲突；
/// 首尾相接不算冲突；已取消的预约释放时段；会议室号跨时间段可复用。
fn overlaps(a0: i64, a1: i64, b0: i64, b1: i64) -> bool {
    a0 < b1 && b0 < a1
}

fn check_conflict(
    appts: &HashMap<String, Appointment>,
    cand: &Appointment,
    exclude: Option<&str>,
) -> RResult<()> {
    for (k, a) in appts {
        if exclude == Some(k.as_str()) {
            continue;
        }
        if a.status == ApptStatus::Cancelled {
            continue;
        }
        if a.room_id != cand.room_id && a.host_id != cand.host_id {
            continue;
        }
        if overlaps(a.starts_at, a.ends_at(), cand.starts_at, cand.ends_at()) {
            return Err(RoomError::Conflict(format!(
                "预约 {k}（会议室 {}，{}–{}）与待创建预约时间冲突",
                a.room_id,
                a.starts_at,
                a.ends_at()
            )));
        }
    }
    Ok(())
}

/// 提醒目标：主持人 + 受邀人联系方式（去重，保持稳定顺序）。
fn reminder_targets(a: &Appointment) -> Vec<String> {
    let mut v = vec![a.host_id.clone()];
    for inv in &a.invitees {
        let c = inv.contact.trim().to_string();
        if c.is_empty() || v.contains(&c) {
            continue;
        }
        v.push(c);
    }
    v
}

/// 当前 Unix 秒（墙钟）。
pub fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 内网准入校验：`peer` 地址必须落在配置声明的内网 CIDR 内。
///
/// 信令层与房间层共用同一个实现，避免准入规则漂移。
pub fn ensure_intranet_peer(cfg: &qm_common::AppConfig, peer: &str) -> qm_common::error::Result<()> {
    let host = peer.split(':').next().unwrap_or(peer);
    let addr = host.parse::<std::net::IpAddr>().map_err(|_| {
        qm_common::error::Error::invalid_argument(format!("peer 地址非法：{peer}"))
    })?;
    let cidrs = cfg.network.parsed_cidrs()?;
    qm_common::error::Error::ensure_private_host(addr, &cidrs).map_err(|_| {
        qm_common::error::Error::signaling(format!(
            "拒绝非内网 peer {peer}：信令只接受 {}",
            cfg.network.cidrs.join(", ")
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 固定时钟起点（所有时间断言都相对它）。
    const T0: i64 = 1_700_000_000;
    const M: i64 = 60;
    const H: i64 = 3600;

    fn peer(id: &str) -> PeerRef {
        PeerRef {
            id: id.to_string(),
            address: "192.168.0.10:5000".to_string(),
            ..Default::default()
        }
    }

    fn peer_pw(id: &str, pw: &str) -> PeerRef {
        let mut p = peer(id);
        p.password = Some(pw.to_string());
        p
    }

    fn mgr() -> RoomManager {
        RoomManager::with_fixed_time(Arc::new(qm_common::AppConfig::default()), T0)
    }

    fn create(m: &RoomManager, id: &str, host: Option<&str>) -> RResult<OpView> {
        m.create_room(&CreateRoom {
            room_id: id.to_string(),
            max_participants: Some(20),
            host: host.map(peer),
            ..Default::default()
        })
    }

    fn view(m: &RoomManager, room: &str) -> RoomView {
        m.room_view(room).unwrap()
    }

    fn evts(m: &RoomManager, room: &str) -> Vec<RoomEvent> {
        m.events_of(room).unwrap()
    }

    fn kinds(v: &OpView) -> Vec<EventKind> {
        v.events.iter().map(|e| e.kind).collect()
    }

    fn appt_req(room: &str, host: &str, starts: i64, dur: i64) -> ApptReq {
        ApptReq {
            title: "例会".to_string(),
            room_id: room.to_string(),
            host_id: host.to_string(),
            host_name: "张三".to_string(),
            starts_at: starts,
            duration_mins: dur,
            reminder_mins: 5,
            ..Default::default()
        }
    }

    // ── 创建 / 唯一性 ─────────────────────────────────────────────

    #[test]
    fn create_room_registers_host_and_is_unique() {
        let m = mgr();
        let v = create(&m, "m1", Some("host")).unwrap();
        assert_eq!(v.peers.len(), 1, "创建者应直接入会：{}", v.note);
        assert_eq!(v.peers[0].role, Role::Host);
        assert_eq!(v.peers[0].state, PeerState::Admitted);
        assert_eq!(v.state, Some(PeerState::Admitted));
        assert_eq!(kinds(&v), vec![EventKind::Created, EventKind::Joined]);
        // 房间 ID 全局唯一
        assert_eq!(
            create(&m, "m1", Some("other")).unwrap_err(),
            RoomError::Duplicate("m1".to_string())
        );
        // 重复创建不产生新事件（pending 队列应为空）
        assert!(m.take_pending_events().is_empty());
    }

    #[test]
    fn create_room_rejects_bad_params() {
        let m = mgr();
        let bad = [
            CreateRoom { room_id: "  ".to_string(), ..Default::default() },
            CreateRoom { room_id: "a/b".to_string(), ..Default::default() },
            CreateRoom { room_id: "x".to_string(), password: Some(String::new()), ..Default::default() },
            CreateRoom { room_id: "x".to_string(), max_participants: Some(MAX_PARTICIPANTS_CAP + 1), ..Default::default() },
            CreateRoom { room_id: "x".repeat(MAX_ROOM_ID + 1), ..Default::default() },
            CreateRoom { room_id: "x".to_string(), password: Some("p".repeat(MAX_PASSWORD_LEN + 1)), ..Default::default() },
        ];
        for r in bad {
            assert!(
                matches!(m.create_room(&r), Err(RoomError::Invalid(_))),
                "非法参数必须被拒绝：{r:?}"
            );
        }
        assert_eq!(m.memory_facts().rooms, 0, "校验失败不得留下房间");
    }

    // ── 密码 / 等候室 / 人数上限 ────────────────────────────────────

    #[test]
    fn password_enforced_both_ways() {
        let m = mgr();
        m.create_room(&CreateRoom {
            room_id: "m1".to_string(),
            password: Some("s3cret".to_string()),
            host: Some(peer("host")),
            max_participants: Some(20),
            ..Default::default()
        })
        .unwrap();
        // 错误密码 → 等候室
        let v = m.join("m1", &peer_pw("p2", "wrong")).unwrap();
        assert_eq!(v.state, Some(PeerState::Waiting));
        assert!(kinds(&v).contains(&EventKind::Waiting));
        // 正确密码 → 直接入会
        let v = m.join("m1", &peer_pw("p3", "s3cret")).unwrap();
        assert_eq!(v.state, Some(PeerState::Admitted));
        // 等候室的人带正确密码重试 → 直接放行（p2 是上一条错密码进入等候室的那个）
        let v = m.join("m1", &peer_pw("p2", "s3cret")).unwrap();
        assert_eq!(v.state, Some(PeerState::Admitted), "密码修正后应直接放行");
        assert!(kinds(&v).contains(&EventKind::Admitted));
        // 已入会者重复 join → Duplicate
        assert_eq!(m.join("m1", &peer("p3")).unwrap_err(), RoomError::Duplicate("p3".to_string()));
        // 新的等待者重复带错密码重试 → 继续等待，且不产生新事件
        let v = m.join("m1", &peer_pw("p4", "nope")).unwrap();
        assert_eq!(v.state, Some(PeerState::Waiting));
        let v = m.join("m1", &peer_pw("p4", "nope")).unwrap();
        assert_eq!(v.state, Some(PeerState::Waiting));
        assert!(v.events.is_empty(), "等待中的重试不得刷事件");
    }

    #[test]
    fn room_full_sends_to_waiting_then_admits_after_capacity_frees() {
        let m = mgr();
        m.create_room(&CreateRoom { room_id: "m1".to_string(), max_participants: Some(2), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        m.join("m1", &peer("p1")).unwrap();
        let v = m.join("m1", &peer("p2")).unwrap();
        assert_eq!(v.state, Some(PeerState::Waiting), "满员应进入等候室");
        assert_eq!(view(&m, "m1").waiting_count, 1);
        assert_eq!(view(&m, "m1").admitted_count, 2, "等候室不算已入会人数");
        // 仍然满员
        let v = m.join("m1", &peer("p3")).unwrap();
        assert_eq!(v.state, Some(PeerState::Waiting), "仍然满员");
        // 释放一个席位后，主持人可以批准等候室的人进来
        m.kick_peer("m1", &peer("host"), "p1").unwrap();
        let v = m.approve_peer("m1", &peer("host"), "p2").unwrap();
        assert!(kinds(&v).contains(&EventKind::Admitted));
        assert_eq!(view(&m, "m1").admitted_count, 2);
        // 房间已满时不能批准
        let v = m.join("m1", &peer("p4")).unwrap();
        assert_eq!(v.state, Some(PeerState::Waiting));
        assert!(matches!(m.approve_peer("m1", &peer("host"), "p4"), Err(RoomError::Invalid(_))));
    }

    #[test]
    fn host_approves_and_denies_waiting_peers() {
        let m = mgr();
        m.create_room(&CreateRoom { room_id: "m1".to_string(), password: Some("pw".to_string()), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        m.join("m1", &peer("p1")).unwrap(); // 无密码 → 等候
        m.join("m1", &peer("p2")).unwrap(); // 无密码 → 等候
        // 普通参会者不能审核
        m.join("m1", &peer_pw("p3", "pw")).unwrap();
        assert_eq!(m.approve_peer("m1", &peer("p3"), "p1").unwrap_err(), RoomError::Forbidden);
        assert_eq!(m.deny_peer("m1", &peer("p3"), "p1").unwrap_err(), RoomError::Forbidden);
        // 主持人批准
        let v = m.approve_peer("m1", &peer("host"), "p1").unwrap();
        assert!(kinds(&v).contains(&EventKind::Admitted));
        assert_eq!(view(&m, "m1").admitted_count, 3);
        // 主持人拒绝
        let v = m.deny_peer("m1", &peer("host"), "p2").unwrap();
        assert!(kinds(&v).contains(&EventKind::Declined));
        assert_eq!(view(&m, "m1").waiting_count, 0);
        // 已入会者不能被「批准」
        assert_eq!(m.approve_peer("m1", &peer("host"), "p1").unwrap_err(), RoomError::NotParticipant);
        // 已被拒绝的人重新 join 会再进等候室
        assert_eq!(m.join("m1", &peer("p2")).unwrap().state, Some(PeerState::Waiting));
        assert_eq!(view(&m, "m1").waiting_count, 1);
    }

    // ── 主持人权限 ─────────────────────────────────────────────────

    #[test]
    fn host_mutes_and_unmutes_peer() {
        let m = mgr();
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        m.join("m1", &peer("p1")).unwrap();
        // 普通参会者不能静音
        assert_eq!(m.mute_peer("m1", &peer("p1"), "host", true).unwrap_err(), RoomError::Forbidden);
        // 主持人静音 → mic_effective 立刻为 false（所有端一致）
        let v = m.mute_peer("m1", &peer("host"), "p1", true).unwrap();
        assert!(kinds(&v).contains(&EventKind::Muted));
        let p = v.peers.iter().find(|p| p.id == "p1").unwrap();
        assert!(!p.mic_effective);
        assert!(p.muted_by_host);
        // 本人开关开着也不算数
        m.set_media("m1", &peer("p1"), Some(true), None, None).unwrap();
        let peers = m.peers_of("m1").unwrap();
        let p = peers.iter().find(|p| p.id == "p1").unwrap();
        assert!(p.mic_on, "本人开关应为 on");
        assert!(!p.mic_effective, "被主持人静音时 mic_effective 必须为 false");
        // 取消静音
        let v = m.mute_peer("m1", &peer("host"), "p1", false).unwrap();
        assert!(kinds(&v).contains(&EventKind::Unmuted));
        let p = v.peers.iter().find(|p| p.id == "p1").unwrap();
        assert!(p.mic_effective);
        // 重复操作是幂等拒绝
        assert!(matches!(m.mute_peer("m1", &peer("host"), "p1", false), Err(RoomError::Invalid(_))));
        // 不存在的目标
        assert_eq!(m.mute_peer("m1", &peer("host"), "ghost", true).unwrap_err(), RoomError::NotParticipant);
    }

    #[test]
    fn mute_all_spares_the_host() {
        let m = mgr();
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        m.join("m1", &peer("p1")).unwrap();
        m.join("m1", &peer("p2")).unwrap();
        m.set_media("m1", &peer("p1"), Some(true), None, None).unwrap();
        m.set_media("m1", &peer("p2"), Some(true), None, None).unwrap();
        m.set_media("m1", &peer("host"), Some(true), None, None).unwrap();
        let v = m.mute_all("m1", &peer("host")).unwrap();
        let peers = m.peers_of("m1").unwrap();
        let host = peers.iter().find(|p| p.id == "host").unwrap();
        let p1 = peers.iter().find(|p| p.id == "p1").unwrap();
        let p2 = peers.iter().find(|p| p.id == "p2").unwrap();
        assert!(host.mic_effective, "全局静音不应静音主持人自己");
        assert!(!p1.mic_effective);
        assert!(!p2.mic_effective);
        assert_eq!(v.events.iter().filter(|e| e.kind == EventKind::Muted).count(), 2);
        // 再次全局静音是幂等的（没有新事件）
        let v = m.mute_all("m1", &peer("host")).unwrap();
        assert!(v.events.is_empty(), "已静音的不应重复发事件");
        // 普通参会者不能全局静音
        assert_eq!(m.mute_all("m1", &peer("p1")).unwrap_err(), RoomError::Forbidden);
    }

    #[test]
    fn kick_requires_host_and_rejects_self() {
        let m = mgr();
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        m.join("m1", &peer("p1")).unwrap();
        // 普通参会者不能移除别人
        assert_eq!(m.kick_peer("m1", &peer("p1"), "host").unwrap_err(), RoomError::Forbidden);
        // 不能移除自己
        assert!(matches!(m.kick_peer("m1", &peer("host"), "host"), Err(RoomError::Invalid(_))));
        // 主持人移除
        let v = m.kick_peer("m1", &peer("host"), "p1").unwrap();
        assert!(v.peers.iter().all(|p| p.id != "p1"));
        assert!(kinds(&v).iter().any(|k| *k == EventKind::Left));
        let log = evts(&m, "m1");
        let e = log.iter().find(|e| e.peer_id == "p1" && e.kind == EventKind::Left).unwrap();
        assert!(e.detail.contains("kicked"));
        // 被移除者再 join 可以重新进来
        assert_eq!(m.join("m1", &peer("p1")).unwrap().state, Some(PeerState::Admitted));
        // 移除不存在的人
        assert_eq!(m.kick_peer("m1", &peer("host"), "ghost").unwrap_err(), RoomError::NotParticipant);
    }

    #[test]
    fn role_management_is_host_only_and_layered() {
        let m = mgr();
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        m.join("m1", &peer("p1")).unwrap();
        m.join("m1", &peer("p2")).unwrap();
        // 只有主持人能改角色
        assert_eq!(m.set_role("m1", &peer("p1"), "p2", Role::CoHost).unwrap_err(), RoomError::Forbidden);
        // 主持人任命联席主持人
        let v = m.set_role("m1", &peer("host"), "p1", Role::CoHost).unwrap();
        assert!(kinds(&v).contains(&EventKind::RoleChanged));
        let p = v.peers.iter().find(|p| p.id == "p1").unwrap();
        assert_eq!(p.role, Role::CoHost);
        // 联席主持人可以管控普通参会者
        m.set_media("m1", &peer("p2"), Some(true), None, None).unwrap();
        let v = m.mute_peer("m1", &peer("p1"), "p2", true).unwrap();
        assert!(kinds(&v).contains(&EventKind::Muted));
        let v = m.kick_peer("m1", &peer("p1"), "p2").unwrap();
        assert!(v.peers.iter().all(|p| p.id != "p2"));
        // 联席主持人**不能**再任命联席主持人（防权限自我膨胀）
        m.join("m1", &peer("p3")).unwrap();
        assert_eq!(m.set_role("m1", &peer("p1"), "p3", Role::CoHost).unwrap_err(), RoomError::Forbidden);
        // 联席主持人不能动主持人
        assert_eq!(m.mute_peer("m1", &peer("p1"), "host", true).unwrap_err(), RoomError::Forbidden);
        assert_eq!(m.kick_peer("m1", &peer("p1"), "host").unwrap_err(), RoomError::Forbidden);
        // 不能转授主持人
        assert!(matches!(m.set_role("m1", &peer("host"), "p3", Role::Host), Err(RoomError::Invalid(_))));
        // 不能改自己的角色
        assert!(matches!(m.set_role("m1", &peer("host"), "host", Role::CoHost), Err(RoomError::Invalid(_))));
        // 不能改成同一个角色（幂等拒绝）
        assert!(matches!(m.set_role("m1", &peer("host"), "p1", Role::CoHost), Err(RoomError::Invalid(_))));
        // 撤销联席主持人
        let v = m.set_role("m1", &peer("host"), "p1", Role::Participant).unwrap();
        let p = v.peers.iter().find(|p| p.id == "p1").unwrap();
        assert_eq!(p.role, Role::Participant);
    }

    #[test]
    fn set_media_updates_only_changed_flags() {
        let m = mgr();
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        let v = m.set_media("m1", &peer("host"), Some(true), Some(true), None).unwrap();
        assert_eq!(kinds(&v), vec![EventKind::MediaChanged]);
        assert_eq!(v.events[0].detail, "mic=on,cam=on");
        // 重复设置同样的值 → 不发事件
        assert!(matches!(m.set_media("m1", &peer("host"), Some(true), None, None), Err(RoomError::Invalid(_))));
        assert!(m.take_pending_events().is_empty());
        // 混合变化只报变化的项
        let v = m.set_media("m1", &peer("host"), None, Some(false), Some(true)).unwrap();
        assert_eq!(v.events[0].detail, "cam=off,screen=on");
        // 不在房间的参会者不能改媒体状态
        assert!(matches!(
            m.set_media("m1", &peer("ghost"), Some(true), None, None),
            Err(RoomError::NotInRoom)
        ));
        // 等候室参会者不能改媒体状态
        m.create_room(&CreateRoom { room_id: "m2".to_string(), password: Some("pw".to_string()), host: Some(peer("h2")), ..Default::default() })
            .unwrap();
        m.join("m2", &peer("w1")).unwrap();
        assert!(matches!(
            m.set_media("m2", &peer("w1"), Some(true), None, None),
            Err(RoomError::Invalid(_))
        ));
        // 快照同时给 mic_on 与 mic_effective，两端口径一致
        let peers = m.peers_of("m1").unwrap();
        let host = peers.iter().find(|p| p.id == "host").unwrap();
        assert!(host.mic_on && host.mic_effective && !host.cam_on && host.screen_share);
    }

    // ── 状态同步 ───────────────────────────────────────────────────

    #[test]
    fn event_seq_is_monotonic_and_room_log_is_ordered() {
        let m = mgr();
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        let s1 = m.events_of("m1").unwrap()[0].seq;
        m.join("m1", &peer("p1")).unwrap();
        m.join("m1", &peer("p2")).unwrap();
        m.set_media("m1", &peer("p1"), Some(true), None, None).unwrap();
        let v = m.mute_peer("m1", &peer("host"), "p1", true).unwrap();
        let got: Vec<u64> = v.events.iter().map(|e| e.seq).collect();
        assert!(got.windows(2).all(|w| w[0] < w[1]), "事件 seq 必须严格递增：{got:?}");
        assert!(v.events.iter().all(|e| e.seq > s1), "跨操作 seq 必须全局递增");
        let log = m.events_of("m1").unwrap();
        assert!(log.windows(2).all(|w| w[0].seq < w[1].seq), "房间事件日志必须按 seq 升序");
        let ks: Vec<EventKind> = log.iter().map(|e| e.kind).collect();
        assert!(ks.contains(&EventKind::Created));
        assert!(ks.contains(&EventKind::Joined));
        assert!(ks.contains(&EventKind::MediaChanged));
        assert!(ks.contains(&EventKind::Muted));
        // 事件可序列化且 kind 是小写字符串
        let js = serde_json::to_string(&log[0]).unwrap();
        assert!(js.contains("\"kind\""), "kind 必须序列化为字符串：{js}");
    }

    #[test]
    fn pending_events_are_drained_per_response() {
        let m = mgr();
        let v = m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        let n1 = v.events.len();
        assert!(n1 >= 2, "创建应产生 Created + Joined：{n1}");
        // 响应已带走的不再排队
        assert!(m.take_pending_events().is_empty(), "事件随响应交付后队列应为空");
        let v1 = m.join("m1", &peer("p1")).unwrap();
        let v2 = m.join("m1", &peer("p2")).unwrap();
        let n2 = v1.events.len() + v2.events.len();
        assert_eq!(n2, 2, "两次 join 各一条事件");
        // 时间戳与固定时钟一致
        let both = [v1, v2];
        assert!(both.iter().all(|v| v.events.iter().all(|e| e.ts_unix == T0)));
    }

    // ── 资源回收（空房 5 分钟自动回收）──────────────────────────────

    #[test]
    fn grace_period_keeps_room_then_reclaims_it() {
        let mut cfg = qm_common::AppConfig::default();
        cfg.room.grace_secs = 300;
        let m = RoomManager::with_fixed_time(Arc::new(cfg), T0);
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        m.leave("m1", &peer("host")).unwrap();
        let f0 = m.memory_facts();
        assert_eq!(f0.rooms, 1, "空房在宽限期内必须保留（支持重连）");
        assert!(view(&m, "m1").reclaim_started);
        // 宽限期不到：不回收
        assert!(m.reap_expired_at(T0 + 200).is_empty());
        assert!(m.room_view("m1").is_ok(), "宽限期内房间仍可查询");
        // 宽限期刚到：仍保留（严格超过才回收）
        assert!(m.reap_expired_at(T0 + 299).is_empty());
        // 超过 300s：回收
        let gone = m.reap_expired_at(T0 + 301);
        assert_eq!(gone, vec!["m1".to_string()]);
        assert!(m.room_view("m1").is_err(), "回收后房间不可查询");
        let f = m.memory_facts();
        assert_eq!(
            f,
            MemoryFacts { rooms: 0, peers: 0, events: 0, appointments: 0 },
            "回收必须整体释放（无残留、无泄漏）：{f:?}"
        );
        // 事件里有 Destroyed（reason=grace_expired）
        let evs = m.take_pending_events();
        let e = evs.iter().find(|e| e.kind == EventKind::Destroyed).unwrap();
        assert!(e.detail.contains("grace_expired(300s)"), "回收原因必须可审计：{}", e.detail);
        // 真实 Instant 口径：宽限期从最后一次活动起算
        let mut cfg2 = qm_common::AppConfig::default();
        cfg2.room.grace_secs = 300;
        let m2 = RoomManager::new(Arc::new(cfg2));
        m2.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        let t0 = Instant::now();
        let until = m2.grace_until("m1").unwrap();
        assert!(until.is_none(), "仍有成员时不进入回收流程");
        m2.leave("m1", &peer("host")).unwrap();
        let until = m2.grace_until("m1").unwrap().expect("空房应有宽限截止");
        let left = (until - t0).as_secs();
        assert!(left <= 300 && left > 250, "宽限剩余应在 300s 内（实际 {left}s）");
    }

    #[test]
    fn reap_skips_rooms_with_members() {
        let mut cfg = qm_common::AppConfig::default();
        cfg.room.grace_secs = 300;
        let m = RoomManager::with_fixed_time(Arc::new(cfg), T0);
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        // 有人还在 → 不回收
        assert!(m.reap_expired_at(T0 + 10_000).is_empty());
        // 只有等候室人 → 也不回收（`admitted_count() == 0` 但房间仍在用）
        m.create_room(&CreateRoom { room_id: "m2".to_string(), password: Some("pw".to_string()), host: Some(peer("h2")), ..Default::default() })
            .unwrap();
        assert!(m.reap_expired_at(T0 + 10_000).is_empty());
        // 全部离开后才回收
        m.leave("m1", &peer("host")).unwrap();
        m.leave("m2", &peer("h2")).unwrap();
        let mut gone = m.reap_expired_at(T0 + 301);
        gone.sort();
        assert_eq!(gone, vec!["m1".to_string(), "m2".to_string()]);
        assert_eq!(m.memory_facts(), MemoryFacts::default());
    }

    #[test]
    fn destroy_room_releases_immediately() {
        let m = mgr();
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        m.join("m1", &peer("p1")).unwrap();
        let f0 = m.memory_facts();
        assert_eq!(f0.rooms, 1);
        // 普通参会者不能销毁
        assert_eq!(m.destroy_room("m1", &peer("p1")).unwrap_err(), RoomError::Forbidden);
        assert_eq!(m.memory_facts(), f0, "销毁失败不应改动状态");
        let v = m.destroy_room("m1", &peer("host")).unwrap();
        assert!(kinds(&v).contains(&EventKind::Destroyed));
        assert!(v.room.is_none() && v.peers.is_empty());
        assert_eq!(
            m.memory_facts(),
            MemoryFacts { rooms: 0, peers: 0, events: 0, appointments: 0 },
            "销毁必须立即释放"
        );
        // 销毁后房间号可以复用
        let v = create(&m, "m1", Some("host2")).unwrap();
        assert_eq!(v.room.as_ref().unwrap().id, "m1");
        // 已销毁的房间不可再操作
        m.destroy_room("m1", &peer("host2")).unwrap();
        match m.leave("m1", &peer("host")) {
            Err(RoomError::NotFound(id)) => assert_eq!(id, "m1"),
            other => panic!("销毁后离开应报 NotFound，实际 {other:?}"),
        }
        assert!(matches!(m.destroy_room("m1", &peer("host")), Err(RoomError::NotFound(_))));
    }

    #[test]
    fn event_log_is_bounded_by_max_events() {
        let mut cfg = qm_common::AppConfig::default();
        cfg.room.max_events = 5;
        let m = RoomManager::with_fixed_time(Arc::new(cfg), T0);
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        for i in 0..20u32 {
            let id = format!("p{i}");
            m.join("m1", &peer(&id)).unwrap();
            m.leave("m1", &peer(&id)).unwrap();
        }
        let evs = m.events_of("m1").unwrap();
        assert_eq!(evs.len(), 5, "事件队列必须按 room.max_events 截断");
        assert!(evs.windows(2).all(|w| w[0].seq < w[1].seq), "截断后仍保持 seq 顺序");
        // 房间 / 参会者计数不受事件截断影响
        let f = m.memory_facts();
        assert_eq!(f.rooms, 1);
        assert_eq!(f.peers, 1, "只有 host 还在");
        assert_eq!(f.events, 5);
    }

    #[test]
    fn activity_resets_the_grace_clock() {
        let mut cfg = qm_common::AppConfig::default();
        cfg.room.grace_secs = 100;
        let m = RoomManager::with_fixed_time(Arc::new(cfg), T0);
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        m.leave("m1", &peer("host")).unwrap(); // 空房 @ T0
        assert!(m.reap_expired_at(T0 + 99).is_empty());
        // 有人重新加入 → 活动时刻刷新，宽限期重新开始
        m.join("m1", &peer("back")).unwrap();
        let f = m.memory_facts();
        assert_eq!(f.peers, 1);
        m.set_fixed_time(T0 + 150);
        // 有人在场时不回收
        assert!(m.reap_expired_at(T0 + 199).is_empty(), "有人在场不得回收");
        // 重新变空后，宽限期从上一次真正的活动时刻（T0+150）起算
        m.leave("m1", &peer("back")).unwrap();
        assert!(m.reap_expired_at(T0 + 249).is_empty(), "宽限期未耗尽不得回收");
        let gone = m.reap_expired_at(T0 + 251);
        assert_eq!(gone, vec!["m1".to_string()]);
        assert_eq!(m.memory_facts(), MemoryFacts::default(), "回收后必须零残留");
    }

    #[test]
    fn leave_records_activity_and_not_in_room_is_rejected() {
        let m = mgr();
        match m.leave("ghost", &peer("p1")) {
            Err(RoomError::NotFound(id)) => assert_eq!(id, "ghost"),
            other => panic!("不存在的房间应报 NotFound，实际 {other:?}"),
        }
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        assert_eq!(m.leave("m1", &peer("ghost")).unwrap_err(), RoomError::NotInRoom);
        let v = m.leave("m1", &peer("host")).unwrap();
        assert!(v.room.as_ref().unwrap().reclaim_started, "全部离开后宽限期开始");
        assert!(v.room.as_ref().unwrap().last_activity_unix >= T0);
        assert!(v.room.as_ref().unwrap().password_set == false);
        assert!(matches!(m.leave("m1", &peer("host")), Err(RoomError::NotInRoom)));
    }

    // ── 状态同步延迟（Issue 强制约束 ≤100ms）────────────────────────

    #[test]
    fn sync_roundtrip_is_fast() {
        const N: u32 = 100;
        let m = mgr();
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), max_participants: Some(0), ..Default::default() })
            .unwrap();
        let start = Instant::now();
        let mut total = Duration::ZERO;
        for i in 0..N {
            let id = format!("x{i}");
            m.join("m1", &peer(&id)).unwrap();
            total += start.elapsed();
            m.peers_of("m1").unwrap();
            total += start.elapsed();
            m.leave("m1", &peer(&id)).unwrap();
            total += start.elapsed();
        }
        let avg = total / (N * 3);
        let budget = Duration::from_millis(100);
        assert!(
            avg <= budget,
            "单次状态操作平均 {avg:?} 超过 100ms 预算（约束：状态同步延迟 ≤100ms）"
        );
    }

    // ── 会议预约（原 YEJ-111 并入）──────────────────────────────────

    #[test]
    fn appt_create_update_cancel_roundtrip() {
        let m = mgr();
        let a = m.create_appt(&appt_req("standup", "h1", T0 + H, 30)).unwrap();
        assert!(a.id.starts_with("appt-"));
        assert_eq!(a.ends_at, T0 + H + 30 * M);
        assert!(!a.room_exists);
        assert_eq!(a.status, ApptStatus::Scheduled);
        // 修改（重设会议室号与提醒）
        let mut patch = ApptPatch::default();
        patch.room_id = "standup2".to_string();
        patch.reminder_mins = Some(10);
        patch.title = "  ".to_string(); // 空标题忽略
        let b = m.update_appt(&a.id, &patch).unwrap();
        assert_eq!(b.room_id, "standup2");
        assert_eq!(b.reminder_mins, 10);
        assert_eq!(b.starts_at, T0 + H, "未传的字段保持原值");
        assert_eq!(b.title, "例会", "空字符串表示保持原值");
        // 密码：设置 + 清除
        let mut p2 = ApptPatch::default();
        p2.password = Some("abc".to_string());
        let b2 = m.update_appt(&a.id, &p2).unwrap();
        assert!(b2.password_set);
        assert!(!serde_json::to_string(&b2).unwrap().contains("abc"), "快照不得包含明文密码");
        let mut p3 = ApptPatch::default();
        p3.password = Some(String::new());
        assert!(!m.update_appt(&a.id, &p3).unwrap().password_set);
        // 受邀名单
        let mut p4 = ApptPatch::default();
        p4.invitees = vec![Invitee { display_name: "小王".to_string(), contact: "wang@local".to_string() }];
        let b4 = m.update_appt(&a.id, &p4).unwrap();
        assert_eq!(b4.invitees.len(), 1);
        // 取消
        let c = m.cancel_appt(&a.id, "临时有事").unwrap();
        assert_eq!(c.status, ApptStatus::Cancelled);
        assert_eq!(c.cancel_reason, "临时有事");
        // 已取消不能重复取消 / 修改
        assert!(matches!(m.cancel_appt(&a.id, "x"), Err(RoomError::Invalid(_))));
        assert!(matches!(m.update_appt(&a.id, &ApptPatch::default()), Err(RoomError::Invalid(_))));
        match m.appt("nope") {
            Err(RoomError::NotFound(id)) => assert_eq!(id, "nope"),
            other => panic!("不存在的预约应报 NotFound，实际 {other:?}"),
        }
        // 按时间范围查询（验收：按会议 ID 与时间范围可查）
        assert_eq!(m.list_appts(Some(T0), Some(T0 + 2 * H)).len(), 1);
        assert!(m.list_appts(Some(T0 + 3 * H), None).is_empty());
        assert_eq!(m.appts_of_room("standup2").len(), 1);
        assert_eq!(m.appts_of_room("standup").len(), 0);
        assert_eq!(m.memory_facts().appointments, 1);
        // 预约 id 自增不重复
        let a2 = m.create_appt(&appt_req("r2", "h2", T0 + 2 * H, 15)).unwrap();
        let a3 = m.create_appt(&appt_req("r3", "h3", T0 + 3 * H, 15)).unwrap();
        assert_ne!(a2.id, a3.id);
    }

    #[test]
    fn appt_conflict_rules_allow_room_reuse_across_time() {
        let m = mgr();
        // 同会议室号时间重叠 → 冲突
        let a = m.create_appt(&appt_req("m1", "h1", T0 + H, 60)).unwrap();
        assert!(matches!(
            m.create_appt(&appt_req("m1", "h2", T0 + H + 30 * M, 30)),
            Err(RoomError::Conflict(_))
        ));
        // 不同会议室号但同一主持人重叠 → 也冲突
        assert!(matches!(
            m.create_appt(&appt_req("m2", "h1", T0 + H + 30 * M, 15)),
            Err(RoomError::Conflict(_))
        ));
        // 首尾相接不算冲突（半开区间 [starts, ends)）
        assert!(m.create_appt(&appt_req("m1", "h2", T0 + H + 60 * M, 30)).is_ok());
        // 完全错开：会议室号可跨时间段复用
        assert!(m.create_appt(&appt_req("m1", "h3", T0 + 5 * H, 30)).is_ok());
        // 已取消的预约释放时段
        m.cancel_appt(&a.id, "x").unwrap();
        assert!(m.create_appt(&appt_req("m1", "h4", T0 + H + 5 * M, 20)).is_ok());
        // 修改后必须重跑冲突规则：把 m1 的一场晚一点的预约挪进 h2 占用段
        let t3 = m.create_appt(&appt_req("m1", "h9", T0 + H + 90 * M, 30)).unwrap();
        let mut patch = ApptPatch::default();
        patch.starts_at = Some(T0 + 2 * H);
        assert!(matches!(m.update_appt(&t3.id, &patch), Err(RoomError::Conflict(_))));
        // 失败的修改不得改动原预约
        assert_eq!(m.appt(&t3.id).unwrap().starts_at, T0 + H + 90 * M);
        // 修改会议室号也要重跑
        let mut patch2 = ApptPatch::default();
        patch2.room_id = "m1".to_string();
        assert!(matches!(
            m.update_appt(&m.create_appt(&appt_req("m9", "h9", T0 + 2 * H, 30)).unwrap().id, &patch2),
            Err(RoomError::Conflict(_))
        ));
        // 已开始不能修改 / 取消
        let l = m.create_appt(&appt_req("m4", "h5", T0, 30)).unwrap();
        m.set_fixed_time(T0 + M);
        assert!(matches!(m.update_appt(&l.id, &ApptPatch::default()), Err(RoomError::Invalid(_))));
        assert!(matches!(m.cancel_appt(&l.id, "x"), Err(RoomError::Invalid(_))));
    }

    #[test]
    fn appt_rejects_bad_params() {
        let m = mgr();
        let bad = [
            ApptReq { title: "  ".to_string(), room_id: "m1".to_string(), host_id: "h".to_string(), host_name: "n".to_string(), starts_at: T0 + H, duration_mins: 30, ..Default::default() },
            ApptReq { title: "x".to_string(), room_id: "".to_string(), host_id: "h".to_string(), host_name: "n".to_string(), starts_at: T0 + H, duration_mins: 30, ..Default::default() },
            ApptReq { title: "x".to_string(), room_id: "m1".to_string(), host_id: "h".to_string(), host_name: "n".to_string(), starts_at: 0, duration_mins: 30, ..Default::default() },
            ApptReq { title: "x".to_string(), room_id: "m1".to_string(), host_id: "h".to_string(), host_name: "n".to_string(), starts_at: T0 + H, duration_mins: 0, ..Default::default() },
            ApptReq { title: "x".to_string(), room_id: "m1".to_string(), host_id: "h".to_string(), host_name: "n".to_string(), starts_at: T0 + H, duration_mins: 1, reminder_mins: 1, ..Default::default() },
            ApptReq { title: "x".to_string(), room_id: "m1".to_string(), host_id: "h".to_string(), host_name: "n".to_string(), starts_at: T0 + H, duration_mins: 30, reminder_mins: 40, ..Default::default() },
            ApptReq { title: "x".to_string(), room_id: "m1".to_string(), host_id: "".to_string(), host_name: "n".to_string(), starts_at: T0 + H, duration_mins: 30, ..Default::default() },
            ApptReq { title: "x".to_string(), room_id: "m1".to_string(), host_id: "h".to_string(), host_name: " ".to_string(), starts_at: T0 + H, duration_mins: 30, ..Default::default() },
            ApptReq { title: "x".to_string(), room_id: "m1".to_string(), host_id: "h".to_string(), host_name: "n".to_string(), starts_at: T0 + H, duration_mins: 30, invitees: vec![Invitee { display_name: " ".to_string(), contact: String::new() }], ..Default::default() },
            ApptReq { title: "x".to_string(), room_id: "m1".to_string(), host_id: "h".to_string(), host_name: "n".to_string(), starts_at: T0 + H, duration_mins: 30, max_participants: Some(MAX_PARTICIPANTS_CAP + 1), ..Default::default() },
        ];
        for r in bad {
            assert!(
                matches!(m.create_appt(&r), Err(RoomError::Invalid(_))),
                "参数非法的预约必须被拒绝：{r:?}"
            );
        }
        assert_eq!(m.memory_facts().appointments, 0);
    }

    #[test]
    fn tick_creates_room_reminds_dedups_and_destroys_after_ending() {
        let mut cfg = qm_common::AppConfig::default();
        cfg.room.grace_secs = 300;
        let m = RoomManager::with_fixed_time(Arc::new(cfg), T0);
        let a = m
            .create_appt(&ApptReq {
                title: "周会".to_string(),
                room_id: "weekly".to_string(),
                host_id: "host".to_string(),
                host_name: "李四".to_string(),
                starts_at: T0 + H,
                password: None,
                duration_mins: 30,
                reminder_mins: 10,
                max_participants: Some(10),
                invitees: vec![Invitee { display_name: "小王".to_string(), contact: "wang@local".to_string() }],
            })
            .unwrap();

        // 提醒窗口之前：不提醒也不建房
        let r0 = m.tick_at(T0 + H - 11 * M);
        assert!(r0.reminders.is_empty() && r0.rooms_created.is_empty());
        // 会前 10 分钟：提醒触发一次
        let r1 = m.tick_at(T0 + H - 10 * M);
        assert_eq!(r1.reminders.len(), 1);
        assert_eq!(r1.reminders[0].lead_mins, 10);
        assert_eq!(r1.reminders[0].targets, vec!["host".to_string(), "wang@local".to_string()]);
        assert!(r1.rooms_created.is_empty(), "未到开始时间不应建房");
        // 去重：重复 tick 不再提醒
        assert!(m.tick_at(T0 + H - 10 * M).reminders.is_empty(), "提醒必须去重");
        assert!(m.tick_at(T0 + H - 1 * M).reminders.is_empty(), "提醒只发一次");
        // 开始前 1 秒也不建房
        assert!(m.tick_at(T0 + H - 1).rooms_created.is_empty());

        // 到点：自动建房，主持人自动入会
        let r2 = m.tick_at(T0 + H);
        assert_eq!(r2.rooms_created, vec!["weekly".to_string()]);
        let r = view(&m, "weekly");
        assert_eq!(r.admitted_count, 1);
        assert_eq!(r.max_participants, Some(10));
        assert_eq!(r.ends_at_unix, Some(T0 + H + 30 * M));
        let peers = m.peers_of("weekly").unwrap();
        assert_eq!(peers[0].role, Role::Host);
        assert_eq!(peers[0].display_name, "李四");
        // 幂等：重复 tick 不重复建房
        assert!(m.tick_at(T0 + H + 5 * M).rooms_created.is_empty());
        assert_eq!(m.memory_facts().rooms, 1);
        // 预约里 room_created / reminder_fired 标记已置位
        assert!(m.appt(&a.id).unwrap().room_exists);
        assert!(m.appt(&a.id).unwrap().reminder_fired);

        // 会议结束：自动销毁（与 5 分钟回收规则对齐：会议房不靠宽限期）
        let r3 = m.tick_at(T0 + H + 30 * M);
        assert_eq!(r3.rooms_destroyed, vec!["weekly".to_string()]);
        assert!(m.room_view("weekly").is_err(), "预约结束必须销毁房间");
        assert_eq!(m.memory_facts().rooms, 0);
        // 已结束的预约不会再建房
        assert!(m.tick_at(T0 + 2 * H).rooms_created.is_empty());
    }

    #[test]
    fn tick_reminder_skipped_when_zero_mins() {
        let m = mgr();
        let a = m.create_appt(&appt_req("m1", "h1", T0 + H, 30)).unwrap();
        let mut p = ApptPatch::default();
        p.reminder_mins = Some(0);
        let a = m.update_appt(&a.id, &p).unwrap();
        assert_eq!(a.reminder_mins, 0);
        assert!(m.tick_at(T0 + H - 1 * M).reminders.is_empty(), "reminder_mins=0 表示不提醒");
        // 会议已开始的那一刻不再提醒
        let b = m.create_appt(&appt_req("m2", "h2", T0 + 2 * H, 30)).unwrap();
        assert!(m.tick_at(T0 + 2 * H).reminders.is_empty(), "会议已开始不再提醒");
        assert!(!m.appt(&b.id).unwrap().reminder_fired);
    }

    #[test]
    fn tick_reclaims_empty_appt_room_after_grace() {
        let mut cfg = qm_common::AppConfig::default();
        cfg.room.grace_secs = 300;
        let m = RoomManager::with_fixed_time(Arc::new(cfg), T0);
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        m.leave("m1", &peer("host")).unwrap();
        // 宽限期未到
        let r1 = m.tick_at(T0 + 100);
        assert!(r1.rooms_reclaimed.is_empty());
        assert!(m.room_view("m1").is_ok());
        // 超过宽限期 → 回收
        let r2 = m.tick_at(T0 + 301);
        assert_eq!(r2.rooms_reclaimed, vec!["m1".to_string()]);
        let f = m.memory_facts();
        assert_eq!(
            f,
            MemoryFacts { rooms: 0, peers: 0, events: 0, appointments: 0 },
            "回收后必须零残留：{f:?}"
        );
        // 幂等：空房不会被重复回收
        assert!(m.tick_at(T0 + 302).rooms_reclaimed.is_empty());
    }

    #[test]
    fn tick_keeps_undestroyed_appt_room_past_grace() {
        // 预约房间在会议进行中：即使超过宽限期也不回收
        let mut cfg = qm_common::AppConfig::default();
        cfg.room.grace_secs = 100;
        let m = RoomManager::with_fixed_time(Arc::new(cfg), T0);
        m.create_appt(&appt_req("m1", "h1", T0, 60)).unwrap();
        assert!(m.tick_at(T0 + 1).rooms_created.len() == 1);
        assert!(m.room_view("m1").is_ok());
        // 会议进行 30 分钟，远超过 100s 宽限期 —— 因为有主持人在场
        assert!(m.tick_at(T0 + 30 * M).rooms_reclaimed.is_empty());
        assert!(m.room_view("m1").is_ok(), "会议中的房间不能被宽限期回收");
        // 会议结束（60 分钟）→ 到期销毁
        let r = m.tick_at(T0 + 60 * M);
        assert_eq!(r.rooms_destroyed, vec!["m1".to_string()]);
    }

    // ── 查询一致性 / 序列化口径 ─────────────────────────────────────

    #[test]
    fn peers_view_orders_admitted_first() {
        let m = mgr();
        m.create_room(&CreateRoom { room_id: "m1".to_string(), password: Some("pw".to_string()), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        m.join("m1", &peer_pw("p1", "pw")).unwrap();
        m.join("m1", &peer("p2")).unwrap(); // 等候
        m.join("m1", &peer("p3")).unwrap(); // 等候
        let peers = m.peers_of("m1").unwrap();
        assert_eq!(peers.len(), 4);
        assert_eq!(
            peers.iter().take_while(|p| p.state == PeerState::Admitted).count(),
            2,
            "已入会者应排在前面"
        );
        assert_eq!(m.waitlist_of("m1").unwrap().len(), 2);
        assert!(m.waitlist_of("m1").unwrap().iter().all(|p| p.state == PeerState::Waiting));
        match m.peers_of("ghost") {
            Err(RoomError::NotFound(id)) => assert_eq!(id, "ghost"),
            other => panic!("不存在的房间应报 NotFound，实际 {other:?}"),
        }
        // 房间列表稳定可回放
        m.create_room(&CreateRoom { room_id: "aa".to_string(), host: Some(peer("h2")), ..Default::default() })
            .unwrap();
        let ids: Vec<String> = m.rooms().iter().map(|r| r.id.clone()).collect();
        assert!(ids.windows(2).all(|w| w[0] <= w[1]), "房间列表必须按房间号排序：{ids:?}");
        // 快照不泄漏明文密码
        let v = view(&m, "m1");
        assert!(v.password_set);
        assert!(!serde_json::to_string(&v).unwrap().contains("pw"), "快照不得包含明文密码");
        // 事件日志查询
        let log = m.events_of("m1").unwrap();
        assert!(log.iter().all(|e| e.ts_unix == T0));
        assert!(matches!(m.events_of("ghost"), Err(RoomError::NotFound(_))));
    }

    #[test]
    fn rooms_listing_is_sorted_and_stable() {
        let m = mgr();
        m.create_room(&CreateRoom { room_id: "z".to_string(), host: Some(peer("h")), ..Default::default() })
            .unwrap();
        m.create_room(&CreateRoom { room_id: "a".to_string(), host: Some(peer("h2")), ..Default::default() })
            .unwrap();
        let ids: Vec<String> = m.rooms().iter().map(|r| r.id.clone()).collect();
        assert_eq!(ids, vec!["a".to_string(), "z".to_string()]);
        // 连续两次结果完全一致（稳定可回放）
        assert_eq!(m.rooms(), m.rooms());
    }

    #[test]
    fn host_without_peer_still_creates_room() {
        // 无 host 的创建（由调度器 / 外部系统创建）也应合法
        let m = mgr();
        let v = m.create_room(&CreateRoom { room_id: "ops".to_string(), password: Some("k".to_string()), max_participants: Some(3), ..Default::default() })
            .unwrap();
        assert_eq!(v.peers.len(), 0);
        assert_eq!(v.peer, "system");
        assert_eq!(kinds(&v), vec![EventKind::Created]);
        assert!(view(&m, "ops").password_set);
        assert_eq!(view(&m, "ops").max_participants, Some(3));
        // 没有主持人 → 任何人都不能执行管控操作
        assert_eq!(m.destroy_room("ops", &peer("anyone")).unwrap_err(), RoomError::Forbidden);
        assert_eq!(m.memory_facts().rooms, 1);
    }

    #[test]
    fn cap_of_resolves_zero_as_unlimited() {
        // Some(0) = 不限制；None = 用配置默认值
        let mut cfg = qm_common::AppConfig::default();
        cfg.room.default_max_participants = 64;
        let m = RoomManager::with_fixed_time(Arc::new(cfg), T0);
        let v = m.create_room(&CreateRoom { room_id: "big".to_string(), max_participants: Some(0), host: Some(peer("h")), ..Default::default() })
            .unwrap();
        assert_eq!(v.room.as_ref().unwrap().max_participants, None, "0 表示不限制");
        for i in 0..70u32 {
            m.join("big", &peer(&format!("p{i}"))).unwrap();
        }
        let v = view(&m, "big");
        assert_eq!(v.admitted_count, 71, "不限制时 64 人默认值不应生效");
        assert!(!v.reclaim_started);
        let v = m.create_room(&CreateRoom { room_id: "dflt".to_string(), host: Some(peer("h2")), ..Default::default() })
            .unwrap();
        assert_eq!(v.room.as_ref().unwrap().max_participants, Some(64), "未指定时用配置默认值");
    }

    #[test]
    fn status_code_mapping_matches_the_doc_table() {
        let cases: [(RoomError, u16); 9] = [
            (RoomError::NotFound("x".to_string()), 404),
            (RoomError::Duplicate("x".to_string()), 409),
            (RoomError::Conflict("x".to_string()), 409),
            (RoomError::Unauthenticated, 403),
            (RoomError::WrongPassword, 403),
            (RoomError::NotInRoom, 403),
            (RoomError::NotParticipant, 403),
            (RoomError::Forbidden, 403),
            (RoomError::Invalid("x".to_string()), 400),
        ];
        for (e, code) in cases {
            assert_eq!(e.status_code(), code, "错误码映射错误：{e}");
            assert!(!e.to_string().is_empty(), "错误必须有人可读描述：{e}");
            assert_eq!(e.to_qm_error().kind(), qm_common::error::ErrorKind::Signaling, "{e}");
        }
    }

    #[test]
    fn status_and_display_are_stable_strings() {
        assert_eq!(Role::Host.to_string(), "host");
        assert_eq!(Role::CoHost.to_string(), "cohost");
        assert_eq!(Role::Participant.to_string(), "participant");
        assert_eq!(PeerState::Admitted.to_string(), "admitted");
        assert_eq!(PeerState::Waiting.to_string(), "waiting");
        assert_eq!(ApptStatus::Scheduled.to_string(), "scheduled");
        assert_eq!(ApptStatus::Cancelled.to_string(), "cancelled");
        let ev = RoomEvent {
            ts_unix: T0,
            seq: 1,
            kind: EventKind::RoleChanged,
            peer_id: "p".to_string(),
            by: "h".to_string(),
            detail: "role=cohost".to_string(),
        };
        let js = serde_json::to_string(&ev).unwrap();
        assert!(js.contains("\"kind\":\"role_changed\""), "事件 kind 必须序列化为小写字符串：{js}");
        // 快照序列化往返完全一致（客户端反序列化口径一致）
        let m = mgr();
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("h")), ..Default::default() })
            .unwrap();
        m.join("m1", &peer("p1")).unwrap();
        let peers = m.peers_of("m1").unwrap();
        let back: Vec<PeerView> = serde_json::from_str(&serde_json::to_string(&peers).unwrap()).unwrap();
        assert_eq!(back, peers, "快照序列化往返必须完全一致");
    }

    #[test]
    fn intranet_peer_check_rejects_public_and_bad_addresses() {
        let cfg = qm_common::AppConfig::default();
        assert!(ensure_intranet_peer(&cfg, "192.168.0.42:5060").is_ok(), "内网地址应放行");
        assert!(ensure_intranet_peer(&cfg, "8.8.8.8:5060").is_err(), "公网地址必须拒绝");
        assert!(ensure_intranet_peer(&cfg, "10.1.2.3:5060").is_err(), "未声明的内网段也拒绝");
        assert!(ensure_intranet_peer(&cfg, "not-an-ip").is_err(), "非法地址必须拒绝");
    }

    #[test]
    fn peer_ref_defaults_and_empty_ids_get_uuid() {
        let a = peer_ref_to_peer(&peer("h")).unwrap();
        assert_eq!(a.id, "h");
        assert_eq!(a.role, Role::Participant);
        assert_eq!(a.mic_effective(), false);
        // 空 id 自动分配 uuid，且两次分配不同
        let empty = PeerRef { address: "192.168.0.1:1".to_string(), ..Default::default() };
        let (x, y) = (peer_id_of(&empty), peer_id_of(&empty));
        assert!(x.len() > 8 && x != y, "空 peer_id 必须分配唯一 uuid：{x} / {y}");
        // display_name 回退到 peer id
        assert_eq!(display_name_of(&peer("p9")), "p9");
        let named = PeerRef { id: "p9".to_string(), display_name: Some("  ".to_string()), ..Default::default() };
        assert_eq!(display_name_of(&named), "p9", "空白昵称回退到 id");
    }






    #[test]
    fn clock_fixed_is_deterministic() {
        let m = mgr();
        m.create_room(&CreateRoom { room_id: "m1".to_string(), host: Some(peer("host")), ..Default::default() })
            .unwrap();
        assert_eq!(view(&m, "m1").created_at_unix, T0, "固定时钟必须可断言");
        m.set_fixed_time(T0 + 1000);
        m.join("m1", &peer("late")).unwrap();
        assert_eq!(view(&m, "m1").last_activity_unix, T0 + 1000);
    }
}
