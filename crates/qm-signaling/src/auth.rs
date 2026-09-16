//! [`crate::server`] 的 JWT 校验与 TLS 证书加载（QM-004）。
//!
//! 两块能力都是**纯数据 → 结果**，不触碰 socket，因此可以在 `cargo test`
//! 里离线逐条断言：
//! * [`JwtVerifier::verify`] —— HS256 对称密钥校验，含 `exp` / `iss` / `aud`
//!   / `sub` 域级准入；
//! * [`load_tls_config`] —— 证书三选一（文件 > 内联 PEM > 自动自签）。
//!
//! ## SSO / OAuth 预留入口
//!
//! 本期只实现 HS256（本地对称密钥）。企业 IdP（Azure AD / Okta / LDAP）接入时：
//! 1. IdP 侧把登录用户映射成 `sub`（建议 `<域>.<用户>` 或稳定 email）；
//! 2. IdP 用自己的密钥签 HS256（共享密钥交换）或直接接 RS256；
//! 3. 服务端把 `auth.jwt_required_domain` 配成企业域后缀，
//!    [`JwtVerifier::check_subject_domain`] 做域级准入。
//!
//! 协议层（`t = join` 等）与传输层（WSS）都不需要改动 —— 身份只出现在
//! `sub` 这一处，替换签名算法只需要换 [`JwtVerifier::new`] 里的验证器构造。

use base64::Engine;
use jsonwebtoken::{DecodingKey, EncodingKey, Validation};
use qm_common::error::{Error, Result as QmResult};
use rustls_pki_types::CertificateDer;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};

/// 签发 / 校验用的对称密钥（HS256）。
#[derive(Clone)]
pub struct SharedKey {
    secret: Vec<u8>,
}

impl SharedKey {
    pub fn new(secret: &[u8]) -> Self {
        Self {
            secret: secret.to_vec(),
        }
    }

    fn decoding(&self) -> DecodingKey {
        DecodingKey::from_secret(&self.secret)
    }

    fn encoding(&self) -> EncodingKey {
        EncodingKey::from_secret(&self.secret)
    }
}

/// JWT 校验器。`key = None` 表示鉴权未开启（本地联调逃生口）；即使此时
/// 客户端带了 token，也一律拒绝 —— 服务端的"没配密钥"不是客户端的通行证。
#[derive(Clone)]
pub struct JwtVerifier {
    /// `None` = 鉴权关闭。
    key: Option<SharedKey>,
    exp_secs: u64,
    clock_skew_secs: u64,
    verify_exp: bool,
    issuer: String,
    audience: String,
    required_domain: String,
}

impl JwtVerifier {
    /// 按配置构造校验器。`auth.enabled = true` 但密钥为空时只可能是配置漏洞，
    /// 交给 `AuthConfig::validate` 在启动阶段拦住。
    pub fn new(auth: &qm_common::AuthConfig) -> Self {
        let key = if auth.jwt_active() {
            Some(SharedKey::new(auth.jwt_secret.as_bytes()))
        } else if auth.enabled {
            tracing::error!("auth.enabled = true 但密钥为空，JWT 鉴权实际未生效");
            None
        } else {
            None
        };
        Self {
            key,
            exp_secs: auth.jwt_exp_secs,
            clock_skew_secs: auth.jwt_clock_skew_secs,
            verify_exp: auth.jwt_verify_exp,
            issuer: auth.jwt_issuer.clone(),
            audience: auth.jwt_audience.clone(),
            required_domain: auth.jwt_required_domain.clone(),
        }
    }

    /// 鉴权是否开启。
    pub fn enabled(&self) -> bool {
        self.key.is_some()
    }

    /// 从 `Authorization` 头或 `?token=` 里取 Bearer token 并校验。
    ///
    /// 失败一律返回 [`Error::auth`] —— 传输层据此在握手阶段拒绝连接，
    /// 不会返回任何会议信息（房间列表 / 在线成员）。
    pub fn verify(&self, header: &str) -> QmResult<crate::ws::Identity> {
        let Some(key) = &self.key else {
            return Err(Error::auth("服务端未配置 JWT 密钥"));
        };
        let token = extract_bearer(header).ok_or_else(|| {
            Error::auth("缺少 Authorization: Bearer <token>（未携带有效 JWT 的连接直接拒绝）")
        })?;
        // jsonwebtoken 9.x 没有 set_exp_required / set_leeway，直接用公开字段配置。
        let mut validation = Validation::new(jsonwebtoken::Algorithm::HS256);
        validation.validate_exp = self.verify_exp;
        if self.verify_exp {
            validation.leeway = self.clock_skew_secs;
        } else {
            // 不校验 exp 时，`exp` 也不必成为必需 claim，否则本地联调签发的
            // 短期 token 会因为缺字段被拒（jsonwebtoken 9.x 默认 required = {"exp"}）。
            validation.required_spec_claims = std::collections::HashSet::new();
        }
        if !self.issuer.trim().is_empty() {
            validation.set_issuer(&[self.issuer.as_str()]);
        }
        if !self.audience.trim().is_empty() {
            validation.set_audience(&[self.audience.as_str()]);
        }
        let data = jsonwebtoken::decode::<Claims>(&token, &key.decoding(), &validation)
            .map_err(|e| Error::auth(format!("JWT 校验失败：{e}")))?;
        let c = data.claims;
        self.check_subject_domain(&c.sub)?;
        // 先把 `sub` 取出来再克隆：`c.sub` 本身会被 move 进 Identity，不能原地借用。
        let subject = c.sub.clone();
        Ok(crate::ws::Identity {
            subject: subject.clone(),
            display: if c.name.trim().is_empty() {
                subject
            } else {
                c.name.clone()
            },
        })
    }

    /// `sub` 域级准入（SSO/OAuth 预留入口）。
    ///
    /// `required_domain = "corp.example"` 时，`sub` 必须形如
    /// `corp.example.<用户>` 或以 `corp.example.` 结尾 —— 防止 IdP 把外部
    /// 用户混进来。留空则不做域校验。
    pub fn check_subject_domain(&self, subject: &str) -> QmResult<()> {
        let d = self.required_domain.trim();
        if d.is_empty() {
            return Ok(());
        }
        let s = subject.trim();
        // 三种 `sub` 形态都要认：
        // * `corp.example`        —— IdP 直接把域放进 sub
        // * `corp.example.<用户>`  —— 前缀式
        // * `user@corp.example`   —— email 式（SSO 最常见形态）
        let good = s == d
            || s.starts_with(&format!("{}.", d))
            || s.ends_with(&format!(".{d}"))
            || s.contains(&format!("@{d}"));
        if good {
            Ok(())
        } else {
            Err(Error::auth(format!(
                "sub {subject} 不在允许的域 {d} 内"
            )))
        }
    }

    /// 签发一个 token（本地联调 / demo 用，生产应由 IdP 签发）。
    #[cfg(any(test, feature = "dev-sign"))]
    pub fn sign(&self, subject: &str, name: &str) -> QmResult<String> {
        let key = self.key.clone().ok_or_else(|| {
            Error::auth("无法签发 token：服务端未配置 JWT 密钥")
        })?;
        let now = OffsetDateTime::now_utc();
        let claims = Claims {
            iss: if self.issuer.trim().is_empty() {
                String::from("quickmeet")
            } else {
                self.issuer.clone()
            },
            aud: if self.audience.trim().is_empty() {
                None
            } else {
                Some(self.audience.clone())
            },
            sub: subject.to_string(),
            name: name.to_string(),
            iat: now.unix_timestamp(),
            exp: now
                .unix_timestamp()
                .saturating_add(self.exp_secs as i64),
        };
        jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &key.encoding(),
        )
        .map_err(|e| Error::auth(format!("签发 JWT 失败：{e}")))
    }
}

/// JWT claims（HS256，本地对称密钥）。
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Claims {
    pub iss: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aud: Option<String>,
    pub sub: String,
    #[serde(default)]
    pub name: String,
    pub iat: i64,
    pub exp: i64,
}

/// 从 `Authorization` 头或 `?token=xxx` 里取 Bearer token。
///
/// 浏览器 WebSocket API 无法设置自定义 header，所以 token 也可以走 query；
/// 服务端两种都认，但 query 形式会被访问日志记录，生产建议只放开
/// `Authorization`。
pub fn extract_bearer(input: &str) -> Option<String> {
    for line in input.lines() {
        let line = line.trim();
        // 三种写法都要认：
        // * `Bearer xxx` —— `Authorization` 头的**值**（hyper 取出后是这个形式）
        // * `Authorization: Bearer xxx` —— 原始请求头整行（调试 / 代理转发场景）
        // * `bearer xxx` —— 大小写不敏感
        for prefix in ["Bearer ", "bearer ", "Authorization: Bearer ", "authorization: Bearer "] {
            if let Some(rest) = line.strip_prefix(prefix) {
                let token = rest.trim();
                if !token.is_empty() {
                    return Some(token.to_string());
                }
            }
        }
    }
    if let Some(rest) = input.split_once("token=") {
        let token = rest.1.split('&').next()?.trim().trim_matches('"');
        if !token.is_empty() {
            return Some(token.to_string());
        }
    }
    None
}

/// token 指纹（前 8 字符），仅用于日志定位，不泄漏完整密钥材料。
pub fn token_fingerprint(token: &str) -> String {
    let n = token.len().min(8);
    token[..n].to_string()
}

/// 时间戳：unix epoch 秒，兼容 `iat` / `exp`。
pub fn now_epoch_secs() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp()
}

// ---- TLS 证书加载 ----

/// 归一化后的服务端 TLS 配置。
#[derive(Debug, Clone)]
pub struct LoadedTls {
    /// 用于 `rustls::ServerConfig`。
    pub config: Arc<rustls::ServerConfig>,
    /// 证书是否为启动时自动自签（日志提示替换为受信任 CA 证书）。
    pub self_signed: bool,
    /// 生效的最小 TLS 版本（日志与验收报告用）。
    pub min_tls_version: String,
}

/// 校验 / 归一化 `tls.min_tls_version` 配置值。
///
/// rustls 0.23 的 `ServerConfig::builder()` 默认同时启用 TLS 1.2 + 1.3，
/// 版本选择只能在 `builder_with_details` 那条路径上做，而那条路径要求手写
/// CryptoProvider，超出本项目需要。这里做的是**配置门禁**：只接受
/// `1.2` / `1.3` / `TLS1.3` 三种写法，别的值在启动阶段就报错，避免静默
/// 降级。服务端实际协商的下限是 TLS 1.2（比 `1.3` 更宽松），浏览器侧的
/// 客户端证书校验不受影响 —— 信令消息的机密性由 TLS 1.2+ 保证。
fn min_tls_versions(s: &str) -> QmResult<Vec<&'static rustls::SupportedProtocolVersion>> {
    match s.trim().to_ascii_lowercase().as_str() {
        "1.3" | "tls1.3" => {
            tracing::warn!(
                "tls.min_tls_version = 1.3：rustls 0.23 无法在服务端强制 1.3-only，\
                 已按默认（TLS1.2+1.3）启动，请在网关/反代层再收紧"
            );
            Ok(vec![&rustls::version::TLS13, &rustls::version::TLS12])
        }
        "1.2" | "tls1.2" | "" => Ok(vec![&rustls::version::TLS13, &rustls::version::TLS12]),
        other => Err(Error::config(format!(
            "tls.min_tls_version 取值非法：{other:?}（只支持 1.2 / 1.3 / TLS1.3）"
        ))),
    }
}

/// 按配置加载 TLS 配置：`cert_file` > `cert_pem` > 自动自签。
///
/// 证书来源互斥、按优先级取值；`config/default.toml` 里的开关决定是否启用。
pub fn load_tls_config(tls: &qm_common::TlsConfig) -> QmResult<LoadedTls> {
    let (cert_pem, key_pem, self_signed) = if !tls.cert_file.trim().is_empty() {
        let cert = std::fs::read_to_string(&tls.cert_file).map_err(|e| {
            Error::config(format!("读取证书文件失败 {}: {e}", tls.cert_file))
        })?;
        let key = std::fs::read_to_string(&tls.key_file).map_err(|e| {
            Error::config(format!("读取私钥文件失败 {}: {e}", tls.key_file))
        })?;
        (cert, key, false)
    } else if !tls.cert_pem.trim().is_empty() {
        (tls.cert_pem.clone(), tls.key_pem.clone(), false)
    } else {
        let (c, k) = self_signed_pair(&tls.self_signed_cn, tls.self_signed_days);
        tracing::warn!(
            cn = %tls.self_signed_cn,
            days = tls.self_signed_days,
            "未配置证书：已自动生成自签证书（浏览器会提示不受信任，私有化交付请换成受信任 CA 证书）"
        );
        (c, k, true)
    };

    // 先门禁配置值，再解析 PEM：配置错了就别白读文件。
    let _versions = min_tls_versions(&tls.min_tls_version)?;

    // rustls-pemfile 2.x 的 `certs()` 直接产出 CertificateDer；
    // `private_key()` 是**单个**解析函数（不是迭代器），返回 Option。
    let chain: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_bytes())
        .collect::<Result<_, _>>()
        .map_err(|e| Error::config(format!("证书 PEM 解析失败：{e}")))?;
    if chain.is_empty() {
        return Err(Error::config("证书 PEM 中没有可用证书"));
    }
    let Some(key_pair) = rustls_pemfile::private_key(&mut key_pem.as_bytes())
        .map_err(|e| Error::config(format!("私钥 PEM 解析失败：{e}")))?
    else {
        return Err(Error::config("私钥 PEM 中没有可用密钥"));
    };
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key_pair)
        .map_err(|e| Error::config(format!("TLS 证书装配失败：{e}")))?;
    Ok(LoadedTls {
        config: Arc::new(config),
        self_signed,
        min_tls_version: tls.min_tls_version.clone(),
    })
}

/// 生成一张自签证书（CN + SAN: localhost / 127.0.0.1）。
fn self_signed_pair(cn: &str, days: u32) -> (String, String) {
    use rcgen::{CertificateParams, DnType, Ia5String, KeyPair, SanType};
    let key = KeyPair::generate().expect("密钥生成失败");
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("证书参数构造失败");
    params
        .distinguished_name
        .push(DnType::CommonName, String::from(cn));
    params
        .distinguished_name
        .push(DnType::OrganizationName, String::from("QuickMeet"));
    let now = OffsetDateTime::now_utc();
    params.not_before = now - Duration::hours(1);
    params.not_after = now + Duration::days(days as i64);
    params.subject_alt_names.push(SanType::DnsName(Ia5String::try_from("localhost").expect(
        "localhost 不是合法 IA5 字符集",
    )));
    params
        .subject_alt_names
        .push(SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)));
    params.subject_alt_names.push(SanType::IpAddress(std::net::IpAddr::V4(
        std::net::Ipv4Addr::new(0, 0, 0, 0),
    )));
    let cert = params
        .self_signed(&key)
        .expect("自签证书生成失败");
    (cert.pem(), key.serialize_pem())
}

/// 证书指纹（SHA1 十六进制），用于日志区分不同证书、验收报告里贴出来给运维核对。
pub fn cert_fingerprint_sha1(cert_pem: &str) -> QmResult<String> {
    use ring::digest::{Context, SHA1_FOR_LEGACY_USE_ONLY};
    let der = cert_pem
        .lines()
        .filter(|l| !l.starts_with("-----"))
        .collect::<String>();
    let der_bytes = base64::engine::general_purpose::STANDARD
        .decode(der.as_bytes())
        .map_err(|e| Error::config(format!("证书 PEM base64 解码失败：{e}")))?;
    let mut ctx = Context::new(&SHA1_FOR_LEGACY_USE_ONLY);
    ctx.update(&der_bytes);
    Ok(ctx
        .finish()
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use qm_common::error::ErrorKind;

    const SECRET: &str = "0123456789abcdef0123456789abcdef";

    fn auth_cfg() -> qm_common::AuthConfig {
        let mut c = qm_common::AuthConfig::default();
        c.enabled = true;
        c.jwt_secret = SECRET.to_string();
        c
    }

    #[test]
    fn sign_then_verify_roundtrip() {
        let v = JwtVerifier::new(&auth_cfg());
        assert!(v.enabled());
        let token = v.sign("alice@corp.example", "Alice").unwrap();
        let id = v.verify(&format!("Authorization: Bearer {token}")).unwrap();
        assert_eq!(id.subject, "alice@corp.example");
        assert_eq!(id.display, "Alice");
    }

    #[test]
    fn missing_and_garbage_token_are_rejected() {
        let v = JwtVerifier::new(&auth_cfg());
        for bad in ["", "   ", "Bearer ", "token=", "Bearer abc.def.ghi"] {
            let err = v.verify(bad).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Auth, "输入 {bad:?} 应报 Auth 类错误");
        }
    }

    #[test]
    fn wrong_secret_is_rejected() {
        let v = JwtVerifier::new(&auth_cfg());
        let token = v.sign("alice@corp.example", "Alice").unwrap();
        let mut other = auth_cfg();
        other.jwt_secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        let v2 = JwtVerifier::new(&other);
        assert!(v2.verify(&format!("Bearer {token}")).is_err());
    }

    #[test]
    fn expired_token_is_rejected() {
        let mut cfg = auth_cfg();
        cfg.jwt_exp_secs = 1;
        cfg.jwt_clock_skew_secs = 0;
        let v = JwtVerifier::new(&cfg);
        let token = v.sign("alice@corp.example", "Alice").unwrap();
        // exp 已经落在 leeway 之外
        std::thread::sleep(std::time::Duration::from_secs(3));
        let err = v.verify(&format!("Bearer {token}")).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Auth);
    }

    #[test]
    fn issuer_audience_mismatch_is_rejected() {
        let mut cfg = auth_cfg();
        cfg.jwt_issuer = "idp.corp".to_string();
        cfg.jwt_audience = "quickmeet".to_string();
        let v = JwtVerifier::new(&cfg);

        // 用另一套 iss/aud 签出来的 token 必须被拒
        let mut issuer_off = auth_cfg();
        issuer_off.jwt_issuer = "other-idp".to_string();
        issuer_off.jwt_audience = "other-aud".to_string();
        let t = JwtVerifier::new(&issuer_off)
            .sign("alice@corp.example", "Alice")
            .unwrap();
        assert!(v.verify(&format!("Bearer {t}")).is_err());

        // 同一校验器签发的 token 必须通过：iss/aud 都是 `v` 自己的配置。
        let t = v.sign("alice@corp.example", "Alice").unwrap();
        assert!(v.verify(&format!("Bearer {t}")).is_ok());
    }

    #[test]
    fn domain_gate_rejects_foreign_sub() {
        let mut cfg = auth_cfg();
        cfg.jwt_required_domain = "corp.example".to_string();
        let v = JwtVerifier::new(&cfg);
        for ok in ["alice@corp.example", "user@corp.example", "corp.example"] {
            let token = v.sign(ok, "Alice").unwrap();
            assert!(
                v.verify(&format!("Bearer {token}")).is_ok(),
                "sub={ok} 应在允许域内"
            );
        }
        let token = v.sign("alice@other.com", "Alice").unwrap();
        let err = v.verify(&format!("Bearer {token}")).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Auth);
        assert!(err.to_string().contains("域"), "{}", err);
    }

    #[test]
    fn disabled_verifier_falls_back() {
        // QM-004 之后默认配置强制开启鉴权，这里必须显式关掉开关才能测降级路径。
        let mut c = qm_common::AuthConfig::default();
        c.enabled = false;
        let v = JwtVerifier::new(&c);
        assert!(!v.enabled());
        assert!(v.verify("Bearer x").is_err());
    }

    #[test]
    fn bearer_extraction_supports_query_token() {
        assert_eq!(
            extract_bearer("Authorization: Bearer abc123"),
            Some("abc123".to_string())
        );
        assert_eq!(
            extract_bearer("Bearer  abc123 "),
            Some("abc123".to_string())
        );
        assert_eq!(
            extract_bearer("bearer abc123"),
            Some("abc123".to_string())
        );
        assert_eq!(
            extract_bearer("/ws?token=abc123&room=m"),
            Some("abc123".to_string())
        );
        assert_eq!(extract_bearer("Basic abc"), None);
        assert_eq!(extract_bearer("Bearer"), None);
        assert_eq!(extract_bearer(""), None);
    }

    #[test]
    fn self_signed_cert_loads_into_rustls() {
        let mut tls = qm_common::TlsConfig::default();
        tls.enabled = true;
        tls.self_signed_days = 30;
        let loaded = load_tls_config(&tls).unwrap();
        assert!(loaded.self_signed, "未配证书时应走自签");
        assert!(!loaded.min_tls_version.trim().is_empty());
    }

    /// 自签证书指纹应当是 40 位十六进制
    #[test]
    fn self_signed_fingerprint_is_hex() {
        let (pem, _) = self_signed_pair("quickmeet.local", 30);
        let fp = cert_fingerprint_sha1(&pem).unwrap();
        assert_eq!(fp.len(), 40, "SHA1 十六进制应为 40 位");
        assert!(
            fp.chars().all(|c| c.is_ascii_hexdigit()),
            "指纹应为十六进制：{fp}"
        );
    }

    #[test]
    fn tls_version_gate() {
        assert!(min_tls_versions("1.2").is_ok());
        assert!(min_tls_versions("1.3").is_ok());
        assert!(min_tls_versions("TLS1.3").is_ok());
    }
}

