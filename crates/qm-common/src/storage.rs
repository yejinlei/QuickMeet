//! 本地持久化（会议数据必须本地留存，禁止出域）。
//!
//! 提供两类原语：
//!   * [`append_jsonl`] — 追加写 JSON Lines，用于信令/事件审计日志，容错、非原子、顺序可靠；
//!   * [`atomic_write_json`] — 原子覆盖写 JSON，用于配置/状态快照，避免写一半读到损坏文件。

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::error::{Error, ErrorKind, Result};

/// 记录写入结果，便于上层统计。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteStats {
    pub bytes: u64,
    pub records: u64,
}

/// 以 JSON Lines 追加写入 `path`。
/// 每次调用写 1 行 + 换行并 `flush`，进程被强杀时最多丢失最后一次调用，
/// 已刷盘记录保持完整（不产生半行）。
pub fn append_jsonl<T: Serialize>(path: impl AsRef<Path>, records: &[T]) -> Result<WriteStats> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| Error::Storage(format!("打开 {path:?} 失败: {e}")))?;
    let mut stats = WriteStats::default();
    for r in records {
        let mut line =
            serde_json::to_string(r).map_err(|e| Error::Storage(format!("序列化失败: {e}")))?;
        line.push('\n');
        f.write_all(line.as_bytes())
            .map_err(|e| Error::Storage(format!("写入 {path:?} 失败: {e}")))?;
        stats.bytes += line.len() as u64;
        stats.records += 1;
    }
    f.flush()
        .map_err(|e| Error::Storage(format!("flush {path:?} 失败: {e}")))?;
    Ok(stats)
}

/// 原子覆盖写 JSON：先写临时文件再 rename，避免半写文件。
pub fn atomic_write_json<T: Serialize>(path: impl AsRef<Path>, value: &T) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let tmp = temp_path(path);
    {
        let data = serde_json::to_vec_pretty(value)
            .map_err(|e| Error::Storage(format!("序列化失败: {e}")))?;
        std::fs::write(&tmp, data)
            .map_err(|e| Error::Storage(format!("写入 {tmp:?} 失败: {e}")))?;
    }
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        Error::Storage(format!("rename {tmp:?} -> {path:?} 失败: {e}"))
    })?;
    Ok(())
}

/// 读回 JSON。
pub fn read_json<T: serde::de::DeserializeOwned>(path: impl AsRef<Path>) -> Result<T> {
    let data =
        std::fs::read(path.as_ref()).map_err(|e| Error::Storage(format!("读取失败: {e}")))?;
    serde_json::from_slice(&data).map_err(|e| Error::Storage(format!("解析失败: {e}")))
}

fn temp_path(path: &Path) -> PathBuf {
    let file = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "quickmeet.tmp".to_string());
    let mut tmp = file.clone();
    tmp.push_str(".tmp");
    path.with_file_name(tmp)
}

/// 校验错误分类。
pub fn storage_error_kind() -> ErrorKind {
    ErrorKind::Storage
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::path::PathBuf;

    #[derive(Debug, Serialize, Deserialize)]
    struct Rec {
        id: u32,
        msg: String,
    }

    fn tmpdir(name: &str) -> PathBuf {
        let p = PathBuf::from("./target/qm_storage_test").join(name);
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn append_jsonl_creates_file_and_is_readable() {
        let dir = tmpdir("append");
        let p = dir.join("events.jsonl");
        let a = append_jsonl(
            &p,
            &[Rec {
                id: 1,
                msg: "start".into(),
            }],
        )
        .unwrap();
        let b = append_jsonl(
            &p,
            &[
                Rec {
                    id: 2,
                    msg: "joined".into(),
                },
                Rec {
                    id: 3,
                    msg: "left".into(),
                },
            ],
        )
        .unwrap();
        assert_eq!(a.records, 1);
        assert_eq!(b.records, 2);
        let text = std::fs::read_to_string(&p).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3, "应为 3 行 JSONL: {text:?}");
        assert!(lines[0].contains("\"id\":1"));
        assert!(
            text.ends_with('\n'),
            "每行必须以换行结尾，保证进程被杀时不产生半行"
        );
        // 追加写不覆盖历史
        assert_eq!(append_jsonl::<Rec>(&p, &[]).unwrap().records, 0);
    }

    #[test]
    fn atomic_write_json_replace_and_reread() {
        let dir = tmpdir("atomic");
        let p = dir.join("state.json");
        atomic_write_json(
            &p,
            &Rec {
                id: 1,
                msg: "v1".into(),
            },
        )
        .unwrap();
        let v: Rec = read_json(&p).unwrap();
        assert_eq!(v.msg, "v1");
        atomic_write_json(
            &p,
            &Rec {
                id: 1,
                msg: "v2".into(),
            },
        )
        .unwrap();
        let v: Rec = read_json(&p).unwrap();
        assert_eq!(v.msg, "v2", "原子写后应读到新值");
        assert!(
            !dir.join("state.json.tmp").exists(),
            "成功后不应残留临时文件"
        );
    }

    #[test]
    fn missing_file_is_storage_error() {
        let dir = tmpdir("missing");
        let err = read_json::<Rec>(dir.join("nope.json")).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Storage);
    }

    #[test]
    fn io_error_maps_to_io_kind() {
        // 读取不存在的目录：底层 io 错误，归类为 Io。
        let e = Error::from(std::io::Error::new(std::io::ErrorKind::NotFound, "x"));
        assert_eq!(e.kind(), ErrorKind::Io);
        assert_eq!(storage_error_kind(), ErrorKind::Storage);
    }

    #[test]
    fn serialize_failure_is_storage_error() {
        use serde::ser::Error as SerdeError;

        // String 总是可序列化；用递归不可序列化结构验证错误路径。
        #[derive(Debug)]
        struct NotSerializable;
        impl Serialize for NotSerializable {
            fn serialize<S: serde::ser::Serializer>(
                &self,
                _s: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                Err(S::Error::custom("not serializable"))
            }
        }
        let dir = tmpdir("serfail");
        let e = atomic_write_json(dir.join("x.json"), &NotSerializable).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Storage);
    }
}
