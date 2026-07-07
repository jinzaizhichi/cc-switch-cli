//! 会话扫描元数据的 sidecar 缓存库。
//!
//! 主库 cc-switch.db 的 schema 与上游项目同步（WebDAV 亦会整库同步到其他
//! 机器），本仓库不得自行加表；而本缓存存的是机器本地的绝对路径且完全可
//! 重建，因此放在独立的 `session-scan-cache.db` 文件里：不参与任何同步，
//! 也无需版本化迁移——打开时幂等建表，结构不兼容时靠 `cache_version` 列
//! 整体失效（见 [`crate::session_manager::cache::SCAN_CACHE_VERSION`]）。
//!
//! 这是纯缓存：任何打开/读/写失败都应由调用方降级为"无缓存"（全量解析），
//! 绝不让缓存故障影响会话扫描本身。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use rusqlite::Connection;

use crate::config::{get_app_config_dir, resolve_config_dir_without_following_user_symlinks};
use crate::error::AppError;
use crate::session_manager::cache::{CachedScanRow, SessionScanCacheEntry};

/// sidecar 缓存库文件名（与主库同目录）。
const SCAN_CACHE_DB_FILE: &str = "session-scan-cache.db";

/// 会话扫描缓存的 sidecar SQLite 存储。
///
/// 与主库互不相干：自持连接、自建表，损坏时可直接删除文件重建。
pub struct ScanCacheStore {
    conn: Mutex<Connection>,
}

/// 使用统计增量同步的字节续传提示（存于 sidecar 的 `session_sync_resume` 表）。
///
/// 主库 `session_log_sync` 的 `(last_modified, last_line_offset)` 仍是权威进度；
/// 本提示只是加速手段：`(last_modified, last_line_offset)` 与权威行完全一致、
/// 且 `byte_offset` 前的尾部字节指纹（`tail_hash`）与文件当前内容吻合时，
/// 解析器才直接 seek 到 `byte_offset` 续读。任何不一致（整库从别的机器同步
/// 进来、文件被截断、同路径整体重写）都应忽略提示回退旧路径。
#[derive(Debug, Clone, PartialEq)]
pub struct SyncResumeHint {
    pub file_path: String,
    /// 对应主库 session_log_sync.last_modified 的快照。
    pub last_modified: i64,
    /// 对应主库 session_log_sync.last_line_offset 的快照。
    pub last_line_offset: i64,
    /// 上次处理完成时的字节位置（换行边界，不含末尾不完整行）。
    pub byte_offset: i64,
    /// 解析器续传状态 JSON（Codex 存整个状态机；Claude 存 session_id）。
    pub state: Option<String>,
    /// `byte_offset` 前至多 64 字节的 FNV-1a 指纹：识别"同路径被整体重写成
    /// 更大文件"的轮转场景（size/mtime 校验无法覆盖）。None 视为提示无效。
    pub tail_hash: Option<i64>,
}

impl ScanCacheStore {
    /// 打开（必要时创建）配置目录下的 sidecar 缓存库。
    pub fn open() -> Result<Self, AppError> {
        let config_dir = resolve_config_dir_without_following_user_symlinks(&get_app_config_dir())?;
        Self::open_at(&config_dir.join(SCAN_CACHE_DB_FILE))
    }

    /// 在指定路径打开缓存库（测试与 `open()` 共用）。
    pub fn open_at(path: &Path) -> Result<Self, AppError> {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| AppError::io(parent, e))?;
            }
        }
        let conn = Connection::open(path)
            .map_err(|e| AppError::Database(format!("打开会话扫描缓存库失败: {e}")))?;
        Self::from_connection(conn)
    }

    /// 内存缓存库（仅测试用）。
    #[cfg(test)]
    pub fn in_memory() -> Result<Self, AppError> {
        let conn = Connection::open_in_memory()
            .map_err(|e| AppError::Database(format!("打开内存缓存库失败: {e}")))?;
        Self::from_connection(conn)
    }

    fn from_connection(conn: Connection) -> Result<Self, AppError> {
        conn.busy_timeout(Duration::from_secs(5))
            .map_err(|e| AppError::Database(e.to_string()))?;
        // 与主库一致：WAL 降低写阻塞，NORMAL 免去逐次 COMMIT 的 fsync；
        // 纯缓存丢最新事务毫无影响。
        let _ = conn.pragma_update(None, "journal_mode", "WAL");
        let _ = conn.pragma_update(None, "synchronous", "NORMAL");

        conn.execute(
            "CREATE TABLE IF NOT EXISTS session_scan_cache (
                file_path TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                mtime_ns INTEGER NOT NULL,
                size INTEGER NOT NULL,
                meta TEXT NOT NULL,
                cache_version INTEGER NOT NULL
            )",
            [],
        )
        .map_err(|e| AppError::Database(format!("创建 session_scan_cache 表失败: {e}")))?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_session_scan_cache_provider
             ON session_scan_cache(provider)",
            [],
        )
        .map_err(|e| AppError::Database(format!("创建 session_scan_cache 索引失败: {e}")))?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS session_sync_resume (
                file_path TEXT PRIMARY KEY,
                last_modified INTEGER NOT NULL,
                last_line_offset INTEGER NOT NULL,
                byte_offset INTEGER NOT NULL,
                state TEXT,
                tail_hash INTEGER
            )",
            [],
        )
        .map_err(|e| AppError::Database(format!("创建 session_sync_resume 表失败: {e}")))?;
        // 纯本地缓存库无版本化迁移：旧文件缺列时就地补列，失败（列已存在）忽略。
        let _ = conn.execute(
            "ALTER TABLE session_sync_resume ADD COLUMN tail_hash INTEGER",
            [],
        );

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// sidecar 缓存库的磁盘路径（不打开连接；诊断/清理用）。
    #[allow(dead_code)]
    pub fn path() -> Result<PathBuf, AppError> {
        let config_dir = resolve_config_dir_without_following_user_symlinks(&get_app_config_dir())?;
        Ok(config_dir.join(SCAN_CACHE_DB_FILE))
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, AppError> {
        self.conn
            .lock()
            .map_err(|_| AppError::Database("会话扫描缓存库连接锁中毒".to_string()))
    }

    /// 读取某个 provider 的全部缓存行，键为绝对文件路径。
    pub fn load_for_provider(
        &self,
        provider: &str,
    ) -> Result<HashMap<String, CachedScanRow>, AppError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT file_path, mtime_ns, size, cache_version, meta
                 FROM session_scan_cache
                 WHERE provider = ?1",
            )
            .map_err(|e| AppError::Database(e.to_string()))?;

        let rows = stmt
            .query_map([provider], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    CachedScanRow {
                        mtime_ns: row.get::<_, i64>(1)?,
                        size: row.get::<_, i64>(2)?,
                        cache_version: row.get::<_, i64>(3)?,
                        meta_json: row.get::<_, String>(4)?,
                    },
                ))
            })
            .map_err(|e| AppError::Database(e.to_string()))?;

        let mut map = HashMap::new();
        for row in rows {
            let (file_path, cached) = row.map_err(|e| AppError::Database(e.to_string()))?;
            map.insert(file_path, cached);
        }
        Ok(map)
    }

    /// 读取缓存的 `SessionMeta` JSON（用于秒开快照）。`provider` 为 `None` 时返回
    /// 全部 provider 的行；只返回 `cache_version` 匹配当前版本的行，让整体版本失效
    /// 时快照自动为空、退回到全量扫描。
    pub fn load_meta_json(
        &self,
        provider: Option<&str>,
        cache_version: i64,
    ) -> Result<Vec<String>, AppError> {
        let conn = self.lock()?;
        let mut out = Vec::new();
        match provider {
            Some(provider) => {
                let mut stmt = conn
                    .prepare(
                        "SELECT meta FROM session_scan_cache
                         WHERE provider = ?1 AND cache_version = ?2",
                    )
                    .map_err(|e| AppError::Database(e.to_string()))?;
                let rows = stmt
                    .query_map(rusqlite::params![provider, cache_version], |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(|e| AppError::Database(e.to_string()))?;
                for row in rows {
                    out.push(row.map_err(|e| AppError::Database(e.to_string()))?);
                }
            }
            None => {
                let mut stmt = conn
                    .prepare("SELECT meta FROM session_scan_cache WHERE cache_version = ?1")
                    .map_err(|e| AppError::Database(e.to_string()))?;
                let rows = stmt
                    .query_map([cache_version], |row| row.get::<_, String>(0))
                    .map_err(|e| AppError::Database(e.to_string()))?;
                for row in rows {
                    out.push(row.map_err(|e| AppError::Database(e.to_string()))?);
                }
            }
        }
        Ok(out)
    }

    /// 在单个事务里批量写入（新增/更新）缓存行。
    pub fn upsert_batch(&self, entries: &[SessionScanCacheEntry]) -> Result<(), AppError> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut conn = self.lock()?;
        let tx = conn
            .transaction()
            .map_err(|e| AppError::Database(e.to_string()))?;
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO session_scan_cache
                        (file_path, provider, mtime_ns, size, meta, cache_version)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(file_path) DO UPDATE SET
                        provider = excluded.provider,
                        mtime_ns = excluded.mtime_ns,
                        size = excluded.size,
                        meta = excluded.meta,
                        cache_version = excluded.cache_version",
                )
                .map_err(|e| AppError::Database(e.to_string()))?;
            for entry in entries {
                stmt.execute(rusqlite::params![
                    entry.file_path,
                    entry.provider,
                    entry.mtime_ns,
                    entry.size,
                    entry.meta_json,
                    entry.cache_version,
                ])
                .map_err(|e| AppError::Database(e.to_string()))?;
            }
        }
        tx.commit().map_err(|e| AppError::Database(e.to_string()))?;
        Ok(())
    }

    /// 在单个事务里批量删除给定路径的缓存行。
    pub fn delete_paths(&self, paths: &[String]) -> Result<(), AppError> {
        if paths.is_empty() {
            return Ok(());
        }
        let mut conn = self.lock()?;
        let tx = conn
            .transaction()
            .map_err(|e| AppError::Database(e.to_string()))?;
        {
            let mut stmt = tx
                .prepare_cached("DELETE FROM session_scan_cache WHERE file_path = ?1")
                .map_err(|e| AppError::Database(e.to_string()))?;
            for path in paths {
                stmt.execute([path])
                    .map_err(|e| AppError::Database(e.to_string()))?;
            }
        }
        tx.commit().map_err(|e| AppError::Database(e.to_string()))?;
        Ok(())
    }

    /// 清空某个 provider 的全部缓存行。
    #[allow(dead_code)]
    pub fn clear_provider(&self, provider: &str) -> Result<(), AppError> {
        let conn = self.lock()?;
        conn.execute(
            "DELETE FROM session_scan_cache WHERE provider = ?1",
            [provider],
        )
        .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(())
    }

    /// 读取某个文件的字节续传提示。
    pub fn load_sync_resume(&self, file_path: &str) -> Result<Option<SyncResumeHint>, AppError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare_cached(
                "SELECT last_modified, last_line_offset, byte_offset, state, tail_hash
                 FROM session_sync_resume WHERE file_path = ?1",
            )
            .map_err(|e| AppError::Database(e.to_string()))?;
        let hint = stmt
            .query_row([file_path], |row| {
                Ok(SyncResumeHint {
                    file_path: file_path.to_string(),
                    last_modified: row.get(0)?,
                    last_line_offset: row.get(1)?,
                    byte_offset: row.get(2)?,
                    state: row.get(3)?,
                    tail_hash: row.get(4)?,
                })
            })
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(AppError::Database(other.to_string())),
            })?;
        Ok(hint)
    }

    /// 写入（覆盖）某个文件的字节续传提示。
    pub fn save_sync_resume(&self, hint: &SyncResumeHint) -> Result<(), AppError> {
        let conn = self.lock()?;
        conn.execute(
            "INSERT OR REPLACE INTO session_sync_resume
                (file_path, last_modified, last_line_offset, byte_offset, state, tail_hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                hint.file_path,
                hint.last_modified,
                hint.last_line_offset,
                hint.byte_offset,
                hint.state,
                hint.tail_hash,
            ],
        )
        .map_err(|e| AppError::Database(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_manager::cache::SCAN_CACHE_VERSION;

    fn entry(
        path: &str,
        provider: &str,
        mtime: i64,
        size: i64,
        version: i64,
    ) -> SessionScanCacheEntry {
        SessionScanCacheEntry {
            file_path: path.to_string(),
            provider: provider.to_string(),
            mtime_ns: mtime,
            size,
            meta_json: format!("{{\"providerId\":\"{provider}\",\"sessionId\":\"{path}\"}}"),
            cache_version: version,
        }
    }

    #[test]
    fn upsert_load_and_delete_roundtrip() {
        let store = ScanCacheStore::in_memory().expect("open memory store");
        store
            .upsert_batch(&[
                entry("/a.jsonl", "claude", 1, 10, SCAN_CACHE_VERSION),
                entry("/b.jsonl", "claude", 2, 20, SCAN_CACHE_VERSION),
                entry("/c.jsonl", "codex", 3, 30, SCAN_CACHE_VERSION),
            ])
            .expect("upsert");

        let claude = store.load_for_provider("claude").expect("load");
        assert_eq!(claude.len(), 2);
        assert_eq!(claude.get("/a.jsonl").unwrap().mtime_ns, 1);

        // Upsert replaces an existing row (same primary key).
        store
            .upsert_batch(&[entry("/a.jsonl", "claude", 99, 10, SCAN_CACHE_VERSION)])
            .expect("upsert replace");
        let claude = store.load_for_provider("claude").expect("reload");
        assert_eq!(claude.get("/a.jsonl").unwrap().mtime_ns, 99);

        store
            .delete_paths(&["/a.jsonl".to_string()])
            .expect("delete");
        let claude = store
            .load_for_provider("claude")
            .expect("reload after delete");
        assert_eq!(claude.len(), 1);
        assert!(claude.contains_key("/b.jsonl"));
    }

    #[test]
    fn meta_json_snapshot_filters_by_provider_and_version() {
        let store = ScanCacheStore::in_memory().expect("open memory store");
        store
            .upsert_batch(&[
                entry("/a.jsonl", "claude", 1, 10, SCAN_CACHE_VERSION),
                entry("/b.jsonl", "codex", 2, 20, SCAN_CACHE_VERSION),
                entry("/old.jsonl", "claude", 3, 30, SCAN_CACHE_VERSION - 1),
            ])
            .expect("upsert");

        let claude = store
            .load_meta_json(Some("claude"), SCAN_CACHE_VERSION)
            .expect("load claude");
        assert_eq!(claude.len(), 1); // old-version row is excluded

        let all = store
            .load_meta_json(None, SCAN_CACHE_VERSION)
            .expect("load all");
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn clear_provider_removes_only_that_provider() {
        let store = ScanCacheStore::in_memory().expect("open memory store");
        store
            .upsert_batch(&[
                entry("/a.jsonl", "claude", 1, 10, SCAN_CACHE_VERSION),
                entry("/c.jsonl", "codex", 3, 30, SCAN_CACHE_VERSION),
            ])
            .expect("upsert");

        store.clear_provider("claude").expect("clear");
        assert!(store.load_for_provider("claude").expect("load").is_empty());
        assert_eq!(store.load_for_provider("codex").expect("load").len(), 1);
    }

    #[test]
    fn open_at_creates_missing_parent_and_persists() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested").join("session-scan-cache.db");

        let store = ScanCacheStore::open_at(&path).expect("open");
        store
            .upsert_batch(&[entry("/a.jsonl", "claude", 1, 10, SCAN_CACHE_VERSION)])
            .expect("upsert");
        drop(store);

        // 重新打开同一文件应能读回已写入的行（幂等建表不清空数据）。
        let store = ScanCacheStore::open_at(&path).expect("reopen");
        assert_eq!(store.load_for_provider("claude").expect("load").len(), 1);
    }

    #[test]
    fn open_at_corrupt_file_reports_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("session-scan-cache.db");
        std::fs::write(&path, b"this is not a sqlite database, definitely").expect("write junk");

        // 打开或建表失败都必须以 Err 返回，交由调用方降级为无缓存扫描。
        let result = ScanCacheStore::open_at(&path)
            .and_then(|store| store.load_for_provider("claude").map(|_| store));
        assert!(result.is_err());
    }
}
