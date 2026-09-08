use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const HEADER: &str = "# AgentRunLeaseTable v1";

#[derive(Debug, Clone)]
pub struct LeaseTableConfig {
    pub lock_retry_interval_ms: u64,
    pub lock_timeout_ms: u64,
    pub stale_lock_age_ms: u64,
    pub max_records: usize,
    pub max_key_bytes: usize,
    pub max_owner_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_line_bytes: usize,
    pub max_file_bytes: u64,
    pub sync_writes: bool,
}

impl Default for LeaseTableConfig {
    fn default() -> Self {
        Self {
            lock_retry_interval_ms: 25,
            lock_timeout_ms: 5_000,
            stale_lock_age_ms: 30_000,
            max_records: 100_000,
            max_key_bytes: 512,
            max_owner_bytes: 512,
            max_metadata_bytes: 4_096,
            max_line_bytes: 16_384,
            max_file_bytes: 32 * 1024 * 1024,
            sync_writes: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRecord {
    pub key: String,
    pub owner: String,
    pub fence: u64,
    pub acquired_at_ms: u64,
    pub renewed_at_ms: u64,
    pub expires_at_ms: u64,
    pub metadata: Option<String>,
}

impl LeaseRecord {
    pub fn is_expired_at(&self, now_ms: u64) -> bool {
        now_ms >= self.expires_at_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseEntrySnapshot {
    pub key: String,
    pub last_fence: u64,
    pub active: Option<LeaseRecord>,
    pub is_expired: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseSnapshot {
    pub generated_at_ms: u64,
    pub entries: Vec<LeaseEntrySnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireOutcome {
    Acquired {
        lease: LeaseRecord,
        replaced_expired: Option<LeaseRecord>,
    },
    HeldByOther {
        lease: LeaseRecord,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenewOutcome {
    Renewed {
        lease: LeaseRecord,
    },
    Missing,
    Expired {
        lease: LeaseRecord,
    },
    NotOwner {
        lease: LeaseRecord,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseOutcome {
    Released {
        key: String,
        last_fence: u64,
    },
    Missing,
    NotOwner {
        lease: LeaseRecord,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseErrorKind {
    InvalidArgument,
    CorruptState,
    Io,
    LockTimeout,
    NumericOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseError {
    pub kind: LeaseErrorKind,
    pub message: String,
}

impl LeaseError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: LeaseErrorKind::InvalidArgument,
            message: message.into(),
        }
    }

    fn corrupt(message: impl Into<String>) -> Self {
        Self {
            kind: LeaseErrorKind::CorruptState,
            message: message.into(),
        }
    }

    fn io(context: &'static str, error: io::Error) -> Self {
        Self {
            kind: LeaseErrorKind::Io,
            message: format!("{context}: {error}"),
        }
    }

    fn lock_timeout(message: impl Into<String>) -> Self {
        Self {
            kind: LeaseErrorKind::LockTimeout,
            message: message.into(),
        }
    }

    fn overflow(message: impl Into<String>) -> Self {
        Self {
            kind: LeaseErrorKind::NumericOverflow,
            message: message.into(),
        }
    }
}

impl Display for LeaseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for LeaseError {}

#[derive(Debug, Clone)]
struct EntryState {
    last_fence: u64,
    active: Option<LeaseRecord>,
}

impl Default for EntryState {
    fn default() -> Self {
        Self {
            last_fence: 0,
            active: None,
        }
    }
}

#[derive(Debug, Default, Clone)]
struct TableState {
    entries: BTreeMap<String, EntryState>,
}

impl TableState {
    fn snapshot(&self, now_ms: u64) -> LeaseSnapshot {
        let entries = self
            .entries
            .iter()
            .map(|(key, entry)| LeaseEntrySnapshot {
                key: key.clone(),
                last_fence: entry.last_fence,
                is_expired: entry
                    .active
                    .as_ref()
                    .is_some_and(|lease| lease.is_expired_at(now_ms)),
                active: entry.active.clone(),
            })
            .collect();

        LeaseSnapshot {
            generated_at_ms: now_ms,
            entries,
        }
    }
}

struct Mutation<T> {
    value: T,
    changed: bool,
}

pub struct AgentRunLeaseTable {
    table_path: PathBuf,
    lock_path: PathBuf,
    config: LeaseTableConfig,
}

impl AgentRunLeaseTable {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self::with_config(path, LeaseTableConfig::default())
    }

    pub fn with_config(path: impl Into<PathBuf>, config: LeaseTableConfig) -> Self {
        let table_path = path.into();
        let file_name = table_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("lease-table");
        let lock_path = table_parent(&table_path).join(format!(".{file_name}.lock"));

        Self {
            table_path,
            lock_path,
            config,
        }
    }

    pub fn acquire_now(
        &self,
        key: &str,
        owner: &str,
        ttl_ms: u64,
        metadata: Option<&str>,
    ) -> Result<AcquireOutcome, LeaseError> {
        self.acquire_at(key, owner, ttl_ms, metadata, current_unix_ms()?)
    }

    pub fn acquire_at(
        &self,
        key: &str,
        owner: &str,
        ttl_ms: u64,
        metadata: Option<&str>,
        now_ms: u64,
    ) -> Result<AcquireOutcome, LeaseError> {
        self.validate_key(key)?;
        self.validate_owner(owner)?;
        self.validate_metadata(metadata)?;
        if ttl_ms == 0 {
            return Err(LeaseError::invalid("ttl_ms must be greater than zero"));
        }

        self.with_locked_state(|state| {
            let is_new = !state.entries.contains_key(key);
            if is_new && state.entries.len() >= self.config.max_records {
                return Err(LeaseError::invalid(format!(
                    "refusing to exceed max_records limit of {}",
                    self.config.max_records
                )));
            }

            let entry = state.entries.entry(key.to_string()).or_default();
            if let Some(active) = entry.active.clone() {
                if !active.is_expired_at(now_ms) {
                    return Ok(Mutation {
                        value: AcquireOutcome::HeldByOther { lease: active },
                        changed: false,
                    });
                }
            }

            let replaced_expired = entry.active.take();
            let fence = entry
                .last_fence
                .checked_add(1)
                .ok_or_else(|| LeaseError::overflow("fence counter overflowed"))?;
            let expires_at_ms = checked_add_ms(now_ms, ttl_ms)?;
            let lease = LeaseRecord {
                key: key.to_string(),
                owner: owner.to_string(),
                fence,
                acquired_at_ms: now_ms,
                renewed_at_ms: now_ms,
                expires_at_ms,
                metadata: metadata.map(ToOwned::to_owned),
            };
            entry.last_fence = fence;
            entry.active = Some(lease.clone());

            Ok(Mutation {
                value: AcquireOutcome::Acquired {
                    lease,
                    replaced_expired,
                },
                changed: true,
            })
        })
    }

    pub fn renew_now(
        &self,
        key: &str,
        owner: &str,
        fence: u64,
        ttl_ms: u64,
        metadata: Option<&str>,
    ) -> Result<RenewOutcome, LeaseError> {
        self.renew_at(key, owner, fence, ttl_ms, metadata, current_unix_ms()?)
    }

    pub fn renew_at(
        &self,
        key: &str,
        owner: &str,
        fence: u64,
        ttl_ms: u64,
        metadata: Option<&str>,
        now_ms: u64,
    ) -> Result<RenewOutcome, LeaseError> {
        self.validate_key(key)?;
        self.validate_owner(owner)?;
        self.validate_metadata(metadata)?;
        if ttl_ms == 0 {
            return Err(LeaseError::invalid("ttl_ms must be greater than zero"));
        }

        self.with_locked_state(|state| {
            let Some(entry) = state.entries.get_mut(key) else {
                return Ok(Mutation {
                    value: RenewOutcome::Missing,
                    changed: false,
                });
            };

            let Some(active) = entry.active.clone() else {
                return Ok(Mutation {
                    value: RenewOutcome::Missing,
                    changed: false,
                });
            };

            if active.fence != fence || active.owner != owner {
                return Ok(Mutation {
                    value: RenewOutcome::NotOwner { lease: active },
                    changed: false,
                });
            }

            if active.is_expired_at(now_ms) {
                return Ok(Mutation {
                    value: RenewOutcome::Expired { lease: active },
                    changed: false,
                });
            }

            let updated = LeaseRecord {
                renewed_at_ms: now_ms,
                expires_at_ms: checked_add_ms(now_ms, ttl_ms)?,
                metadata: metadata
                    .map(ToOwned::to_owned)
                    .or_else(|| active.metadata.clone()),
                ..active
            };
            entry.active = Some(updated.clone());

            Ok(Mutation {
                value: RenewOutcome::Renewed { lease: updated },
                changed: true,
            })
        })
    }

    pub fn release(
        &self,
        key: &str,
        owner: &str,
        fence: u64,
    ) -> Result<ReleaseOutcome, LeaseError> {
        self.validate_key(key)?;
        self.validate_owner(owner)?;

        self.with_locked_state(|state| {
            let Some(entry) = state.entries.get_mut(key) else {
                return Ok(Mutation {
                    value: ReleaseOutcome::Missing,
                    changed: false,
                });
            };

            let Some(active) = entry.active.clone() else {
                return Ok(Mutation {
                    value: ReleaseOutcome::Missing,
                    changed: false,
                });
            };

            if active.fence != fence || active.owner != owner {
                return Ok(Mutation {
                    value: ReleaseOutcome::NotOwner { lease: active },
                    changed: false,
                });
            }

            entry.active = None;
            Ok(Mutation {
                value: ReleaseOutcome::Released {
                    key: key.to_string(),
                    last_fence: entry.last_fence,
                },
                changed: true,
            })
        })
    }

    pub fn snapshot_now(&self) -> Result<LeaseSnapshot, LeaseError> {
        self.snapshot_at(current_unix_ms()?)
    }

    pub fn snapshot_at(&self, now_ms: u64) -> Result<LeaseSnapshot, LeaseError> {
        let state = self.load_state()?;
        Ok(state.snapshot(now_ms))
    }

    pub fn sweep_expired_now(&self) -> Result<Vec<LeaseRecord>, LeaseError> {
        self.sweep_expired_at(current_unix_ms()?)
    }

    pub fn sweep_expired_at(&self, now_ms: u64) -> Result<Vec<LeaseRecord>, LeaseError> {
        self.with_locked_state(|state| {
            let mut removed = Vec::new();
            for entry in state.entries.values_mut() {
                if let Some(active) = entry.active.clone() {
                    if active.is_expired_at(now_ms) {
                        removed.push(active);
                        entry.active = None;
                    }
                }
            }

            Ok(Mutation {
                changed: !removed.is_empty(),
                value: removed,
            })
        })
    }

    fn validate_key(&self, key: &str) -> Result<(), LeaseError> {
        validate_field("key", key, self.config.max_key_bytes)
    }

    fn validate_owner(&self, owner: &str) -> Result<(), LeaseError> {
        validate_field("owner", owner, self.config.max_owner_bytes)
    }

    fn validate_metadata(&self, metadata: Option<&str>) -> Result<(), LeaseError> {
        if let Some(value) = metadata {
            validate_optional_field("metadata", value, self.config.max_metadata_bytes)?;
        }
        Ok(())
    }

    fn with_locked_state<T, F>(&self, mutator: F) -> Result<T, LeaseError>
    where
        F: FnOnce(&mut TableState) -> Result<Mutation<T>, LeaseError>,
    {
        let _lock = TableLock::acquire(&self.lock_path, &self.config)?;
        let mut state = self.load_state()?;
        let mutation = mutator(&mut state)?;
        if mutation.changed {
            self.persist_state(&state)?;
        }
        Ok(mutation.value)
    }

    fn load_state(&self) -> Result<TableState, LeaseError> {
        if !self.table_path.exists() {
            return Ok(TableState::default());
        }

        let file = File::open(&self.table_path)
            .map_err(|error| LeaseError::io("failed to open lease table", error))?;
        let metadata = file
            .metadata()
            .map_err(|error| LeaseError::io("failed to stat lease table", error))?;
        if metadata.len() > self.config.max_file_bytes {
            return Err(LeaseError::corrupt(format!(
                "lease table is {} bytes, which exceeds the configured limit of {} bytes",
                metadata.len(),
                self.config.max_file_bytes
            )));
        }

        let reader = BufReader::new(file);
        let mut state = TableState::default();
        let mut saw_header = false;

        for (index, line_result) in reader.lines().enumerate() {
            let line_number = index + 1;
            let line = line_result
                .map_err(|error| LeaseError::io("failed to read lease table line", error))?;
            if line.is_empty() {
                continue;
            }

            if !saw_header {
                if line != HEADER {
                    return Err(LeaseError::corrupt(format!(
                        "line {line_number} must be the header {HEADER:?}"
                    )));
                }
                saw_header = true;
                continue;
            }

            if line.len() > self.config.max_line_bytes {
                return Err(LeaseError::corrupt(format!(
                    "line {line_number} exceeds the configured line limit of {} bytes",
                    self.config.max_line_bytes
                )));
            }

            let fields = split_tsv(&line, 7, line_number)?;
            let key = unescape_field(fields[0], line_number, "key")?;
            validate_field("key", &key, self.config.max_key_bytes)?;

            let last_fence = parse_u64_field(fields[1], line_number, "last_fence")?;
            let owner = unescape_field(fields[2], line_number, "owner")?;
            let acquired_at_ms = parse_u64_field(fields[3], line_number, "acquired_at_ms")?;
            let renewed_at_ms = parse_u64_field(fields[4], line_number, "renewed_at_ms")?;
            let expires_at_ms = parse_u64_field(fields[5], line_number, "expires_at_ms")?;
            let metadata = unescape_field(fields[6], line_number, "metadata")?;

            let active = if owner.is_empty() {
                if acquired_at_ms != 0
                    || renewed_at_ms != 0
                    || expires_at_ms != 0
                    || !metadata.is_empty()
                {
                    return Err(LeaseError::corrupt(format!(
                        "line {line_number} has inactive data with non-zero fields"
                    )));
                }
                None
            } else {
                validate_field("owner", &owner, self.config.max_owner_bytes)?;
                validate_optional_field("metadata", &metadata, self.config.max_metadata_bytes)?;
                Some(LeaseRecord {
                    key: key.clone(),
                    owner,
                    fence: last_fence,
                    acquired_at_ms,
                    renewed_at_ms,
                    expires_at_ms,
                    metadata: if metadata.is_empty() {
                        None
                    } else {
                        Some(metadata)
                    },
                })
            };

            let previous = state.entries.insert(
                key,
                EntryState {
                    last_fence,
                    active,
                },
            );
            if previous.is_some() {
                return Err(LeaseError::corrupt(format!(
                    "line {line_number} defines a duplicate key"
                )));
            }
        }

        if !saw_header {
            return Ok(TableState::default());
        }

        Ok(state)
    }

    fn persist_state(&self, state: &TableState) -> Result<(), LeaseError> {
        let parent = table_parent(&self.table_path);
        fs::create_dir_all(parent)
            .map_err(|error| LeaseError::io("failed to create lease table directory", error))?;

        let tmp_path = temp_path_for(&self.table_path)?;
        let persist_result = (|| -> Result<(), LeaseError> {
            let mut file = File::create(&tmp_path)
                .map_err(|error| LeaseError::io("failed to create temporary lease table", error))?;
            writeln!(file, "{HEADER}")
                .map_err(|error| LeaseError::io("failed to write lease table header", error))?;

            for (key, entry) in &state.entries {
                let (owner, acquired_at_ms, renewed_at_ms, expires_at_ms, metadata) =
                    match &entry.active {
                        Some(lease) => (
                            lease.owner.as_str(),
                            lease.acquired_at_ms,
                            lease.renewed_at_ms,
                            lease.expires_at_ms,
                            lease.metadata.as_deref().unwrap_or(""),
                        ),
                        None => ("", 0, 0, 0, ""),
                    };

                writeln!(
                    file,
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    escape_field(key),
                    entry.last_fence,
                    escape_field(owner),
                    acquired_at_ms,
                    renewed_at_ms,
                    expires_at_ms,
                    escape_field(metadata)
                )
                .map_err(|error| LeaseError::io("failed to write lease table row", error))?;
            }

            if self.config.sync_writes {
                file.sync_all()
                    .map_err(|error| LeaseError::io("failed to fsync lease table", error))?;
            }
            Ok(())
        })();

        if let Err(error) = persist_result {
            let _ = fs::remove_file(&tmp_path);
            return Err(error);
        }

        #[cfg(windows)]
        if self.table_path.exists() {
            fs::remove_file(&self.table_path).map_err(|error| {
                LeaseError::io("failed to replace the existing lease table on Windows", error)
            })?;
        }

        fs::rename(&tmp_path, &self.table_path)
            .map_err(|error| LeaseError::io("failed to atomically replace lease table", error))?;

        if self.config.sync_writes {
            sync_parent_dir(parent)?;
        }

        Ok(())
    }
}

struct TableLock {
    path: PathBuf,
}

impl TableLock {
    fn acquire(lock_path: &Path, config: &LeaseTableConfig) -> Result<Self, LeaseError> {
        fs::create_dir_all(table_parent(lock_path))
            .map_err(|error| LeaseError::io("failed to create lock directory", error))?;

        let started_at_ms = current_unix_ms()?;
        loop {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(lock_path)
            {
                Ok(mut file) => {
                    let payload = format!(
                        "pid={}\ncreated_at_ms={}\n",
                        std::process::id(),
                        current_unix_ms()?
                    );
                    file.write_all(payload.as_bytes())
                        .map_err(|error| LeaseError::io("failed to write lock file", error))?;
                    if config.sync_writes {
                        file.sync_all()
                            .map_err(|error| LeaseError::io("failed to fsync lock file", error))?;
                    }
                    return Ok(Self {
                        path: lock_path.to_path_buf(),
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    if stale_lock_should_be_reclaimed(lock_path, config.stale_lock_age_ms)? {
                        match fs::remove_file(lock_path) {
                            Ok(()) => continue,
                            Err(remove_error)
                                if remove_error.kind() == io::ErrorKind::NotFound =>
                            {
                                continue
                            }
                            Err(remove_error) => {
                                return Err(LeaseError::io(
                                    "failed to remove stale lock file",
                                    remove_error,
                                ))
                            }
                        }
                    }

                    let now_ms = current_unix_ms()?;
                    if now_ms.saturating_sub(started_at_ms) >= config.lock_timeout_ms {
                        return Err(LeaseError::lock_timeout(format!(
                            "timed out waiting {} ms for lock {}",
                            config.lock_timeout_ms,
                            lock_path.display()
                        )));
                    }

                    let sleep_ms = config.lock_retry_interval_ms.max(1);
                    thread::sleep(Duration::from_millis(sleep_ms));
                }
                Err(error) => {
                    return Err(LeaseError::io(
                        "failed to create exclusive lock file",
                        error,
                    ))
                }
            }
        }
    }
}

impl Drop for TableLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn run_cli(args: Vec<String>) -> Result<String, LeaseError> {
    if args.len() < 2 {
        return Err(LeaseError::invalid(usage()));
    }

    match args[1].as_str() {
        "acquire" => {
            if args.len() < 6 {
                return Err(LeaseError::invalid(usage()));
            }
            let table = AgentRunLeaseTable::new(&args[2]);
            let ttl_ms = parse_duration_ms(&args[5])?;
            let metadata = if args.len() > 6 {
                Some(args[6..].join(" "))
            } else {
                None
            };
            let outcome = table.acquire_now(&args[3], &args[4], ttl_ms, metadata.as_deref())?;
            Ok(acquire_outcome_json(&outcome))
        }
        "renew" => {
            if args.len() < 7 {
                return Err(LeaseError::invalid(usage()));
            }
            let table = AgentRunLeaseTable::new(&args[2]);
            let fence = parse_u64_arg(&args[5], "fence")?;
            let ttl_ms = parse_duration_ms(&args[6])?;
            let metadata = if args.len() > 7 {
                Some(args[7..].join(" "))
            } else {
                None
            };
            let outcome =
                table.renew_now(&args[3], &args[4], fence, ttl_ms, metadata.as_deref())?;
            Ok(renew_outcome_json(&outcome))
        }
        "release" => {
            if args.len() != 6 {
                return Err(LeaseError::invalid(usage()));
            }
            let table = AgentRunLeaseTable::new(&args[2]);
            let fence = parse_u64_arg(&args[5], "fence")?;
            let outcome = table.release(&args[3], &args[4], fence)?;
            Ok(release_outcome_json(&outcome))
        }
        "inspect" => {
            if args.len() < 3 || args.len() > 4 {
                return Err(LeaseError::invalid(usage()));
            }
            let table = AgentRunLeaseTable::new(&args[2]);
            let snapshot = table.snapshot_now()?;
            if args.len() == 4 {
                let key = &args[3];
                let entry = snapshot.entries.iter().find(|entry| &entry.key == key);
                Ok(single_entry_snapshot_json(snapshot.generated_at_ms, entry))
            } else {
                Ok(snapshot_json(&snapshot))
            }
        }
        "sweep" => {
            if args.len() != 3 {
                return Err(LeaseError::invalid(usage()));
            }
            let table = AgentRunLeaseTable::new(&args[2]);
            let removed = table.sweep_expired_now()?;
            Ok(sweep_json(&removed))
        }
        _ => Err(LeaseError::invalid(usage())),
    }
}

fn main() {
    match run_cli(env::args().collect()) {
        Ok(output) => {
            println!("{output}");
        }
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    }
}

fn usage() -> String {
    [
        "Usage:",
        "  AgentRunLeaseTable acquire <table_path> <key> <owner> <ttl> [metadata]",
        "  AgentRunLeaseTable renew <table_path> <key> <owner> <fence> <ttl> [metadata]",
        "  AgentRunLeaseTable release <table_path> <key> <owner> <fence>",
        "  AgentRunLeaseTable inspect <table_path> [key]",
        "  AgentRunLeaseTable sweep <table_path>",
        "",
        "Duration examples: 45s, 5m, 1h30m, 1500ms",
    ]
    .join("\n")
}

fn validate_field(label: &str, value: &str, max_bytes: usize) -> Result<(), LeaseError> {
    if value.trim().is_empty() {
        return Err(LeaseError::invalid(format!("{label} must not be empty")));
    }
    if value.len() > max_bytes {
        return Err(LeaseError::invalid(format!(
            "{label} is {} bytes and exceeds the configured limit of {max_bytes}",
            value.len()
        )));
    }
    if value.contains('\0') {
        return Err(LeaseError::invalid(format!(
            "{label} must not contain NUL bytes"
        )));
    }
    Ok(())
}

fn validate_optional_field(label: &str, value: &str, max_bytes: usize) -> Result<(), LeaseError> {
    if value.len() > max_bytes {
        return Err(LeaseError::invalid(format!(
            "{label} is {} bytes and exceeds the configured limit of {max_bytes}",
            value.len()
        )));
    }
    if value.contains('\0') {
        return Err(LeaseError::invalid(format!(
            "{label} must not contain NUL bytes"
        )));
    }
    Ok(())
}

fn table_parent(path: &Path) -> &Path {
    path.parent().unwrap_or_else(|| Path::new("."))
}

fn current_unix_ms() -> Result<u64, LeaseError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| LeaseError::invalid(format!("system clock is before UNIX_EPOCH: {error}")))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| LeaseError::overflow("current time does not fit into u64 milliseconds"))
}

fn checked_add_ms(base: u64, delta: u64) -> Result<u64, LeaseError> {
    base.checked_add(delta)
        .ok_or_else(|| LeaseError::overflow("millisecond timestamp overflowed"))
}

fn parse_u64_arg(value: &str, label: &str) -> Result<u64, LeaseError> {
    value.parse::<u64>().map_err(|error| {
        LeaseError::invalid(format!("{label} must be an unsigned integer: {error}"))
    })
}

fn split_tsv<'a>(
    line: &'a str,
    expected_fields: usize,
    line_number: usize,
) -> Result<Vec<&'a str>, LeaseError> {
    let parts = line.split('\t').collect::<Vec<_>>();
    if parts.len() != expected_fields {
        return Err(LeaseError::corrupt(format!(
            "line {line_number} expected {expected_fields} tab-separated fields but found {}",
            parts.len()
        )));
    }
    Ok(parts)
}

fn parse_u64_field(value: &str, line_number: usize, label: &str) -> Result<u64, LeaseError> {
    value.parse::<u64>().map_err(|error| {
        LeaseError::corrupt(format!(
            "line {line_number} has an invalid {label} field: {error}"
        ))
    })
}

fn escape_field(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '\t' => escaped.push_str("\\t"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            other => escaped.push(other),
        }
    }
    escaped
}

fn unescape_field(value: &str, line_number: usize, label: &str) -> Result<String, LeaseError> {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }

        let Some(escaped) = chars.next() else {
            return Err(LeaseError::corrupt(format!(
                "line {line_number} has a trailing escape in the {label} field"
            )));
        };

        match escaped {
            '\\' => output.push('\\'),
            't' => output.push('\t'),
            'n' => output.push('\n'),
            'r' => output.push('\r'),
            other => {
                return Err(LeaseError::corrupt(format!(
                    "line {line_number} has an invalid escape sequence \\{other} in the {label} field"
                )))
            }
        }
    }
    Ok(output)
}

fn stale_lock_should_be_reclaimed(
    lock_path: &Path,
    stale_lock_age_ms: u64,
) -> Result<bool, LeaseError> {
    if stale_lock_age_ms == 0 {
        return Ok(true);
    }

    let metadata = match fs::metadata(lock_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(LeaseError::io("failed to stat existing lock file", error)),
    };

    let modified = metadata
        .modified()
        .map_err(|error| LeaseError::io("failed to read lock modification time", error))?;
    let age = SystemTime::now()
        .duration_since(modified)
        .unwrap_or_else(|_| Duration::from_secs(0));
    Ok(age.as_millis() >= u128::from(stale_lock_age_ms))
}

fn temp_path_for(target: &Path) -> Result<PathBuf, LeaseError> {
    let file_name = target
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("lease-table");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| LeaseError::invalid(format!("system clock is before UNIX_EPOCH: {error}")))?
        .as_nanos();
    Ok(table_parent(target).join(format!(
        ".{file_name}.tmp-{}-{suffix}",
        std::process::id()
    )))
}

#[cfg(unix)]
fn sync_parent_dir(parent: &Path) -> Result<(), LeaseError> {
    let dir = File::open(parent)
        .map_err(|error| LeaseError::io("failed to open parent directory for fsync", error))?;
    dir.sync_all()
        .map_err(|error| LeaseError::io("failed to fsync parent directory", error))
}

#[cfg(not(unix))]
fn sync_parent_dir(_parent: &Path) -> Result<(), LeaseError> {
    Ok(())
}

fn parse_duration_ms(input: &str) -> Result<u64, LeaseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(LeaseError::invalid("duration must not be empty"));
    }

    let chars = trimmed.as_bytes();
    let mut index = 0usize;
    let mut total_ms = 0u64;

    while index < chars.len() {
        let start = index;
        while index < chars.len() && chars[index].is_ascii_digit() {
            index += 1;
        }
        if start == index {
            return Err(LeaseError::invalid(format!(
                "duration {trimmed:?} must contain a number before each unit"
            )));
        }

        let number = trimmed[start..index].parse::<u64>().map_err(|error| {
            LeaseError::invalid(format!("duration component is not a valid integer: {error}"))
        })?;

        let multiplier = if trimmed[index..].starts_with("ms") {
            index += 2;
            1u64
        } else if trimmed[index..].starts_with('s') {
            index += 1;
            1_000u64
        } else if trimmed[index..].starts_with('m') {
            index += 1;
            60_000u64
        } else if trimmed[index..].starts_with('h') {
            index += 1;
            3_600_000u64
        } else if trimmed[index..].starts_with('d') {
            index += 1;
            86_400_000u64
        } else {
            return Err(LeaseError::invalid(format!(
                "duration component in {trimmed:?} is missing a unit; use ms, s, m, h, or d"
            )));
        };

        let component_ms = number.checked_mul(multiplier).ok_or_else(|| {
            LeaseError::overflow(format!("duration component {number} overflowed"))
        })?;
        total_ms = total_ms
            .checked_add(component_ms)
            .ok_or_else(|| LeaseError::overflow("total duration overflowed"))?;
    }

    if total_ms == 0 {
        return Err(LeaseError::invalid("duration must be greater than zero"));
    }

    Ok(total_ms)
}

fn json_escape(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 8);
    for ch in value.chars() {
        match ch {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{08}' => output.push_str("\\b"),
            '\u{0c}' => output.push_str("\\f"),
            control if control.is_control() => {
                output.push_str(&format!("\\u{:04x}", control as u32));
            }
            other => output.push(other),
        }
    }
    output
}

fn json_string(value: &str) -> String {
    format!("\"{}\"", json_escape(value))
}

fn option_json_string(value: Option<&str>) -> String {
    match value {
        Some(value) => json_string(value),
        None => "null".to_string(),
    }
}

fn json_array(items: &[String]) -> String {
    let mut output = String::from("[");
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        output.push_str(item);
    }
    output.push(']');
    output
}

fn lease_json(lease: &LeaseRecord) -> String {
    format!(
        "{{\"key\":{},\"owner\":{},\"fence\":{},\"acquired_at_ms\":{},\"renewed_at_ms\":{},\"expires_at_ms\":{},\"metadata\":{}}}",
        json_string(&lease.key),
        json_string(&lease.owner),
        lease.fence,
        lease.acquired_at_ms,
        lease.renewed_at_ms,
        lease.expires_at_ms,
        option_json_string(lease.metadata.as_deref())
    )
}

fn entry_snapshot_json(entry: &LeaseEntrySnapshot) -> String {
    let active = entry
        .active
        .as_ref()
        .map(lease_json)
        .unwrap_or_else(|| "null".to_string());
    format!(
        "{{\"key\":{},\"last_fence\":{},\"is_expired\":{},\"active\":{active}}}",
        json_string(&entry.key),
        entry.last_fence,
        if entry.is_expired { "true" } else { "false" },
    )
}

fn snapshot_json(snapshot: &LeaseSnapshot) -> String {
    let entries = snapshot
        .entries
        .iter()
        .map(entry_snapshot_json)
        .collect::<Vec<_>>();
    format!(
        "{{\"generated_at_ms\":{},\"entries\":{}}}",
        snapshot.generated_at_ms,
        json_array(&entries)
    )
}

fn single_entry_snapshot_json(generated_at_ms: u64, entry: Option<&LeaseEntrySnapshot>) -> String {
    let entry_json = entry
        .map(entry_snapshot_json)
        .unwrap_or_else(|| "null".to_string());
    format!(
        "{{\"generated_at_ms\":{},\"entry\":{entry_json}}}",
        generated_at_ms
    )
}

fn acquire_outcome_json(outcome: &AcquireOutcome) -> String {
    match outcome {
        AcquireOutcome::Acquired {
            lease,
            replaced_expired,
        } => format!(
            "{{\"kind\":\"acquired\",\"lease\":{},\"replaced_expired\":{}}}",
            lease_json(lease),
            replaced_expired
                .as_ref()
                .map(lease_json)
                .unwrap_or_else(|| "null".to_string())
        ),
        AcquireOutcome::HeldByOther { lease } => format!(
            "{{\"kind\":\"held_by_other\",\"lease\":{}}}",
            lease_json(lease)
        ),
    }
}

fn renew_outcome_json(outcome: &RenewOutcome) -> String {
    match outcome {
        RenewOutcome::Renewed { lease } => {
            format!("{{\"kind\":\"renewed\",\"lease\":{}}}", lease_json(lease))
        }
        RenewOutcome::Missing => "{\"kind\":\"missing\"}".to_string(),
        RenewOutcome::Expired { lease } => {
            format!("{{\"kind\":\"expired\",\"lease\":{}}}", lease_json(lease))
        }
        RenewOutcome::NotOwner { lease } => {
            format!("{{\"kind\":\"not_owner\",\"lease\":{}}}", lease_json(lease))
        }
    }
}

fn release_outcome_json(outcome: &ReleaseOutcome) -> String {
    match outcome {
        ReleaseOutcome::Released { key, last_fence } => format!(
            "{{\"kind\":\"released\",\"key\":{},\"last_fence\":{last_fence}}}",
            json_string(key)
        ),
        ReleaseOutcome::Missing => "{\"kind\":\"missing\"}".to_string(),
        ReleaseOutcome::NotOwner { lease } => {
            format!("{{\"kind\":\"not_owner\",\"lease\":{}}}", lease_json(lease))
        }
    }
}

fn sweep_json(removed: &[LeaseRecord]) -> String {
    let items = removed.iter().map(lease_json).collect::<Vec<_>>();
    format!(
        "{{\"kind\":\"swept\",\"removed_count\":{},\"removed\":{}}}",
        removed.len(),
        json_array(&items)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_table(label: &str) -> (AgentRunLeaseTable, PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "{label}-{}-{}.lease",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        (AgentRunLeaseTable::new(&path), path)
    }

    fn cleanup(path: &Path) {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("lease-table");
        let lock_path = table_parent(path).join(format!(".{file_name}.lock"));
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(lock_path);
    }

    #[test]
    fn acquires_first_lease_with_fence_one() {
        let (table, path) = temp_table("acquire-first");
        let outcome = table
            .acquire_at("job:42", "worker-a", 5_000, Some("kind=test"), 1_000)
            .unwrap();

        match outcome {
            AcquireOutcome::Acquired {
                lease,
                replaced_expired,
            } => {
                assert!(replaced_expired.is_none());
                assert_eq!(lease.fence, 1);
                assert_eq!(lease.expires_at_ms, 6_000);
            }
            other => panic!("expected acquired, got {other:?}"),
        }

        let snapshot = table.snapshot_at(1_500).unwrap();
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].last_fence, 1);
        assert!(!snapshot.entries[0].is_expired);
        cleanup(&path);
    }

    #[test]
    fn refuses_to_take_live_lease() {
        let (table, path) = temp_table("refuse-live");
        table.acquire_at("job:42", "worker-a", 5_000, None, 1_000)
            .unwrap();
        let outcome = table
            .acquire_at("job:42", "worker-b", 5_000, None, 2_000)
            .unwrap();

        match outcome {
            AcquireOutcome::HeldByOther { lease } => {
                assert_eq!(lease.owner, "worker-a");
                assert_eq!(lease.fence, 1);
            }
            other => panic!("expected held_by_other, got {other:?}"),
        }
        cleanup(&path);
    }

    #[test]
    fn steals_expired_lease_and_increments_fence() {
        let (table, path) = temp_table("steal-expired");
        table.acquire_at("job:42", "worker-a", 5_000, None, 1_000)
            .unwrap();
        let outcome = table
            .acquire_at("job:42", "worker-b", 5_000, Some("retry=1"), 7_000)
            .unwrap();

        match outcome {
            AcquireOutcome::Acquired {
                lease,
                replaced_expired,
            } => {
                assert_eq!(lease.owner, "worker-b");
                assert_eq!(lease.fence, 2);
                assert_eq!(replaced_expired.unwrap().owner, "worker-a");
            }
            other => panic!("expected acquired, got {other:?}"),
        }
        cleanup(&path);
    }

    #[test]
    fn renew_requires_matching_owner_and_fence() {
        let (table, path) = temp_table("renew-match");
        table.acquire_at("job:42", "worker-a", 5_000, None, 1_000)
            .unwrap();

        let not_owner = table
            .renew_at("job:42", "worker-b", 1, 5_000, None, 2_000)
            .unwrap();
        assert!(matches!(not_owner, RenewOutcome::NotOwner { .. }));

        let renewed = table
            .renew_at("job:42", "worker-a", 1, 8_000, Some("phase=stream"), 3_000)
            .unwrap();
        match renewed {
            RenewOutcome::Renewed { lease } => {
                assert_eq!(lease.expires_at_ms, 11_000);
                assert_eq!(lease.metadata.as_deref(), Some("phase=stream"));
            }
            other => panic!("expected renewed, got {other:?}"),
        }
        cleanup(&path);
    }

    #[test]
    fn release_preserves_last_fence_for_future_acquires() {
        let (table, path) = temp_table("release-preserve");
        table.acquire_at("job:42", "worker-a", 5_000, None, 1_000)
            .unwrap();
        let release = table.release("job:42", "worker-a", 1).unwrap();
        assert_eq!(
            release,
            ReleaseOutcome::Released {
                key: "job:42".to_string(),
                last_fence: 1
            }
        );

        let reacquired = table
            .acquire_at("job:42", "worker-b", 5_000, None, 2_000)
            .unwrap();
        match reacquired {
            AcquireOutcome::Acquired { lease, .. } => assert_eq!(lease.fence, 2),
            other => panic!("expected acquired, got {other:?}"),
        }
        cleanup(&path);
    }

    #[test]
    fn sweep_removes_only_expired_records() {
        let (table, path) = temp_table("sweep-expired");
        table.acquire_at("job:old", "worker-a", 1_000, None, 1_000)
            .unwrap();
        table.acquire_at("job:new", "worker-b", 10_000, None, 1_500)
            .unwrap();

        let removed = table.sweep_expired_at(3_000).unwrap();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].key, "job:old");

        let snapshot = table.snapshot_at(3_000).unwrap();
        let old = snapshot
            .entries
            .iter()
            .find(|entry| entry.key == "job:old")
            .unwrap();
        let new = snapshot
            .entries
            .iter()
            .find(|entry| entry.key == "job:new")
            .unwrap();

        assert!(old.active.is_none());
        assert!(new.active.is_some());
        cleanup(&path);
    }

    #[test]
    fn parses_compound_durations() {
        assert_eq!(parse_duration_ms("1500ms").unwrap(), 1_500);
        assert_eq!(parse_duration_ms("45s").unwrap(), 45_000);
        assert_eq!(parse_duration_ms("2m30s").unwrap(), 150_000);
        assert_eq!(parse_duration_ms("1h15m5s").unwrap(), 4_505_000);
    }

    #[test]
    fn field_escaping_round_trips() {
        let original = "line1\tline2\\line3\nline4\r";
        let escaped = escape_field(original);
        let roundtrip = unescape_field(&escaped, 2, "metadata").unwrap();
        assert_eq!(roundtrip, original);
    }
}

/*
This solves agent lease coordination for AI workers, MCP tool runners, queue consumers, cron jobs, GitHub Actions sidecars, and small DevOps services that need fencing tokens without pulling in Redis, Postgres advisory locks, or etcd. Built because a very common April 2026 problem is running background agent work on cheap ephemeral infrastructure where duplicate execution is worse than a retry, but the team still wants something dead simple they can audit in one file. A lot of shops have one machine, one PVC, or one shared workspace volume and just need safe lease semantics now.

Use it when you need exactly-once-ish ownership for a unit of work, plus heartbeat renewal, crash recovery, stale lock cleanup, and a fence number that lets downstream systems reject writes from an old worker after a lease has been stolen. The trick: this file keeps a durable last_fence value even after release, so a stale worker can never come back later with an old token and look current again. It also writes through a temp file and rename flow so the table is resilient across abrupt restarts.

Drop this into a Rust service, a CI helper binary, an agent orchestrator, a model evaluation runner, or a small edge control plane that has shared disk but does not justify a full coordination stack yet. Pavan can compile it as a tiny CLI for shell scripts or lift the `AgentRunLeaseTable` type straight into application code. Search terms people actually use are here on purpose: Rust distributed lock file, Rust lease table, fencing token example, crash safe file lock, AI agent worker deduplication, queue consumer leader election, and no-Redis job coordination.
*/