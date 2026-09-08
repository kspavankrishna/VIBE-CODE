use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Incrementally assembles interleaved tool-call deltas into deterministic,
/// validated call payloads without pulling in a JSON crate.
#[derive(Debug, Clone)]
pub struct AssemblerConfig {
    pub max_inflight_calls: usize,
    pub max_name_bytes: usize,
    pub max_argument_bytes: usize,
    pub max_fragments_per_call: usize,
    pub max_duplicate_fingerprints_per_call: usize,
    pub max_idle_ms: u64,
    pub auto_complete_on_valid_json: bool,
    pub require_explicit_final: bool,
    pub require_json_object_or_array: bool,
    pub allow_name_updates_after_arguments: bool,
}

impl Default for AssemblerConfig {
    fn default() -> Self {
        Self {
            max_inflight_calls: 128,
            max_name_bytes: 256,
            max_argument_bytes: 1_048_576,
            max_fragments_per_call: 4_096,
            max_duplicate_fingerprints_per_call: 256,
            max_idle_ms: 120_000,
            auto_complete_on_valid_json: true,
            require_explicit_final: false,
            require_json_object_or_array: true,
            allow_name_updates_after_arguments: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallDelta {
    pub stream_id: String,
    pub observed_at_ms: u64,
    pub sequence: Option<u64>,
    pub provider: Option<String>,
    pub call_id: Option<String>,
    pub index: Option<u32>,
    pub name_fragment: Option<String>,
    pub arguments_fragment: Option<String>,
    pub is_final: bool,
}

impl ToolCallDelta {
    pub fn new(stream_id: impl Into<String>, observed_at_ms: u64) -> Self {
        Self {
            stream_id: stream_id.into(),
            observed_at_ms,
            sequence: None,
            provider: None,
            call_id: None,
            index: None,
            name_fragment: None,
            arguments_fragment: None,
            is_final: false,
        }
    }

    pub fn with_sequence(mut self, sequence: u64) -> Self {
        self.sequence = Some(sequence);
        self
    }

    pub fn with_provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = Some(provider.into());
        self
    }

    pub fn with_call_id(mut self, call_id: impl Into<String>) -> Self {
        self.call_id = Some(call_id.into());
        self
    }

    pub fn with_index(mut self, index: u32) -> Self {
        self.index = Some(index);
        self
    }

    pub fn with_name_fragment(mut self, fragment: impl Into<String>) -> Self {
        self.name_fragment = Some(fragment.into());
        self
    }

    pub fn with_arguments_fragment(mut self, fragment: impl Into<String>) -> Self {
        self.arguments_fragment = Some(fragment.into());
        self
    }

    pub fn final_fragment(mut self) -> Self {
        self.is_final = true;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinishReason {
    JsonCompleted,
    ExplicitFinal,
    FinalizedByCaller,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    IdleTimeout,
    EmptyName,
    EmptyArguments,
    NameAfterArguments,
    NameTooLarge,
    ArgumentsTooLarge,
    TooManyFragments,
    MalformedJson,
    IncompleteJson,
    SequenceRegression,
}

impl Display for RejectReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::IdleTimeout => "idle timeout",
            Self::EmptyName => "missing tool name",
            Self::EmptyArguments => "missing tool arguments",
            Self::NameAfterArguments => "tool name changed after arguments started",
            Self::NameTooLarge => "tool name too large",
            Self::ArgumentsTooLarge => "tool arguments too large",
            Self::TooManyFragments => "too many fragments",
            Self::MalformedJson => "malformed json",
            Self::IncompleteJson => "incomplete json",
            Self::SequenceRegression => "sequence regression",
        };
        f.write_str(label)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedToolCall {
    pub key: String,
    pub stream_id: String,
    pub provider: Option<String>,
    pub call_id: Option<String>,
    pub index: Option<u32>,
    pub name: String,
    pub arguments: String,
    pub fragment_count: usize,
    pub first_seen_ms: u64,
    pub last_update_ms: u64,
    pub reason: RejectReason,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssembledToolCall {
    pub key: String,
    pub fingerprint: u64,
    pub stream_id: String,
    pub provider: Option<String>,
    pub call_id: Option<String>,
    pub index: Option<u32>,
    pub name: String,
    pub arguments: String,
    pub argument_bytes: usize,
    pub fragment_count: usize,
    pub first_seen_ms: u64,
    pub last_update_ms: u64,
    pub finish_reason: FinishReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallSnapshot {
    pub key: String,
    pub stream_id: String,
    pub provider: Option<String>,
    pub call_id: Option<String>,
    pub index: Option<u32>,
    pub name: String,
    pub arguments: String,
    pub argument_bytes: usize,
    pub fragment_count: usize,
    pub first_seen_ms: u64,
    pub last_update_ms: u64,
    pub json_started: bool,
    pub json_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AssemblerUpdate {
    pub accepted_key: Option<String>,
    pub completed: Vec<AssembledToolCall>,
    pub rejected: Vec<RejectedToolCall>,
    pub pending: Vec<ToolCallSnapshot>,
    pub duplicate_suppressed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssemblyErrorKind {
    EmptyStreamId,
    InvalidAnonymousUpgrade,
    MaxInflightCallsExceeded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssemblyError {
    pub kind: AssemblyErrorKind,
    pub message: String,
}

impl Display for AssemblyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for AssemblyError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootKind {
    Object,
    Array,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum JsonTrackerError {
    InvalidStart(char),
    UnexpectedCloser(char),
    MismatchedCloser { expected: char, got: char },
    UnterminatedString,
    TrailingNonWhitespace(char),
}

impl Display for JsonTrackerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidStart(ch) => write!(f, "json must start with '{{' or '[' but saw {ch:?}"),
            Self::UnexpectedCloser(ch) => write!(f, "unexpected closing delimiter {ch:?}"),
            Self::MismatchedCloser { expected, got } => {
                write!(f, "expected closing delimiter {expected:?} but saw {got:?}")
            }
            Self::UnterminatedString => f.write_str("json string literal is unterminated"),
            Self::TrailingNonWhitespace(ch) => {
                write!(f, "unexpected trailing non-whitespace character {ch:?}")
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
struct JsonBoundaryTracker {
    root_kind: Option<RootKind>,
    stack: Vec<char>,
    in_string: bool,
    escape_next: bool,
    started: bool,
    complete: bool,
}

impl JsonBoundaryTracker {
    fn ingest(&mut self, fragment: &str) -> Result<(), JsonTrackerError> {
        for ch in fragment.chars() {
            if self.complete {
                if !ch.is_whitespace() {
                    return Err(JsonTrackerError::TrailingNonWhitespace(ch));
                }
                continue;
            }

            if !self.started {
                if ch.is_whitespace() {
                    continue;
                }
                self.started = true;
                match ch {
                    '{' => {
                        self.root_kind = Some(RootKind::Object);
                        self.stack.push('{');
                    }
                    '[' => {
                        self.root_kind = Some(RootKind::Array);
                        self.stack.push('[');
                    }
                    other => return Err(JsonTrackerError::InvalidStart(other)),
                }
                continue;
            }

            if self.in_string {
                if self.escape_next {
                    self.escape_next = false;
                    continue;
                }
                match ch {
                    '\\' => self.escape_next = true,
                    '"' => self.in_string = false,
                    _ => {}
                }
                continue;
            }

            match ch {
                '"' => self.in_string = true,
                '{' | '[' => self.stack.push(ch),
                '}' | ']' => {
                    let open = self
                        .stack
                        .pop()
                        .ok_or(JsonTrackerError::UnexpectedCloser(ch))?;
                    let expected = match open {
                        '{' => '}',
                        '[' => ']',
                        _ => unreachable!("tracker only stores json delimiters"),
                    };
                    if expected != ch {
                        return Err(JsonTrackerError::MismatchedCloser { expected, got: ch });
                    }
                    if self.stack.is_empty() {
                        self.complete = true;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn validate_for_finish(&self) -> Result<(), JsonTrackerError> {
        if self.in_string {
            return Err(JsonTrackerError::UnterminatedString);
        }
        if !self.complete {
            return Err(JsonTrackerError::UnexpectedCloser('∅'));
        }
        Ok(())
    }

    fn is_started(&self) -> bool {
        self.started
    }

    fn is_complete(&self) -> bool {
        self.complete
    }

    fn root_kind(&self) -> Option<RootKind> {
        self.root_kind
    }
}

#[derive(Debug, Clone)]
struct CallState {
    key: String,
    stream_id: String,
    provider: Option<String>,
    call_id: Option<String>,
    index: Option<u32>,
    name: String,
    arguments: String,
    tracker: JsonBoundaryTracker,
    first_seen_ms: u64,
    last_update_ms: u64,
    last_sequence: Option<u64>,
    fragment_count: usize,
    saw_arguments: bool,
    seen_fingerprints: HashSet<u64>,
    fingerprint_order: VecDeque<u64>,
}

impl CallState {
    fn new(key: String, delta: &ToolCallDelta) -> Self {
        Self {
            key,
            stream_id: delta.stream_id.clone(),
            provider: delta.provider.clone(),
            call_id: delta.call_id.clone(),
            index: delta.index,
            name: String::new(),
            arguments: String::new(),
            tracker: JsonBoundaryTracker::default(),
            first_seen_ms: delta.observed_at_ms,
            last_update_ms: delta.observed_at_ms,
            last_sequence: None,
            fragment_count: 0,
            saw_arguments: false,
            seen_fingerprints: HashSet::new(),
            fingerprint_order: VecDeque::new(),
        }
    }

    fn snapshot(&self) -> ToolCallSnapshot {
        ToolCallSnapshot {
            key: self.key.clone(),
            stream_id: self.stream_id.clone(),
            provider: self.provider.clone(),
            call_id: self.call_id.clone(),
            index: self.index,
            name: self.name.clone(),
            arguments: self.arguments.clone(),
            argument_bytes: self.arguments.len(),
            fragment_count: self.fragment_count,
            first_seen_ms: self.first_seen_ms,
            last_update_ms: self.last_update_ms,
            json_started: self.tracker.is_started(),
            json_complete: self.tracker.is_complete(),
        }
    }
}

#[derive(Debug, Default)]
pub struct InterleavedToolCallAssembler {
    config: AssemblerConfig,
    pending: HashMap<String, CallState>,
}

impl InterleavedToolCallAssembler {
    pub fn new() -> Self {
        Self::with_config(AssemblerConfig::default())
    }

    pub fn with_config(config: AssemblerConfig) -> Self {
        Self {
            config,
            pending: HashMap::new(),
        }
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn snapshot(&self) -> Vec<ToolCallSnapshot> {
        self.sorted_pending_snapshots()
    }

    pub fn ingest(&mut self, delta: ToolCallDelta) -> Result<AssemblerUpdate, AssemblyError> {
        if delta.stream_id.trim().is_empty() {
            return Err(AssemblyError {
                kind: AssemblyErrorKind::EmptyStreamId,
                message: "stream_id must not be empty".to_string(),
            });
        }

        let mut update = AssemblerUpdate::default();
        self.collect_idle(delta.observed_at_ms, &mut update);

        let key = self.resolve_key_for_delta(&delta)?;
        update.accepted_key = Some(key.clone());

        if !self.pending.contains_key(&key) {
            if self.pending.len() >= self.config.max_inflight_calls {
                return Err(AssemblyError {
                    kind: AssemblyErrorKind::MaxInflightCallsExceeded,
                    message: format!(
                        "cannot track more than {} inflight tool calls",
                        self.config.max_inflight_calls
                    ),
                });
            }
            self.pending
                .insert(key.clone(), CallState::new(key.clone(), &delta));
        }

        let mut state = self
            .pending
            .remove(&key)
            .expect("pending call must exist immediately after insertion");

        if let Some(provider) = &delta.provider {
            if state.provider.is_none() {
                state.provider = Some(provider.clone());
            }
        }
        if let Some(call_id) = &delta.call_id {
            if state.call_id.is_none() {
                state.call_id = Some(call_id.clone());
            }
        }

        if let Some(sequence) = delta.sequence {
            if let Some(last_sequence) = state.last_sequence {
                if sequence < last_sequence {
                    update.rejected.push(self.reject_state(
                        state,
                        delta.observed_at_ms,
                        RejectReason::SequenceRegression,
                        format!(
                            "sequence {sequence} is older than the last accepted fragment {last_sequence}"
                        ),
                    ));
                    update.pending = self.sorted_pending_snapshots();
                    return Ok(update);
                }
            }
            state.last_sequence = Some(sequence);
        }

        let fingerprint = fingerprint_delta(&delta);
        if remember_fingerprint(
            &mut state.seen_fingerprints,
            &mut state.fingerprint_order,
            fingerprint,
            self.config.max_duplicate_fingerprints_per_call,
        ) {
            state.last_update_ms = delta.observed_at_ms;
            update.duplicate_suppressed = true;
            self.pending.insert(key, state);
            update.pending = self.sorted_pending_snapshots();
            return Ok(update);
        }

        state.fragment_count += 1;
        if state.fragment_count > self.config.max_fragments_per_call {
            update.rejected.push(self.reject_state(
                state,
                delta.observed_at_ms,
                RejectReason::TooManyFragments,
                format!(
                    "fragment count {} exceeded the configured limit {}",
                    self.config.max_fragments_per_call + 1,
                    self.config.max_fragments_per_call
                ),
            ));
            update.pending = self.sorted_pending_snapshots();
            return Ok(update);
        }

        state.last_update_ms = delta.observed_at_ms;

        if let Some(fragment) = delta.name_fragment.as_deref() {
            if state.saw_arguments && !self.config.allow_name_updates_after_arguments {
                update.rejected.push(self.reject_state(
                    state,
                    delta.observed_at_ms,
                    RejectReason::NameAfterArguments,
                    "received an additional name fragment after arguments had started".to_string(),
                ));
                update.pending = self.sorted_pending_snapshots();
                return Ok(update);
            }
            state.name.push_str(fragment);
            let name_len = state.name.len();
            if name_len > self.config.max_name_bytes {
                update.rejected.push(self.reject_state(
                    state,
                    delta.observed_at_ms,
                    RejectReason::NameTooLarge,
                    format!(
                        "tool name is {} bytes and exceeds the configured limit {}",
                        name_len,
                        self.config.max_name_bytes
                    ),
                ));
                update.pending = self.sorted_pending_snapshots();
                return Ok(update);
            }
        }

        if let Some(fragment) = delta.arguments_fragment.as_deref() {
            state.saw_arguments = true;
            state.arguments.push_str(fragment);
            let argument_len = state.arguments.len();
            if argument_len > self.config.max_argument_bytes {
                update.rejected.push(self.reject_state(
                    state,
                    delta.observed_at_ms,
                    RejectReason::ArgumentsTooLarge,
                    format!(
                        "arguments are {} bytes and exceed the configured limit {}",
                        argument_len,
                        self.config.max_argument_bytes
                    ),
                ));
                update.pending = self.sorted_pending_snapshots();
                return Ok(update);
            }

            if let Err(error) = state.tracker.ingest(fragment) {
                update.rejected.push(self.reject_state(
                    state,
                    delta.observed_at_ms,
                    RejectReason::MalformedJson,
                    error.to_string(),
                ));
                update.pending = self.sorted_pending_snapshots();
                return Ok(update);
            }
        }

        let should_attempt_completion = delta.is_final
            || (!self.config.require_explicit_final
                && self.config.auto_complete_on_valid_json
                && state.tracker.is_complete());

        if should_attempt_completion {
            match self.complete_state(
                state,
                if delta.is_final {
                    FinishReason::ExplicitFinal
                } else {
                    FinishReason::JsonCompleted
                },
            ) {
                Ok(call) => update.completed.push(call),
                Err(rejected) => update.rejected.push(rejected),
            }
        } else {
            self.pending.insert(key, state);
        }

        update.pending = self.sorted_pending_snapshots();
        Ok(update)
    }

    pub fn expire_idle(&mut self, now_ms: u64) -> Vec<RejectedToolCall> {
        let mut rejected = Vec::new();
        self.collect_idle_into(now_ms, &mut rejected);
        rejected
    }

    pub fn finalize_all(&mut self, now_ms: u64) -> AssemblerUpdate {
        let mut update = AssemblerUpdate::default();
        self.collect_idle(now_ms, &mut update);

        let keys = self
            .pending
            .keys()
            .cloned()
            .collect::<Vec<String>>();
        for key in keys {
            let state = self
                .pending
                .remove(&key)
                .expect("key taken from pending map must still exist");
            match self.complete_state(state, FinishReason::FinalizedByCaller) {
                Ok(call) => update.completed.push(call),
                Err(rejected) => update.rejected.push(rejected),
            }
        }

        update.pending = self.sorted_pending_snapshots();
        update
    }

    fn collect_idle(&mut self, now_ms: u64, update: &mut AssemblerUpdate) {
        self.collect_idle_into(now_ms, &mut update.rejected);
    }

    fn collect_idle_into(&mut self, now_ms: u64, rejected: &mut Vec<RejectedToolCall>) {
        if self.config.max_idle_ms == 0 {
            return;
        }

        let stale_keys = self
            .pending
            .iter()
            .filter_map(|(key, state)| {
                if now_ms.saturating_sub(state.last_update_ms) > self.config.max_idle_ms {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();

        for key in stale_keys {
            if let Some(state) = self.pending.remove(&key) {
                let idle_for_ms = now_ms.saturating_sub(state.last_update_ms);
                rejected.push(self.reject_state(
                    state,
                    now_ms,
                    RejectReason::IdleTimeout,
                    format!("no fragment arrived for {} ms", idle_for_ms),
                ));
            }
        }
    }

    fn resolve_key_for_delta(&mut self, delta: &ToolCallDelta) -> Result<String, AssemblyError> {
        if let Some(call_id) = delta.call_id.as_deref() {
            let id_key = make_id_key(&delta.stream_id, call_id);
            if self.pending.contains_key(&id_key) {
                return Ok(id_key);
            }

            if let Some(index) = delta.index {
                let index_key = make_index_key(&delta.stream_id, index);
                if let Some(mut state) = self.pending.remove(&index_key) {
                    state.key = id_key.clone();
                    state.call_id = Some(call_id.to_string());
                    self.pending.insert(id_key.clone(), state);
                    return Ok(id_key);
                }
            }

            return Ok(id_key);
        }

        if let Some(index) = delta.index {
            return Ok(make_index_key(&delta.stream_id, index));
        }

        let anonymous_key = make_anonymous_key(&delta.stream_id);
        if self.pending.contains_key(&anonymous_key) {
            return Ok(anonymous_key);
        }

        let has_any_anonymous_upgrade = self
            .pending
            .keys()
            .any(|key| key.starts_with(&format!("{}::anonymous", delta.stream_id)));
        if has_any_anonymous_upgrade {
            return Err(AssemblyError {
                kind: AssemblyErrorKind::InvalidAnonymousUpgrade,
                message: format!(
                    "stream {} already contains an anonymous in-flight call and cannot disambiguate another one",
                    delta.stream_id
                ),
            });
        }

        Ok(anonymous_key)
    }

    fn complete_state(
        &self,
        state: CallState,
        finish_reason: FinishReason,
    ) -> Result<AssembledToolCall, RejectedToolCall> {
        let last_update_ms = state.last_update_ms;
        if state.name.trim().is_empty() {
            return Err(self.reject_state(
                state,
                last_update_ms,
                RejectReason::EmptyName,
                "tool call finished without a usable name".to_string(),
            ));
        }
        if state.arguments.trim().is_empty() {
            return Err(self.reject_state(
                state,
                last_update_ms,
                RejectReason::EmptyArguments,
                "tool call finished without any arguments".to_string(),
            ));
        }
        if let Err(error) = state.tracker.validate_for_finish() {
            let reason = match error {
                JsonTrackerError::UnexpectedCloser('∅') => RejectReason::IncompleteJson,
                _ => RejectReason::MalformedJson,
            };
            return Err(self.reject_state(state, last_update_ms, reason, error.to_string()));
        }
        if self.config.require_json_object_or_array && state.tracker.root_kind().is_none() {
            return Err(self.reject_state(
                state,
                last_update_ms,
                RejectReason::MalformedJson,
                "arguments never established a top-level json object or array".to_string(),
            ));
        }

        let argument_bytes = state.arguments.len();
        Ok(AssembledToolCall {
            key: state.key,
            fingerprint: fingerprint_call(&state.name, &state.arguments),
            stream_id: state.stream_id,
            provider: state.provider,
            call_id: state.call_id,
            index: state.index,
            name: state.name,
            arguments: state.arguments,
            argument_bytes,
            fragment_count: state.fragment_count,
            first_seen_ms: state.first_seen_ms,
            last_update_ms: state.last_update_ms,
            finish_reason,
        })
    }

    fn reject_state(
        &self,
        state: CallState,
        at_ms: u64,
        reason: RejectReason,
        detail: String,
    ) -> RejectedToolCall {
        RejectedToolCall {
            key: state.key,
            stream_id: state.stream_id,
            provider: state.provider,
            call_id: state.call_id,
            index: state.index,
            name: state.name,
            arguments: state.arguments,
            fragment_count: state.fragment_count,
            first_seen_ms: state.first_seen_ms,
            last_update_ms: at_ms.max(state.last_update_ms),
            reason,
            detail,
        }
    }

    fn sorted_pending_snapshots(&self) -> Vec<ToolCallSnapshot> {
        let mut items = self
            .pending
            .values()
            .map(CallState::snapshot)
            .collect::<Vec<_>>();
        items.sort_by(|left, right| {
            left.first_seen_ms
                .cmp(&right.first_seen_ms)
                .then_with(|| left.key.cmp(&right.key))
        });
        items
    }
}

fn remember_fingerprint(
    fingerprints: &mut HashSet<u64>,
    order: &mut VecDeque<u64>,
    fingerprint: u64,
    max_entries: usize,
) -> bool {
    if fingerprints.contains(&fingerprint) {
        return true;
    }

    fingerprints.insert(fingerprint);
    order.push_back(fingerprint);

    while order.len() > max_entries {
        if let Some(oldest) = order.pop_front() {
            fingerprints.remove(&oldest);
        }
    }
    false
}

fn make_id_key(stream_id: &str, call_id: &str) -> String {
    format!("{stream_id}::id::{call_id}")
}

fn make_index_key(stream_id: &str, index: u32) -> String {
    format!("{stream_id}::index::{index}")
}

fn make_anonymous_key(stream_id: &str) -> String {
    format!("{stream_id}::anonymous")
}

fn fingerprint_delta(delta: &ToolCallDelta) -> u64 {
    let mut bytes = Vec::with_capacity(
        delta.stream_id.len()
            + delta.provider.as_ref().map_or(0, String::len)
            + delta.call_id.as_ref().map_or(0, String::len)
            + delta.name_fragment.as_ref().map_or(0, String::len)
            + delta.arguments_fragment.as_ref().map_or(0, String::len)
            + 64,
    );
    bytes.extend_from_slice(delta.stream_id.as_bytes());
    bytes.extend_from_slice(b"|");
    if let Some(provider) = &delta.provider {
        bytes.extend_from_slice(provider.as_bytes());
    }
    bytes.extend_from_slice(b"|");
    if let Some(call_id) = &delta.call_id {
        bytes.extend_from_slice(call_id.as_bytes());
    }
    bytes.extend_from_slice(b"|");
    if let Some(index) = delta.index {
        bytes.extend_from_slice(index.to_string().as_bytes());
    }
    bytes.extend_from_slice(b"|");
    if let Some(name_fragment) = &delta.name_fragment {
        bytes.extend_from_slice(name_fragment.as_bytes());
    }
    bytes.extend_from_slice(b"|");
    if let Some(arguments_fragment) = &delta.arguments_fragment {
        bytes.extend_from_slice(arguments_fragment.as_bytes());
    }
    bytes.extend_from_slice(b"|");
    bytes.extend_from_slice(if delta.is_final { b"1" } else { b"0" });
    fnv1a64(&bytes)
}

fn fingerprint_call(name: &str, arguments: &str) -> u64 {
    let mut bytes = Vec::with_capacity(name.len() + arguments.len() + 1);
    bytes.extend_from_slice(name.as_bytes());
    bytes.push(b'|');
    bytes.extend_from_slice(arguments.as_bytes());
    fnv1a64(&bytes)
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x0000_0100_0000_01B3;
    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> AssemblerConfig {
        AssemblerConfig {
            max_idle_ms: 50,
            ..AssemblerConfig::default()
        }
    }

    #[test]
    fn assembles_interleaved_calls_and_upgrades_index_to_call_id() {
        let mut assembler = InterleavedToolCallAssembler::with_config(cfg());

        let first = assembler
            .ingest(
                ToolCallDelta::new("stream-a", 0)
                    .with_index(0)
                    .with_name_fragment("web_search")
                    .with_arguments_fragment("{\"query\":\"rust"),
            )
            .unwrap();
        assert_eq!(first.completed.len(), 0);
        assert_eq!(first.pending.len(), 1);

        let second = assembler
            .ingest(
                ToolCallDelta::new("stream-a", 1)
                    .with_index(1)
                    .with_name_fragment("fetch_url")
                    .with_arguments_fragment("{\"url\":\"https://exa"),
            )
            .unwrap();
        assert_eq!(second.pending.len(), 2);

        let third = assembler
            .ingest(
                ToolCallDelta::new("stream-a", 2)
                    .with_index(0)
                    .with_call_id("call-0")
                    .with_arguments_fragment(" tool calls\"}"),
            )
            .unwrap();
        assert_eq!(third.completed.len(), 1);
        assert_eq!(third.completed[0].call_id.as_deref(), Some("call-0"));
        assert_eq!(third.completed[0].name, "web_search");
        assert_eq!(
            third.completed[0].arguments,
            "{\"query\":\"rust tool calls\"}"
        );

        let fourth = assembler
            .ingest(
                ToolCallDelta::new("stream-a", 3)
                    .with_index(1)
                    .with_arguments_fragment("mple.com\"}")
                    .final_fragment(),
            )
            .unwrap();
        assert_eq!(fourth.completed.len(), 1);
        assert_eq!(fourth.completed[0].name, "fetch_url");
        assert_eq!(
            fourth.completed[0].arguments,
            "{\"url\":\"https://example.com\"}"
        );
        assert_eq!(assembler.pending_len(), 0);
    }

    #[test]
    fn suppresses_duplicate_delta_replays() {
        let mut assembler = InterleavedToolCallAssembler::with_config(cfg());
        let delta = ToolCallDelta::new("stream-b", 0)
            .with_index(0)
            .with_name_fragment("list_models")
            .with_arguments_fragment("{\"provider\":\"openai");

        let first = assembler.ingest(delta.clone()).unwrap();
        assert!(!first.duplicate_suppressed);
        assert_eq!(first.pending.len(), 1);

        let replay = assembler.ingest(delta).unwrap();
        assert!(replay.duplicate_suppressed);
        assert_eq!(replay.pending.len(), 1);
        assert_eq!(replay.completed.len(), 0);
    }

    #[test]
    fn rejects_sequence_regressions() {
        let mut assembler = InterleavedToolCallAssembler::with_config(cfg());
        assembler
            .ingest(
                ToolCallDelta::new("stream-c", 0)
                    .with_index(0)
                    .with_sequence(5)
                    .with_name_fragment("tool")
                    .with_arguments_fragment("{\"ok\":"),
            )
            .unwrap();

        let update = assembler
            .ingest(
                ToolCallDelta::new("stream-c", 1)
                    .with_index(0)
                    .with_sequence(4)
                    .with_arguments_fragment("true}"),
            )
            .unwrap();

        assert_eq!(update.rejected.len(), 1);
        assert_eq!(update.rejected[0].reason, RejectReason::SequenceRegression);
        assert_eq!(assembler.pending_len(), 0);
    }

    #[test]
    fn rejects_name_changes_after_arguments_by_default() {
        let mut assembler = InterleavedToolCallAssembler::with_config(cfg());
        assembler
            .ingest(
                ToolCallDelta::new("stream-d", 0)
                    .with_index(0)
                    .with_name_fragment("foo")
                    .with_arguments_fragment("{\"x\":1"),
            )
            .unwrap();

        let update = assembler
            .ingest(
                ToolCallDelta::new("stream-d", 1)
                    .with_index(0)
                    .with_name_fragment("_bar"),
            )
            .unwrap();

        assert_eq!(update.rejected.len(), 1);
        assert_eq!(update.rejected[0].reason, RejectReason::NameAfterArguments);
    }

    #[test]
    fn rejects_malformed_json_when_finalized() {
        let mut assembler = InterleavedToolCallAssembler::with_config(cfg());
        assembler
            .ingest(
                ToolCallDelta::new("stream-e", 0)
                    .with_index(0)
                    .with_name_fragment("broken")
                    .with_arguments_fragment("{\"x\":"),
            )
            .unwrap();

        let update = assembler.finalize_all(10);
        assert_eq!(update.completed.len(), 0);
        assert_eq!(update.rejected.len(), 1);
        assert_eq!(update.rejected[0].reason, RejectReason::IncompleteJson);
    }

    #[test]
    fn expires_idle_calls() {
        let mut assembler = InterleavedToolCallAssembler::with_config(cfg());
        assembler
            .ingest(
                ToolCallDelta::new("stream-f", 0)
                    .with_index(0)
                    .with_name_fragment("slow_tool")
                    .with_arguments_fragment("{\"x\":1"),
            )
            .unwrap();

        let expired = assembler.expire_idle(100);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].reason, RejectReason::IdleTimeout);
        assert_eq!(assembler.pending_len(), 0);
    }

    #[test]
    fn allows_auto_completion_without_explicit_final() {
        let mut assembler = InterleavedToolCallAssembler::with_config(cfg());
        let update = assembler
            .ingest(
                ToolCallDelta::new("stream-g", 0)
                    .with_index(0)
                    .with_name_fragment("tool")
                    .with_arguments_fragment("{\"done\":true}"),
            )
            .unwrap();

        assert_eq!(update.completed.len(), 1);
        assert_eq!(update.completed[0].finish_reason, FinishReason::JsonCompleted);
    }
}

/*
This solves Rust streaming tool call assembly, interleaved function call parsing, LLM tool delta reconstruction, OpenAI Responses API tool-call merging, Anthropic tool use streaming, Vercel AI SDK backend normalization, and agent runtime reliability when fragments arrive out of order or get replayed. Built because modern agent backends in April 2026 keep dealing with partial tool names, split JSON arguments, duplicate SSE chunks, and streams that start with an index and only later reveal the final call id. That sounds small until a production agent replays the same tool twice or closes JSON too early and hits a real API with bad arguments.

Use it when you need one small Rust file that can sit inside a gateway, orchestrator, worker, edge service, or research agent runtime and turn streaming tool-call fragments into complete validated calls. The trick: it does not try to be a full JSON parser or an opinionated framework. It only tracks the parts you actually need for deterministic assembly: key resolution, duplicate suppression, sequence monotonicity, JSON boundary state, idle eviction, and safe completion rules.

Drop this into any Rust codebase that handles streamed model output, agent tool execution, Model Context Protocol bridges, SSE consumers, WebSocket backends, or provider adapters. Pavan can fork this file, wire it into an async runtime, and immediately get better safety around streaming tool calls without adding a crate graph just to recover complete function arguments from partial deltas.
*/