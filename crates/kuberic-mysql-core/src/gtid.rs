//! Structured MySQL GTID sets and set-only relations.

use core::fmt;
use core::str::FromStr;

use crate::{IdentityErrorKind, ServerUuid};

/// Largest sequence number accepted by qualified Oracle MySQL 8.4.11.
pub const MAX_SEQUENCE: u64 = 9_223_372_036_854_775_806;

/// A validated GTID transaction tag.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct GtidTag(String);

impl GtidTag {
    /// Validates a MySQL 8.4 transaction tag.
    pub fn new(value: impl Into<String>) -> Result<Self, GtidParseErrorKind> {
        let value = value.into();
        let mut characters = value.chars();
        let valid_first = characters
            .next()
            .is_some_and(|character| character.is_ascii_alphabetic() || character == '_');
        let valid_rest =
            characters.all(|character| character.is_ascii_alphanumeric() || character == '_');
        if !valid_first || !valid_rest || value.len() > 32 {
            return Err(GtidParseErrorKind::InvalidTag);
        }
        Ok(Self(value))
    }

    /// Returns the exact validated tag.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A GTID source: server UUID plus optional transaction tag.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct GtidSource {
    server_uuid: ServerUuid,
    tag: Option<GtidTag>,
}

impl GtidSource {
    /// Creates a source identity.
    #[must_use]
    pub const fn new(server_uuid: ServerUuid, tag: Option<GtidTag>) -> Self {
        Self { server_uuid, tag }
    }

    /// Returns the source server UUID.
    #[must_use]
    pub const fn server_uuid(&self) -> &ServerUuid {
        &self.server_uuid
    }

    /// Returns the optional transaction tag.
    #[must_use]
    pub const fn tag(&self) -> Option<&GtidTag> {
        self.tag.as_ref()
    }
}

/// One inclusive, non-empty sequence interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GtidInterval {
    start: u64,
    end: u64,
}

impl GtidInterval {
    /// Returns the inclusive first sequence.
    #[must_use]
    pub const fn start(self) -> u64 {
        self.start
    }

    /// Returns the inclusive last sequence.
    #[must_use]
    pub const fn end(self) -> u64 {
        self.end
    }
}

/// Normalized intervals belonging to one GTID source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceHistory {
    source: GtidSource,
    intervals: Vec<GtidInterval>,
}

impl SourceHistory {
    /// Returns the source identity.
    #[must_use]
    pub const fn source(&self) -> &GtidSource {
        &self.source
    }

    /// Returns normalized inclusive intervals.
    #[must_use]
    pub fn intervals(&self) -> &[GtidInterval] {
        &self.intervals
    }
}

/// The only public comparison relation between GTID sets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GtidRelation {
    /// The sets contain exactly the same transactions.
    Equal,
    /// The left set is strictly contained by the right set.
    ProperSubset,
    /// The left set strictly contains the right set.
    ProperSuperset,
    /// Each set contains transactions absent from the other.
    Incomparable,
}

/// A machine-matchable GTID parsing failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum GtidParseErrorKind {
    /// A comma-separated source component was empty.
    EmptySource,
    /// The source UUID was malformed.
    InvalidUuid,
    /// The optional transaction tag was malformed.
    InvalidTag,
    /// The source contained no sequence interval.
    MissingInterval,
    /// An interval token was empty.
    EmptyInterval,
    /// A sequence was not unsigned decimal text.
    InvalidSequence,
    /// A sequence was zero or above [`MAX_SEQUENCE`].
    SequenceOutOfRange,
    /// A textual range was malformed or was not strictly ascending.
    InvalidRange,
}

/// A structured GTID parse error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GtidParseError {
    component: usize,
    token: usize,
    kind: GtidParseErrorKind,
}

impl GtidParseError {
    fn new(component: usize, token: usize, kind: GtidParseErrorKind) -> Self {
        Self {
            component,
            token,
            kind,
        }
    }

    /// Returns the zero-based source component index.
    #[must_use]
    pub const fn component(&self) -> usize {
        self.component
    }

    /// Returns the zero-based colon token index.
    #[must_use]
    pub const fn token(&self) -> usize {
        self.token
    }

    /// Returns the machine-matchable reason.
    #[must_use]
    pub const fn kind(&self) -> &GtidParseErrorKind {
        &self.kind
    }
}

impl fmt::Display for GtidParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid GTID component {} token {}: {:?}",
            self.component, self.token, self.kind
        )
    }
}

impl std::error::Error for GtidParseError {}

/// A normalized lineage-scoped MySQL GTID set.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GtidSet {
    entries: Vec<SourceHistory>,
}

impl GtidSet {
    /// Returns an empty GTID set.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Returns normalized source histories in deterministic encoding order.
    #[must_use]
    pub fn entries(&self) -> &[SourceHistory] {
        &self.entries
    }

    /// Returns whether every transaction in `self` is present in `other`.
    #[must_use]
    pub fn is_subset_of(&self, other: &Self) -> bool {
        self.entries.iter().all(|left| {
            other
                .entry(&left.source)
                .is_some_and(|right| intervals_contained(&left.intervals, &right.intervals))
        })
    }

    /// Compares two histories using set containment only.
    #[must_use]
    pub fn relation(&self, other: &Self) -> GtidRelation {
        let left_subset = self.is_subset_of(other);
        let right_subset = other.is_subset_of(self);
        match (left_subset, right_subset) {
            (true, true) => GtidRelation::Equal,
            (true, false) => GtidRelation::ProperSubset,
            (false, true) => GtidRelation::ProperSuperset,
            (false, false) => GtidRelation::Incomparable,
        }
    }

    fn entry(&self, source: &GtidSource) -> Option<&SourceHistory> {
        self.entries.iter().find(|entry| &entry.source == source)
    }
}

impl FromStr for GtidSet {
    type Err = GtidParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Ok(Self::empty());
        }

        let mut entries: Vec<SourceHistory> = Vec::new();
        for (component_index, raw_component) in value.split(',').enumerate() {
            let component = if component_index == 0 {
                raw_component
            } else {
                raw_component
                    .strip_prefix("\r\n")
                    .or_else(|| raw_component.strip_prefix('\n'))
                    .unwrap_or(raw_component)
            };
            if component.is_empty() {
                return Err(GtidParseError::new(
                    component_index,
                    0,
                    GtidParseErrorKind::EmptySource,
                ));
            }
            let tokens: Vec<&str> = component.split(':').collect();
            let uuid = ServerUuid::new(tokens[0]).map_err(|error| {
                let kind = match error.kind() {
                    IdentityErrorKind::MalformedUuid | IdentityErrorKind::NilUuid => {
                        GtidParseErrorKind::InvalidUuid
                    }
                    _ => GtidParseErrorKind::InvalidUuid,
                };
                GtidParseError::new(component_index, 0, kind)
            })?;
            if tokens.len() < 2 {
                return Err(GtidParseError::new(
                    component_index,
                    1,
                    GtidParseErrorKind::MissingInterval,
                ));
            }

            let mut tag = None;
            let mut group_started = false;
            let mut group_has_interval = false;
            let mut saw_interval = false;
            for (token_index, token) in tokens.iter().enumerate().skip(1) {
                let tag_like = token
                    .bytes()
                    .any(|byte| byte.is_ascii_alphabetic() || byte == b'_');
                if tag_like {
                    if group_started && !group_has_interval {
                        return Err(GtidParseError::new(
                            component_index,
                            token_index,
                            GtidParseErrorKind::MissingInterval,
                        ));
                    }
                    tag =
                        Some(GtidTag::new(*token).map_err(|kind| {
                            GtidParseError::new(component_index, token_index, kind)
                        })?);
                    group_started = true;
                    group_has_interval = false;
                    continue;
                }

                let interval = parse_interval(component_index, token_index, token)?;
                let source = GtidSource::new(uuid.clone(), tag.clone());
                if let Some(existing) = entries.iter_mut().find(|entry| entry.source == source) {
                    existing.intervals.push(interval);
                } else {
                    entries.push(SourceHistory {
                        source,
                        intervals: vec![interval],
                    });
                }
                group_started = true;
                group_has_interval = true;
                saw_interval = true;
            }
            if !saw_interval || !group_has_interval {
                return Err(GtidParseError::new(
                    component_index,
                    tokens.len(),
                    GtidParseErrorKind::MissingInterval,
                ));
            }
        }

        for entry in &mut entries {
            normalize_intervals(&mut entry.intervals);
        }
        entries.sort_by(|left, right| {
            left.source
                .server_uuid
                .as_str()
                .cmp(right.source.server_uuid.as_str())
                .then_with(|| {
                    left.source
                        .tag
                        .as_ref()
                        .map(GtidTag::as_str)
                        .cmp(&right.source.tag.as_ref().map(GtidTag::as_str))
                })
        });
        Ok(Self { entries })
    }
}

impl fmt::Display for GtidSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (entry_index, entry) in self.entries.iter().enumerate() {
            if entry_index != 0 {
                formatter.write_str(",")?;
            }
            write!(formatter, "{}", entry.source.server_uuid)?;
            if let Some(tag) = &entry.source.tag {
                write!(formatter, ":{}", tag.as_str())?;
            }
            for interval in &entry.intervals {
                if interval.start == interval.end {
                    write!(formatter, ":{}", interval.start)?;
                } else {
                    write!(formatter, ":{}-{}", interval.start, interval.end)?;
                }
            }
        }
        Ok(())
    }
}

fn parse_interval(
    component: usize,
    token_index: usize,
    token: &str,
) -> Result<GtidInterval, GtidParseError> {
    if token.is_empty() {
        return Err(GtidParseError::new(
            component,
            token_index,
            GtidParseErrorKind::EmptyInterval,
        ));
    }
    if let Some((start, end)) = token.split_once('-') {
        if start.is_empty() || end.is_empty() || end.contains('-') {
            return Err(GtidParseError::new(
                component,
                token_index,
                GtidParseErrorKind::InvalidRange,
            ));
        }
        let start = parse_sequence(component, token_index, start)?;
        let end = parse_sequence(component, token_index, end)?;
        if end <= start {
            return Err(GtidParseError::new(
                component,
                token_index,
                GtidParseErrorKind::InvalidRange,
            ));
        }
        Ok(GtidInterval { start, end })
    } else {
        let sequence = parse_sequence(component, token_index, token)?;
        Ok(GtidInterval {
            start: sequence,
            end: sequence,
        })
    }
}

fn parse_sequence(component: usize, token: usize, value: &str) -> Result<u64, GtidParseError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(GtidParseError::new(
            component,
            token,
            GtidParseErrorKind::InvalidSequence,
        ));
    }
    let sequence = value.parse::<u64>().map_err(|_| {
        GtidParseError::new(component, token, GtidParseErrorKind::SequenceOutOfRange)
    })?;
    if !(1..=MAX_SEQUENCE).contains(&sequence) {
        return Err(GtidParseError::new(
            component,
            token,
            GtidParseErrorKind::SequenceOutOfRange,
        ));
    }
    Ok(sequence)
}

fn normalize_intervals(intervals: &mut Vec<GtidInterval>) {
    intervals.sort_by_key(|interval| interval.start);
    let mut merged: Vec<GtidInterval> = Vec::with_capacity(intervals.len());
    for interval in intervals.drain(..) {
        if let Some(previous) = merged.last_mut()
            && interval.start <= previous.end.saturating_add(1)
        {
            previous.end = previous.end.max(interval.end);
            continue;
        }
        merged.push(interval);
    }
    *intervals = merged;
}

fn intervals_contained(left: &[GtidInterval], right: &[GtidInterval]) -> bool {
    let mut right_index = 0;
    for wanted in left {
        while right_index < right.len() && right[right_index].end < wanted.start {
            right_index += 1;
        }
        if right_index == right.len()
            || right[right_index].start > wanted.start
            || right[right_index].end < wanted.end
        {
            return false;
        }
    }
    true
}
