#![allow(dead_code)]

use std::fs;
use std::path::PathBuf;

use serde::Deserialize;

use crate::query::{ColumnKind, QueryId, RawColumn, RawResult, RawValue};

#[derive(Clone, Debug, Deserialize)]
pub struct FixtureFile {
    pub fixture_schema: String,
    pub cases: Vec<FixtureCase>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct FixtureCase {
    pub scenario: String,
    pub query_id: String,
    #[serde(default)]
    pub columns: Vec<FixtureColumn>,
    #[serde(default)]
    pub rows: Vec<Vec<serde_json::Value>>,
    pub error: Option<FixtureError>,
    pub expected: Expected,
    pub evidence_origin: EvidenceOrigin,
}

#[derive(Clone, Debug, Deserialize)]
pub struct FixtureError {
    pub kind: String,
    pub stage: String,
    pub surface: Option<String>,
    pub code: Option<u16>,
    pub sql_state: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct FixtureColumn {
    pub name: String,
    pub kind: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Expected {
    pub core_class: String,
    pub diagnostic: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EvidenceOrigin {
    pub category: String,
    pub commit: Option<String>,
    pub tag: Option<String>,
    pub url: Option<String>,
    pub path: Option<String>,
    pub sha256: Option<String>,
    pub range: Option<String>,
    pub derivation: Option<String>,
    pub qualification: Option<String>,
}

pub fn load_fixture_files() -> Vec<FixtureFile> {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mysql-8.4.11");
    let mut paths = fs::read_dir(directory)
        .expect("fixture directory")
        .map(|entry| entry.expect("fixture entry").path())
        .collect::<Vec<_>>();
    paths.sort();
    paths
        .into_iter()
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .map(|path| {
            serde_json::from_slice(&fs::read(path).expect("read fixture")).expect("parse fixture")
        })
        .collect()
}

pub fn fixture(scenario: &str) -> FixtureCase {
    load_fixture_files()
        .into_iter()
        .flat_map(|file| file.cases)
        .find(|case| case.scenario == scenario)
        .unwrap_or_else(|| panic!("missing fixture scenario {scenario}"))
}

impl FixtureCase {
    pub fn query(&self) -> QueryId {
        match self.query_id.as_str() {
            "Mysql8411ProductIdentityV1" => QueryId::Mysql8411ProductIdentityV1,
            "Mysql8411LocalStateV1" => QueryId::Mysql8411LocalStateV1,
            "Mysql8411GroupMembersV1" => QueryId::Mysql8411GroupMembersV1,
            "Mysql8411LocalMemberStatsV1" => QueryId::Mysql8411LocalMemberStatsV1,
            "Mysql8411ExecutedGtidsV1" => QueryId::Mysql8411ExecutedGtidsV1,
            other => panic!("unknown query id {other}"),
        }
    }

    pub fn raw(&self) -> RawResult {
        assert!(self.error.is_none(), "error fixture has no raw result");
        RawResult {
            columns: self
                .columns
                .iter()
                .map(|column| RawColumn {
                    name: column.name.clone(),
                    kind: match column.kind.as_str() {
                        "VarString" => ColumnKind::VarString,
                        "String" => ColumnKind::String,
                        "Long" => ColumnKind::Long,
                        "LongLong" => ColumnKind::LongLong,
                        other => panic!("unknown column kind {other}"),
                    },
                    nullable: column.nullable,
                })
                .collect(),
            rows: self
                .rows
                .iter()
                .map(|row| row.iter().map(raw_value).collect())
                .collect(),
        }
    }

    pub fn validate_input(&self) -> Result<(), &'static str> {
        match (
            self.error.is_some(),
            self.columns.is_empty(),
            self.rows.is_empty(),
        ) {
            (true, true, true) => Ok(()),
            (true, _, _) => Err("error input must not also declare result metadata or rows"),
            (false, false, _) => Ok(()),
            (false, true, _) => Err("result input must declare selected metadata"),
        }
    }
}

pub fn validate_required_origin(case: &FixtureCase) -> Result<(), &'static str> {
    let required = match case.scenario.as_str() {
        "plugin-absent-members"
        | "never-started-members-placeholder"
        | "stopped-members-placeholder"
        | "recovering-member"
        | "never-started-filtered-stats-absent" => "oracle-owned",
        "other-oracle-patch"
        | "non-oracle-lookalike"
        | "unexpected-required-null"
        | "incomplete-members-row"
        | "members-schema-type-drift"
        | "duplicate-member-id"
        | "duplicate-member-address"
        | "duplicate-local-stats-row"
        | "malformed-read-only-switch"
        | "future-member-role"
        | "future-additional-column"
        | "invalid-empty-group-name"
        | "stopped-placeholder-invalid-member-id"
        | "stopped-placeholder-invalid-host"
        | "stopped-placeholder-invalid-port" => "synthetic",
        "oracle-community-8.4.11-product"
        | "online-local-state"
        | "online-members"
        | "online-local-view"
        | "valid-empty-executed-gtids"
        | "native-tagged-and-newline-gtids"
        | "gtid-boundary-9223372036854775806"
        | "gtid-boundary-9223372036854775807"
        | "uds-transport-failure"
        | "authentication-failure"
        | "members-table-permission-denied"
        | "stats-table-permission-denied" => "native-required",
        _ => return Err("scenario has no required origin rule"),
    };
    if case.evidence_origin.category == required {
        Ok(())
    } else {
        Err("scenario origin category does not match its required source")
    }
}

fn raw_value(value: &serde_json::Value) -> RawValue {
    match value {
        serde_json::Value::Null => RawValue::Null,
        serde_json::Value::String(value) => RawValue::Bytes(value.as_bytes().to_vec()),
        serde_json::Value::Number(value) if value.is_u64() => {
            RawValue::Unsigned(value.as_u64().expect("unsigned JSON number"))
        }
        serde_json::Value::Number(value) => {
            RawValue::Signed(value.as_i64().expect("signed JSON number"))
        }
        other => panic!("unsupported raw fixture value {other}"),
    }
}
