#![allow(dead_code)]

use std::fs;
use std::path::PathBuf;

use serde::Deserialize;

use crate::query::{ColumnKind, QueryId, RawColumn, RawResult, RawValue};

#[derive(Debug, Deserialize)]
pub struct FixtureFile {
    pub fixture_schema: String,
    pub cases: Vec<FixtureCase>,
}

#[derive(Debug, Deserialize)]
pub struct FixtureCase {
    pub scenario: String,
    pub query_id: String,
    pub columns: Vec<FixtureColumn>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub expected: Expected,
    pub evidence_origin: EvidenceOrigin,
}

#[derive(Debug, Deserialize)]
pub struct FixtureColumn {
    pub name: String,
    pub kind: String,
    pub nullable: bool,
}

#[derive(Debug, Deserialize)]
pub struct Expected {
    pub core_class: String,
    pub diagnostic: String,
}

#[derive(Debug, Deserialize)]
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
