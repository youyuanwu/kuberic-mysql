//! Strict translation from Kuberic Public V1 values into the MySQL lifecycle seam.

mod driver;
mod service;
mod translation;

pub use driver::{MysqlPublicV1Replicator, MysqlPublicV1ReplicatorFactory};
pub use service::{MysqlPublicV1Service, MysqlPublicV1ServiceConfig};
pub use translation::{
    CallbackOperationLedger, EpochTranslator, KubericTranslationError, ReplicationEndpoint,
    TranslationResult, translate_process_session_id, translate_replica_identity,
    translate_replica_information, translate_replica_set_configuration, translate_role,
    translate_storage_binding,
};
