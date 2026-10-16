//! Oracle MySQL support for Kuberic.
//!
//! The crate separates deterministic safety types, native observation, and
//! local process ownership into explicit namespaces:
//!
//! - [`core`] models identity, GTID history, native views, and authority.
//! - [`adapter`] observes one exact Oracle MySQL instance over a private UDS.
//! - [`service`] owns fresh local processes, member-local lifecycle and proxy
//!   control, and the feature-gated Kuberic Public V1 adapter.

pub mod adapter;
pub mod core;
pub mod service;
