//! Central fail-closed adapter-to-core error mapping.

use std::io::ErrorKind;

use crate::core::{CollectionFailure, IncoherentReason, UnsupportedReason};

use crate::adapter::decode::DecodeError;
use crate::adapter::diagnostic::{
    AdapterDiagnostic, CoreOutcomeClass, ServerErrorClass, SocketIssue,
};
use crate::adapter::request::SocketPathError;
use crate::adapter::session::SessionError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ErrorDisposition {
    Collection(CollectionFailure),
    Incoherent(IncoherentReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MappedError {
    pub(crate) disposition: ErrorDisposition,
    pub(crate) diagnostic: AdapterDiagnostic,
}

pub(crate) fn map_socket(error: SocketPathError) -> MappedError {
    let issue = match error {
        SocketPathError::Metadata(ErrorKind::NotFound) => SocketIssue::Missing,
        SocketPathError::Metadata(ErrorKind::PermissionDenied) => SocketIssue::Inaccessible,
        SocketPathError::Metadata(_) | SocketPathError::NotAbsolute | SocketPathError::NonUtf8 => {
            SocketIssue::Metadata
        }
        SocketPathError::Symlink => SocketIssue::Symlink,
        SocketPathError::NotSocket => SocketIssue::NotSocket,
        SocketPathError::Replaced => SocketIssue::Replaced,
        SocketPathError::UnsupportedPlatform => SocketIssue::UnsupportedPlatform,
    };
    MappedError {
        disposition: ErrorDisposition::Collection(CollectionFailure::Unreachable),
        diagnostic: AdapterDiagnostic::Socket { issue },
    }
}

pub(crate) fn map_session(error: &SessionError) -> MappedError {
    let diagnostic = error.diagnostic();
    let failure = match &diagnostic {
        AdapterDiagnostic::Transport { .. } => CollectionFailure::Unreachable,
        AdapterDiagnostic::Server { class, .. } => match class {
            ServerErrorClass::Authentication => CollectionFailure::AuthenticationFailure,
            ServerErrorClass::Permission => CollectionFailure::PermissionDenied,
            ServerErrorClass::Other => {
                CollectionFailure::Unsupported(UnsupportedReason::CollectorCapability)
            }
        },
        AdapterDiagnostic::Client { .. } => {
            CollectionFailure::Unsupported(UnsupportedReason::CollectorCapability)
        }
        _ => CollectionFailure::Unsupported(UnsupportedReason::CollectorCapability),
    };
    MappedError {
        disposition: ErrorDisposition::Collection(failure),
        diagnostic,
    }
}

pub(crate) fn map_decode(error: DecodeError) -> MappedError {
    let diagnostic = error.diagnostic;
    let disposition = match diagnostic.core_class() {
        CoreOutcomeClass::Absent => ErrorDisposition::Collection(CollectionFailure::Absent),
        CoreOutcomeClass::Unreachable => {
            ErrorDisposition::Collection(CollectionFailure::Unreachable)
        }
        CoreOutcomeClass::AuthenticationFailure => {
            ErrorDisposition::Collection(CollectionFailure::AuthenticationFailure)
        }
        CoreOutcomeClass::PermissionDenied => {
            ErrorDisposition::Collection(CollectionFailure::PermissionDenied)
        }
        CoreOutcomeClass::Malformed(reason) => {
            ErrorDisposition::Collection(CollectionFailure::Malformed(reason))
        }
        CoreOutcomeClass::Unsupported(reason) => {
            ErrorDisposition::Collection(CollectionFailure::Unsupported(reason))
        }
        CoreOutcomeClass::Incoherent(reason) => ErrorDisposition::Incoherent(reason),
        CoreOutcomeClass::Valid
        | CoreOutcomeClass::Partial
        | CoreOutcomeClass::Stale(_)
        | CoreOutcomeClass::FutureDated => ErrorDisposition::Collection(
            CollectionFailure::Unsupported(UnsupportedReason::CollectorCapability),
        ),
    };
    MappedError {
        disposition,
        diagnostic,
    }
}
