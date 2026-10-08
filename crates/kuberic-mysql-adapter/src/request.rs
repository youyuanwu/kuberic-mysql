//! Validated native-observation request values.

use core::fmt;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt};

use kuberic_mysql_core::{ExactBinding, ObservationProvenance};

use crate::{ClockContext, ClockError, ObservationClock};

/// A validated absolute path to an existing, non-symlink Unix socket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnixSocketPath {
    path: PathBuf,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    changed_seconds: i64,
    #[cfg(unix)]
    changed_nanoseconds: i64,
}

impl UnixSocketPath {
    /// Validates path shape and the current filesystem object type.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, SocketPathError> {
        let path = path.into();
        if !path.is_absolute() {
            return Err(SocketPathError::NotAbsolute);
        }
        if path.as_os_str().to_str().is_none() {
            return Err(SocketPathError::NonUtf8);
        }
        let metadata =
            fs::symlink_metadata(&path).map_err(|error| SocketPathError::Metadata(error.kind()))?;
        if metadata.file_type().is_symlink() {
            return Err(SocketPathError::Symlink);
        }
        #[cfg(unix)]
        if !metadata.file_type().is_socket() {
            return Err(SocketPathError::NotSocket);
        }
        #[cfg(not(unix))]
        return Err(SocketPathError::UnsupportedPlatform);
        Ok(Self {
            path,
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(unix)]
            changed_seconds: metadata.ctime(),
            #[cfg(unix)]
            changed_nanoseconds: metadata.ctime_nsec(),
        })
    }

    /// Returns the validated path.
    #[must_use]
    pub fn as_path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn revalidate(&self) -> Result<(), SocketPathError> {
        let metadata = fs::symlink_metadata(&self.path)
            .map_err(|error| SocketPathError::Metadata(error.kind()))?;
        if metadata.file_type().is_symlink() {
            return Err(SocketPathError::Symlink);
        }
        #[cfg(unix)]
        {
            if !metadata.file_type().is_socket() {
                return Err(SocketPathError::NotSocket);
            }
            if metadata.dev() != self.device
                || metadata.ino() != self.inode
                || metadata.ctime() != self.changed_seconds
                || metadata.ctime_nsec() != self.changed_nanoseconds
            {
                return Err(SocketPathError::Replaced);
            }
        }
        #[cfg(not(unix))]
        return Err(SocketPathError::UnsupportedPlatform);
        Ok(())
    }
}

/// A Unix-socket path validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketPathError {
    /// The path was not absolute.
    NotAbsolute,
    /// The client library cannot represent the path without changing its bytes.
    NonUtf8,
    /// Filesystem metadata could not be read.
    Metadata(ErrorKind),
    /// The final path component was a symbolic link.
    Symlink,
    /// The final path component was not a Unix socket.
    NotSocket,
    /// The socket object changed after request validation.
    Replaced,
    /// Unix socket validation is unavailable on this platform.
    UnsupportedPlatform,
}

impl fmt::Display for SocketPathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid Unix socket path: {self:?}")
    }
}

impl std::error::Error for SocketPathError {}

/// Observer credentials whose secret is never displayed or publicly exposed.
pub struct ObserverCredentials {
    username: String,
    password: Box<[u8]>,
}

impl ObserverCredentials {
    /// Validates and stores one local observer credential.
    pub fn new(
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, RequestError> {
        let username = username.into();
        if username.is_empty() || username.chars().any(char::is_control) {
            return Err(RequestError::InvalidUsername);
        }
        let password = password.into().into_bytes().into_boxed_slice();
        Ok(Self { username, password })
    }

    pub(crate) fn username(&self) -> &str {
        &self.username
    }

    pub(crate) fn password(&self) -> &str {
        std::str::from_utf8(&self.password).expect("password originated as UTF-8")
    }
}

impl fmt::Debug for ObserverCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObserverCredentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl Drop for ObserverCredentials {
    fn drop(&mut self) {
        self.password.fill(0);
    }
}

/// A complete request to observe exactly one caller-selected local server.
///
/// Directory privacy and ownership are caller-established preconditions. This
/// type validates only the final absolute, non-symlink socket object.
#[derive(Debug)]
pub struct ObservationRequest<C> {
    binding: ExactBinding,
    socket: UnixSocketPath,
    credentials: ObserverCredentials,
    provenance: ObservationProvenance,
    clock: ClockContext<C>,
}

impl<C: ObservationClock> ObservationRequest<C> {
    /// Validates cross-field attempt identity and constructs the request.
    pub fn new(
        binding: ExactBinding,
        socket: UnixSocketPath,
        credentials: ObserverCredentials,
        provenance: ObservationProvenance,
        clock: ClockContext<C>,
    ) -> Result<Self, RequestError> {
        if provenance.attempt() != &binding.parts().attempt {
            return Err(RequestError::AttemptMismatch);
        }
        Ok(Self {
            binding,
            socket,
            credentials,
            provenance,
            clock,
        })
    }

    /// Returns the exact expected binding.
    #[must_use]
    pub const fn binding(&self) -> &ExactBinding {
        &self.binding
    }

    /// Returns the sole allowed transport target.
    #[must_use]
    pub const fn socket(&self) -> &UnixSocketPath {
        &self.socket
    }

    /// Returns non-secret collection provenance.
    #[must_use]
    pub const fn provenance(&self) -> &ObservationProvenance {
        &self.provenance
    }

    /// Returns the caller clock and deadline context.
    #[must_use]
    pub const fn clock(&self) -> &ClockContext<C> {
        &self.clock
    }

    pub(crate) const fn credentials(&self) -> &ObserverCredentials {
        &self.credentials
    }
}

/// A request construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestError {
    /// The provenance attempt differed from the exact binding attempt.
    AttemptMismatch,
    /// The observer user name was empty or contained a control character.
    InvalidUsername,
    /// The clock context was invalid.
    Clock(ClockError),
}

impl From<ClockError> for RequestError {
    fn from(error: ClockError) -> Self {
        Self::Clock(error)
    }
}

impl fmt::Display for RequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid observation request: {self:?}")
    }
}

impl std::error::Error for RequestError {}
