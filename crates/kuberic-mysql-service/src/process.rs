//! Exact-child command execution, attestation, signaling, and reaping.

use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::{
    LifecycleOperation, MysqlInstanceConfig, MysqlInstanceError, OwnershipError, ProductError,
};

const POLL_INTERVAL: Duration = Duration::from_millis(25);
const MAX_VERSION_BYTES: u64 = 8 * 1024;

pub(crate) fn launcher_command(config: &MysqlInstanceConfig) -> Command {
    let mut command = Command::new(config.launcher());
    command
        .arg("-p")
        .arg("unconfined")
        .arg("--")
        .arg(config.mysqld());
    command
}

pub(crate) fn verify_product(config: &MysqlInstanceConfig) -> Result<(), MysqlInstanceError> {
    config.revalidate_executables()?;
    let log =
        File::create(config.runtime().version_log()).map_err(|error| MysqlInstanceError::Io {
            operation: LifecycleOperation::ProductValidation,
            kind: error.kind(),
        })?;
    let stderr = log.try_clone().map_err(|error| MysqlInstanceError::Io {
        operation: LifecycleOperation::ProductValidation,
        kind: error.kind(),
    })?;
    let mut child = launcher_command(config)
        .arg("--version")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|_| MysqlInstanceError::Product(ProductError::InspectionFailed))?;
    let status = wait_bounded(
        &mut child,
        config.timeouts().initialization,
        config.timeouts().shutdown,
        LifecycleOperation::ProductValidation,
    )?;
    if !status.success() {
        return Err(MysqlInstanceError::Product(ProductError::InspectionFailed));
    }
    let mut log =
        File::open(config.runtime().version_log()).map_err(|error| MysqlInstanceError::Io {
            operation: LifecycleOperation::ProductValidation,
            kind: error.kind(),
        })?;
    let size = log
        .seek(SeekFrom::End(0))
        .map_err(|error| MysqlInstanceError::Io {
            operation: LifecycleOperation::ProductValidation,
            kind: error.kind(),
        })?;
    if size > MAX_VERSION_BYTES {
        return Err(MysqlInstanceError::Product(
            ProductError::UnsupportedIdentity,
        ));
    }
    log.seek(SeekFrom::Start(0))
        .map_err(|error| MysqlInstanceError::Io {
            operation: LifecycleOperation::ProductValidation,
            kind: error.kind(),
        })?;
    let mut output = String::new();
    log.read_to_string(&mut output)
        .map_err(|_| MysqlInstanceError::Product(ProductError::UnsupportedIdentity))?;
    let exact = output.contains("Ver 8.4.11 ")
        && output.contains(" for Linux on x86_64 ")
        && output.contains("(MySQL Community Server - GPL)");
    if !exact {
        return Err(MysqlInstanceError::Product(
            ProductError::UnsupportedIdentity,
        ));
    }
    Ok(())
}

pub(crate) fn wait_bounded(
    child: &mut Child,
    timeout: Duration,
    reap_timeout: Duration,
    operation: LifecycleOperation,
) -> Result<ExitStatus, MysqlInstanceError> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().map_err(|error| MysqlInstanceError::Io {
            operation,
            kind: error.kind(),
        })? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let prior = MysqlInstanceError::Timeout(operation);
            if let Err(cleanup) = kill_and_reap_bounded(child, reap_timeout) {
                return Err(MysqlInstanceError::CleanupAfter {
                    prior: Box::new(prior),
                    cleanup: Box::new(cleanup),
                });
            }
            return Err(prior);
        }
        thread::sleep(POLL_INTERVAL);
    }
}

pub(crate) fn attest_child(
    child: &mut Child,
    config: &MysqlInstanceConfig,
) -> Result<(), MysqlInstanceError> {
    if child
        .try_wait()
        .map_err(|_| MysqlInstanceError::Ownership(OwnershipError::InspectionFailed))?
        .is_some()
    {
        return Err(MysqlInstanceError::Ownership(OwnershipError::ChildExited));
    }
    let pid = fs::read_to_string(config.runtime().pid())
        .map_err(|_| MysqlInstanceError::Ownership(OwnershipError::InvalidPidFile))?
        .trim()
        .parse::<u32>()
        .map_err(|_| MysqlInstanceError::Ownership(OwnershipError::InvalidPidFile))?;
    if pid != child.id() {
        return Err(MysqlInstanceError::Ownership(OwnershipError::PidMismatch));
    }
    let executable = fs::canonicalize(format!("/proc/{pid}/exe"))
        .map_err(|_| MysqlInstanceError::Ownership(OwnershipError::InspectionFailed))?;
    let expected = fs::canonicalize(config.mysqld())
        .map_err(|_| MysqlInstanceError::Ownership(OwnershipError::InspectionFailed))?;
    if executable != expected {
        return Err(MysqlInstanceError::Ownership(
            OwnershipError::ExecutableMismatch,
        ));
    }
    Ok(())
}

pub(crate) fn terminate_and_reap(
    child: &mut Child,
    timeout: Duration,
) -> Result<(), MysqlInstanceError> {
    if child
        .try_wait()
        .map_err(|error| MysqlInstanceError::Io {
            operation: LifecycleOperation::Shutdown,
            kind: error.kind(),
        })?
        .is_some()
    {
        return Ok(());
    }
    let term_status = Command::new("/bin/kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .map_err(|error| MysqlInstanceError::Io {
            operation: LifecycleOperation::Shutdown,
            kind: error.kind(),
        })?;
    if !term_status.success() {
        return Err(MysqlInstanceError::ChildFailure {
            operation: LifecycleOperation::Shutdown,
            code: term_status.code(),
        });
    }
    let deadline = Instant::now() + timeout;
    loop {
        if child
            .try_wait()
            .map_err(|error| MysqlInstanceError::Io {
                operation: LifecycleOperation::Shutdown,
                kind: error.kind(),
            })?
            .is_some()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return kill_and_reap_bounded(child, timeout);
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn kill_and_reap_bounded(child: &mut Child, timeout: Duration) -> Result<(), MysqlInstanceError> {
    if let Err(error) = child.kill() {
        if child
            .try_wait()
            .map_err(|wait_error| MysqlInstanceError::Io {
                operation: LifecycleOperation::Shutdown,
                kind: wait_error.kind(),
            })?
            .is_some()
        {
            return Ok(());
        }
        return Err(MysqlInstanceError::Io {
            operation: LifecycleOperation::Shutdown,
            kind: error.kind(),
        });
    }
    let deadline = Instant::now() + timeout;
    loop {
        if child
            .try_wait()
            .map_err(|error| MysqlInstanceError::Io {
                operation: LifecycleOperation::Shutdown,
                kind: error.kind(),
            })?
            .is_some()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(MysqlInstanceError::Timeout(LifecycleOperation::Shutdown));
        }
        thread::sleep(POLL_INTERVAL);
    }
}

pub(crate) fn wait_for_absence(path: &Path, timeout: Duration) -> Result<(), MysqlInstanceError> {
    let deadline = Instant::now() + timeout;
    loop {
        if fs::symlink_metadata(path)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(MysqlInstanceError::SocketStillPresent);
        }
        thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_wait_kills_and_reaps_timeout() {
        let mut child = Command::new("/usr/bin/sleep").arg("60").spawn().unwrap();
        let error = wait_bounded(
            &mut child,
            Duration::from_millis(10),
            Duration::from_secs(1),
            LifecycleOperation::Initialization,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            MysqlInstanceError::Timeout(LifecycleOperation::Initialization)
        ));
        assert!(child.try_wait().unwrap().is_some());
    }

    #[test]
    fn shutdown_escalates_to_kill_and_reaps() {
        let mut child = Command::new("/usr/bin/dash")
            .args(["-c", "trap '' TERM; while :; do :; done"])
            .spawn()
            .unwrap();
        terminate_and_reap(&mut child, Duration::from_millis(10)).unwrap();
        assert!(child.try_wait().unwrap().is_some());
    }
}
