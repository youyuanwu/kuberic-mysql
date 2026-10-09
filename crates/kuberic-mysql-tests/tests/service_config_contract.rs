#[path = "service_common/mod.rs"]
mod common;

use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::time::Duration;

use common::{TestRoot, timeouts};
use kuberic_mysql::service::{
    ConfigError, MysqlInstanceConfig, MysqlInstanceError, MysqlInstanceManager, MysqlInstanceState,
    MysqlOperationTimeouts, ProductError,
};

#[test]
fn exact_render_separates_persistent_data_from_disposable_runtime() {
    let root = TestRoot::new("render");
    let config = root.config();
    let rendered = config.render_server_config();
    let expected = format!(
        "[mysqld]\n\
         datadir={}\n\
         socket={}\n\
         pid-file={}\n\
         log-error={}\n\
         tmpdir={}/tmp\n\
         secure-file-priv={}/secure-files\n\
         log-bin={}/mysql-bin\n\
         relay-log={}/relay-bin\n\
         skip-networking=ON\n\
         mysqlx=OFF\n\
         server-id=1\n\
         binlog-format=ROW\n\
         binlog-checksum=NONE\n\
         relay-log-recovery=ON\n\
         gtid-mode=ON\n\
         enforce-gtid-consistency=ON\n\
         plugin-load-add=group_replication.so\n\
         loose-group-replication-group-name=cccccccc-cccc-cccc-cccc-cccccccccccc\n\
         loose-group-replication-local-address=127.0.0.1:33061\n\
         loose-group-replication-group-seeds=127.0.0.1:33061\n\
         loose-group-replication-single-primary-mode=ON\n\
         loose-group-replication-enforce-update-everywhere-checks=OFF\n\
         loose-group-replication-start-on-boot=OFF\n\
         loose-group-replication-bootstrap-group=OFF\n",
        root.data.display(),
        root.scratch.join("mysql.sock").display(),
        root.scratch.join("mysqld.pid").display(),
        root.scratch.join("mysqld.err").display(),
        root.scratch.display(),
        root.scratch.display(),
        root.scratch.display(),
        root.scratch.display(),
    );

    assert_eq!(rendered, expected);
    for line in rendered.lines().filter(|line| {
        [
            "socket=",
            "pid-file=",
            "log-error=",
            "tmpdir=",
            "secure-file-priv=",
            "log-bin=",
            "relay-log=",
        ]
        .iter()
        .any(|prefix| line.starts_with(prefix))
    }) {
        assert!(line.contains(root.scratch.to_str().unwrap()), "{line}");
        assert!(!line.contains(root.data.to_str().unwrap()), "{line}");
    }
}

#[test]
fn roots_must_be_fresh_distinct_normalized_and_non_symlinked() {
    let root = TestRoot::new("paths");
    fs::create_dir(&root.data).unwrap();
    let existing = MysqlInstanceConfig::new(
        "/usr/bin/sleep",
        "/usr/bin/env",
        &root.data,
        &root.scratch,
        timeouts(),
    )
    .unwrap_err();
    assert_eq!(existing, ConfigError::RootAlreadyExists);
    fs::remove_dir(&root.data).unwrap();

    let overlap = MysqlInstanceConfig::new(
        "/usr/bin/sleep",
        "/usr/bin/env",
        &root.data,
        root.data.join("runtime"),
        timeouts(),
    )
    .unwrap_err();
    assert_eq!(overlap, ConfigError::RootOverlap);

    let alias = root.root.join("alias");
    symlink("/usr/bin", &alias).unwrap();
    let linked_binary = MysqlInstanceConfig::new(
        alias.join("sleep"),
        "/usr/bin/env",
        &root.data,
        &root.scratch,
        timeouts(),
    )
    .unwrap_err();
    assert_eq!(linked_binary, ConfigError::Symlink);

    let lexical = MysqlInstanceConfig::new(
        "/usr/bin/sleep",
        "/usr/bin/env",
        root.root.join("missing").join("..").join("data"),
        &root.scratch,
        timeouts(),
    )
    .unwrap_err();
    assert_eq!(lexical, ConfigError::PathNotNormalized);
}

#[test]
fn overlapping_existing_parent_is_rejected_as_overlap() {
    let root = TestRoot::new("overlap");
    let nested = root.data.join("runtime");
    let error = MysqlInstanceConfig::new(
        "/usr/bin/sleep",
        "/usr/bin/env",
        &root.data,
        &nested,
        timeouts(),
    )
    .unwrap_err();
    assert_eq!(error, ConfigError::RootOverlap);
}

#[test]
fn initialization_creates_only_owner_private_roots_and_runtime_directories() {
    let root = TestRoot::new("permissions");
    let mut manager = MysqlInstanceManager::new(root.config());
    manager.initialize().unwrap();

    for path in [
        root.data.as_path(),
        root.scratch.as_path(),
        manager
            .config()
            .runtime()
            .scratch_root()
            .join("tmp")
            .as_path(),
        manager
            .config()
            .runtime()
            .scratch_root()
            .join("secure-files")
            .as_path(),
    ] {
        let mode = fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "{}", path.display());
    }
    assert!(root.data.read_dir().unwrap().next().is_none());
}

#[test]
fn every_deadline_must_be_positive() {
    for position in 0..4 {
        let mut values = [Duration::from_secs(1); 4];
        values[position] = Duration::ZERO;
        assert_eq!(
            MysqlOperationTimeouts::new(values[0], values[1], values[2], values[3]),
            Err(ConfigError::ZeroTimeout)
        );
    }
}

#[test]
fn fresh_root_parent_must_be_private_and_owned_by_the_caller() {
    let root = TestRoot::new("parent");
    let public_parent = root.root.join("public");
    fs::create_dir(&public_parent).unwrap();
    fs::set_permissions(&public_parent, fs::Permissions::from_mode(0o777)).unwrap();
    let public_error = MysqlInstanceConfig::new(
        "/usr/bin/sleep",
        "/usr/bin/env",
        public_parent.join("data"),
        public_parent.join("scratch"),
        timeouts(),
    )
    .unwrap_err();
    assert_eq!(public_error, ConfigError::InvalidRootParent);

    let foreign_error = MysqlInstanceConfig::new(
        "/usr/bin/sleep",
        "/usr/bin/env",
        format!("/tmp/kms-foreign-data-{}", std::process::id()),
        root.root.join("scratch"),
        timeouts(),
    )
    .unwrap_err();
    assert_eq!(foreign_error, ConfigError::InvalidRootParent);
}

#[test]
fn generated_runtime_paths_never_enter_data_root() {
    let root = TestRoot::new("layout");
    let config = root.config();
    for path in [
        config.runtime().config(),
        config.runtime().socket(),
        config.runtime().pid(),
    ] {
        assert!(path.starts_with(&root.scratch));
        assert!(!path.starts_with(&root.data));
    }
    assert_eq!(config.data_root(), Path::new(&root.data));
}

#[test]
fn unsupported_product_identity_fails_and_releases_fresh_roots() {
    let root = TestRoot::new("product");
    fs::write(
        &root.launcher,
        "#!/bin/sh\nwhile [ \"$1\" != \"--\" ]; do shift; done\nshift 2\necho 'Ver 8.4.12 for Linux on x86_64 (MySQL Community Server - GPL)'\n",
    )
    .unwrap();
    fs::set_permissions(&root.launcher, fs::Permissions::from_mode(0o700)).unwrap();
    let config = MysqlInstanceConfig::new(
        "/usr/bin/sleep",
        &root.launcher,
        &root.data,
        &root.scratch,
        timeouts(),
    )
    .unwrap();
    let mut manager = MysqlInstanceManager::new(config);
    assert!(matches!(
        manager.initialize(),
        Err(MysqlInstanceError::Product(
            ProductError::UnsupportedIdentity
        ))
    ));
    assert!(!root.data.exists());
    assert!(!root.scratch.exists());
}

#[test]
fn ownership_race_faults_without_removing_an_unclaimed_root() {
    let root = TestRoot::new("race");
    let config = root.config();
    fs::create_dir(&root.data).unwrap();
    let mut manager = MysqlInstanceManager::new(config);

    assert!(matches!(
        manager.initialize(),
        Err(MysqlInstanceError::Config(ConfigError::RootAlreadyExists))
    ));
    assert_eq!(manager.state(), MysqlInstanceState::Faulted);
    assert!(root.data.is_dir());
    assert!(!root.scratch.exists());
}

#[test]
fn socket_path_must_fit_the_linux_unix_address() {
    let root = TestRoot::new("socket");
    let long = root.root.join("x".repeat(120));
    let error = MysqlInstanceConfig::new(
        "/usr/bin/sleep",
        "/usr/bin/env",
        &root.data,
        long,
        timeouts(),
    )
    .unwrap_err();
    assert_eq!(error, ConfigError::PathNotRepresentable);
}

#[test]
fn option_file_metacharacters_are_rejected_in_every_path() {
    let root = TestRoot::new("option");
    for suffix in ["hash#root", "back\\slash", "space root", "trailing "] {
        let error = MysqlInstanceConfig::new(
            "/usr/bin/sleep",
            "/usr/bin/env",
            root.root.join(suffix),
            &root.scratch,
            timeouts(),
        )
        .unwrap_err();
        assert_eq!(error, ConfigError::PathNotRepresentable, "{suffix:?}");
    }
}
