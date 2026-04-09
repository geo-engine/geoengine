#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::print_stderr)] // okay in tests

use assert_cmd::cargo_bin;
use geoengine_services::test_data;
use std::process::{ChildStderr, Command, Stdio};

struct DroppingServer {
    process: std::process::Child,
}

impl Drop for DroppingServer {
    fn drop(&mut self) {
        self.process.kill().unwrap();
    }
}

impl DroppingServer {
    fn new(schema_name: &str) -> Self {
        let process = Command::new(cargo_bin!("geoengine-server"))
            .env("GEOENGINE_WEB__BACKEND", "postgres")
            .env("GEOENGINE_POSTGRES__SCHEMA", schema_name)
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        Self { process }
    }

    fn stderr(&mut self) -> ChildStderr {
        self.process.stderr.take().expect("failed to read stderr")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn it_starts_without_warnings_and_accepts_connections() {
    use geoengine_services::config::get_config_element;

    const SCHEMA_NAME: &str = "it_starts_without_warnings_and_accepts_connections";

    fn run_server_and_check_warnings() {
        let mut server = DroppingServer::new(SCHEMA_NAME);

        // read log output and check for warnings
        let startup_result = Command::new(cargo_bin!("geoengine-cli"))
            .args([
                "check-successful-startup",
                "--max-lines",
                "200",
                "--fail-on-warnings",
            ])
            .stdin(Stdio::from(server.stderr()))
            .output()
            .unwrap();

        assert!(
            startup_result.status.success(),
            "failed to check startup: {startup_result:?}",
        );

        // once log outputs stop, perform a test request
        let heartbeat_result = Command::new(cargo_bin!("geoengine-cli"))
            .args(["heartbeat", "--server-url", "http://127.0.0.1:3030/api"])
            .output()
            .unwrap();

        assert!(
            heartbeat_result.status.success(),
            "server is not alive: {heartbeat_result:?}",
        );
    }

    // change cwd s.t. the config file can be found
    std::env::set_current_dir(test_data!("..")).expect("failed to set current directory");

    // create a fresh schema for this test
    let config = get_config_element::<geoengine_services::config::Postgres>().unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host={host} port={port} user={user} password={password} dbname={database}",
            host = config.host,
            user = config.user,
            port = config.port,
            password = config.password,
            database = config.database
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE; CREATE SCHEMA {SCHEMA_NAME}",
        ))
        .await
        .unwrap();

    let result = std::panic::catch_unwind(move || {
        // run server 1st time -> initialization
        run_server_and_check_warnings();

        // run server 2nd time on initialized schema
        run_server_and_check_warnings();
    });

    client
        .batch_execute(&format!("DROP SCHEMA {SCHEMA_NAME};",))
        .await
        .unwrap();

    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
