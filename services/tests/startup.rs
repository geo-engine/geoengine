use assert_cmd::prelude::*;
use geoengine_services::test_data;
use serial_test::serial;
use std::{
    io::BufRead,
    process::{Command, Stdio},
};

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
        let process = Command::cargo_bin("main")
            .unwrap()
            .env("GEOENGINE_WEB__BACKEND", "postgres")
            .env("GEOENGINE_POSTGRES__SCHEMA", schema_name)
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        Self { process }
    }

    fn stderr_lines(&mut self) -> impl Iterator<Item = String> {
        let mut reader =
            std::io::BufReader::new(self.process.stderr.take().expect("failed to read stderr"));

        std::iter::from_fn(move || {
            let mut buf = String::new();
            reader
                .read_line(&mut buf)
                .ok()
                .filter(|_| !buf.is_empty())
                .map(|_| buf)
        })
    }
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn it_starts_without_warnings_and_accepts_connections() {
    use geoengine_services::util::config::get_config_element;

    const SCHEMA_NAME: &str = "it_starts_without_warnings_and_accepts_connections";

    async fn run_server_and_check_warnings() {
        let mut server = DroppingServer::new(SCHEMA_NAME);

        // read log output and check for warnings
        let mut startup_succesful = false;
        for line in server.stderr_lines().take(100) {
            // eprintln!("Line: {line}");

            assert!(!line.contains("WARN"), "Warning in log output: {line}");

            if line.contains("Tokio runtime found") {
                startup_succesful = true;
                break;
            }
        }

        // once log outputs stop, perform a test request
        reqwest::get("http://127.0.0.1:3030/info")
            .await
            .expect("failed to connect to server");

        assert!(startup_succesful);
    }

    // change cwd s.t. the config file can be found
    std::env::set_current_dir(test_data!("..")).expect("failed to set current directory");

    // create a fresh schema for this test
    let config = get_config_element::<geoengine_services::util::config::Postgres>().unwrap();
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
        tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move {
                // run server 1st time -> initialization
                run_server_and_check_warnings().await;

                // run server 2nd time on initialized schmea
                run_server_and_check_warnings().await;
            })
        });
    });

    client
        .batch_execute(&format!("DROP SCHEMA {SCHEMA_NAME};",))
        .await
        .unwrap();

    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
