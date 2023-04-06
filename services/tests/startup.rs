use assert_cmd::prelude::*;

use geoengine_services::test_data;
use serial_test::serial;
use std::{
    io::BufRead,
    process::{Command, Stdio},
};

#[cfg(feature = "pro")]
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn it_starts_postgres_backend_with_no_warnings() {
    use geoengine_services::util::config::get_config_element;

    const SCHEMA_NAME: &str = "it_starts_postgres_backend_with_no_warnings";

    async fn run_server_and_check_warnings() {
        let mut server = Command::cargo_bin("main")
            .unwrap()
            .env("GEOENGINE_WEB__BACKEND", "postgres")
            .env("GEOENGINE_POSTGRES__SCHEMA", SCHEMA_NAME)
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let stderr = server.stderr.take().expect("failed to read stderr");

        let mut reader = std::io::BufReader::new(stderr);

        let mut startup_succesful = false;
        for _ in 0..99 {
            let mut buf = String::new();
            reader.read_line(&mut buf).unwrap();

            // println!("{buf}");

            assert!(!buf.contains("WARN"));

            if buf.contains("Tokio runtime found") {
                startup_succesful = true;
                break;
            }
        }

        server.kill().unwrap();

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

    assert!(result.is_ok());
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn it_starts_in_memory_backend_with_no_warnings() {
    // change cwd s.t. the config file can be found
    std::env::set_current_dir(test_data!("..")).expect("failed to set current directory");

    let mut server = Command::cargo_bin("main")
        .unwrap()
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let stderr = server.stderr.take().expect("failed to read stderr");

    let mut reader = std::io::BufReader::new(stderr);

    let mut startup_succesful = false;
    for _ in 0..99 {
        let mut buf = String::new();
        reader.read_line(&mut buf).unwrap();

        // println!("{buf}");

        assert!(!buf.contains("WARN"));

        if buf.contains("Tokio runtime found") {
            startup_succesful = true;
            break;
        }
    }

    server.kill().unwrap();

    assert!(startup_succesful);
}
