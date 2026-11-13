/// Fails to compile if cargo does not build `gdalsource-process` for the tests.
#[test]
fn gdalsource_process_is_built_and_runs() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_gdalsource-process"))
        .output()
        .expect("gdalsource-process should start");

    assert!(!output.status.success(), "no arguments must be an error");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Usage: gdalsource-process"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
