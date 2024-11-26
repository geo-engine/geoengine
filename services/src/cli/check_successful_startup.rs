use clap::Parser;
use tokio::io::AsyncBufReadExt;

/// Checks the program's `STDERR` for successful startup
#[derive(Debug, Parser)]
pub struct CheckSuccessfulStartup {
    /// Timeout in seconds
    #[arg(long, default_value = "60")]
    timeout: u16,

    /// Maximum number of lines to check
    #[arg(long, default_value = "1000")]
    max_lines: u16,

    /// Fail on warnings
    #[arg(long, default_value = "false")]
    fail_on_warnings: bool,

    /// Output `STDIN` to `STDERR`
    #[arg(long, default_value = "false")]
    output_stdin: bool,
}

/// Checks the program's `STDERR` for successful startup
#[allow(clippy::print_stderr)]
pub async fn check_successful_startup(params: CheckSuccessfulStartup) -> Result<(), anyhow::Error> {
    eprintln!(
        "Checking for successful startup with timeout of {} seconds and {} lines",
        params.timeout, params.max_lines
    );

    let success = tokio::time::timeout(
        std::time::Duration::from_secs(params.timeout.into()),
        check_lines(
            params.max_lines,
            params.fail_on_warnings,
            params.output_stdin,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("Timeout"))??;

    if success {
        eprintln!("Server started successfully");
        Ok(())
    } else {
        Err(anyhow::anyhow!("Server did not start successfully"))
    }
}

#[allow(clippy::print_stderr)]
async fn check_lines(
    max_lines: u16,
    fail_on_warnings: bool,
    output_stdin: bool,
) -> Result<bool, anyhow::Error> {
    let mut line_reader = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut lines_left = max_lines;

    while let Some(line) = line_reader.next_line().await? {
        if output_stdin {
            eprintln!("{line}");
        }

        if line.contains("Tokio runtime found") {
            return Ok(true);
        }

        if fail_on_warnings && line.contains("WARN") {
            return Err(anyhow::anyhow!("Warning in log output: {line}"));
        }

        lines_left -= 1;
        if lines_left == 0 {
            break;
        }
    }

    Ok(false)
}
