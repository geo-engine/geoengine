use clap::Parser;
use url::Url;

/// Checks if the Geo Engine server is alive
#[derive(Debug, Parser)]
pub struct Heartbeat {
    /// Server URL
    #[arg(long)]
    server_url: Url,
}

const API_ENDPOINT: &str = "info";

/// Checks the program's `STDERR` for successful startup
#[allow(clippy::print_stderr)]
pub async fn check_heartbeat(params: Heartbeat) -> Result<(), anyhow::Error> {
    let server_path = canonicalize_url(params.server_url).join(API_ENDPOINT)?;
    let server_response = reqwest::get(server_path).await?;

    if server_response.status().is_success() {
        eprintln!("Server is alive");
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Server {url} is not alive. Status: {status}",
            url = server_response.url(),
            status = server_response.status()
        ))
    }
}

/// Canonicalizes a URL by ensuring it ends with a slash
fn canonicalize_url(mut url: Url) -> Url {
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    url
}
