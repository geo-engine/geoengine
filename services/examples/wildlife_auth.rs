#![allow(clippy::unwrap_used, clippy::print_stderr, clippy::dbg_macro)] // ok for example
#![allow(dead_code)] // TODO: remove

#[derive(serde::Deserialize, Debug)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: usize,
    refresh_expires_in: usize,
    #[serde(rename = "not-before-policy")]
    not_before_policy: usize,
    scope: String,
}

#[tokio::main]
async fn main() {
    let user = "geoengine";
    let password = std::env::var("PASSWORD").unwrap_or_else(|_| {
        panic!("Please set the PASSWORD environment variable");
    });

    dbg!(user, &password);

    let response = reqwest::Client::new()
        .post("https://webapp.senckenberg.de/auth/realms/wildlive-portal/protocol/openid-connect/token")
        .body( // TODO: urlencode
            serde_urlencoded::to_string([
                ("grant_type", "client_credentials"),
                ("client_id", user),
                ("client_secret", &password),
                ]).unwrap()
            ,
        )
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await
        .unwrap();

    // dbg!(&response.json::<serde_json::Value>().await);

    let token_response: TokenResponse = response.json().await.unwrap();

    dbg!(&token_response);

    let response = reqwest::Client::new()
        .get("https://wildlive.senckenberg.de/api/check-credentials")
        .bearer_auth(token_response.access_token.as_str())
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
}
