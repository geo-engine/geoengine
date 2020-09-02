use geoengine_services::server;

#[tokio::main]
async fn main() {
    server::start_server(None).await.expect("server run");
}
