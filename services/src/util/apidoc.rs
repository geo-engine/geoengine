use utoipa::Modify;

pub struct OpenApiServerInfo;

impl Modify for OpenApiServerInfo {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let web_config: crate::util::config::Web =
            crate::util::config::get_config_element().expect("web config");

        let mut api_url = web_config.api_url().expect("external address").to_string();
        api_url.pop(); //remove trailing slash because codegen requires it

        openapi.servers = Some(vec![utoipa::openapi::ServerBuilder::new()
            .url(api_url)
            .build()]);
    }
}
