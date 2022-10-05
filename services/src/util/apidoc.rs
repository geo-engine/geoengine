use utoipa::Modify;

pub struct OpenApiServerInfo;

impl Modify for OpenApiServerInfo {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let web_config: crate::util::config::Web =
            crate::util::config::get_config_element().expect("web config");

        let external_address = web_config.external_address().expect("external address");

        openapi.servers = Some(vec![utoipa::openapi::ServerBuilder::new()
            .url(external_address.to_string())
            .build()]);
    }
}
