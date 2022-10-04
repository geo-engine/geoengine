use super::Result;
use log::info;

/// Set thread local gdal options and revert them on drop
pub(crate) struct TemporaryGdalThreadLocalConfigOptions {
    original_configs: Vec<(String, Option<String>)>,
}

impl TemporaryGdalThreadLocalConfigOptions {
    /// Set thread local gdal options and revert them on drop
    pub fn new(configs: &[(String, String)]) -> Result<Self> {
        let mut original_configs = vec![];

        for (key, value) in configs {
            let old = gdal::config::get_thread_local_config_option(key, "").map(|value| {
                if value.is_empty() {
                    None
                } else {
                    Some(value)
                }
            })?;

            // TODO: check if overriding existing config (local & global) is ok for the given key
            gdal::config::set_thread_local_config_option(key, value)?;
            info!("set {}={}", key, value);

            original_configs.push((key.clone(), old));
        }

        Ok(Self { original_configs })
    }
}

impl Drop for TemporaryGdalThreadLocalConfigOptions {
    fn drop(&mut self) {
        for (key, value) in &self.original_configs {
            if let Some(value) = value {
                let _result = gdal::config::set_thread_local_config_option(key, value);
            } else {
                let _result = gdal::config::clear_thread_local_config_option(key);
            }
        }
    }
}
