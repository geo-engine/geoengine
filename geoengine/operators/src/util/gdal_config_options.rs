use super::Result;

/// Set thread local gdal options and revert them on drop
pub struct GdalConfigOptions {
    original_configs: Vec<(String, Option<String>)>,
}

impl GdalConfigOptions {
    /// Set thread local gdal options and revert them on drop
    pub fn new(configs: &[(String, String)]) -> Result<Self> {
        let mut original_configs = vec![];

        for (key, value) in configs {
            let old = gdal::config::get_config_option(key, "")
                .map(|value| if value.is_empty() { None } else { Some(value) })?;

            // TODO: check if overriding existing config (local & global) is ok for the given key
            gdal::config::set_config_option(key, value)?;
            tracing::trace!("set {key}={value}");

            original_configs.push((key.clone(), old));
        }

        Ok(Self { original_configs })
    }
}

impl Drop for GdalConfigOptions {
    fn drop(&mut self) {
        for (key, value) in &self.original_configs {
            if let Some(value) = value {
                let _result = gdal::config::set_config_option(key, value);
            } else {
                let _result = gdal::config::clear_config_option(key);
            }
        }
    }
}

/// Set thread local gdal options and revert them on drop
pub struct TemporaryGdalThreadLocalConfigOptions {
    original_configs: Vec<(String, Option<String>)>,
}

impl TemporaryGdalThreadLocalConfigOptions {
    /// Set thread local gdal options and revert them on drop
    pub fn new(configs: &[(String, String)]) -> Result<Self> {
        let mut original_configs = vec![];

        for (key, value) in configs {
            let old = gdal::config::get_thread_local_config_option(key, "")
                .map(|value| if value.is_empty() { None } else { Some(value) })?;

            // TODO: check if overriding existing config (local & global) is ok for the given key
            gdal::config::set_thread_local_config_option(key, value)?;
            tracing::trace!("set {key}={value}");

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_reverts_config_options() {
        let config_options = vec![("foo".to_owned(), "bar".to_owned())];

        {
            let _config = GdalConfigOptions::new(config_options.as_slice()).unwrap();

            assert_eq!(
                gdal::config::get_config_option("foo", "default").unwrap(),
                "bar".to_owned()
            );
        }

        assert_eq!(
            gdal::config::get_config_option("foo", "").unwrap(),
            String::new()
        );
    }

    #[test]
    fn it_reverts_temp_config_options() {
        let config_options = vec![("foo".to_owned(), "bar".to_owned())];

        {
            let _config =
                TemporaryGdalThreadLocalConfigOptions::new(config_options.as_slice()).unwrap();

            assert_eq!(
                gdal::config::get_config_option("foo", "default").unwrap(),
                "bar".to_owned()
            );
        }

        assert_eq!(
            gdal::config::get_config_option("foo", "").unwrap(),
            String::new()
        );
    }
}
