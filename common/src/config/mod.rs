use lazy_static::lazy_static;
use std::sync::RwLock;
use config::{Config, File};

lazy_static! {
	pub static ref CONFIG: RwLock<Config> = {
	    let mut config = Config::default();

	    // TODO: logging
	    if !config.merge(File::with_name("conf/default.toml")).is_ok() {
	        eprintln!("Could not load from conf/default.toml");
	    }

	    if !config.merge(File::with_name("conf/settings.toml")).is_ok() {
	        eprintln!("Could not load from conf/settings.toml");
	    }

        RwLock::new(config)
	};
}
