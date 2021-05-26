use anyhow::Result;
use vergen::{vergen, Config, TimestampKind};

fn main() -> Result<()> {
    let mut config = Config::default();

    *config.build_mut().kind_mut() = TimestampKind::DateOnly;

    vergen(config)
}
