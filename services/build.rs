use vergen::{vergen, Config, TimestampKind};

fn main() {
    let mut config = Config::default();

    *config.build_mut().kind_mut() = TimestampKind::DateOnly;

    vergen(config).expect("Unable to generate version info");
}
