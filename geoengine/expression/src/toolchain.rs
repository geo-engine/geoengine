//! Included by `build.rs` as well, so it may only use `std`.

use std::io::{BufWriter, Write};

/// Writes a minimal rustup toolchain file based on the toolchain file used by the geoengine crate
pub fn write_minimal_toolchain_file(
    writer: &mut BufWriter<impl Write + Unpin>,
    rust_toolchain_toml: &str,
) -> Result<(), std::io::Error> {
    const COMPONENTS_LINE_PREFIX: &str = "components = ";
    const REPLACEMENT_COMPONENTS: &str = r#"["rustc", "cargo", "rust-std"]"#;

    for line in rust_toolchain_toml.lines() {
        if line.starts_with(COMPONENTS_LINE_PREFIX) {
            writer.write_all(COMPONENTS_LINE_PREFIX.as_bytes())?;
            writer.write_all(REPLACEMENT_COMPONENTS.as_bytes())?;
        } else {
            writer.write_all(line.as_bytes())?;
        }
        writer.write_all(b"\n")?;
    }

    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;

    #[test]
    fn it_write_minimal_toolchain_file() {
        const INPUT: &str = indoc! {r#"
        [toolchain]
        channel = "1.91.1"
        components = ["cargo", "rustfmt", "rust-src", "clippy", "llvm-tools"]
        "#};
        const EXPECTED_OUTPUT: &str = indoc! {r#"
        [toolchain]
        channel = "1.91.1"
        components = ["rustc", "cargo", "rust-std"]
        "#};

        let mut output = Vec::new();
        {
            let mut writer = BufWriter::new(&mut output);
            write_minimal_toolchain_file(&mut writer, INPUT).unwrap();
        }
        let output_str = String::from_utf8(output).unwrap();
        assert_eq!(output_str, EXPECTED_OUTPUT);
    }
}
