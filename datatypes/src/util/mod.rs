mod any;
pub mod arrow;
mod byte_size;
mod db_types;
pub mod gdal;
pub mod helpers;
pub mod identifiers;
mod image;
pub mod ranges;
mod result;
pub mod test;
pub mod well_known_data;

pub use self::identifiers::Identifier;
pub use any::{AsAny, AsAnyArc};
pub use byte_size::ByteSize;
pub use db_types::{HashMapTextTextDbType, NotNanF64, StringPair, TextTextKeyValue};
pub use image::assert_image_equals;
pub use result::Result;
use std::path::{Path, PathBuf};

/// Canonicalize `base`/`sub_path` and ensure the `sub_path` doesn't escape the `base`
/// returns an error if the `sub_path` escapes the `base`
///
/// This only works if the `Path` you are referring to actually exists.
///
pub fn canonicalize_subpath(base: &Path, sub_path: &Path) -> Result<PathBuf> {
    let base = base.canonicalize()?;
    let path = base.join(sub_path).canonicalize()?;

    if path.starts_with(&base) {
        Ok(path)
    } else {
        Err(crate::error::Error::SubPathMustNotEscapeBasePath {
            base,
            sub_path: sub_path.into(),
        })
    }
}

#[cfg(test)]
mod mod_tests {
    use super::*;
    #[test]
    fn it_doesnt_escape_base_path() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path();
        std::fs::create_dir_all(tmp_path.join("foo/bar/foobar")).unwrap();
        std::fs::create_dir_all(tmp_path.join("foo/barfoo")).unwrap();

        assert_eq!(
            canonicalize_subpath(&tmp_path.join("foo/bar"), Path::new("foobar"))
                .unwrap()
                .to_string_lossy(),
            tmp_path.join("foo/bar/foobar").to_string_lossy()
        );

        assert!(canonicalize_subpath(&tmp_path.join("foo/bar"), Path::new("../barfoo")).is_err());
    }
}
