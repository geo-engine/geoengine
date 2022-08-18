use serde::{Deserialize, Serialize};
use std::fmt::Display;

pub fn hide_gdal_errors() {
    gdal::config::set_error_handler(|_, _, _| {});
}

// TODO: push to `rust-gdal`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ResamplingMethod {
    Nearest,
    Average,
    Bilinear,
    Cubic,
    CubicSpline,
    Lanczos,
}

impl Display for ResamplingMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResamplingMethod::Nearest => write!(f, "NEAREST"),
            ResamplingMethod::Average => write!(f, "AVERAGE"),
            ResamplingMethod::Bilinear => write!(f, "BILINEAR"),
            ResamplingMethod::Cubic => write!(f, "CUBIC"),
            ResamplingMethod::CubicSpline => write!(f, "CUBICSPLINE"),
            ResamplingMethod::Lanczos => write!(f, "LANCZOS"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_resampling_method() {
        let input = "\"NEAREST\"";
        let method = serde_json::from_str::<ResamplingMethod>(input).unwrap();

        assert_eq!(method, ResamplingMethod::Nearest);
        assert_eq!(method.to_string(), "NEAREST");
    }
}
