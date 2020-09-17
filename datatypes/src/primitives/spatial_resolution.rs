/// The spatial resolution in SRS units
#[derive(Copy, Clone, Debug)]
pub struct SpatialResolution {
    pub x_axis_resolution: f64,
    pub y_axis_resolution: f64,
}

impl SpatialResolution {
    /// Create a new SpatialResolution object
    pub fn new(x_axis_resolution: f64, y_axis_resolution: f64) -> Self { // TODO: check for positive and > 0?
        SpatialResolution {
            x_axis_resolution,
            y_axis_resolution
        }
    }
}

impl Default for SpatialResolution {
    fn default() -> Self {
        SpatialResolution {
            x_axis_resolution: 0.1,
            y_axis_resolution: 0.1
        }
    }
}

impl From<(f64, f64)> for SpatialResolution {
    fn from(xy_axis_resolution: (f64, f64)) -> Self {
        SpatialResolution {
            x_axis_resolution: xy_axis_resolution.0,
            y_axis_resolution: xy_axis_resolution.1
        }
    }
}