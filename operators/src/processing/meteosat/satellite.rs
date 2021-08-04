//! # Satellite
//! Contains the `Satellite` struct and utility methods
//! for working with Meteosat satellites 8-11

use crate::error::Error;
use crate::util::Result;
use geoengine_datatypes::primitives::Coordinate2D;

const VIEW_ANGLE: f64 = -13642337.0 * 3000.403165817;
const VIEW_ANGLE_CH11: f64 = -40927014.0 * 1000.134348869;

const C1: f64 = 1.19104273e-16;
const C2: f64 = 0.0143877523;

/// Represents the parameters of a Meteosat satellite
#[derive(Debug, PartialEq)]
pub struct Satellite {
    pub name: &'static str,
    pub meteosat_id: u8,
    pub msg_id: u8,
    pub channels: [Channel; 12],
}

/// Represents a channel of a meteosat satellite.
#[derive(Debug, PartialEq)]
pub struct Channel {
    pub name: &'static str,
    view_angle_factor: f64,
    pub cwl: f64,
    pub etsr: f64,
    pub vc: f64,
    pub alpha: f64,
    pub beta: f64,
}

impl Satellite {
    /// Retrieves the Meteosat satellite for the given `name`.
    /// The name comparison is *case sensitive*.
    /// # Examples
    /// ```
    /// assert!(Sattelite::satellite_by_name("Meteosat-8").is_ok());
    /// assert!(Sattelite::satellite_by_name("Meteosat-42").is_err());
    /// ```
    pub fn satellite_by_name(name: &str) -> Result<&'static Satellite> {
        match name {
            "Meteosat-8" => Ok(&METEOSAT_08),
            "Meteosat-9" => Ok(&METEOSAT_09),
            "Meteosat-10" => Ok(&METEOSAT_10),
            "Meteosat-11" => Ok(&METEOSAT_11),
            _ => Err(Error::InvalidMeteosatSatellite),
        }
    }

    /// Retrieves the Meteosat satellite for the `msg_id` (1-4).
    /// # Examples
    /// ```
    /// assert!(Sattelite::satellite_by_msg_id(1).is_ok());
    /// assert!(Sattelite::satellite_by_msg_id(42).is_err());
    /// ```
    pub fn satellite_by_msg_id(msg_id: u8) -> Result<&'static Satellite> {
        match msg_id {
            1 => Ok(&METEOSAT_08),
            2 => Ok(&METEOSAT_09),
            3 => Ok(&METEOSAT_10),
            4 => Ok(&METEOSAT_11),
            _ => Err(Error::InvalidMeteosatSatellite),
        }
    }

    /// Returns the channel for the given id (0-11).
    /// # Examples
    /// ```
    /// let s = Satellite::by_msg_id(1);
    /// assert!(s.get_channel(0).is_ok());
    /// assert!(s.get_channel(42).is_err());
    /// ```
    pub fn get_channel(&self, channel_id: usize) -> Result<&Channel> {
        match self.channels.get(channel_id) {
            Some(c) => Ok(c),
            _ => Err(Error::InvalidChannel {
                channel: channel_id,
            }),
        }
    }

    /// Returns the HRV channel
    pub fn get_hrv(&self) -> &Channel {
        unsafe { self.channels.get_unchecked(11) }
    }
}

impl Channel {
    /// Computes the satellite's view angle for the given
    /// channel and coordinate.
    pub fn view_angle(&self, geos_coordinate: Coordinate2D) -> Coordinate2D {
        geos_coordinate * self.view_angle_factor * Coordinate2D { x: -1.0, y: 1.0 }
    }

    /// Computes lat/lon coordinates of the satellite's view
    /// angle for the given channel and coordinate.
    pub fn view_angle_lat_lon(&self, geos_coordinate: Coordinate2D, sub_lon: f64) -> (f64, f64) {
        let view_angle = self.view_angle(geos_coordinate);

        let (x, y) = (view_angle.x.to_radians(), view_angle.y.to_radians());

        let (sinx, siny) = (x.sin(), y.sin());
        let (cosx, cosy) = (x.cos(), y.cos());

        let cos2y = cosy.powi(2);
        let sin2y = siny.powi(2);
        let cosxcosy = cosx * cosy;
        let cos2yconstsin2y = cos2y + 1.006803 * sin2y;

        let sd_part1 = (42164.0 * cosxcosy).powi(2);
        let sd_part2 = cos2yconstsin2y * 1737121856.0;
        let sd = (sd_part1 - sd_part2).sqrt();

        let sn_part1 = 42164.0 * cosxcosy - sd;
        let sn_part2 = cos2yconstsin2y;
        let sn = sn_part1 / sn_part2;

        let s1 = 42164.0 - sn * cosxcosy;
        let s2 = sn * sinx * cosy;
        let s3 = -1.0 * sn * siny;
        let sxy = (s1.powi(2) + s2.powi(2)).sqrt();

        //
        let lon_rad_part = s2 / s1;
        let lat_rad_part = 1.006804 * s3 / sxy;
        let (lat, lon) = (
            lat_rad_part.atan(),
            lon_rad_part.atan() + sub_lon.to_radians(),
        );
        (lat, lon)
    }

    /// Calculates the temperature in K from the given channel and radiance value.
    pub fn calculate_temperature_from_radiance(&self, radiance: f64) -> f64 {
        let temp = (C1 * 1.0e6 * self.vc.powi(3)) / (1.0e-5 * radiance);
        ((C2 * 100.0 * self.vc / (temp + 1.0).ln()) - self.beta) / self.alpha
    }
}

//
// The following constants are from: "The Conversion from Effective Radiances to Equivalent Brightness Temperatures"
// https://www.eumetsat.int/website/wcm/idc/idcplg?IdcService=GET_FILE&dDocName=PDF_EFFECT_RAD_TO_BRIGHTNESS&RevisionSelectionMethod=LatestReleased&Rendition=Web
//

static METEOSAT_08: Satellite = Satellite {
    name: "Meteosat-8",
    meteosat_id: 8,
    msg_id: 1,
    channels: [
        Channel {
            name: "VIS006",
            view_angle_factor: VIEW_ANGLE,
            cwl: 0.639,
            etsr: 65.2296,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
        Channel {
            name: "VIS008",
            view_angle_factor: VIEW_ANGLE,
            cwl: 0.809,
            etsr: 73.0127,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
        Channel {
            name: "IR_016",
            view_angle_factor: VIEW_ANGLE,
            cwl: 1.635,
            etsr: 62.3715,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
        Channel {
            name: "IR_039",
            view_angle_factor: VIEW_ANGLE,
            cwl: 3.965,
            etsr: 0.0000,
            vc: 2567.3300,
            alpha: 0.9956,
            beta: 3.4100,
        },
        Channel {
            name: "WV_062",
            view_angle_factor: VIEW_ANGLE,
            cwl: 6.337,
            etsr: 0.0000,
            vc: 1598.1030,
            alpha: 0.9962,
            beta: 2.2180,
        },
        Channel {
            name: "WV_073",
            view_angle_factor: VIEW_ANGLE,
            cwl: 7.362,
            etsr: 0.0000,
            vc: 1362.0810,
            alpha: 0.9991,
            beta: 0.4780,
        },
        Channel {
            name: "IR_087",
            view_angle_factor: VIEW_ANGLE,
            cwl: 8.718,
            etsr: 0.0000,
            vc: 1149.0690,
            alpha: 0.9996,
            beta: 0.1790,
        },
        Channel {
            name: "IR_097",
            view_angle_factor: VIEW_ANGLE,
            cwl: 9.668,
            etsr: 0.0000,
            vc: 1034.3430,
            alpha: 0.9999,
            beta: 0.0600,
        },
        Channel {
            name: "IR_108",
            view_angle_factor: VIEW_ANGLE,
            cwl: 10.763,
            etsr: 0.0000,
            vc: 930.6470,
            alpha: 0.9983,
            beta: 0.6250,
        },
        Channel {
            name: "IR_120",
            view_angle_factor: VIEW_ANGLE,
            cwl: 11.938,
            etsr: 0.0000,
            vc: 839.6600,
            alpha: 0.9988,
            beta: 0.3970,
        },
        Channel {
            name: "IR_134",
            view_angle_factor: VIEW_ANGLE,
            cwl: 13.355,
            etsr: 0.0000,
            vc: 752.3870,
            alpha: 0.9981,
            beta: 0.5780,
        },
        Channel {
            name: "HRV",
            view_angle_factor: VIEW_ANGLE_CH11,
            cwl: 0.674,
            etsr: 78.7599,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
    ],
};

static METEOSAT_09: Satellite = Satellite {
    name: "Meteosat-9",
    meteosat_id: 9,
    msg_id: 2,
    channels: [
        Channel {
            name: "VIS006",
            view_angle_factor: VIEW_ANGLE,
            cwl: 0.639,
            etsr: 65.2065,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
        Channel {
            name: "VIS008",
            view_angle_factor: VIEW_ANGLE,
            cwl: 0.809,
            etsr: 73.1869,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
        Channel {
            name: "IR_016",
            view_angle_factor: VIEW_ANGLE,
            cwl: 1.635,
            etsr: 61.9923,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
        Channel {
            name: "IR_039",
            view_angle_factor: VIEW_ANGLE,
            cwl: 3.965,
            etsr: 0.0000,
            vc: 2568.8320,
            alpha: 0.9954,
            beta: 3.4380,
        },
        Channel {
            name: "WV_062",
            view_angle_factor: VIEW_ANGLE,
            cwl: 6.337,
            etsr: 0.0000,
            vc: 1600.5480,
            alpha: 0.9963,
            beta: 2.1850,
        },
        Channel {
            name: "WV_073",
            view_angle_factor: VIEW_ANGLE,
            cwl: 7.362,
            etsr: 0.0000,
            vc: 1360.3300,
            alpha: 0.9991,
            beta: 0.4700,
        },
        Channel {
            name: "IR_087",
            view_angle_factor: VIEW_ANGLE,
            cwl: 8.718,
            etsr: 0.0000,
            vc: 1148.6200,
            alpha: 0.9996,
            beta: 0.1790,
        },
        Channel {
            name: "IR_097",
            view_angle_factor: VIEW_ANGLE,
            cwl: 9.668,
            etsr: 0.0000,
            vc: 1035.2890,
            alpha: 0.9999,
            beta: 0.0560,
        },
        Channel {
            name: "IR_108",
            view_angle_factor: VIEW_ANGLE,
            cwl: 10.763,
            etsr: 0.0000,
            vc: 931.7000,
            alpha: 0.9983,
            beta: 0.6400,
        },
        Channel {
            name: "IR_120",
            view_angle_factor: VIEW_ANGLE,
            cwl: 11.938,
            etsr: 0.0000,
            vc: 836.4450,
            alpha: 0.9988,
            beta: 0.4080,
        },
        Channel {
            name: "IR_134",
            view_angle_factor: VIEW_ANGLE,
            cwl: 13.355,
            etsr: 0.0000,
            vc: 751.7920,
            alpha: 0.9981,
            beta: 0.5610,
        },
        Channel {
            name: "HRV",
            view_angle_factor: VIEW_ANGLE_CH11,
            cwl: 0.674,
            etsr: 79.0113,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
    ],
};

static METEOSAT_10: Satellite = Satellite {
    name: "Meteosat-10",
    meteosat_id: 10,
    msg_id: 3,
    channels: [
        Channel {
            name: "VIS006",
            view_angle_factor: VIEW_ANGLE,
            cwl: 0.639,
            etsr: 65.5148,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
        Channel {
            name: "VIS008",
            view_angle_factor: VIEW_ANGLE,
            cwl: 0.809,
            etsr: 73.1807,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
        Channel {
            name: "IR_016",
            view_angle_factor: VIEW_ANGLE,
            cwl: 1.635,
            etsr: 62.0208,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
        Channel {
            name: "IR_039",
            view_angle_factor: VIEW_ANGLE,
            cwl: 3.965,
            etsr: 0.0000,
            vc: 2547.7710,
            alpha: 0.9915,
            beta: 2.9002,
        },
        Channel {
            name: "WV_062",
            view_angle_factor: VIEW_ANGLE,
            cwl: 6.337,
            etsr: 0.0000,
            vc: 1595.6210,
            alpha: 0.9960,
            beta: 2.0337,
        },
        Channel {
            name: "WV_073",
            view_angle_factor: VIEW_ANGLE,
            cwl: 7.362,
            etsr: 0.0000,
            vc: 1360.3370,
            alpha: 0.9991,
            beta: 0.4340,
        },
        Channel {
            name: "IR_087",
            view_angle_factor: VIEW_ANGLE,
            cwl: 8.718,
            etsr: 0.0000,
            vc: 1148.1300,
            alpha: 0.9996,
            beta: 0.1714,
        },
        Channel {
            name: "IR_097",
            view_angle_factor: VIEW_ANGLE,
            cwl: 9.668,
            etsr: 0.0000,
            vc: 1034.7150,
            alpha: 0.9999,
            beta: 0.0527,
        },
        Channel {
            name: "IR_108",
            view_angle_factor: VIEW_ANGLE,
            cwl: 10.763,
            etsr: 0.0000,
            vc: 929.8420,
            alpha: 0.9983,
            beta: 0.6084,
        },
        Channel {
            name: "IR_120",
            view_angle_factor: VIEW_ANGLE,
            cwl: 11.938,
            etsr: 0.0000,
            vc: 838.6590,
            alpha: 0.9988,
            beta: 0.3882,
        },
        Channel {
            name: "IR_134",
            view_angle_factor: VIEW_ANGLE,
            cwl: 13.355,
            etsr: 0.0000,
            vc: 750.6530,
            alpha: 0.9982,
            beta: 0.5390,
        },
        Channel {
            name: "HRV",
            view_angle_factor: VIEW_ANGLE_CH11,
            cwl: 0.674,
            etsr: 78.9416,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
    ],
};

static METEOSAT_11: Satellite = Satellite {
    name: "Meteosat-11",
    meteosat_id: 11,
    msg_id: 4,
    channels: [
        Channel {
            name: "VIS006",
            view_angle_factor: VIEW_ANGLE,
            cwl: 0.639,
            etsr: 65.2656,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
        Channel {
            name: "VIS008",
            view_angle_factor: VIEW_ANGLE,
            cwl: 0.809,
            etsr: 73.1692,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
        Channel {
            name: "IR_016",
            view_angle_factor: VIEW_ANGLE,
            cwl: 1.635,
            etsr: 61.9416,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
        Channel {
            name: "IR_039",
            view_angle_factor: VIEW_ANGLE,
            cwl: 3.965,
            etsr: 0.0000,
            vc: 2555.2800,
            alpha: 0.9916,
            beta: 2.9438,
        },
        Channel {
            name: "WV_062",
            view_angle_factor: VIEW_ANGLE,
            cwl: 6.337,
            etsr: 0.0000,
            vc: 1596.0800,
            alpha: 0.9959,
            beta: 2.0780,
        },
        Channel {
            name: "WV_073",
            view_angle_factor: VIEW_ANGLE,
            cwl: 7.362,
            etsr: 0.0000,
            vc: 1361.7480,
            alpha: 0.9990,
            beta: 0.4929,
        },
        Channel {
            name: "IR_087",
            view_angle_factor: VIEW_ANGLE,
            cwl: 8.718,
            etsr: 0.0000,
            vc: 1147.4330,
            alpha: 0.9996,
            beta: 0.1731,
        },
        Channel {
            name: "IR_097",
            view_angle_factor: VIEW_ANGLE,
            cwl: 9.668,
            etsr: 0.0000,
            vc: 1034.8510,
            alpha: 0.9998,
            beta: 0.0597,
        },
        Channel {
            name: "IR_108",
            view_angle_factor: VIEW_ANGLE,
            cwl: 10.763,
            etsr: 0.0000,
            vc: 931.1220,
            alpha: 0.9983,
            beta: 0.6256,
        },
        Channel {
            name: "IR_120",
            view_angle_factor: VIEW_ANGLE,
            cwl: 11.938,
            etsr: 0.0000,
            vc: 839.1130,
            alpha: 0.9988,
            beta: 0.4002,
        },
        Channel {
            name: "IR_134",
            view_angle_factor: VIEW_ANGLE,
            cwl: 13.355,
            etsr: 0.0000,
            vc: 748.5850,
            alpha: 0.9981,
            beta: 0.5635,
        },
        Channel {
            name: "HRV",
            view_angle_factor: VIEW_ANGLE_CH11,
            cwl: 0.674,
            etsr: 79.0035,
            vc: 0.0000,
            alpha: 0.0000,
            beta: 0.0000,
        },
    ],
};
