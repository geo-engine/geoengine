//! Contains utility methods to calculate values required for
//! applying solar correction
//! This was ported from C-code available at: <http://www.psa.es/sdg/sunpos.htm>

use time::OffsetDateTime;

const EARTH_MEAN_RADIUS: f64 = 6371.01; // In km
const ASTRONOMICAL_UNIT: f64 = 149_597_890.0; // In km

#[derive(Debug, Copy, Clone)]
pub struct SunPos {
    greenwich_mean_sidereal_time: f64,
    right_ascension: f64,
    declination: f64,
}

impl SunPos {
    pub fn solar_azimuth_zenith(&self, lat: f64, lon: f64) -> (f64, f64) {
        let local_mean_sidereal_time =
            (self.greenwich_mean_sidereal_time * 15.0 + lon).to_radians();
        let hour_angle = local_mean_sidereal_time - self.right_ascension;

        let mut zenith = (lat.to_radians().cos() * hour_angle.cos() * self.declination.cos()
            + self.declination.sin() * lat.to_radians().sin())
        .acos();

        let parallax = (EARTH_MEAN_RADIUS / ASTRONOMICAL_UNIT) * zenith.sin();
        zenith += parallax;

        let dx = self.declination.tan() * lat.to_radians().cos()
            - lat.to_radians().sin() * hour_angle.cos();

        let dy = -hour_angle.sin();

        let mut azimuth = dy.atan2(dx);

        if azimuth < 0.0 {
            azimuth += std::f64::consts::TAU;
        }

        (azimuth.to_degrees(), zenith.to_degrees())
    }

    /// Compute the sunpos at the given date.
    pub fn new(timestamp: &OffsetDateTime) -> SunPos {
        let (decimal_hours, elapsed_julian_days) = Self::elapsed_julian_days(timestamp);
        let (ecliptic_longitude, ecliptic_obliquity) =
            Self::ecliptic_coordinates(elapsed_julian_days);

        let (right_ascension, declination) =
            Self::celestial_coordinates(ecliptic_longitude, ecliptic_obliquity);

        let greenwich_mean_sidereal_time =
            6.697_424_324_2 + 0.065_709_828_3 * elapsed_julian_days + decimal_hours;

        SunPos {
            greenwich_mean_sidereal_time,
            right_ascension,
            declination,
        }
    }

    /// Calculates difference in days between the current Julian Day
    /// and JD 2451545.0, which is noon 1 January 2000 Universal Time
    fn elapsed_julian_days(timestamp: &OffsetDateTime) -> (f64, f64) {
        let year = i64::from(timestamp.year());
        let month = i64::from(u8::from(timestamp.month()));
        let day = i64::from(timestamp.day());

        let hour = f64::from(timestamp.hour());
        let minute = f64::from(timestamp.minute());
        let second = f64::from(timestamp.second());

        let decimal_hours = hour + (minute + second / 60.0) / 60.0;
        let aux1 = (month - 14) / 12;
        let aux2 = (1461 * (year + 4800 + aux1)) / 4 + (367 * (month - 2 - 12 * aux1)) / 12
            - (3 * ((year + 4900 + aux1) / 100)) / 4
            + day
            - 32075;
        let julian_date = aux2 as f64 - 0.5 + decimal_hours / 24.0;
        (decimal_hours, julian_date - 2_451_545.0)
    }

    /// Calculate ecliptic coordinates (ecliptic longitude and obliquity of the
    /// ecliptic in radians but without limiting the angle to be less than 2*Pi
    /// (i.e., the result may be greater than 2*Pi)
    fn ecliptic_coordinates(elapsed_julian_days: f64) -> (f64, f64) {
        let omega = 2.1429 - 0.001_039_459_4 * elapsed_julian_days;
        let mean_longitude = 4.895_063_0 + 0.017_202_791_698 * elapsed_julian_days; // Radians
        let mean_anomaly = 6.240_060_0 + 0.017_201_969_9 * elapsed_julian_days;
        let ecliptic_longitude = mean_longitude
            + 0.033_416_07 * mean_anomaly.sin()
            + 0.000_348_94 * (2.0 * mean_anomaly).sin()
            - 0.000_113_4
            - 0.000_020_3 * omega.sin();
        let ecliptic_obliquity =
            0.409_092_8 - 6.2140e-9 * elapsed_julian_days + 0.000_039_6 * omega.cos();

        (ecliptic_longitude, ecliptic_obliquity)
    }

    /// Calculate celestial coordinates ( right ascension and declination ) in radians
    /// but without limiting the angle to be less than 2*Pi (i.e., the result may be
    /// greater than 2*Pi)
    fn celestial_coordinates(ecliptic_longitude: f64, ecliptic_obliquity: f64) -> (f64, f64) {
        let sin_ecliptic_longitude = ecliptic_longitude.sin();
        let dy = ecliptic_obliquity.cos() * sin_ecliptic_longitude;
        let dx = ecliptic_longitude.cos();

        let mut right_acension = dy.atan2(dx);

        if right_acension < 0.0 {
            right_acension += std::f64::consts::TAU;
        }

        let declination = (ecliptic_obliquity.sin() * sin_ecliptic_longitude).asin();

        (right_acension, declination)
    }
}

#[cfg(test)]
mod tests {
    use crate::util::sunpos::SunPos;
    use time::macros::datetime;

    #[tokio::test]
    async fn test_ok() {
        let ts = datetime!(1900-01-01 0:00:00 UTC);
        let chk = SunPos::new(&ts);
        let (lat, lon) = (8.810_983_605_709_731, 50.809_415_350_423_98);
        let (azimuth, zenith) = chk.solar_azimuth_zenith(lat, lon);

        // Those values were confirmed by the reference implementation (<http://www.psa.es/sdg/sunpos.htm>)
        assert!((112.824_776_919_019_5 - azimuth).abs() < f64::EPSILON);
        assert!((130.168_997_660_038_34 - zenith).abs() < f64::EPSILON);
    }
}
