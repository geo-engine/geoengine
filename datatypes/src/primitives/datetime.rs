use crate::error::ErrorSource;
use crate::primitives::TimeInstance;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Offset, Timelike};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::cmp::Ordering;
use std::ops::{Add, Sub};
use std::str::FromStr;

/// An object that composes the date and a timestamp with time zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DateTime {
    datetime: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UtcOffset {
    hours: i8,
    minutes: i8,
}

pub const UTC: UtcOffset = UtcOffset {
    hours: 0,
    minutes: 0,
};

impl DateTime {
    /// The minimum possible `DateTime`.
    pub const MIN: DateTime = DateTime {
        datetime: chrono::MIN_DATETIME,
    };
    /// The maximum possible `DateTime`.
    pub const MAX: DateTime = DateTime {
        datetime: chrono::MAX_DATETIME,
    };

    /// Creates a new `DateTime` from year, month day and hour, minute, second values.
    /// Assumes the time is in UTC.
    ///
    /// # Panics
    /// Panics if the values are out of range.
    ///
    pub fn new_utc(year: i32, month: u8, day: u8, hour: u8, minute: u8, second: u8) -> Self {
        let datetime = NaiveDate::from_ymd(year, month.into(), day.into()).and_hms(
            hour.into(),
            minute.into(),
            second.into(),
        );
        let datetime = chrono::DateTime::<chrono::Utc>::from_utc(datetime, chrono::Utc);

        Self { datetime }
    }

    /// Creates a new `DateTime` from year, month day and hour, minute, second values.
    /// Assumes the time is in UTC.
    ///
    /// If the date or the time would overflow, it returns `None`
    ///
    pub fn new_utc_checked(
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
    ) -> Option<Self> {
        let date = NaiveDate::from_ymd_opt(year, month.into(), day.into())?;
        let datetime = date.and_hms_opt(hour.into(), minute.into(), second.into())?;
        let datetime = chrono::DateTime::<chrono::Utc>::from_utc(datetime, chrono::Utc);

        Some(Self { datetime })
    }

    /// Creates a new `DateTime` from year, month day and hour, minute, second values.
    /// Assumes the time is in UTC.
    ///
    /// # Panics
    /// Panics if the values are out of range.
    ///
    pub fn new_utc_with_millis(
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        millis: u16,
    ) -> Self {
        let datetime = NaiveDate::from_ymd(year, month.into(), day.into()).and_hms_milli(
            hour.into(),
            minute.into(),
            second.into(),
            millis.into(),
        );
        let datetime = chrono::DateTime::<chrono::Utc>::from_utc(datetime, chrono::Utc);

        Self { datetime }
    }

    /// Creates a new `DateTime` from year, month day and hour, minute, second values.
    /// Assumes the time is in UTC.
    ///
    /// If the date or the time would overflow, it returns `None`
    ///
    pub fn new_utc_checked_with_millis(
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        millis: u16,
    ) -> Option<Self> {
        let date = NaiveDate::from_ymd_opt(year, month.into(), day.into())?;
        let datetime =
            date.and_hms_milli_opt(hour.into(), minute.into(), second.into(), millis.into())?;
        let datetime = chrono::DateTime::<chrono::Utc>::from_utc(datetime, chrono::Utc);

        Some(Self { datetime })
    }

    pub fn parse_from_str(
        input: &str,
        format: &DateTimeParseFormat,
    ) -> Result<Self, DateTimeError> {
        match (format.has_time(), format.has_tz()) {
            (true, true) => {
                let datetime = chrono::DateTime::<chrono::FixedOffset>::parse_from_str(
                    input,
                    format._to_parse_format(),
                )
                .map_err(|e| DateTimeError::DateParse {
                    source: Box::new(e),
                })?;

                Ok(Self {
                    datetime: datetime.into(),
                })
            }
            (true, false) => {
                let datetime =
                    chrono::NaiveDateTime::parse_from_str(input, format._to_parse_format())
                        .map_err(|e| DateTimeError::DateParse {
                            source: Box::new(e),
                        })?;

                Ok(Self {
                    datetime: chrono::DateTime::<chrono::Utc>::from_utc(datetime, chrono::Utc),
                })
            }
            (false, true) => Err(DateTimeError::CannotParseOnlyDateWithTimeZone),
            (false, false) => {
                let datetime = chrono::NaiveDate::parse_from_str(input, format._to_parse_format())
                    .map_err(|e| DateTimeError::DateParse {
                        source: Box::new(e),
                    })?
                    .and_hms(0, 0, 0);

                Ok(Self {
                    datetime: chrono::DateTime::<chrono::Utc>::from_utc(datetime, chrono::Utc),
                })
            }
        }
    }

    pub fn parse_from_rfc3339(input: &str) -> Result<Self, DateTimeError> {
        let date_time = chrono::DateTime::<chrono::FixedOffset>::parse_from_rfc3339(input)
            .map_err(|e| DateTimeError::DateParse {
                source: Box::new(e),
            })?;

        Ok(date_time.into())
    }

    // TODO: is this inprecise?
    pub fn parse_relaxed(input: &str) -> Result<Self, DateTimeError> {
        let date_time = chrono::DateTime::<chrono::FixedOffset>::from_str(input).map_err(|e| {
            DateTimeError::DateParse {
                source: Box::new(e),
            }
        })?;

        Ok(date_time.into())
    }

    pub fn format(&self, format: &DateTimeParseFormat) -> String {
        let chrono_date_time: chrono::DateTime<chrono::FixedOffset> = self.into();
        let parse_format = format._to_parse_format();

        chrono_date_time.format(parse_format).to_string()
    }

    pub fn to_rfc3339(self) -> String {
        let chrono_date_time: chrono::DateTime<chrono::FixedOffset> = self.into();
        chrono_date_time.to_rfc3339()
    }

    /// Now in UTC.
    pub fn now() -> Self {
        chrono::offset::Utc::now().into()
    }

    pub fn day_of_year(&self) -> u16 {
        // TODO: calculate offset?
        self.datetime.ordinal() as u16
    }

    pub fn year(&self) -> i32 {
        self.datetime.year()
    }
    pub fn month(&self) -> u8 {
        self.datetime.month() as u8
    }

    /// Zero-based month from 0-11.
    pub fn month0(&self) -> u8 {
        self.month() - 1
    }

    pub fn day(&self) -> u8 {
        self.datetime.day() as u8
    }
    pub fn hour(&self) -> u8 {
        self.datetime.hour() as u8
    }
    pub fn minute(&self) -> u8 {
        self.datetime.minute() as u8
    }
    pub fn second(&self) -> u8 {
        self.datetime.second() as u8
    }
    pub fn millisecond(&self) -> u16 {
        self.datetime.timestamp_subsec_millis() as u16
    }

    /// Creates a new `DateTime` from the original one by changing the year.
    ///
    /// Returns `None` if the year overflows the date.
    pub fn with_year(&self, year: i32) -> Option<Self> {
        Some(Self {
            datetime: self.datetime.with_year(year)?,
        })
    }
}

impl UtcOffset {
    fn _to_fixed_offset(self) -> chrono::FixedOffset {
        let minute_in_secs = 60;
        let hour_in_secs = 3600;
        chrono::FixedOffset::east(
            i32::from(self.hours) * hour_in_secs + i32::from(self.minutes) * minute_in_secs,
        )
    }

    fn _from_fixed_offset(offset: chrono::FixedOffset) -> Self {
        let offset_seconds = offset.local_minus_utc();

        if offset_seconds == 0 {
            return UTC;
        }

        let minute_in_secs = 60;
        let hour_in_secs = 3600;

        let hours = (offset.local_minus_utc() / hour_in_secs) as i8;
        let minutes = (offset.local_minus_utc() % hour_in_secs / minute_in_secs) as i8;

        Self { hours, minutes }
    }
}

impl From<DateTime> for TimeInstance {
    fn from(datetime: DateTime) -> Self {
        Self::from(&datetime)
    }
}

impl From<&DateTime> for TimeInstance {
    fn from(datetime: &DateTime) -> Self {
        TimeInstance::from_millis_unchecked(datetime.datetime.timestamp_millis())
    }
}

impl TryFrom<TimeInstance> for DateTime {
    type Error = DateTimeError;

    fn try_from(time_instance: TimeInstance) -> Result<Self, Self::Error> {
        use chrono::TimeZone;

        if time_instance < TimeInstance::MIN || time_instance > TimeInstance::MAX {
            return Err(DateTimeError::OutOfBounds);
        }

        match chrono::Utc.timestamp_millis_opt(time_instance.inner()) {
            chrono::LocalResult::Single(datetime) => Ok(Self { datetime }),
            chrono::LocalResult::None | chrono::LocalResult::Ambiguous(_, _) => {
                Err(DateTimeError::OutOfBounds)
            }
        }
    }
}

impl From<chrono::DateTime<chrono::FixedOffset>> for DateTime {
    fn from(datetime: chrono::DateTime<chrono::FixedOffset>) -> Self {
        Self {
            datetime: datetime.into(),
        }
    }
}

impl From<chrono::DateTime<chrono::Utc>> for DateTime {
    fn from(datetime: chrono::DateTime<chrono::Utc>) -> Self {
        Self { datetime }
    }
}

impl From<DateTime> for chrono::DateTime<chrono::FixedOffset> {
    fn from(datetime: DateTime) -> Self {
        Self::from(&datetime)
    }
}

impl From<&DateTime> for chrono::DateTime<chrono::FixedOffset> {
    fn from(datetime: &DateTime) -> Self {
        datetime.datetime.into()
    }
}

pub enum DateTimeParseFormat {
    Ymd,
    Unix,
    Custom {
        fmt: String,
        has_tz: bool,
        has_time: bool,
    },
}

impl DateTimeParseFormat {
    pub fn custom(fmt: String) -> Self {
        let has_tz = fmt.contains("[tz]");
        let has_time = fmt.contains("[hour]")
            || fmt.contains("[minute]")
            || fmt.contains("[second]")
            || fmt.contains("[millis]");
        let fmt = Self::_parse_custom_format(fmt);
        DateTimeParseFormat::Custom {
            fmt,
            has_tz,
            has_time,
        }
    }

    fn has_tz(&self) -> bool {
        match self {
            DateTimeParseFormat::Ymd | DateTimeParseFormat::Unix => false,
            DateTimeParseFormat::Custom {
                fmt: _,
                has_tz,
                has_time: _,
            } => *has_tz,
        }
    }

    fn has_time(&self) -> bool {
        match self {
            DateTimeParseFormat::Ymd => false,
            DateTimeParseFormat::Unix => true,
            DateTimeParseFormat::Custom {
                fmt: _,
                has_tz: _,
                has_time,
            } => *has_time,
        }
    }

    fn _to_parse_format(&self) -> &str {
        match self {
            DateTimeParseFormat::Ymd => "%Y-%m-%d",
            DateTimeParseFormat::Unix => "%s",
            DateTimeParseFormat::Custom {
                fmt,
                has_tz: _,
                has_time: _,
            } => fmt,
        }
    }

    fn _parse_custom_format(mut fmt: String) -> String {
        // TODO: speed up with proper parser
        fmt = fmt.replace("[year]", "%Y");
        fmt = fmt.replace("[month]", "%m");
        fmt = fmt.replace("[day]", "%d");
        fmt = fmt.replace("[hour]", "%H");
        fmt = fmt.replace("[minute]", "%M");
        fmt = fmt.replace("[second]", "%S");
        fmt = fmt.replace("[millis]", "%.f");
        fmt = fmt.replace("[tz]", "%z");
        fmt
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)), module(error))] // disables default `Snafu` suffix
pub enum DateTimeError {
    #[snafu(display("Failed to parse date: {}", source))]
    DateParse {
        source: Box<dyn ErrorSource>,
    },
    CannotParseOnlyDateWithTimeZone,
    OutOfBounds,
}

impl<'de> Deserialize<'de> for DateTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let chrono_datatime = chrono::DateTime::<chrono::FixedOffset>::parse_from_rfc3339(&s)
            .map_err(serde::de::Error::custom)?;
        Ok(chrono_datatime.into())
    }
}

impl Serialize for DateTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let chrono_data_time = chrono::DateTime::<chrono::FixedOffset>::from(self);
        serializer.serialize_str(&chrono_data_time.to_rfc3339())
    }
}

impl PartialOrd for DateTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        chrono::DateTime::<chrono::FixedOffset>::from(self)
            .partial_cmp(&chrono::DateTime::<chrono::FixedOffset>::from(other))
    }
}

impl Ord for DateTime {
    fn cmp(&self, other: &Self) -> Ordering {
        chrono::DateTime::<chrono::FixedOffset>::from(self)
            .cmp(&chrono::DateTime::<chrono::FixedOffset>::from(other))
    }
}

impl Add<Duration> for DateTime {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        let duration = chrono::Duration::milliseconds(rhs.num_milliseconds());

        chrono::DateTime::<chrono::FixedOffset>::from(self)
            // TODO: do we ignore the overflow?
            .add(duration)
            .into()
    }
}

impl Sub<Duration> for DateTime {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        let duration = chrono::Duration::milliseconds(rhs.num_milliseconds());

        chrono::DateTime::<chrono::FixedOffset>::from(self)
            // TODO: do we ignore the overflow?
            .sub(duration)
            .into()
    }
}

#[cfg(feature = "postgres")]
mod sql {
    /// TODO: we cannot store `UtcOffset` in postgres, so we have to stick with UTC or store it separately.
    use super::*;
    use postgres_protocol::types::{timestamp_from_sql, timestamp_to_sql};
    use postgres_types::{
        accepts, private::BytesMut, to_sql_checked, FromSql, IsNull, ToSql, Type, WrongType,
    };
    use std::error::Error;

    impl<'a> FromSql<'a> for DateTime {
        fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
            if !<Self as postgres_types::FromSql>::accepts(ty) {
                return Err(Box::new(WrongType::new::<Self>(ty.clone())));
            }

            let timestamp_microseconds = timestamp_from_sql(raw)?;
            let timestamp_milliseconds = timestamp_microseconds / 1_000;
            let time_instance = TimeInstance::try_from(timestamp_milliseconds)?;

            match DateTime::try_from(time_instance) {
                Ok(date_time) => Ok(date_time),
                Err(err) => Err(Box::new(err)),
            }
        }

        postgres_types::accepts!(TIMESTAMPTZ);
    }

    impl ToSql for DateTime {
        fn to_sql(
            &self,
            _: &Type,
            w: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            let timestamp_milliseconds = TimeInstance::from(self).inner();
            let timestamp_microseconds = timestamp_milliseconds * 1_000;

            timestamp_to_sql(timestamp_microseconds, w);

            Ok(IsNull::No)
        }

        accepts!(TIMESTAMPTZ);

        to_sql_checked!();
    }
}

pub struct Duration {
    milliseconds: i64,
}

/// TODO: out of bounds checks
impl Duration {
    pub const SECOND: Self = Self::milliseconds(1_000);
    pub const MINUTE: Self = Self::seconds(60);
    pub const HOUR: Self = Self::minutes(60);
    pub const DAY: Self = Self::hours(24);

    pub const fn days(days: i64) -> Self {
        Self::milliseconds(days * Self::DAY.milliseconds)
    }

    pub const fn hours(hours: i64) -> Self {
        Self::milliseconds(hours * Self::HOUR.milliseconds)
    }

    pub const fn minutes(minutes: i64) -> Self {
        Self::milliseconds(minutes * Self::MINUTE.milliseconds)
    }

    pub const fn seconds(seconds: i64) -> Self {
        Self::milliseconds(seconds * Self::SECOND.milliseconds)
    }

    pub const fn milliseconds(milliseconds: i64) -> Self {
        Self { milliseconds }
    }

    pub const fn num_milliseconds(&self) -> i64 {
        self.milliseconds
    }

    pub const fn num_seconds(&self) -> i64 {
        self.milliseconds / Self::SECOND.milliseconds
    }

    pub const fn num_minutes(&self) -> i64 {
        self.milliseconds / Self::MINUTE.milliseconds
    }

    pub const fn num_hours(&self) -> i64 {
        self.milliseconds / Self::HOUR.milliseconds
    }

    pub const fn num_days(&self) -> i64 {
        self.milliseconds / Self::DAY.milliseconds
    }

    pub const fn is_zero(&self) -> bool {
        self.milliseconds == 0
    }
}

// TODO: must this be checked?
impl Add<Duration> for Duration {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        Self {
            milliseconds: self.milliseconds + rhs.milliseconds,
        }
    }
}

// TODO: must this be checked?
impl Sub<Duration> for Duration {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        Self {
            milliseconds: self.milliseconds - rhs.milliseconds,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        let format = DateTimeParseFormat::custom(
            "[year]-[month]-[day]T[hour]:[minute]:[second][millis]".to_string(),
        );

        let result = DateTime::parse_from_str("2020-01-02T03:04:05.006", &format).unwrap();
        let expected = DateTime::new_utc_with_millis(2020, 1, 2, 3, 4, 5, 6);

        assert_eq!(result, expected);
    }
}
