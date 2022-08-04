use crate::error::ErrorSource;
use crate::primitives::TimeInstance;
use chrono::{Datelike, NaiveDate, Timelike};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::Display;
use std::ops::{Add, Sub};
use std::str::FromStr;

/// An object that composes the date and a timestamp with time zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DateTime {
    datetime: chrono::DateTime<chrono::Utc>,
}

impl DateTime {
    /// The minimum possible `DateTime`.
    pub const MIN: DateTime = DateTime {
        datetime: chrono::DateTime::<chrono::Utc>::MAX_UTC,
    };
    /// The maximum possible `DateTime`.
    pub const MAX: DateTime = DateTime {
        datetime: chrono::DateTime::<chrono::Utc>::MAX_UTC,
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

impl FromStr for DateTime {
    type Err = DateTimeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let date_time = chrono::DateTime::<chrono::FixedOffset>::from_str(input).map_err(|e| {
            DateTimeError::DateParse {
                source: Box::new(e),
            }
        })?;

        Ok(date_time.into())
    }
}

impl Display for DateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.datetime
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        )
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

/// We allow C's strftime parameters:
///
/// | Specifier |                               Replaced By                              |          Example         |
/// |:---------:|:----------------------------------------------------------------------:|:------------------------:|
/// | %a        | Abbreviated weekday name                                               | Sun                      |
/// | %A        | Full weekday name                                                      | Sunday                   |
/// | %b        | Abbreviated month name                                                 | Mar                      |
/// | %B        | Full month name                                                        | March                    |
/// | %c        | Date and time representation                                           | Sun Aug 19 02:56:02 2012 |
/// | %d        | Day of the month (01-31)                                               | 19                       |
/// | %H        | Hour in 24h format (00-23)                                             | 14                       |
/// | %I        | Hour in 12h format (01-12)                                             | 05                       |
/// | %j        | Day of the year (001-366)                                              | 231                      |
/// | %m        | Month as a decimal number (01-12)                                      | 08                       |
/// | %M        | Minute (00-59)                                                         | 55                       |
/// | %p        | AM or PM designation                                                   | PM                       |
/// | %S        | Second (00-61)                                                         | 02                       |
/// | %U        | Week number with the first Sunday as the first day of week one (00-53) | 33                       |
/// | %w        | Weekday as a decimal number with Sunday as 0 (0-6)                     | 4                        |
/// | %W        | Week number with the first Monday as the first day of week one (00-53) | 34                       |
/// | %x        | Date representation                                                    | 08/19/12                 |
/// | %X        | Time representation                                                    | 02:50:06                 |
/// | %y        | Year, last two digits (00-99)                                          | 01                       |
/// | %Y        | Year                                                                   | 2012                     |
/// | %Z        | Timezone name or abbreviation                                          | CDT                      |
/// | %z        | Timezone offset                                                        | +0930                    |
/// | %:z       | Timezone offset with colon                                             | +09:30                   |
/// | %%        | A % sign                                                               | %                        |
///
/// Additionally, we allow some specifiers for subseconds:
///
/// | Specifier | Replaced By                                                            | Example    |
/// |-----------|------------------------------------------------------------------------|------------|
/// | %f        | The fractional seconds (in nanoseconds) since last whole second. 6     | 026490000  |
/// | %.f       | Similar to .%f but left-aligned. These all consume the leading dot. 6  | .026490    |
/// | %.3f      | Similar to .%f but left-aligned but fixed to a length of 3. 6          | .026       |
/// | %.6f      | Similar to .%f but left-aligned but fixed to a length of 6. 6          | .026490    |
/// | %.9f      | Similar to .%f but left-aligned but fixed to a length of 9. 6          | .026490000 |
/// | %3f       | Similar to %.3f but without the leading dot. 6                         | 026        |
/// | %6f       | Similar to %.6f but without the leading dot. 6                         | 026490     |
/// | %9f       | Similar to %.9f but without the leading dot. 6                         | 026490000  |
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DateTimeParseFormat {
    fmt: String,
    has_tz: bool,
    has_time: bool,
}

enum FormatStrLoopState {
    Normal,
    Percent(String),
}

impl DateTimeParseFormat {
    pub fn custom(fmt: String) -> Self {
        let (has_tz, has_time) = {
            let mut has_tz = false;
            let mut has_time = false;

            let has_tz_values: HashSet<&str> = ["%Z", "%z", "%:z"].into();
            let has_time_values: HashSet<&str> =
                ["%a", "%A", "%M", "%p", "%S", "%X", "%H", "%I"].into();

            let mut state = FormatStrLoopState::Normal;

            for c in fmt.chars() {
                match state {
                    FormatStrLoopState::Normal => {
                        if c == '%' {
                            state = FormatStrLoopState::Percent("%".to_string());
                        }
                    }
                    FormatStrLoopState::Percent(ref mut s) => {
                        s.push(c);

                        if c == '%' {
                            // was escaped percentage
                            state = FormatStrLoopState::Normal;
                        } else if c.is_ascii_alphabetic() {
                            if has_tz_values.contains(s.as_str()) {
                                has_tz = true;
                            }
                            if has_time_values.contains(s.as_str()) {
                                has_time = true;
                            }

                            state = FormatStrLoopState::Normal;
                        }
                    }
                }
            }

            (has_tz, has_time)
        };

        DateTimeParseFormat {
            fmt,
            has_tz,
            has_time,
        }
    }

    pub fn unix() -> Self {
        let fmt = "%s".to_owned();
        Self {
            fmt,
            has_tz: false,
            has_time: true,
        }
    }

    pub fn ymd() -> Self {
        let fmt = "%Y-%m-%d".to_owned();
        Self {
            fmt,
            has_tz: false,
            has_time: false,
        }
    }

    fn has_tz(&self) -> bool {
        self.has_tz
    }

    fn has_time(&self) -> bool {
        self.has_time
    }

    pub fn is_empty(&self) -> bool {
        self.fmt.is_empty()
    }

    fn _to_parse_format(&self) -> &str {
        &self.fmt
    }
}

impl<'de> Deserialize<'de> for DateTimeParseFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Self::custom(s))
    }
}

impl Serialize for DateTimeParseFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.fmt)
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
        <Self as FromStr>::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl Serialize for DateTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.datetime.to_string())
    }
}

impl PartialOrd for DateTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.datetime.partial_cmp(&other.datetime)
    }
}

impl Ord for DateTime {
    fn cmp(&self, other: &Self) -> Ordering {
        self.datetime.cmp(&other.datetime)
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

#[derive(Debug, Clone, Copy)]
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

    pub const fn num_milliseconds(self) -> i64 {
        self.milliseconds
    }

    pub const fn num_seconds(self) -> i64 {
        self.milliseconds / Self::SECOND.milliseconds
    }

    pub const fn num_minutes(self) -> i64 {
        self.milliseconds / Self::MINUTE.milliseconds
    }

    pub const fn num_hours(self) -> i64 {
        self.milliseconds / Self::HOUR.milliseconds
    }

    pub const fn num_days(self) -> i64 {
        self.milliseconds / Self::DAY.milliseconds
    }

    pub const fn is_zero(self) -> bool {
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
        let format = DateTimeParseFormat::custom("%Y-%m-%dT%H:%M:%S%.3f".to_string());

        let result = DateTime::parse_from_str("2020-01-02T03:04:05.006", &format).unwrap();
        let expected = DateTime::new_utc_with_millis(2020, 1, 2, 3, 4, 5, 6);

        assert_eq!(result, expected);
    }

    #[test]
    fn to_string() {
        assert_eq!(
            DateTime::new_utc(2010, 1, 2, 3, 4, 5).to_string(),
            "2010-01-02T03:04:05.000Z"
        );
        assert_eq!(
            DateTime::new_utc(-2010, 1, 2, 3, 4, 5).to_string(),
            "-2010-01-02T03:04:05.000Z"
        );
    }

    #[test]
    fn from_string() {
        assert_eq!(
            DateTime::new_utc(2010, 1, 2, 3, 4, 5),
            DateTime::from_str("2010-01-02T03:04:05.000Z").unwrap()
        );
        assert_eq!(
            DateTime::new_utc(-2010, 1, 2, 3, 4, 5),
            DateTime::from_str("-2010-01-02T03:04:05.000Z").unwrap()
        );
    }

    #[test]
    fn has_tz() {
        assert!(!DateTimeParseFormat::custom("%Y-%m-%dT%H:%M:%S%.3f".to_string()).has_tz());
        assert!(DateTimeParseFormat::custom("%Y-%m-%dT%H:%M:%S%z".to_string()).has_tz());
    }

    #[test]
    fn has_time() {
        assert!(!DateTimeParseFormat::custom("%Y-%m-%d".to_string()).has_time());
        assert!(DateTimeParseFormat::custom("%Y-%m-%dT%H:%M:%S%z".to_string()).has_time());
    }
}
