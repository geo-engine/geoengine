use crate::error::{self, Error};
use crate::util::Result;
use csv::{Position, Reader};
use futures::future;
use futures::stream::{self, BoxStream, StreamExt};
use geoengine_datatypes::collections::PointCollection;
use geoengine_datatypes::primitives::{Coordinate, TimeInterval};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use std::fs::File;
use std::i64;
use std::path::PathBuf;

/// Parameters for the CSV Source Operator
///
/// # Examples
///
/// ```rust
/// use serde_json::{Result, Value};
/// use geoengine_operators::Operator;
/// use geoengine_operators::source::CsvSourceParameters;
/// use geoengine_operators::source::csv::{CsvGeometrySpecification, CsvTimeSpecification};
///
/// let json_string = r#"
///     {
///         "type": "csv_source",
///         "params": {
///             "file_path": "/foo/bar.csv",
///             "field_separator": ",",
///             "geometry": {
///                 "type": "xy",
///                 "x": "x",
///                 "y": "y"
///             }
///         }
///     }"#;
///
/// let operator: Operator = serde_json::from_str(json_string).unwrap();
///
/// assert_eq!(operator, Operator::CsvSource {
///     params: CsvSourceParameters {
///         file_path: "/foo/bar.csv".into(),
///         field_separator: ',',
///         geometry: CsvGeometrySpecification::XY { x: "x".into(), y: "y".into() },
///         time: CsvTimeSpecification::None,
///     },
///     sources: Default::default(),
/// });
/// ```
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvSourceParameters {
    pub file_path: PathBuf,
    pub field_separator: char,
    pub geometry: CsvGeometrySpecification,
    #[serde(default)]
    pub time: CsvTimeSpecification,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CsvGeometrySpecification {
    XY { x: String, y: String },
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CsvTimeSpecification {
    None,
}

impl Default for CsvTimeSpecification {
    fn default() -> Self {
        Self::None
    }
}

pub struct CsvSource {
    parameters: CsvSourceParameters,
    csv_reader: Reader<File>,
}

impl CsvSource {
    pub fn new(parameters: CsvSourceParameters) -> Result<Self> {
        ensure!(
            parameters.field_separator.is_ascii(),
            error::CsvSource {
                details: "Delimiter must be ASCII character"
            }
        );

        Ok(Self {
            csv_reader: csv::ReaderBuilder::new()
                .delimiter(parameters.field_separator as u8)
                .has_headers(true)
                .from_path(parameters.file_path.as_path())
                .context(error::CsvSourceReader {})?,
            parameters,
        })
    }

    /// Read points from CSV file
    ///
    /// # Examples
    /// ```rust
    /// use futures::executor::{block_on_stream, block_on};
    /// use geoengine_datatypes::collections::FeatureCollection;
    /// use std::io::{Seek, SeekFrom, Write};
    /// use geoengine_operators::source::{CsvSource, CsvSourceParameters};
    /// use geoengine_operators::source::csv::{CsvGeometrySpecification, CsvTimeSpecification};
    /// use futures::TryStreamExt;
    /// use futures;
    ///
    /// let mut fake_file = tempfile::NamedTempFile::new().unwrap();
    /// write!(
    ///     fake_file,
    ///     "\
    /// x,y
    /// 0,1
    /// 2,3
    /// 4,5
    /// "
    ///     )
    ///     .unwrap();
    ///     fake_file.seek(SeekFrom::Start(0)).unwrap();
    ///
    /// let mut csv_source = CsvSource::new(CsvSourceParameters {
    ///     file_path: fake_file.path().into(),
    ///     field_separator: ',',
    ///     geometry: CsvGeometrySpecification::XY {
    ///         x: "x".into(),
    ///         y: "y".into(),
    ///     },
    ///     time: CsvTimeSpecification::None,
    /// })
    /// .unwrap();
    ///
    /// let mut stream = block_on_stream(csv_source.read_points(2));
    ///
    /// assert_eq!(stream.next().unwrap().unwrap().len(), 2);
    /// assert_eq!(stream.next().unwrap().unwrap().len(), 1);
    /// assert!(stream.next().is_none());
    ///
    /// ```
    pub fn read_points(
        &mut self,
        items_per_chunk: usize,
    ) -> BoxStream<Result<PointCollection, Error>> {
        let header = match self.setup_read() {
            Ok(header) => header,
            Err(error) => return stream::once(future::ready(Err(error))).boxed(),
        };

        let csv_stream = stream::iter(
            self.csv_reader
                .records()
                .skip(if header.has_header { 1 } else { 0 }), // skip header if set
        );

        // filter out values that could not be parsed by the csv parser
        // TODO: log/notify errors
        let csv_stream = csv_stream.filter_map(|result| future::ready(result.ok()));

        // try to get all fields out of the row
        // TODO: log/notify errors
        let csv_stream = csv_stream.filter_map(move |row| {
            future::ready({
                let result = || -> Result<ParsedRow> {
                    let x: f64 = row
                        .get(header.x_index)
                        .context(error::CsvSource {
                            details: "Cannot find x index key",
                        })?
                        .parse()
                        .map_err(|_| error::Error::CsvSource {
                            details: "Cannot parse x coordinate".to_string(),
                        })?;
                    let y: f64 = row
                        .get(header.y_index)
                        .context(error::CsvSource {
                            details: "Cannot find y index key",
                        })?
                        .parse()
                        .map_err(|_| error::Error::CsvSource {
                            details: "Cannot parse y coordinate".to_string(),
                        })?;

                    Ok(ParsedRow {
                        coordinate: (x, y).into(),
                        time_interval: unlimited_time_interval(),
                    })
                };
                result().ok()
            })
        });

        csv_stream
            .chunks(items_per_chunk)
            .map(move |chunk| {
                if chunk.is_empty() {
                    return Ok(PointCollection::empty());
                }

                let mut builder = PointCollection::builder();
                for result in chunk {
                    builder.append_coordinate(result.coordinate)?;
                    builder.append_time_interval(result.time_interval)?;
                    builder.finish_row()?;
                }
                Ok(builder.build()?)
            })
            .boxed()
    }

    fn setup_read(&mut self) -> Result<ParsedHeader> {
        self.csv_reader
            .seek(Position::new())
            .context(error::CsvSourceReader {})?; // start at beginning

        ensure!(
            self.csv_reader.has_headers(),
            error::CsvSource {
                details: "CSV file must contain header",
            }
        );

        let header = self.csv_reader.headers().context(error::CsvSourceReader)?;

        let CsvGeometrySpecification::XY { x, y } = &self.parameters.geometry;
        let x_index = header
            .iter()
            .position(|v| v == x)
            .context(error::CsvSource {
                details: "Cannot find x index in csv header",
            })?;
        let y_index = header
            .iter()
            .position(|v| v == y)
            .context(error::CsvSource {
                details: "Cannot find y index in csv header",
            })?;

        Ok(ParsedHeader {
            has_header: true,
            x_index,
            y_index,
        })
    }
}

struct ParsedHeader {
    pub has_header: bool,
    pub x_index: usize,
    pub y_index: usize,
}

struct ParsedRow {
    pub coordinate: Coordinate,
    pub time_interval: TimeInterval,
    // TODO: fields
}

fn unlimited_time_interval() -> TimeInterval {
    TimeInterval::new_unchecked(i64::min_value(), i64::max_value())
}

#[cfg(test)]
mod tests {

    use super::*;
    use futures::executor::block_on_stream;
    use geoengine_datatypes::collections::FeatureCollection;
    use std::io::{Seek, SeekFrom, Write};

    #[test]
    fn errorneous_point_rows() {
        let mut fake_file = tempfile::NamedTempFile::new().unwrap();
        write!(
            fake_file,
            "\
x,y
0,1
CORRUPT
4,5
"
        )
        .unwrap();
        fake_file.seek(SeekFrom::Start(0)).unwrap();

        let mut csv_source = CsvSource::new(CsvSourceParameters {
            file_path: fake_file.path().into(),
            field_separator: ',',
            geometry: CsvGeometrySpecification::XY {
                x: "x".into(),
                y: "y".into(),
            },
            time: CsvTimeSpecification::None,
        })
        .unwrap();

        let mut stream = block_on_stream(csv_source.read_points(1));

        assert_eq!(stream.next().unwrap().unwrap().len(), 1);
        assert_eq!(stream.next().unwrap().unwrap().len(), 1);
        assert!(stream.next().is_none());
    }

    #[test]
    fn corrupt_point_header() {
        let mut fake_file = tempfile::NamedTempFile::new().unwrap();
        write!(
            fake_file,
            "\
x,z
0,1
2,3
4,5
"
        )
        .unwrap();
        fake_file.seek(SeekFrom::Start(0)).unwrap();

        let mut csv_source = CsvSource::new(CsvSourceParameters {
            file_path: fake_file.path().into(),
            field_separator: ',',
            geometry: CsvGeometrySpecification::XY {
                x: "x".into(),
                y: "y".into(),
            },
            time: CsvTimeSpecification::None,
        })
        .unwrap();

        let mut stream = block_on_stream(csv_source.read_points(1));

        assert!(stream.next().unwrap().is_err());
        assert!(stream.next().is_none());
    }
}
