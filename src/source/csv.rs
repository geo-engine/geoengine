use crate::error;
use crate::error::Error;
use crate::util::Result;
use csv::{Position, Reader};
use futures::stream::{self, StreamExt, TryStream};
use geoengine_datatypes::collections::PointCollection;
use geoengine_datatypes::primitives::TimeInterval;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use std::fs::File;
use std::i64;
use std::path::PathBuf;

/// Parameters for the GDAL Source Operator
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
            error::CsvSourceError {
                details: "Delimiter must be ASCII character"
            }
        );

        Ok(Self {
            csv_reader: csv::ReaderBuilder::new()
                .delimiter(parameters.field_separator as u8)
                .has_headers(true)
                .from_path(parameters.file_path.as_path())
                .context(error::CsvSourceReaderError {})?,
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
    ) -> impl TryStream<Ok = PointCollection, Error = Error, Item = Result<PointCollection, Error>> + '_
    {
        self.csv_reader.seek(Position::new()).unwrap(); // start at beginning

        let has_header = self.csv_reader.has_headers();
        assert!(has_header);
        let header = self.csv_reader.headers().unwrap(); // TODO: error handling

        let (x_index, y_index) =
            if let CsvGeometrySpecification::XY { x, y } = &self.parameters.geometry {
                // TODO: handle unwraps properly
                (
                    header.iter().position(|v| v == x).unwrap(),
                    header.iter().position(|v| v == y).unwrap(),
                )
            } else {
                todo!("handle the cases")
            };

        let csv_stream =
            stream::iter(
                self.csv_reader
                    .records()
                    .skip(if has_header { 1 } else { 0 }),
            );

        csv_stream.chunks(items_per_chunk).map(move |chunk| {
            if chunk.is_empty() {
                return Ok(PointCollection::empty());
            }

            let mut builder = PointCollection::builder();
            for result in chunk {
                if let Ok(result) = result {
                    let x: f64 = result.get(x_index).unwrap().parse().unwrap();
                    let y: f64 = result.get(y_index).unwrap().parse().unwrap();

                    builder.append_coordinate((x, y).into()).unwrap();
                    builder
                        .append_time_interval(unlimited_time_interval())
                        .unwrap();
                    builder.finish_row().unwrap();
                } else {
                    // TODO: handle errors
                }
            }
            Ok(builder.build().unwrap()) // TODO: handle error
        })
    }
}

fn unlimited_time_interval() -> TimeInterval {
    TimeInterval::new_unchecked(i64::min_value(), i64::max_value())
}
