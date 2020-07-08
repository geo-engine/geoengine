use std::fs::File;
use std::path::PathBuf;
use std::pin::Pin;

use csv::{Position, Reader, StringRecord};
use futures::task::{Context, Poll};
use futures::Stream;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use geoengine_datatypes::collections::{
    BuilderProvider, FeatureCollectionBuilder, FeatureCollectionRowBuilder,
    GeoFeatureCollectionRowBuilder, MultiPointCollection,
};
use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D, TimeInterval};

use crate::error::{self};
use crate::util::Result;

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

enum ReaderState {
    Untouched(Reader<File>),
    OnGoing {
        header: ParsedHeader,
        records: csv::StringRecordsIntoIter<File>,
    },
    Error,
}

impl ReaderState {
    pub fn setup_once(&mut self, geometry_specification: CsvGeometrySpecification) -> Result<()> {
        if let ReaderState::Untouched(..) = self {
            // pass
        } else {
            return Ok(());
        }

        let old_state = std::mem::replace(self, ReaderState::Error);

        if let ReaderState::Untouched(mut csv_reader) = old_state {
            let header = match CsvSource::setup_read(geometry_specification, &mut csv_reader) {
                Ok(header) => header,
                Err(error) => return Err(error),
            };

            let mut records = csv_reader.into_records();

            // consume the first row, which is the header
            if header.has_header {
                // TODO: throw error
                records.next();
            }

            *self = ReaderState::OnGoing { header, records }
        }

        Ok(())
    }
}

pub struct CsvSource {
    parameters: CsvSourceParameters,
    csv_reader: ReaderState,
    bbox: BoundingBox2D,
    chunk_size: usize,
}

impl CsvSource {
    /// Creates a new `CsvSource`
    ///
    /// # Errors
    ///
    /// This constructor fails if the delimiter is not an ASCII character.
    /// Furthermore, there are IO errors from the reader.
    ///
    pub fn new(
        parameters: CsvSourceParameters,
        bbox: BoundingBox2D,
        chunk_size: usize,
    ) -> Result<Self> {
        ensure!(
            parameters.field_separator.is_ascii(),
            error::CsvSource {
                details: "Delimiter must be ASCII character"
            }
        );

        Ok(Self {
            csv_reader: ReaderState::Untouched(
                csv::ReaderBuilder::new()
                    .delimiter(parameters.field_separator as u8)
                    .has_headers(true)
                    .from_path(parameters.file_path.as_path())
                    .context(error::CsvSourceReader {})?,
            ),
            parameters,
            bbox,
            chunk_size,
        })
    }

    fn setup_read(
        geometry_specification: CsvGeometrySpecification,
        csv_reader: &mut Reader<File>,
    ) -> Result<ParsedHeader> {
        csv_reader
            .seek(Position::new())
            .context(error::CsvSourceReader {})?; // start at beginning

        ensure!(
            csv_reader.has_headers(),
            error::CsvSource {
                details: "CSV file must contain header",
            }
        );

        let header = csv_reader.headers().context(error::CsvSourceReader)?;

        let CsvGeometrySpecification::XY { x, y } = geometry_specification;
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

    /// Parse a single CSV row
    fn parse_row(header: &ParsedHeader, row: &StringRecord) -> Result<ParsedRow> {
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
            time_interval: TimeInterval::default(),
        })
    }
}

impl Stream for CsvSource {
    type Item = Result<MultiPointCollection>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // TODO: do work in separate task -> async
        let computation_result = || -> Result<Option<MultiPointCollection>> {
            // TODO: is clone necessary?
            let geometry_specification = self.parameters.geometry.clone();
            self.csv_reader.setup_once(geometry_specification)?;

            let bbox = self.bbox;
            let chunk_size = self.chunk_size;

            let (header, records) = match &mut self.csv_reader {
                ReaderState::OnGoing { header, records } => (header, records),
                ReaderState::Error => return Ok(None),
                _ => unreachable!(),
            };

            let mut builder = MultiPointCollection::builder().finish_header();
            let mut number_of_entries = 0; // TODO: add size/len to builder

            while number_of_entries < chunk_size {
                let record = match records.next() {
                    Some(r) => r,
                    None => break,
                };

                let row = record.with_context(|| error::CsvSourceReader)?;
                let parsed_row = CsvSource::parse_row(header, &row)?;

                if bbox.contains_coordinate(&parsed_row.coordinate) {
                    builder.push_geometry(parsed_row.coordinate.into())?;
                    builder.push_time_interval(parsed_row.time_interval)?;
                    builder.finish_row();

                    number_of_entries += 1;
                }
            }

            // TODO: is this the correct cancellation criterion?
            if number_of_entries > 0 {
                let collection = builder.build()?;
                Ok(Some(collection))
            } else {
                Ok(None)
            }
        }();

        Poll::Ready(match computation_result {
            Ok(Some(collection)) => Some(Ok(collection)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct ParsedHeader {
    pub has_header: bool,
    pub x_index: usize,
    pub y_index: usize,
}

struct ParsedRow {
    pub coordinate: Coordinate2D,
    pub time_interval: TimeInterval,
    // TODO: fields
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};

    use futures::executor::block_on_stream;

    use geoengine_datatypes::collections::FeatureCollection;

    use super::*;

    #[test]
    fn read_points() {
        let mut fake_file = tempfile::NamedTempFile::new().unwrap();
        write!(
            fake_file,
            "\
x,y
0,1
2,3
4,5
"
        )
        .unwrap();
        fake_file.seek(SeekFrom::Start(0)).unwrap();

        let csv_source = CsvSource::new(
            CsvSourceParameters {
                file_path: fake_file.path().into(),
                field_separator: ',',
                geometry: CsvGeometrySpecification::XY {
                    x: "x".into(),
                    y: "y".into(),
                },
                time: CsvTimeSpecification::None,
            },
            BoundingBox2D::new_unchecked((0., 0.).into(), (5., 5.).into()),
            2,
        )
        .unwrap();

        let mut stream = block_on_stream(csv_source);

        assert_eq!(stream.next().unwrap().unwrap().len(), 2);
        assert_eq!(stream.next().unwrap().unwrap().len(), 1);
        assert!(stream.next().is_none());
    }

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

        let csv_source = CsvSource::new(
            CsvSourceParameters {
                file_path: fake_file.path().into(),
                field_separator: ',',
                geometry: CsvGeometrySpecification::XY {
                    x: "x".into(),
                    y: "y".into(),
                },
                time: CsvTimeSpecification::None,
            },
            BoundingBox2D::new_unchecked((0., 0.).into(), (5., 5.).into()),
            1,
        )
        .unwrap();

        let mut stream = block_on_stream(csv_source);

        assert_eq!(stream.next().unwrap().unwrap().len(), 1);
        assert!(stream.next().unwrap().is_err());
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

        let csv_source = CsvSource::new(
            CsvSourceParameters {
                file_path: fake_file.path().into(),
                field_separator: ',',
                geometry: CsvGeometrySpecification::XY {
                    x: "x".into(),
                    y: "y".into(),
                },
                time: CsvTimeSpecification::None,
            },
            BoundingBox2D::new_unchecked((0., 0.).into(), (5., 5.).into()),
            1,
        )
        .unwrap();

        let mut stream = block_on_stream(csv_source);

        assert!(stream.next().unwrap().is_err());
        assert!(stream.next().is_none());
    }
}
