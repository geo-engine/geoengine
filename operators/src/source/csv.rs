use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::{fs::File, sync::atomic::AtomicBool};

use csv::{Position, Reader, StringRecord};
use futures::stream::BoxStream;
use futures::task::{Context, Poll};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use geoengine_datatypes::collections::{
    BuilderProvider, GeoFeatureCollectionRowBuilder, MultiPointCollection, VectorDataType,
};
use geoengine_datatypes::{
    primitives::{BoundingBox2D, Coordinate2D, TimeInterval},
    spatial_reference::SpatialReference,
};

use crate::engine::{
    InitializedVectorOperator, QueryContext, SourceOperator, TypedVectorQueryProcessor,
    VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
};
use crate::engine::{QueryProcessor, VectorQueryRectangle};
use crate::error;
use crate::util::Result;
use async_trait::async_trait;
use std::sync::atomic::Ordering;

/// Parameters for the CSV Source Operator
///
/// # Examples
///
/// ```rust
/// use serde_json::{Result, Value};
/// use geoengine_operators::source::{CsvSourceParameters, CsvSource};
/// use geoengine_operators::source::{CsvGeometrySpecification, CsvTimeSpecification};
///
/// let json_string = r#"
///     {
///         "type": "CsvSource",
///         "params": {
///             "filePath": "/foo/bar.csv",
///             "fieldSeparator": ",",
///             "geometry": {
///                 "type": "xy",
///                 "x": "x",
///                 "y": "y"
///             }
///         }
///     }"#;
///
/// let operator: CsvSource = serde_json::from_str(json_string).unwrap();
///
/// assert_eq!(operator, CsvSource {
///     params: CsvSourceParameters {
///         file_path: "/foo/bar.csv".into(),
///         field_separator: ',',
///         geometry: CsvGeometrySpecification::XY { x: "x".into(), y: "y".into() },
///         time: CsvTimeSpecification::None,
///     },
/// });
/// ```
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CsvSourceParameters {
    pub file_path: PathBuf,
    pub field_separator: char,
    pub geometry: CsvGeometrySpecification,
    #[serde(default)]
    pub time: CsvTimeSpecification,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CsvGeometrySpecification {
    #[allow(clippy::upper_case_acronyms)]
    XY { x: String, y: String },
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
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
            let header = match CsvSourceStream::setup_read(geometry_specification, &mut csv_reader)
            {
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

pub struct CsvSourceStream {
    parameters: CsvSourceParameters,
    bbox: BoundingBox2D,
    chunk_size: usize,
    reader_state: Arc<Mutex<ReaderState>>,
    thread_is_computing: Arc<AtomicBool>,
    #[allow(clippy::option_option)]
    poll_result: Arc<Mutex<Option<Option<Result<MultiPointCollection>>>>>,
}

pub type CsvSource = SourceOperator<CsvSourceParameters>;

#[typetag::serde]
#[async_trait]
impl VectorOperator for CsvSource {
    async fn initialize(
        self: Box<Self>,
        _context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        let initialized_source = InitializedCsvSource {
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint, // TODO: get as user input
                spatial_reference: SpatialReference::epsg_4326().into(), // TODO: get as user input
                columns: Default::default(), // TODO: get when source allows loading other columns
            },
            state: self.params,
        };

        Ok(initialized_source.boxed())
    }
}

pub struct InitializedCsvSource {
    result_descriptor: VectorResultDescriptor,
    state: CsvSourceParameters,
}

impl InitializedVectorOperator for InitializedCsvSource {
    fn query_processor(&self) -> Result<crate::engine::TypedVectorQueryProcessor> {
        Ok(TypedVectorQueryProcessor::MultiPoint(
            CsvSourceProcessor {
                params: self.state.clone(),
            }
            .boxed(),
        ))
    }

    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }
}

impl CsvSourceStream {
    /// Creates a new `CsvSource`
    ///
    /// # Errors
    ///
    /// This constructor fails if the delimiter is not an ASCII character.
    /// Furthermore, there are IO errors from the reader.
    ///
    // TODO: include time interval, e.g. QueryRectangle parameter
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
            reader_state: Arc::new(Mutex::new(ReaderState::Untouched(
                csv::ReaderBuilder::new()
                    .delimiter(parameters.field_separator as u8)
                    .has_headers(true)
                    .from_path(parameters.file_path.as_path())
                    .context(error::CsvSourceReader {})?,
            ))),
            thread_is_computing: Arc::new(AtomicBool::new(false)),
            poll_result: Arc::new(Mutex::new(None)),
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
            .map_err(|_error| error::Error::CsvSource {
                details: "Cannot parse x coordinate".to_string(),
            })?;
        let y: f64 = row
            .get(header.y_index)
            .context(error::CsvSource {
                details: "Cannot find y index key",
            })?
            .parse()
            .map_err(|_error| error::Error::CsvSource {
                details: "Cannot parse y coordinate".to_string(),
            })?;

        Ok(ParsedRow {
            coordinate: (x, y).into(),
            time_interval: TimeInterval::default(),
        })
    }
}

impl Stream for CsvSourceStream {
    type Item = Result<MultiPointCollection>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // TODO: handle lock poisoning on multiple occasions

        if self.thread_is_computing.load(Ordering::Relaxed) {
            return Poll::Pending;
        }

        let mut poll_result = self.poll_result.lock().unwrap();
        if poll_result.is_some() {
            let x = poll_result.take().unwrap();
            return Poll::Ready(x);
        }

        self.thread_is_computing.store(true, Ordering::Relaxed);

        let is_working = self.thread_is_computing.clone();
        let reader_state = self.reader_state.clone();
        let poll_result = self.poll_result.clone();

        let bbox = self.bbox;
        let chunk_size = self.chunk_size;
        let parameters = self.parameters.clone();
        let waker = cx.waker().clone();

        tokio::task::spawn_blocking(move || {
            let mut csv_reader = reader_state.lock().unwrap();
            let computation_result = || -> Result<Option<MultiPointCollection>> {
                // TODO: is clone necessary?
                let geometry_specification = parameters.geometry.clone();
                csv_reader.setup_once(geometry_specification)?;

                let (header, records) = match &mut *csv_reader {
                    ReaderState::OnGoing { header, records } => (header, records),
                    ReaderState::Error => return Ok(None),
                    ReaderState::Untouched(_) => unreachable!(),
                };

                let mut builder = MultiPointCollection::builder().finish_header();
                let mut number_of_entries = 0; // TODO: add size/len to builder

                while number_of_entries < chunk_size {
                    let record = match records.next() {
                        Some(r) => r,
                        None => break,
                    };

                    let row = record.with_context(|| error::CsvSourceReader)?;
                    let parsed_row = CsvSourceStream::parse_row(header, &row)?;

                    // TODO: filter time
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

            *poll_result.lock().unwrap() = Some(match computation_result {
                Ok(Some(collection)) => Some(Ok(collection)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            });
            is_working.store(false, Ordering::Relaxed);

            waker.wake();
        });

        Poll::Pending
    }
}

#[derive(Debug)]
struct CsvSourceProcessor {
    params: CsvSourceParameters,
}

#[async_trait]
impl QueryProcessor for CsvSourceProcessor {
    type Output = MultiPointCollection;
    type SpatialBounds = BoundingBox2D;

    async fn query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        _ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        // TODO: properly handle chunk_size
        Ok(CsvSourceStream::new(self.params.clone(), query.spatial_bounds, 10)?.boxed())
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

    use geoengine_datatypes::primitives::SpatialResolution;

    use super::*;
    use crate::engine::MockQueryContext;
    use geoengine_datatypes::collections::{FeatureCollectionInfos, ToGeoJson};

    #[tokio::test]
    async fn read_points() {
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

        let mut csv_source = CsvSourceStream::new(
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

        assert_eq!(csv_source.next().await.unwrap().unwrap().len(), 2);
        assert_eq!(csv_source.next().await.unwrap().unwrap().len(), 1);
        assert!(csv_source.next().await.is_none());
    }

    #[tokio::test]
    async fn erroneous_point_rows() {
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

        let mut csv_source = CsvSourceStream::new(
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

        assert_eq!(csv_source.next().await.unwrap().unwrap().len(), 1);
        assert!(csv_source.next().await.unwrap().is_err());
        assert_eq!(csv_source.next().await.unwrap().unwrap().len(), 1);
        assert!(csv_source.next().await.is_none());
    }

    #[tokio::test]
    async fn corrupt_point_header() {
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

        let mut csv_source = CsvSourceStream::new(
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

        assert!(csv_source.next().await.unwrap().is_err());
        assert!(csv_source.next().await.is_none());
    }

    #[tokio::test]
    async fn processor() {
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

        let params = CsvSourceParameters {
            file_path: fake_file.path().into(),
            field_separator: ',',
            geometry: CsvGeometrySpecification::XY {
                x: "x".into(),
                y: "y".into(),
            },
            time: CsvTimeSpecification::None,
        };

        let p = CsvSourceProcessor { params };

        let query = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new_unchecked(
                Coordinate2D::new(0., 0.),
                Coordinate2D::new(3., 3.),
            ),
            time_interval: TimeInterval::new_unchecked(0, 1),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::new(10 * 8 * 2);

        let r: Vec<Result<MultiPointCollection>> =
            p.query(query, &ctx).await.unwrap().collect().await;

        assert_eq!(r.len(), 1);

        assert_eq!(
            r[0].as_ref().unwrap().to_geo_json(),
            serde_json::json!({
                "type": "FeatureCollection",
                "features": [{
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [0.0, 1.0]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }, {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [2.0, 3.0]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }]
            })
            .to_string()
        );
    }

    #[test]
    fn operator() {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        write!(
            temp_file,
            "\
x;y
0;1
2;3
4;5
"
        )
        .unwrap();
        temp_file.seek(SeekFrom::Start(0)).unwrap();

        let params = CsvSourceParameters {
            file_path: temp_file.path().into(),
            field_separator: ';',
            geometry: CsvGeometrySpecification::XY {
                x: "x".into(),
                y: "y".into(),
            },
            time: CsvTimeSpecification::None,
        };

        let operator = CsvSource { params }.boxed();

        let operator_json = serde_json::to_string(&operator).unwrap();

        assert_eq!(
            operator_json,
            serde_json::json!({
                "type": "CsvSource",
                "params": {
                    "filePath": temp_file.path(),
                    "fieldSeparator": ";",
                    "geometry": {
                        "type": "xy",
                        "x": "x",
                        "y": "y"
                    },
                    "time": "None"
                }
            })
            .to_string()
        );

        let _operator: Box<dyn VectorOperator> = serde_json::from_str(&operator_json).unwrap();
    }
}
