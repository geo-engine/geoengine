use std::iter::Peekable;
use std::marker::PhantomData;
use std::ops::{Add, DerefMut};
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::task::Poll;
use std::{
    collections::{HashMap, HashSet},
    iter::Fuse,
};
use std::{ffi::OsStr, fmt::Debug};

use chrono::DateTime;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use futures::stream::BoxStream;
use futures::task::{Context, Waker};
use futures::Stream;
use futures::StreamExt;
use gdal::vector::sql::{Dialect, ResultSet};
use gdal::vector::{Feature, FeatureIterator, FieldValue, Layer, LayerCaps, OGRwkbGeometryType};
use gdal::{Dataset, DatasetOptions, GdalOpenFlags};
use log::debug;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tokio::task::spawn_blocking;

use geoengine_datatypes::collections::{
    BuilderProvider, FeatureCollection, FeatureCollectionBuilder, FeatureCollectionInfos,
    FeatureCollectionModifications, FeatureCollectionRowBuilder, GeoFeatureCollectionRowBuilder,
    VectorDataType,
};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, Coordinate2D, FeatureDataType, FeatureDataValue, Geometry,
    MultiLineString, MultiPoint, MultiPolygon, NoGeometry, TimeInstance, TimeInterval, TimeStep,
    TypedGeometry,
};
use geoengine_datatypes::util::arrow::ArrowTyped;

use crate::engine::{OperatorDatasets, QueryProcessor, VectorQueryRectangle};
use crate::error::Error;
use crate::util::Result;
use crate::{
    engine::{
        InitializedVectorOperator, MetaData, QueryContext, SourceOperator,
        TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
    },
    error,
};
use async_trait::async_trait;
use gdal::errors::GdalError;
use geoengine_datatypes::dataset::DatasetId;
use std::convert::{TryFrom, TryInto};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OgrSourceParameters {
    pub dataset: DatasetId,
    pub attribute_projection: Option<Vec<String>>,
}

impl OperatorDatasets for OgrSourceParameters {
    fn datasets_collect(&self, datasets: &mut Vec<DatasetId>) {
        datasets.push(self.dataset.clone());
    }
}

pub type OgrSource = SourceOperator<OgrSourceParameters>;

///  - `file_name`: path to the input file
///  - `layer_name`: name of the layer to load
///  - `time`: the type of the time attribute(s)
///  - `columns`: a mapping of the columns to data, time, space. Columns that are not listed are skipped when parsing.
///  - `force_ogr_time_filter`: bool. force external time filter via ogr layer, even though data types don't match. Might not work
///  - `force_ogr_spatial_filter`: bool. force external spatial filter via ogr layer.
///    (result: empty collection), but has better performance for wfs requests (optional, false if not provided)
///  - `on_error`: specify the type of error handling
///  - `provenance`: specify the provenance of a file
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OgrSourceDataset {
    pub file_name: PathBuf,
    pub layer_name: String,
    pub data_type: Option<VectorDataType>,
    #[serde(default)]
    pub time: OgrSourceDatasetTimeType,
    pub default_geometry: Option<TypedGeometry>,
    pub columns: Option<OgrSourceColumnSpec>,
    #[serde(default)]
    pub force_ogr_time_filter: bool,
    #[serde(default)]
    pub force_ogr_spatial_filter: bool,
    pub on_error: OgrSourceErrorSpec,
    pub sql_query: Option<String>,
    pub attribute_query: Option<String>,
}

impl OgrSourceDataset {
    pub fn project_columns(&mut self, attribute_projection: &Option<Vec<String>>) {
        if let Some(columns) = self.columns.as_mut() {
            columns.project_columns(attribute_projection);
        }
    }
}

/// The type of the time attribute(s):
///  - "none": no time information is mapped
///  - "start": only start information is mapped. duration has to specified in the duration attribute
///  - "start+end": start and end information is mapped
///  - "start+duration": start and duration information is mapped
///
/// There are different options within these variants:
///  - `start_field` and `end_field`: the name of the field that contains time information
///  - `start_format` and `start_format`: a mapping of a field type to a time value (cf. `OgrSourceDatasetTimeType`)
///  - `duration`: the duration of the time validity for all features in the file
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum OgrSourceDatasetTimeType {
    None,
    #[serde(rename_all = "camelCase")]
    Start {
        start_field: String,
        start_format: OgrSourceTimeFormat,
        duration: OgrSourceDurationSpec,
    },
    #[serde(rename = "start+end")]
    #[serde(rename_all = "camelCase")]
    StartEnd {
        start_field: String,
        start_format: OgrSourceTimeFormat,
        end_field: String,
        end_format: OgrSourceTimeFormat,
    },
    #[serde(rename = "start+duration")]
    #[serde(rename_all = "camelCase")]
    StartDuration {
        start_field: String,
        start_format: OgrSourceTimeFormat,
        duration_field: String,
    },
}

/// If no time is specified, expect to parse none
impl Default for OgrSourceDatasetTimeType {
    fn default() -> Self {
        Self::None
    }
}

///  A mapping for a column to the start time [if time != "none"]
///   - format: define the format of the column
///   - "custom": define a custom format in the attribute `custom_format`
///   - "seconds": time column is numeric and contains seconds as UNIX timestamp
///   - "auto": time is parsed by OGR
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(tag = "format")]
#[serde(rename_all = "camelCase")]
pub enum OgrSourceTimeFormat {
    #[serde(rename_all = "camelCase")]
    Custom {
        custom_format: String,
    },
    Seconds,
    Auto,
}

impl Default for OgrSourceTimeFormat {
    fn default() -> Self {
        Self::Auto
    }
}

/// A mapping of the columns to data, time, space. Columns that are not listed are skipped when parsing.
///  - `format_specifics`: Format specific options if any.
///  - x: the name of the column containing the x coordinate (or the wkt string) [if CSV file]
///  - y: the name of the column containing the y coordinate [if CSV file with y column]
///  - float: an array of column names containing float values
///  - int: an array of column names containing int values
///  - text: an array of column names containing alpha-numeric values
///  - rename: a. optional map of column names from data source to the name in the resulting collection
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OgrSourceColumnSpec {
    pub format_specifics: Option<FormatSpecifics>,
    pub x: String,
    pub y: Option<String>,
    pub int: Vec<String>,
    pub float: Vec<String>,
    pub text: Vec<String>,
    pub rename: Option<HashMap<String, String>>,
}

impl OgrSourceColumnSpec {
    pub fn project_columns(&mut self, attribute_projection: &Option<Vec<String>>) {
        let attributes: HashSet<&String> =
            if let Some(attribute_projection) = attribute_projection.as_ref() {
                attribute_projection.iter().collect()
            } else {
                return;
            };

        self.int.retain(|attribute| attributes.contains(attribute));
        self.float
            .retain(|attribute| attributes.contains(attribute));
        self.text.retain(|attribute| attributes.contains(attribute));
    }
}

/// This enum provides all format specific options
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum FormatSpecifics {
    Csv { header: CsvHeader },
}

/// For CSV files this tells gdal whether or not the file
/// contains a header line.
/// The value `Auto` enables gdal's auto detection.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CsvHeader {
    Yes,
    No,
    Auto,
}

impl CsvHeader {
    fn as_gdal_param(&self) -> String {
        format!(
            "HEADERS={}",
            match self {
                Self::Yes => "YES",
                Self::No => "NO",
                Self::Auto => "AUTO",
            }
        )
    }
}

/// Specify the type of error handling
///  - "ignore": invalid column values are kept as null, missing/invalid geom features are skipped
///  - "abort": invalid column values and missing/invalid geoms result in abort
#[derive(Copy, Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OgrSourceErrorSpec {
    Ignore,
    Abort,
}

impl OgrSourceErrorSpec {
    /// handle the given error depending on the spec
    fn on_error<T>(self, error: error::Error) -> Result<Option<T>> {
        match self {
            OgrSourceErrorSpec::Ignore => Ok(None),
            OgrSourceErrorSpec::Abort => Err(error),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum OgrSourceDurationSpec {
    Infinite,
    Zero,
    Value(TimeStep),
}

impl Add<OgrSourceDurationSpec> for TimeInstance {
    type Output = Result<TimeInstance>;

    fn add(self, rhs: OgrSourceDurationSpec) -> Self::Output {
        match rhs {
            OgrSourceDurationSpec::Infinite => Ok(TimeInstance::MAX),
            OgrSourceDurationSpec::Zero => Ok(self),
            OgrSourceDurationSpec::Value(step) => (self + step).context(error::DataType),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OgrSourceState {
    dataset_information:
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
}

pub struct InitializedOgrSource {
    result_descriptor: VectorResultDescriptor,
    state: OgrSourceState,
}

#[typetag::serde]
#[async_trait]
impl VectorOperator for OgrSource {
    async fn initialize(
        self: Box<Self>,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn crate::engine::InitializedVectorOperator>> {
        // TODO: check rename of fields are valid

        let info: Box<
            dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        > = context.meta_data(&self.params.dataset).await?;

        let initialized_source = InitializedOgrSource {
            result_descriptor: info.result_descriptor().await?,
            state: OgrSourceState {
                dataset_information: info,
            },
        };

        Ok(initialized_source.boxed())
    }
}

impl OgrSource {
    fn ogr_geometry_type(geometry: &gdal::vector::Geometry) -> VectorDataType {
        match geometry.geometry_type() {
            OGRwkbGeometryType::wkbPoint | OGRwkbGeometryType::wkbMultiPoint => {
                VectorDataType::MultiPoint
            }
            OGRwkbGeometryType::wkbLineString | OGRwkbGeometryType::wkbMultiLineString => {
                VectorDataType::MultiLineString
            }
            OGRwkbGeometryType::wkbPolygon | OGRwkbGeometryType::wkbMultiPolygon => {
                VectorDataType::MultiPolygon
            }
            _ => {
                // TODO: is *data* a reasonable fallback type? or throw an error?
                VectorDataType::Data
            }
        }
    }
}

impl InitializedVectorOperator for InitializedOgrSource {
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        // TODO: simplify with macro
        Ok(match self.result_descriptor.data_type {
            VectorDataType::Data => TypedVectorQueryProcessor::Data(
                OgrSourceProcessor::new(self.state.dataset_information.clone()).boxed(),
            ),
            VectorDataType::MultiPoint => TypedVectorQueryProcessor::MultiPoint(
                OgrSourceProcessor::new(self.state.dataset_information.clone()).boxed(),
            ),
            VectorDataType::MultiLineString => TypedVectorQueryProcessor::MultiLineString(
                OgrSourceProcessor::new(self.state.dataset_information.clone()).boxed(),
            ),
            VectorDataType::MultiPolygon => TypedVectorQueryProcessor::MultiPolygon(
                OgrSourceProcessor::new(self.state.dataset_information.clone()).boxed(),
            ),
        })
    }

    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }
}

pub struct OgrSourceProcessor<G>
where
    G: Geometry + ArrowTyped,
{
    dataset_information:
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
    _collection_type: PhantomData<FeatureCollection<G>>,
}

impl<G> OgrSourceProcessor<G>
where
    G: Geometry + ArrowTyped,
{
    pub fn new(
        dataset_information: Box<
            dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        >,
    ) -> Self {
        Self {
            dataset_information,
            _collection_type: Default::default(),
        }
    }
}

#[async_trait]
impl<G> QueryProcessor for OgrSourceProcessor<G>
where
    G: Geometry + ArrowTyped + 'static + std::marker::Unpin + TryFromOgrGeometry,
    FeatureCollectionRowBuilder<G>: FeatureCollectionBuilderGeometryHandler<G>,
{
    type Output = FeatureCollection<G>;
    type SpatialBounds = BoundingBox2D;

    async fn query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        Ok(OgrSourceStream::new(
            self.dataset_information.loading_info(query).await?,
            query,
            ctx.chunk_byte_size(),
        )
        .boxed())
    }
}

pub struct OgrSourceStream<G>
where
    G: Geometry + ArrowTyped,
{
    worker_thread_is_idle: bool,
    worker_thread_terminated: bool,
    poll_result_receiver: Receiver<Option<Result<FeatureCollection<G>>>>,
    work_query_sender: SyncSender<WorkQuery>,
    _geometry_type: PhantomData<G>,
}

struct WorkQuery {
    waker: Waker,
}

enum FeaturesProvider<'a> {
    Layer(Layer<'a>),
    ResultSet(ResultSet<'a>),
}

impl FeaturesProvider<'_> {
    fn features(&mut self) -> FeatureIterator {
        match self {
            FeaturesProvider::Layer(l) => l.features(),
            FeaturesProvider::ResultSet(r) => r.features(),
        }
    }

    fn layer_ref(&self) -> &Layer {
        match self {
            FeaturesProvider::Layer(l) => l,
            FeaturesProvider::ResultSet(r) => &**r,
        }
    }

    fn set_spatial_filter(&mut self, spatial_bounds: &BoundingBox2D) {
        match self {
            FeaturesProvider::Layer(l) => {
                l.set_spatial_filter_rect(
                    spatial_bounds.lower_left().x,
                    spatial_bounds.lower_left().y,
                    spatial_bounds.upper_right().x,
                    spatial_bounds.upper_right().y,
                );
            }
            FeaturesProvider::ResultSet(r) => {
                r.deref_mut().set_spatial_filter_rect(
                    spatial_bounds.lower_left().x,
                    spatial_bounds.lower_left().y,
                    spatial_bounds.upper_right().x,
                    spatial_bounds.upper_right().y,
                );
            }
        }
    }

    fn set_attribute_filter(&mut self, attribute_query: &str) -> Result<()> {
        match self {
            FeaturesProvider::Layer(l) => l.set_attribute_filter(attribute_query)?,
            FeaturesProvider::ResultSet(r) => {
                r.deref_mut().set_attribute_filter(attribute_query)?
            }
        };

        Ok(())
    }

    fn has_gdal_capability(&self, caps: LayerCaps) -> bool {
        self.layer_ref().has_capability(caps)
    }
}

impl<G> OgrSourceStream<G>
where
    G: Geometry + ArrowTyped + 'static + TryFromOgrGeometry + TryFrom<TypedGeometry>,
    FeatureCollectionRowBuilder<G>: FeatureCollectionBuilderGeometryHandler<G>,
{
    pub fn new(
        dataset_information: OgrSourceDataset,
        query_rectangle: VectorQueryRectangle,
        chunk_byte_size: usize,
    ) -> Self {
        // We need two slots for the channel in case of an error: first output `Err`, then output `None` to close the `Stream`
        let (poll_result_sender, poll_result_receiver) = mpsc::sync_channel(2);
        let (work_query_sender, work_query_receiver) = mpsc::sync_channel(1);

        // This stream spawns a thread early since GDAL's data types are not `Send` and we need to create everything inside this thread.
        spawn_blocking(move || {
            let mut work_query = match work_query_receiver.recv() {
                Ok(work_query) => work_query,
                Err(_) => return, // sender disconnected, so there will be no new work
            };

            if let Err(error) = Self::compute_thread(
                &mut work_query,
                &dataset_information,
                &work_query_receiver,
                &poll_result_sender,
                &query_rectangle,
                chunk_byte_size,
            ) {
                poll_result_sender.send(Some(Err(error))).unwrap();
                poll_result_sender.send(None).unwrap();
                work_query.waker.wake();
            };
        });

        Self {
            worker_thread_is_idle: true,
            worker_thread_terminated: false,
            poll_result_receiver,
            work_query_sender,
            _geometry_type: Default::default(),
        }
    }

    fn open_csv_dataset(dataset_info: &OgrSourceDataset) -> Result<Dataset> {
        let columns = dataset_info
            .columns
            .as_ref()
            .ok_or(error::Error::OgrSourceColumnsSpecMissing)?;

        let allowed_drivers = Some(vec!["CSV"]);

        let mut dataset_options = DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_VECTOR,
            allowed_drivers: allowed_drivers.as_deref(),
            ..DatasetOptions::default()
        };

        let headers = if let Some(FormatSpecifics::Csv { header }) = &columns.format_specifics {
            header.as_gdal_param()
        } else {
            CsvHeader::Auto.as_gdal_param()
        };

        // TODO: make column x optional or allow other indication for data collection
        if columns.x.is_empty() {
            let open_opts = &[
                headers.as_str(),
                // "AUTODETECT_TYPE=YES", // This breaks tests
            ];
            dataset_options.open_options = Some(open_opts);
            return Ok(Dataset::open_ex(&dataset_info.file_name, dataset_options)?);
        }

        if let Some(y) = &columns.y {
            let open_opts = &[
                &format!("X_POSSIBLE_NAMES={}", columns.x),
                &format!("Y_POSSIBLE_NAMES={}", y),
                headers.as_str(),
                "AUTODETECT_TYPE=YES",
            ];
            dataset_options.open_options = Some(open_opts);
            return Ok(Dataset::open_ex(&dataset_info.file_name, dataset_options)?);
        }

        let open_opts = &[
            &format!("GEOM_POSSIBLE_NAMES={}", columns.x),
            headers.as_str(),
            "AUTODETECT_TYPE=YES",
        ];
        dataset_options.open_options = Some(open_opts);
        Ok(Dataset::open_ex(&dataset_info.file_name, dataset_options)?)
    }

    fn is_csv(dataset_info: &OgrSourceDataset) -> bool {
        if let Some("csv" | "tsv") = dataset_info.file_name.extension().and_then(OsStr::to_str) {
            true
        } else {
            dataset_info.file_name.as_path().starts_with("CSV:")
        }
    }

    fn open_gdal_dataset(dataset_info: &OgrSourceDataset) -> Result<Dataset> {
        if Self::is_csv(dataset_info) {
            Self::open_csv_dataset(dataset_info)
        } else {
            Ok(Dataset::open_ex(
                &dataset_info.file_name,
                DatasetOptions {
                    open_flags: GdalOpenFlags::GDAL_OF_VECTOR,
                    ..Default::default()
                },
            )?)
        }
    }

    fn compute_thread(
        work_query: &mut WorkQuery,
        dataset_information: &OgrSourceDataset,
        work_query_receiver: &Receiver<WorkQuery>,
        poll_result_sender: &SyncSender<Option<Result<FeatureCollection<G>>>>,
        query_rectangle: &VectorQueryRectangle,
        chunk_byte_size: usize,
    ) -> Result<()> {
        // TODO: add OGR time filter if forced
        let dataset = Self::open_gdal_dataset(dataset_information)?;

        let mut features_provider = if let Some(sql) = dataset_information.sql_query.as_ref() {
            FeaturesProvider::ResultSet(
                dataset
                    .execute_sql(sql, None, Dialect::DEFAULT)?
                    .ok_or(error::Error::OgrSqlQuery)?,
            )
        } else {
            FeaturesProvider::Layer(dataset.layer_by_name(&dataset_information.layer_name)?)
        };

        let use_ogr_spatial_filter = dataset_information.force_ogr_spatial_filter
            || features_provider.has_gdal_capability(gdal::vector::LayerCaps::OLCFastSpatialFilter);

        if use_ogr_spatial_filter {
            debug!(
                "using spatial filter {:?} for layer {:?}",
                query_rectangle.spatial_bounds, &dataset_information.layer_name
            );
            // NOTE: the OGR-filter may be inaccurately allowing more features that should be returned in a "strict" fashion.
            features_provider.set_spatial_filter(&query_rectangle.spatial_bounds);
        }

        if let Some(attribute_query) = &dataset_information.attribute_query {
            debug!(
                "using attribute filter {:?} for layer {:?}",
                attribute_query, &dataset_information.layer_name
            );
            features_provider.set_attribute_filter(attribute_query)?;
        }

        let (data_types, feature_collection_builder) =
            Self::initialize_types_and_builder(dataset_information);

        let time_extractor = Self::initialize_time_extractors(dataset_information);

        let mut features = features_provider.features().fuse().peekable();

        if features.peek().is_none() {
            // emit empty dataset and finish

            let empty_collection = feature_collection_builder
                .finish_header()
                .build()
                .map_err(Into::into);

            if poll_result_sender
                .send(Some(empty_collection))
                .and_then(|_| poll_result_sender.send(None))
                .is_ok()
            {
                work_query.waker.wake_by_ref();
            }

            return Ok(());
        }

        let mut emitted_non_empty_collections = false;

        while features.peek().is_some() {
            let batch_result = Self::compute_batch(
                &mut features,
                feature_collection_builder.clone(),
                dataset_information,
                &data_types,
                query_rectangle,
                &time_extractor,
                chunk_byte_size,
                use_ogr_spatial_filter,
            );

            let batch_result = if let Some(rename) = dataset_information
                .columns
                .as_ref()
                .and_then(|c| c.rename.as_ref())
            {
                let names: Vec<_> = rename.iter().collect();
                batch_result
                    .and_then(|c| c.rename_columns(names.as_slice()).context(error::DataType))
            } else {
                batch_result
            };

            let is_empty = batch_result
                .as_ref()
                .map_or(false, FeatureCollection::is_empty);

            // don't emit an empty collection if there were non-empty results previously
            if is_empty && emitted_non_empty_collections {
                break;
            }

            emitted_non_empty_collections = true;

            match poll_result_sender.send(Some(batch_result)) {
                Ok(_) => work_query.waker.wake_by_ref(),
                Err(_) => return Ok(()), // receiver disconnected, so this thread can abort
            };

            *work_query = match work_query_receiver.recv() {
                Ok(work_query) => work_query,
                Err(_) => return Ok(()), // sender disconnected, so there will be no new work
            };
        }

        if poll_result_sender.send(None).is_ok() {
            work_query.waker.wake_by_ref();
        }

        Ok(())
    }

    fn create_time_parser(
        time_format: &OgrSourceTimeFormat,
    ) -> Box<dyn Fn(FieldValue) -> Result<TimeInstance> + '_> {
        debug!("{:?}", time_format);

        match time_format {
            OgrSourceTimeFormat::Auto => Box::new(move |field: FieldValue| match field {
                FieldValue::DateValue(value) => Ok(value.and_hms(0, 0, 0).naive_utc().into()),
                FieldValue::DateTimeValue(value) => Ok(value.naive_utc().into()),
                _ => Err(Error::OgrFieldValueIsNotDateTime),
            }),
            OgrSourceTimeFormat::Custom { custom_format } => Box::new(move |field: FieldValue| {
                let date = field.into_string().ok_or(Error::OgrFieldValueIsNotString)?;
                let date_time_result = DateTime::parse_from_str(&date, custom_format)
                    .map(|t| t.timestamp_millis())
                    .or_else(|_| {
                        NaiveDateTime::parse_from_str(&date, custom_format)
                            .map(|n| n.timestamp_millis())
                    })
                    .or_else(|_| {
                        NaiveDate::parse_from_str(&date, custom_format)
                            .map(|d| d.and_hms(0, 0, 0).timestamp_millis())
                    });
                Ok(date_time_result?.try_into()?)
            }),
            OgrSourceTimeFormat::Seconds => Box::new(move |field: FieldValue| match field {
                FieldValue::IntegerValue(v) => {
                    TimeInstance::from_millis(i64::from(v) * 1000).context(error::DataType)
                }
                FieldValue::Integer64Value(v) => {
                    TimeInstance::from_millis(v * 1000).context(error::DataType)
                }
                FieldValue::StringValue(v) => DateTime::parse_from_str(&v, "%s")
                    .context(error::TimeParse)
                    .and_then(|d| d.timestamp_millis().try_into().context(error::DataType)),
                _ => Err(Error::OgrFieldValueIsNotValidForSeconds),
            }),
        }
    }

    fn initialize_time_extractors(
        dataset_information: &OgrSourceDataset,
    ) -> Box<dyn Fn(&Feature) -> Result<TimeInterval> + '_> {
        // TODO: exploit rust-gdal `datetime` feature

        match &dataset_information.time {
            OgrSourceDatasetTimeType::None => {
                Box::new(move |_feature: &Feature| Ok(TimeInterval::default()))
            }
            OgrSourceDatasetTimeType::Start {
                start_field,
                start_format,
                duration,
            } => {
                let time_start_parser = Self::create_time_parser(start_format);

                Box::new(move |feature: &Feature| {
                    let field_value = feature.field(&start_field)?;
                    if let Some(field_value) = field_value {
                        let time_start = time_start_parser(field_value)?;
                        TimeInterval::new(time_start, (time_start + *duration)?).map_err(Into::into)
                    } else {
                        // TODO: throw error or use some user defined default time (like for geometries)?
                        Ok(TimeInterval::default())
                    }
                })
            }
            OgrSourceDatasetTimeType::StartEnd {
                start_field,
                start_format,
                end_field,
                end_format,
            } => {
                let time_start_parser = Self::create_time_parser(start_format);
                let time_end_parser = Self::create_time_parser(end_format);

                Box::new(move |feature: &Feature| {
                    let start_field_value = feature.field(&start_field)?;
                    let end_field_value = feature.field(&end_field)?;

                    if let (Some(start_field_value), Some(end_field_value)) =
                        (start_field_value, end_field_value)
                    {
                        let time_start = time_start_parser(start_field_value)?;
                        let time_end = time_end_parser(end_field_value)?;

                        TimeInterval::new(time_start, time_end).map_err(Into::into)
                    } else {
                        // TODO: throw error or use some user defined default time (like for geometries)?
                        Ok(TimeInterval::default())
                    }
                })
            }
            OgrSourceDatasetTimeType::StartDuration {
                start_field,
                start_format,
                duration_field,
            } => {
                let time_start_parser = Self::create_time_parser(start_format);

                Box::new(move |feature: &Feature| {
                    let start_field_value = feature.field(&start_field)?;
                    let duration_field_value = feature.field(&duration_field)?;

                    if let (Some(start_field_value), Some(duration_field_value)) =
                        (start_field_value, duration_field_value)
                    {
                        let time_start = time_start_parser(start_field_value)?;
                        let duration = i64::from(
                            duration_field_value
                                .into_int()
                                .ok_or(Error::OgrFieldValueIsNotValidForSeconds)?,
                        );

                        TimeInterval::new(time_start, time_start + duration).map_err(Into::into)
                    } else {
                        // TODO: throw error or use some user defined default time (like for geometries)?
                        Ok(TimeInterval::default())
                    }
                })
            }
        }
    }

    fn initialize_types_and_builder(
        dataset_information: &OgrSourceDataset,
    ) -> (
        HashMap<String, FeatureDataType>,
        FeatureCollectionBuilder<G>,
    ) {
        let mut data_types = HashMap::new();
        let mut feature_collection_builder = FeatureCollection::<G>::builder();
        // TODO: what to do if there is nothing specified?
        if let Some(ref column_spec) = dataset_information.columns {
            // TODO: error handling instead of unwrap
            for attribute in &column_spec.int {
                data_types.insert(attribute.clone(), FeatureDataType::Int);
                feature_collection_builder
                    .add_column(attribute.clone(), FeatureDataType::Int)
                    .unwrap();
            }
            for attribute in &column_spec.float {
                data_types.insert(attribute.clone(), FeatureDataType::Float);
                feature_collection_builder
                    .add_column(attribute.clone(), FeatureDataType::Float)
                    .unwrap();
            }
            for attribute in &column_spec.text {
                data_types.insert(attribute.clone(), FeatureDataType::Text);
                feature_collection_builder
                    .add_column(attribute.clone(), FeatureDataType::Text)
                    .unwrap();
            }
        }
        (data_types, feature_collection_builder)
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_batch(
        feature_iterator: &mut Peekable<Fuse<FeatureIterator<'_>>>,
        feature_collection_builder: FeatureCollectionBuilder<G>,
        dataset_information: &OgrSourceDataset,
        data_types: &HashMap<String, FeatureDataType>,
        query_rectangle: &VectorQueryRectangle,
        time_extractor: &dyn Fn(&Feature) -> Result<TimeInterval>,
        chunk_byte_size: usize,
        was_spatial_filtered_by_ogr: bool,
    ) -> Result<FeatureCollection<G>> {
        let mut builder = feature_collection_builder.finish_header();

        let default_geometry: Option<G> = match &dataset_information.default_geometry {
            Some(tg) => Some(tg.clone().try_into()?),
            None => None,
        };

        for feature in feature_iterator {
            if let Err(error) = Self::add_feature_to_batch(
                dataset_information.on_error,
                &default_geometry,
                data_types,
                query_rectangle,
                time_extractor,
                &mut builder,
                &feature,
                dataset_information.force_ogr_time_filter,
                was_spatial_filtered_by_ogr,
            ) {
                match dataset_information.on_error {
                    OgrSourceErrorSpec::Ignore => continue,
                    OgrSourceErrorSpec::Abort => return Err(error),
                }
            }

            if !builder.is_empty() && builder.byte_size() >= chunk_byte_size {
                break;
            }
        }

        builder.build().map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    fn add_feature_to_batch(
        error_spec: OgrSourceErrorSpec,
        default_geometry: &Option<G>,
        data_types: &HashMap<String, FeatureDataType>,
        query_rectangle: &VectorQueryRectangle,
        time_extractor: &dyn Fn(&Feature) -> Result<TimeInterval, Error>,
        builder: &mut FeatureCollectionRowBuilder<G>,
        feature: &Feature,
        was_time_filtered_by_ogr: bool,
        was_spatial_filtered_by_ogr: bool,
    ) -> Result<()> {
        let time_interval = time_extractor(feature)?;

        // filter out data items not in the query time interval
        if !was_time_filtered_by_ogr && !time_interval.intersects(&query_rectangle.time_interval) {
            return Ok(());
        }

        let geometry: G = match <G as TryFromOgrGeometry>::try_from(
            feature.geometry_by_index(0).map_err(Into::into),
        ) {
            Ok(g) => g,
            Err(Error::Gdal {
                source: GdalError::InvalidFieldIndex { method_name, index },
            }) => match default_geometry.as_ref() {
                Some(g) => g.clone(),
                None => {
                    return Err(Error::Gdal {
                        source: GdalError::InvalidFieldIndex { method_name, index },
                    })
                }
            },
            Err(e) => return Err(e),
        };

        // filter out geometries that are not contained in the query's bounding box
        if !was_spatial_filtered_by_ogr
            && !geometry.intersects_bbox(&query_rectangle.spatial_bounds)
        {
            return Ok(());
        }

        builder.push_generic_geometry(geometry)?;
        builder.push_time_interval(time_interval)?;

        for (column, data_type) in data_types {
            let field = feature.field(&column);

            match data_type {
                FeatureDataType::Text => {
                    #[allow(clippy::match_same_arms)]
                    let text_option = match field {
                        Ok(Some(FieldValue::IntegerValue(v))) => Some(v.to_string()),
                        Ok(Some(FieldValue::Integer64Value(v))) => Some(v.to_string()),
                        Ok(Some(FieldValue::StringValue(s))) => Some(s),
                        Ok(Some(FieldValue::RealValue(v))) => Some(v.to_string()),
                        Ok(Some(FieldValue::DateTimeValue(v))) => Some(v.to_string()), //TODO: allow multiple date columns
                        Ok(Some(FieldValue::DateValue(v))) => Some(v.to_string()),
                        Ok(None) => None,
                        Ok(Some(v)) => error_spec.on_error(Error::OgrColumnFieldTypeMismatch {
                            expected: "Text".to_string(),
                            field_value: v,
                        })?, // TODO: handle other types
                        Err(e) => error_spec.on_error(Error::Gdal { source: e })?,
                    };

                    builder.push_data(column, FeatureDataValue::NullableText(text_option))?;
                }
                FeatureDataType::Float => {
                    #[allow(clippy::match_same_arms)]
                    let value_option = match field {
                        Ok(Some(FieldValue::IntegerValue(v))) => Some(f64::from(v)),
                        Ok(Some(FieldValue::StringValue(s))) => f64::from_str(&s).ok(),
                        Ok(Some(FieldValue::RealValue(v))) => Some(v),
                        Ok(None) => None,
                        Ok(Some(v)) => error_spec.on_error(Error::OgrColumnFieldTypeMismatch {
                            expected: "Float".to_string(),
                            field_value: v,
                        })?, // TODO: handle other types
                        Err(e) => error_spec.on_error(Error::Gdal { source: e })?,
                    };

                    builder.push_data(column, FeatureDataValue::NullableFloat(value_option))?;
                }
                FeatureDataType::Int => {
                    #[allow(clippy::match_same_arms)]
                    let value_option = match field {
                        Ok(Some(FieldValue::IntegerValue(v))) => Some(i64::from(v)),
                        Ok(Some(FieldValue::Integer64Value(v))) => Some(v),
                        Ok(Some(FieldValue::StringValue(s))) => i64::from_str(&s).ok(),
                        Ok(Some(FieldValue::RealValue(v))) => Some(v as i64),
                        Ok(None) => None,
                        Ok(Some(v)) => error_spec.on_error(Error::OgrColumnFieldTypeMismatch {
                            expected: "Int".to_string(),
                            field_value: v,
                        })?, // TODO: handle other types
                        Err(e) => error_spec.on_error(Error::Gdal { source: e })?,
                    };

                    builder.push_data(column, FeatureDataValue::NullableInt(value_option))?;
                }
                FeatureDataType::Category => {
                    #[allow(clippy::match_same_arms)]
                    let _value_option: Option<u8> = match field {
                        Ok(None) => None,
                        Ok(Some(v)) => error_spec.on_error(Error::OgrColumnFieldTypeMismatch {
                            expected: "Category".to_string(),
                            field_value: v,
                        })?, // TODO: handle other types
                        Err(e) => error_spec.on_error(Error::Gdal { source: e })?,
                    };
                }
                FeatureDataType::Bool => {
                    #[allow(clippy::match_same_arms)]
                    let value_option = match field {
                        Ok(Some(FieldValue::IntegerValue(v))) => Some(v != 0),
                        Ok(Some(FieldValue::Integer64Value(v))) => Some(v != 0),
                        Ok(Some(FieldValue::StringValue(s))) => bool::from_str(&s).ok(),
                        Ok(Some(FieldValue::RealValue(v))) => Some(v != 0.0),
                        Ok(None) => None,
                        Ok(Some(v)) => error_spec.on_error(Error::OgrColumnFieldTypeMismatch {
                            expected: "Bool".to_string(),
                            field_value: v,
                        })?, // TODO: handle other types
                        Err(e) => error_spec.on_error(Error::Gdal { source: e })?,
                    };

                    builder.push_data(column, FeatureDataValue::NullableBool(value_option))?;
                }
            }
        }

        builder.finish_row();

        Ok(())
    }
}

impl<G> Stream for OgrSourceStream<G>
where
    G: Geometry + ArrowTyped + 'static + std::marker::Unpin,
{
    type Item = Result<FeatureCollection<G>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.worker_thread_terminated {
            // error was sent out previously, now stop the stream
            return Poll::Ready(None);
        }

        match self.poll_result_receiver.try_recv() {
            Ok(poll_result) => {
                self.as_mut().worker_thread_is_idle = true;

                return Poll::Ready(poll_result);
            }
            Err(TryRecvError::Empty) => {
                // nothing to do
            }
            Err(TryRecvError::Disconnected) => {
                self.as_mut().worker_thread_terminated = true;

                return Poll::Ready(Some(Err(Error::WorkerThread {
                    reason: "Channel on worker thread died".to_string(),
                })));
            }
        };

        if self.worker_thread_is_idle {
            let work_query = WorkQuery {
                waker: cx.waker().clone(),
            };

            match self.work_query_sender.try_send(work_query) {
                Ok(_) => {
                    self.as_mut().worker_thread_is_idle = true;
                }

                Err(TrySendError::Full(_)) => {
                    // The thread has still work to do
                }

                Err(TrySendError::Disconnected(_)) => {
                    self.as_mut().worker_thread_terminated = true;

                    return Poll::Ready(Some(Err(Error::WorkerThread {
                        reason: "Channel on worker thread died".to_string(),
                    })));
                }
            };
        }

        Poll::Pending
    }
}

// use `TryFrom` in `datatypes` if this is used on more than one occasion
pub trait TryFromOgrGeometry: Sized {
    fn try_from(geometry: Result<&gdal::vector::Geometry>) -> Result<Self>;
}

/// Implement direct conversions from OGR geometries to our geometries
/// Unfortunately, we cannot convert to `geo`'s geometries since the implementation panics on unknown types.
impl TryFromOgrGeometry for MultiPoint {
    fn try_from(geometry: Result<&gdal::vector::Geometry>) -> Result<Self> {
        fn coordinate(geometry: &gdal::vector::Geometry) -> Coordinate2D {
            let (x, y, _) = geometry.get_point(0);
            Coordinate2D::new(x, y)
        }

        let geometry = geometry?;

        match geometry.geometry_type() {
            OGRwkbGeometryType::wkbPoint => Ok(MultiPoint::new(vec![coordinate(geometry)])?),
            OGRwkbGeometryType::wkbMultiPoint => {
                let coordinates = (0..geometry.geometry_count())
                    .map(|i| coordinate(&unsafe { geometry.get_unowned_geometry(i) }))
                    .collect();

                Ok(MultiPoint::new(coordinates)?)
            }
            _ => Err(Error::InvalidType {
                expected: format!("{:?}", VectorDataType::MultiPoint),
                found: format!("{:?}", OgrSource::ogr_geometry_type(geometry)),
            }),
        }
    }
}

impl TryFromOgrGeometry for MultiLineString {
    fn try_from(geometry: Result<&gdal::vector::Geometry>) -> Result<Self> {
        fn coordinates(geometry: &gdal::vector::Geometry) -> Vec<Coordinate2D> {
            geometry
                .get_point_vec()
                .into_iter()
                .map(|(x, y, _z)| Coordinate2D::new(x, y))
                .collect()
        }

        let geometry = geometry?;

        match geometry.geometry_type() {
            OGRwkbGeometryType::wkbLineString => {
                Ok(MultiLineString::new(vec![coordinates(geometry)])?)
            }
            OGRwkbGeometryType::wkbMultiLineString => Ok(MultiLineString::new(
                (0..geometry.geometry_count())
                    .map(|i| coordinates(&unsafe { geometry.get_unowned_geometry(i) }))
                    .collect(),
            )?),
            _ => Err(Error::InvalidType {
                expected: format!("{:?}", VectorDataType::MultiPoint),
                found: format!("{:?}", OgrSource::ogr_geometry_type(geometry)),
            }),
        }
    }
}

impl TryFromOgrGeometry for MultiPolygon {
    fn try_from(geometry: Result<&gdal::vector::Geometry>) -> Result<Self> {
        fn coordinates(geometry: &gdal::vector::Geometry) -> Vec<Coordinate2D> {
            geometry
                .get_point_vec()
                .into_iter()
                .map(|(x, y, _z)| Coordinate2D::new(x, y))
                .collect()
        }
        fn rings(geometry: &gdal::vector::Geometry) -> Vec<Vec<Coordinate2D>> {
            let ring_count = geometry.geometry_count();
            (0..ring_count)
                .map(|i| coordinates(&unsafe { geometry.get_unowned_geometry(i) }))
                .collect()
        }

        let geometry = geometry?;

        match geometry.geometry_type() {
            OGRwkbGeometryType::wkbPolygon => Ok(MultiPolygon::new(vec![rings(geometry)])?),
            OGRwkbGeometryType::wkbMultiPolygon => Ok(MultiPolygon::new(
                (0..geometry.geometry_count())
                    .map(|i| rings(&unsafe { geometry.get_unowned_geometry(i) }))
                    .collect(),
            )?),
            _ => Err(Error::InvalidType {
                expected: format!("{:?}", VectorDataType::MultiPoint),
                found: format!("{:?}", OgrSource::ogr_geometry_type(geometry)),
            }),
        }
    }
}

impl TryFromOgrGeometry for NoGeometry {
    fn try_from(_geometry: Result<&gdal::vector::Geometry>) -> Result<Self> {
        Ok(NoGeometry)
    }
}

pub trait FeatureCollectionBuilderGeometryHandler<G>
where
    G: Geometry,
{
    fn push_generic_geometry(&mut self, geometry: G) -> Result<()>;
}

impl FeatureCollectionBuilderGeometryHandler<MultiPoint>
    for FeatureCollectionRowBuilder<MultiPoint>
{
    fn push_generic_geometry(&mut self, geometry: MultiPoint) -> Result<()> {
        self.push_geometry(geometry).map_err(Into::into)
    }
}

impl FeatureCollectionBuilderGeometryHandler<MultiLineString>
    for FeatureCollectionRowBuilder<MultiLineString>
{
    fn push_generic_geometry(&mut self, geometry: MultiLineString) -> Result<()> {
        self.push_geometry(geometry).map_err(Into::into)
    }
}

impl FeatureCollectionBuilderGeometryHandler<MultiPolygon>
    for FeatureCollectionRowBuilder<MultiPolygon>
{
    fn push_generic_geometry(&mut self, geometry: MultiPolygon) -> Result<()> {
        self.push_geometry(geometry).map_err(Into::into)
    }
}

impl FeatureCollectionBuilderGeometryHandler<NoGeometry>
    for FeatureCollectionRowBuilder<NoGeometry>
{
    fn push_generic_geometry(&mut self, _geometry: NoGeometry) -> Result<()> {
        Ok(()) // do nothing
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{MockExecutionContext, MockQueryContext, StaticMetaData};
    use crate::source::ogr_source::FormatSpecifics::Csv;
    use crate::test_data;
    use futures::TryStreamExt;
    use geoengine_datatypes::collections::{
        DataCollection, GeometryCollection, MultiPointCollection, MultiPolygonCollection,
    };
    use geoengine_datatypes::dataset::InternalDatasetId;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, FeatureData, SpatialResolution, TimeGranularity,
    };
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
    use geoengine_datatypes::util::Identifier;
    use serde_json::json;

    #[test]
    #[allow(clippy::too_many_lines)]
    fn specification_serde() {
        let spec = OgrSourceDataset {
            file_name: "foobar.csv".into(),
            layer_name: "foobar".to_string(),
            data_type: Some(VectorDataType::MultiPoint),
            time: OgrSourceDatasetTimeType::Start {
                start_field: "start".to_string(),
                start_format: OgrSourceTimeFormat::Custom {
                    custom_format: "YYYY-MM-DD".to_string(),
                },
                duration: OgrSourceDurationSpec::Value(TimeStep {
                    granularity: TimeGranularity::Seconds,
                    step: 42,
                }),
            },
            default_geometry: Some(TypedGeometry::MultiPoint(
                MultiPoint::new(vec![[1.0, 2.0].into()]).unwrap(),
            )),
            columns: Some(OgrSourceColumnSpec {
                format_specifics: Some(FormatSpecifics::Csv {
                    header: CsvHeader::Auto,
                }),
                x: "x".to_string(),
                y: Some("y".to_string()),
                float: vec!["num".to_string()],
                int: vec!["dec1".to_string(), "dec2".to_string()],
                text: vec!["text".to_string()],
                rename: None,
            }),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Ignore,
            sql_query: None,
            attribute_query: None,
        };

        let serialized_spec = serde_json::to_string(&spec).unwrap();

        assert_eq!(
            serialized_spec,
            json!({
                "fileName": "foobar.csv",
                "layerName": "foobar",
                "dataType": "MultiPoint",
                "time": {
                    "type": "start",
                    "startField": "start",
                    "startFormat": {
                        "format": "custom",
                        "customFormat": "YYYY-MM-DD"
                    },
                    "duration": {
                        "type": "value",
                        "granularity": "Seconds",
                        "step": 42
                    }
                },
                "defaultGeometry": {
                    "MultiPoint": {
                        "coordinates":[{"x":1.0,"y":2.0}]
                    }
                },
                "columns": {
                    "formatSpecifics": {
                        "csv": {
                            "header": "auto",
                        }
                    },
                    "x": "x",
                    "y": "y",
                    "int": ["dec1", "dec2"],
                    "float": ["num"],
                    "text": ["text"],
                    "rename": null
                },
                "forceOgrTimeFilter": false,
                "forceOgrSpatialFilter": false,
                "onError": "ignore",
                "sqlQuery": null,
                "attributeQuery": null
            })
            .to_string()
        );

        let deserialized_spec: OgrSourceDataset = serde_json::from_str(
            &json!({
                "fileName": "foobar.csv",
                "layerName": "foobar",
                "dataType": "MultiPoint",
                "time": {
                    "type": "start",
                    "startField": "start",
                    "startFormat": {
                        "format": "custom",
                        "customFormat": "YYYY-MM-DD"
                    },
                    "duration": {
                        "type": "value",
                        "granularity": "Seconds",
                        "step": 42
                    }
                },
                "defaultGeometry": {
                    "MultiPoint": {
                        "coordinates":[{"x":1.0,"y":2.0}]
                    }
                },
                "columns": {
                    "formatSpecifics": {
                        "csv": {
                            "header": "auto",
                        }
                    },
                    "x": "x",
                    "y": "y",
                    "int": ["dec1", "dec2"],
                    "float": ["num"],
                    "text": ["text"]
                },
                "forceOgrTimeFilter": false,
                "forceOgrSpatialFilter": false,
                "onError": "ignore",
                "provenance": {
                    "citation": "Foo Bar",
                    "license": "CC",
                    "uri": "foo:bar"
                }
            })
            .to_string(),
        )
        .unwrap();

        assert_eq!(deserialized_spec, spec);
    }

    #[tokio::test]
    async fn empty_geojson() -> Result<()> {
        let dataset_information = OgrSourceDataset {
            file_name: test_data!("vector/data/empty.json").into(),
            layer_name: "empty".to_string(),
            data_type: None,
            time: OgrSourceDatasetTimeType::None,
            default_geometry: None,
            columns: None,
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Ignore,
            sql_query: None,
            attribute_query: None,
        };

        let info = StaticMetaData {
            loading_info: dataset_information,
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: Default::default(),
            },
            phantom: Default::default(),
        };

        let query_processor = OgrSourceProcessor::<MultiPoint>::new(Box::new(info));

        let context = MockQueryContext::new(usize::MAX);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((0., 0.).into(), (1., 1.).into())?,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.)?,
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await?;

        assert_eq!(result.len(), 1);
        assert!(result[0].is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn error() -> Result<()> {
        let dataset_information = OgrSourceDataset {
            file_name: "".into(),
            layer_name: "".to_string(),
            data_type: None,
            time: OgrSourceDatasetTimeType::None,
            default_geometry: None,
            columns: None,
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Ignore,
            sql_query: None,
            attribute_query: None,
        };

        let info = StaticMetaData {
            loading_info: dataset_information,
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: Default::default(),
            },
            phantom: Default::default(),
        };

        let query_processor = OgrSourceProcessor::<MultiPoint>::new(Box::new(info));

        let context = MockQueryContext::new(usize::MAX);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((0., 0.).into(), (1., 1.).into())?,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.)?,
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<Result<MultiPointCollection>> = query.collect().await;

        assert_eq!(result.len(), 1);
        assert!(result[0].is_err());

        Ok(())
    }

    #[tokio::test]
    async fn on_error_ignore() -> Result<()> {
        let dataset_information = OgrSourceDataset {
            file_name: test_data!("vector/data/missing_geo.json").into(),
            layer_name: "missing_geo".to_string(),
            data_type: None,
            time: OgrSourceDatasetTimeType::None,
            default_geometry: None,
            columns: None,
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Ignore,
            sql_query: None,
            attribute_query: None,
        };
        let info = StaticMetaData {
            loading_info: dataset_information,
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: Default::default(),
            },
            phantom: Default::default(),
        };

        let query_processor = OgrSourceProcessor::<MultiPoint>::new(Box::new(info));

        let context = MockQueryContext::new(usize::MAX);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((0., 0.).into(), (5., 5.).into())?,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.)?,
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await?;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0],
            MultiPointCollection::from_data(
                MultiPoint::many(vec![vec![(0.0, 0.1)], vec![(1.0, 1.1), (2.0, 2.1)]])?,
                vec![Default::default(); 2],
                HashMap::new(),
            )?
        );

        Ok(())
    }

    #[tokio::test]
    async fn ne_10m_ports_bbox_filter() -> Result<()> {
        let dataset = DatasetId::Internal {
            dataset_id: InternalDatasetId::new(),
        };
        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            dataset.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: None,
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: Default::default(),
                },
                phantom: Default::default(),
            }),
        );

        let source = OgrSource {
            params: OgrSourceParameters {
                dataset,
                attribute_projection: None,
            },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await?;

        assert_eq!(
            source.result_descriptor().data_type,
            VectorDataType::MultiPoint
        );
        assert_eq!(
            source.result_descriptor().spatial_reference,
            SpatialReference::epsg_4326().into()
        );

        let query_processor = source.query_processor()?.multi_point().unwrap();

        let context = MockQueryContext::new(usize::MAX);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((1.85, 50.88).into(), (4.82, 52.95).into())?,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.)?,
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 10);

        let coordinates = MultiPoint::many(vec![
            (2.933_686_69, 51.23),
            (3.204_593_64_f64, 51.336_388_89),
            (4.651_413_428, 51.805_833_33),
            (4.11, 51.95),
            (4.386_160_188, 50.886_111_11),
            (3.767_373_38, 51.114_444_44),
            (4.293_757_362, 51.297_777_78),
            (1.850_176_678, 50.965_833_33),
            (2.170_906_949, 51.021_666_67),
            (4.292_873_969, 51.927_222_22),
        ])?;

        assert_eq!(
            result[0],
            MultiPointCollection::from_data(
                coordinates,
                vec![Default::default(); 10],
                HashMap::new(),
            )?
        );

        Ok(())
    }

    #[tokio::test]
    async fn ne_10m_ports_force_spatial_filter() -> Result<()> {
        let dataset = DatasetId::Internal {
            dataset_id: InternalDatasetId::new(),
        };
        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            dataset.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: None,
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: true,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: Default::default(),
                },
                phantom: Default::default(),
            }),
        );

        let source = OgrSource {
            params: OgrSourceParameters {
                dataset,
                attribute_projection: None,
            },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await?;

        assert_eq!(
            source.result_descriptor().data_type,
            VectorDataType::MultiPoint
        );
        assert_eq!(
            source.result_descriptor().spatial_reference,
            SpatialReference::epsg_4326().into()
        );

        let query_processor = source.query_processor()?.multi_point().unwrap();

        let context = MockQueryContext::new(usize::MAX);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((1.85, 50.88).into(), (4.82, 52.95).into())?,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.)?,
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 10);

        let coordinates = MultiPoint::many(vec![
            (2.933_686_69, 51.23),
            (3.204_593_64_f64, 51.336_388_89),
            (4.651_413_428, 51.805_833_33),
            (4.11, 51.95),
            (4.386_160_188, 50.886_111_11),
            (3.767_373_38, 51.114_444_44),
            (4.293_757_362, 51.297_777_78),
            (1.850_176_678, 50.965_833_33),
            (2.170_906_949, 51.021_666_67),
            (4.292_873_969, 51.927_222_22),
        ])?;

        assert_eq!(
            result[0],
            MultiPointCollection::from_data(
                coordinates,
                vec![Default::default(); 10],
                HashMap::new(),
            )?
        );

        Ok(())
    }

    #[tokio::test]
    async fn ne_10m_ports_fast_spatial_filter() -> Result<()> {
        let dataset = DatasetId::Internal {
            dataset_id: InternalDatasetId::new(),
        };
        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            dataset.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!(
                        "vector/data/ne_10m_ports/with_spatial_index/ne_10m_ports.gpkg"
                    )
                    .into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: None,
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: Default::default(),
                },
                phantom: Default::default(),
            }),
        );

        let source = OgrSource {
            params: OgrSourceParameters {
                dataset,
                attribute_projection: None,
            },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await?;

        assert_eq!(
            source.result_descriptor().data_type,
            VectorDataType::MultiPoint
        );
        assert_eq!(
            source.result_descriptor().spatial_reference,
            SpatialReference::epsg_4326().into()
        );

        let query_processor = source.query_processor()?.multi_point().unwrap();

        let context = MockQueryContext::new(usize::MAX);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((1.85, 50.88).into(), (4.82, 52.95).into())?,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.)?,
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 10);

        let coordinates = MultiPoint::many(vec![
            (1.850_176_678, 50.965_833_33),
            (2.170_906_949, 51.021_666_67),
            (2.933_686_69, 51.23),
            (3.204_593_64_f64, 51.336_388_89),
            (3.767_373_38, 51.114_444_44),
            (4.11, 51.95),
            (4.292_873_969, 51.927_222_22),
            (4.293_757_362, 51.297_777_78),
            (4.386_160_188, 50.886_111_11),
            (4.651_413_428, 51.805_833_33),
        ])?;

        assert_eq!(
            result[0],
            MultiPointCollection::from_data(
                coordinates,
                vec![Default::default(); 10],
                HashMap::new(),
            )?
        );

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn ne_10m_ports_columns() -> Result<()> {
        let id = DatasetId::Internal {
            dataset_id: InternalDatasetId::new(),
        };
        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            id.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: "".to_string(),
                        y: None,
                        int: vec!["scalerank".to_string()],
                        float: vec!["natlscale".to_string()],
                        text: vec![
                            "featurecla".to_string(),
                            "name".to_string(),
                            "website".to_string(),
                        ],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        ("natlscale".to_string(), FeatureDataType::Float),
                        ("scalerank".to_string(), FeatureDataType::Int),
                        ("featurecla".to_string(), FeatureDataType::Int),
                        ("name".to_string(), FeatureDataType::Text),
                        ("website".to_string(), FeatureDataType::Text),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                },
                phantom: Default::default(),
            }),
        );

        let source = OgrSource {
            params: OgrSourceParameters {
                dataset: id.clone(),
                attribute_projection: None,
            },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await?;

        assert_eq!(
            source.result_descriptor().data_type,
            VectorDataType::MultiPoint
        );
        assert_eq!(
            source.result_descriptor().spatial_reference,
            SpatialReference::epsg_4326().into()
        );

        let query_processor = source.query_processor()?.multi_point().unwrap();

        let context = MockQueryContext::new(usize::MAX);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((1.85, 50.88).into(), (4.82, 52.95).into())?,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.)?,
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 10);

        let coordinates = MultiPoint::many(vec![
            (2.933_686_69, 51.23),
            (3.204_593_64_f64, 51.336_388_89),
            (4.651_413_428, 51.805_833_33),
            (4.11, 51.95),
            (4.386_160_188, 50.886_111_11),
            (3.767_373_38, 51.114_444_44),
            (4.293_757_362, 51.297_777_78),
            (1.850_176_678, 50.965_833_33),
            (2.170_906_949, 51.021_666_67),
            (4.292_873_969, 51.927_222_22),
        ])?;

        let natlscale = FeatureData::NullableFloat(
            [5.0_f64, 5.0, 5.0, 10.0, 20.0, 20.0, 30.0, 30.0, 30.0, 30.0]
                .iter()
                .map(|v| Some(*v))
                .collect(),
        );

        let scalerank = FeatureData::NullableInt(
            [8, 8, 8, 7, 6, 6, 5, 5, 5, 5]
                .iter()
                .map(|v| Some(*v))
                .collect(),
        );

        let featurecla = FeatureData::NullableText(
            [
                "Port", "Port", "Port", "Port", "Port", "Port", "Port", "Port", "Port", "Port",
            ]
            .iter()
            .map(|&v| Some(v.to_string()))
            .collect(),
        );

        let website = FeatureData::NullableText(
            [
                "www.portofoostende.be",
                "www.zeebruggeport.be",
                "",
                "",
                "www.portdebruxelles.irisnet.be",
                "www.havengent.be",
                "www.portofantwerp.be",
                "www.calais-port.com",
                "www.portdedunkerque.fr",
                "www.portofrotterdam.com",
            ]
            .iter()
            .map(|&v| {
                if v.is_empty() {
                    None
                } else {
                    Some(v.to_string())
                }
            })
            .collect(),
        );

        let name = FeatureData::NullableText(
            [
                "Oostende (Ostend)",
                "Zeebrugge",
                "Dordrecht",
                "Europoort",
                "Brussel (Bruxelles)",
                "Gent (Ghent)",
                "Antwerpen",
                "Calais",
                "Dunkerque",
                "Rotterdam",
            ]
            .iter()
            .map(|&v| Some(v.to_string()))
            .collect(),
        );

        assert_eq!(
            result[0],
            MultiPointCollection::from_data(
                coordinates,
                vec![Default::default(); 10],
                [
                    ("natlscale".to_string(), natlscale),
                    ("scalerank".to_string(), scalerank),
                    ("featurecla".to_string(), featurecla),
                    ("website".to_string(), website),
                    ("name".to_string(), name),
                ]
                .iter()
                .cloned()
                .collect(),
            )?
        );

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn ne_10m_ports() -> Result<()> {
        let id = DatasetId::Internal {
            dataset_id: InternalDatasetId::new(),
        };
        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            id.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: None,
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: Default::default(),
                },
                phantom: Default::default(),
            }),
        );

        let source = OgrSource {
            params: OgrSourceParameters {
                dataset: id.clone(),
                attribute_projection: None,
            },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await?;

        assert_eq!(
            source.result_descriptor().data_type,
            VectorDataType::MultiPoint
        );
        assert_eq!(
            source.result_descriptor().spatial_reference,
            SpatialReference::epsg_4326().into()
        );

        let query_processor = source.query_processor()?.multi_point().unwrap();

        let context = MockQueryContext::new(usize::MAX);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new(
                        (-180.0, -90.0).into(),
                        (180.0, 90.0).into(),
                    )?,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.)?,
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 1081);

        let coordinates = MultiPoint::many(vec![
            (-69.923_557_13, 12.4375),
            (-58.951_413_43, -34.153_333_33),
            (-59.004_947, -34.098_888_89),
            (-62.100_883_39, -38.894_444_44),
            (-62.300_530_04, -38.783_055_56),
            (-62.259_893_99, -38.791_944_44),
            (-61.852_296_82, 17.122_777_78),
            (115.738_103_7, -32.0475),
            (151.209_540_6, -33.973_055_56),
            (151.284_216_7, -23.851_111_11),
            (2.933_686_69, 51.23),
            (3.204_593_64_f64, 51.336_388_89),
            (27.458_303_89, 42.47),
            (50.607_597_17, 26.198_611_11),
            (50.658_833_92, 26.158_611_11),
            (-64.673_027_09, 32.379_444_44),
            (-38.473_203_77, -3.7075),
            (-43.124_617_2, -22.879_166_67),
            (-48.635_453_47, -26.238_055_56),
            (-65.467_726_74, 47.034_444_44),
            (-53.956_890_46, 48.163_333_33),
            (-124.924_440_5, 49.670_833_33),
            (-123.434_393_4, 48.436_388_89),
            (-55.751_060_07, 47.100_277_78),
            (-60.239_340_4, 46.208_888_89),
            (-64.924_617_2, 47.796_388_89),
            (-71.052_650_18, 48.43),
            (-57.936_336_87, 48.960_555_56),
            (-65.752_296_82, 44.625_277_78),
            (-53.217_020_02, 47.690_277_78),
            (-62.704_416_96, 45.674_722_22),
            (-65.317_726_74, 43.749_722_22),
            (-73.118_080_09, 46.041_111_11),
            (-56.059_893_99, 49.499_722_22),
            (-52.692_343_93, 47.5625),
            (-73.153_356_89, -37.028_333_33),
            (-73.158_480_57, -37.098_055_56),
            (-73.102_296_82, -36.705_277_78),
            (-71.618_080_09, -33.5875),
            (121.441_813_9, 28.690_277_78),
            (119.585_983_5, 39.906_666_67),
            (114.26702, 22.574_166_67),
            (122.235_100_1, 40.695_833_33),
            (-83.074_087_16, 10.005_555_56),
            (-75.703_356_89, 20.919_166_67),
            (-79.469_316_84, 22.529_166_67),
            (-80.007_597_17, 22.940_277_78),
            (-77.239_517_08, 21.563_055_56),
            (33.940_047_11, 35.127_777_78),
            (8.707_243_816, 53.864_722_22),
            (9.835_806_832, 54.473_611_11),
            (8.489_517_079, 53.4875),
            (7.368_963_486, 53.096_388_89),
            (11.439_163_72, 53.906_388_89),
            (10.806_890_46, 55.298_888_89),
            (12.617_020_02, 56.034_444_44),
            (10.524_970_55, 55.466_388_89),
            (10.788_103_65, 55.306_944_44),
            (10.541_460_54, 57.437_777_78),
            (9.504_416_961, 55.251_111_11),
            (9.868_433_451, 55.854_166_67),
            (11.085_983_51, 55.669_444_44),
            (9.493_404_005, 55.492_777_78),
            (11.121_083_63, 54.832_222_22),
            (11.857_067_14, 54.770_833_33),
            (10.052_650_18, 56.460_277_78),
            (10.704_947, 54.939_722_22),
            (10.617_020_02, 55.058_611_11),
            (9.551_943_463, 55.705_555_56),
            (10.588_810_37, 57.718_611_11),
            (-70.700_530_04, 19.803_611_11),
            (-70.018_080_09, 18.420_277_78),
            (0.071_260_306, 35.934_444_44),
            (31.759_187_28, 31.466_666_67),
            (33.941_283_86, 26.742_222_22),
            (-5.400_353_357, 36.147_777_78),
            (-5.920_023_557, 43.578_611_11),
            (-8.250_883_392, 43.479_722_22),
            (-6.935_806_832, 37.192_777_78),
            (-1.918_786_808, 43.323_333_33),
            (-6.334_570_082, 36.619_444_44),
            (-6.258_833_922, 36.52),
            (26.941_813_9, 60.461_111_11),
            (24.616_666_67, 65.683_055_56),
            (-1.142_343_934, 44.659_166_67),
            (0.073_380_448, 49.365_833_33),
            (0.369_493_522, 49.761_944_44),
            (-1.223_910_483, 46.157_222_22),
            (-2.022_850_412, 48.644_444_44),
            (3.710_247_35, 43.404_722_22),
            (-4.756_713_781, 55.954_722_22),
            (-1.986_513_545, 50.714_444_44),
            (-5.053_533_569, 50.152_777_78),
            (-3.171_790_342, 55.9825),
            (-2.467_903_416, 56.704_444_44),
            (-2.984_746_761, 51.558_888_89),
            (-1.356_183_746, 54.906_944_44),
            (-1.157_067_138, 54.608_055_56),
            (41.652_826_86, 42.155),
            (23.656_537_1, 37.937_222_22),
            (23.633_686_69, 37.941_111_11),
            (23.643_050_65, 37.934_444_44),
            (-61.750_530_04, 12.047_222_22),
            (-52.271_967_02, 4.853_055_556),
            (14.540_753_83, 45.299_722_22),
            (13.634_923_44, 45.080_555_56),
            (15.891_637_22, 43.726_388_89),
            (-72.703_710_25, 19.448_333_33),
            (131.242_520_6, -0.876_944_444),
            (76.310_424_03, 9.491_944_444),
            (82.274_617_2, 16.977_777_78),
            (78.187_043_58, 8.758_333_333),
            (-8.471_260_306, 54.271_666_67),
            (13.938_280_33, 37.090_277_78),
            (8.307_950_53, 40.562_777_78),
            (14.160_070_67, 40.803_333_33),
            (16.287_750_29, 41.325_555_56),
            (17.956_007_07, 40.646_388_89),
            (17.123_380_45, 39.086_111_11),
            (17.975_677_27, 40.055_833_33),
            (8.026_914_016, 43.882_777_78),
            (9.409_363_958, 41.213_333_33),
            (9.850_353_357, 44.087_222_22),
            (12.435_453_47, 37.789_722_22),
            (15.559_187_28, 38.200_277_78),
            (16.587_750_29, 41.208_333_33),
            (13.552_650_18, 45.788_611_11),
            (17.300_353_36, 40.956_111_11),
            (12.902_473_5, 43.920_833_33),
            (8.392_167_256, 40.843_611_11),
            (12.250_353_36, 44.462_222_22),
            (15.287_573_62, 37.066_111_11),
            (17.188_987_04, 40.488_055_56),
            (14.442_873_97, 40.75),
            (129.950_353_4, 32.901_666_67),
            (135.383_333_3, 34.691_388_89),
            (137.108_480_6, 36.775_833_33),
            (133.006_183_7, 34.068_055_56),
            (132.235_276_8, 34.176_944_44),
            (135.188_457, 34.153_055_56),
            (136.606_537_1, 36.616_944_44),
            (139.743_757_4, 35.506_388_89),
            (130.873_557_1, 33.908_333_33),
            (135.385_276_8, 35.481_666_67),
            (136.553_356_9, 34.608_055_56),
            (135.193_404, 35.543_333_33),
            (140.954_063_6, 42.335_833_33),
            (139.124_793_9, 37.955),
            (131.869_316_8, 33.251_388_89),
            (129.707_773_9, 33.156_944_44),
            (138.509_894, 35.014_444_44),
            (135.133_333_3, 34.116_388_89),
            (135.127_090_7, 34.223_055_56),
            (139.654_417, 35.304_444_44),
            (80.224_970_55, 6.033_333_333),
            (21.537_043_58, 57.403_055_56),
            (-92.652_120_14, 18.530_277_78),
            (-94.534_570_08, 17.975_833_33),
            (-94.400_530_04, 18.1275),
            (-112.258_480_6, 27.339_166_67),
            (-89.670_553_59, 21.313_611_11),
            (-97.370_553_59, 20.940_555_56),
            (103.459_187_3, 4.249_444_444),
            (4.783_686_69, 52.958_055_56),
            (4.651_413_428, 51.805_833_33),
            (6.955_477_032, 53.320_555_56),
            (8.757_420_495, 58.454_166_67),
            (10.224_970_55, 59.736_666_67),
            (11.375_323_91, 59.118_055_56),
            (16.541_637_22, 68.789_166_67),
            (10.025_500_59, 59.045_277_78),
            (10.652_826_86, 59.432_222_22),
            (10.224_970_55, 59.123_611_11),
            (10.404_947, 59.265),
            (172.710_247_3, -43.606_111_11),
            (59.420_553_59, 22.656_666_67),
            (56.737_926_97, 24.378_333_33),
            (-79.568_786_81, 8.955_555_556),
            (-79.904_770_32_f64, 9.350_277_778),
            (-82.434_570_08, 8.365),
            (-77.141_813_9, -12.045_277_78),
            (-72.104_240_28, -16.997_777_78),
            (14.268_610_13, 53.905),
            (-8.692_873_969, 41.185),
            (-8.821_436_985, 38.489_722_22),
            (-8.719_670_2, 40.643_611_11),
            (-8.823_027_091, 41.686_388_89),
            (28.083_863_37, 45.441_666_67),
            (132.907_597_2, 42.804_722_22),
            (32.404_947, 67.135),
            (142.040_047_1, 47.050_555_56),
            (142.758_480_6, 46.626_666_67),
            (29.767_373_38, 59.986_944_44),
            (132.886_160_2, 42.8),
            (37.804_416_96, 44.715),
            (39.066_666_67, 44.091_944_44),
            (49.670_906_95, 27.028_611_11),
            (49.673_027_09, 27.087_777_78),
            (48.522_497_06, 28.415),
            (12.476_383_98, 56.889_444_44),
            (14.305_477_03, 55.925_555_56),
            (17.938_810_37, 62.640_833_33),
            (12.541_460_54, 56.198_611_11),
            (14.858_303_89, 56.1625),
            (15.585_983_51, 56.16),
            (17.958_127_21, 58.913_333_33),
            (16.470_730_27, 57.265),
            (17.107_950_53, 58.661_666_67),
            (21.470_376_91, 65.308_888_89),
            (17.659_187_28, 59.169_722_22),
            (13.151_590_11, 55.368_055_56),
            (11.904_770_32_f64, 58.348_055_56),
            (13.820_730_27, 55.421_388_89),
            (100.584_923_4, 13.695_833_33),
            (100.890_930_5, 13.073_888_89),
            (100.918_610_1, 13.171_388_89),
            (-61.489_517_08, 10.388_333_33),
            (34.640_577_15, 36.796_944_44),
            (36.773_203_77, 46.751_388_89),
            (36.485_630_15, 45.355),
            (-122.359_010_6, 37.913_888_89),
            (-75.519_846_88, 39.716_666_67),
            (-70.968_963_49, 42.241_388_89),
            (-159.353_003_5, 21.953_888_89),
            (-121.888_987, 36.605_555_56),
            (-77.952_650_18, 34.191_944_44),
            (-75.122_673_73, 39.903_611_11),
            (-74.102_473_5, 40.6625),
            (-76.555_653_71, 39.2325),
            (-81.559_187_28, 30.380_833_33),
            (-76.426_207_3, 36.980_555_56),
            (-93.957_067_14, 29.843_055_56),
            (-76.288_280_33, 36.816_666_67),
            (-77.952_650_18, 34.191_944_44),
            (-93.957_067_14, 29.843_055_56),
            (-77.037_220_26, 38.801_111_11),
            (-76.426_207_3, 36.980_555_56),
            (-122.608_657_2, 47.651_388_89),
            (-122.703_886_9, 45.634_722_22),
            (-64.590_400_47, 10.249_166_67),
            (121.725_836_966_943_46, 25.220_760_141_757_456),
            (-13.623_954_376_704_255, 28.948_964_844_057_482),
            (-64.928_415_300_364_52, 18.332_123_222_010_704),
            (-62.750_002_499_299_285, 17.278_827_021_185_45),
            (-90.056_371_243_811_14, 29.934_199_968_138_62),
            (-78.727_663_615_817_95, 26.538_410_634_413_9),
            (-77.56, 44.1),
            (-81.73, 43.75),
            (-79.91, 44.75),
            (-86.48, 42.11),
            (-87.78, 42.73),
            (-87.6, 45.1),
            (-87.05, 45.76),
            (-121.5, 38.58),
            (1.73, 52.61),
            (-2.96, 56.46),
            (-3.83, 57.83),
            (-3.08, 58.43),
            (-5.46, 56.41),
            (-4.46, 54.15),
            (-7.31, 55.0),
            (-3.4, 54.86),
            (13.093_757_36, -7.833_055_556),
            (12.190_223_79, -5.55),
            (12.320_376_91, -6.121_388_889),
            (19.453_003_53, 41.307_777_78),
            (-68.918_256_77, 12.119_166_67),
            (56.356_537_1, 25.178_333_33),
            (-67.508_480_57, -46.433_055_56),
            (-65.904_063_6, -47.755_555_56),
            (-69.218_256_77, -51.611_666_67),
            (-67.702_473_5, -53.794_444_44),
            (-67.718_963_49, -49.299_722_22),
            (153.141_460_5, -30.3075),
            (121.893_404, -33.871_944_44),
            (137.756_713_8, -32.488_611_11),
            (148.25053, -20.020_833_33),
            (152.388_457, -24.767_222_22),
            (139.737_927, -17.553_611_11),
            (113.642_874, -24.895),
            (145.243_757_4, -15.459_722_22),
            (123.605_123_7, -17.292_222_22),
            (149.905_300_4, -37.071_944_44),
            (146.820_200_2, -41.109_722_22),
            (114.600_706_7, -28.772_777_78),
            (140.833_333_3, -17.489_444_44),
            (137.639_517_1, -35.655),
            (152.906_007_1, -25.295),
            (145.320_553_6, -42.158_333_33),
            (89.585_983_51, 22.486_944_44),
            (-37.039_517_08, -10.925_277_78),
            (-38.521_436_98, -3.7175),
            (-51.042_697_29, 0.032_222_222),
            (-48.654_240_28, -26.898_888_89),
            (-52.323_733_8, -31.781_944_44),
            (-125.238_633_7, 50.031_944_44),
            (-60.342_343_93, 53.384_444_44),
            (-132.141_107_2, 54.006_666_67),
            (-67.571_260_31, 48.841_944_44),
            (-84.543_050_65, 73.068_611_11),
            (-65.834_923_44, 48.138_611_11),
            (-127.483_686_7, 50.721_111_11),
            (-61.355_653_71, 45.61),
            (-123.166_666_7, 49.683_055_56),
            (-58.540_753_83, 48.535_277_78),
            (-129.989_517_1, 55.928_611_11),
            (-126.654_063_6, 49.9175),
            (-131.819_670_2, 53.255_277_78),
            (-132.985_806_8, 69.431_388_89),
            (-73.825_677_27, -41.868_611_11),
            (-73.766_843_35, -42.482_777_78),
            (-70.634_393_4, -26.350_555_56),
            (-72.509_363_96, -51.731_111_11),
            (-70.484_570_08, -25.397_222_22),
            (113.671_083_6, 22.640_277_78),
            (120.221_260_3, 31.931_944_44),
            (119.408_480_6, 34.745_277_78),
            (109.50053, 18.239_722_22),
            (110.406_360_4, 21.169_722_22),
            (122.102_650_2, 30.003_333_33),
            (-6.617_020_024, 4.743_055_556),
            (-75.525_677_27, 10.403_333_33),
            (-75.585_276_8, 9.520_277_778),
            (-78.759_540_64_f64, 1.813_055_556),
            (-76.733_863_37, 8.087_777_778),
            (-74.502_296_82, 20.353_055_56),
            (-75.550_883_39, 20.713_333_33),
            (-82.793_227_33, 21.903_333_33),
            (7.891_283_863, 54.1775),
            (10.868_256_77, 53.952_222_22),
            (8.692_167_256, 56.952_777_78),
            (-80.720_906_95, -0.935_555_556),
            (-79.642_697_29, 0.989_444_444),
            (-13.858_833_92, 28.741_388_89),
            (-5.310_424_028, 35.890_277_78),
            (-2.922_850_412, 35.28),
            (-17.105_123_67, 28.088_055_56),
            (-17.766_666_67, 28.675_833_33),
            (24.474_617_2, 58.381_111_11),
            (27.689_870_44, 62.888_333_33),
            (29.777_090_69, 62.604_444_44),
            (27.874_793_88, 62.3125),
            (179.309_364, -16.817_777_78),
            (-1.620_376_914, 49.646_388_89),
            (-0.326_383_981, 49.190_555_56),
            (8.755_300_353, 42.565_833_33),
            (8.900_353_357, 41.676_944_44),
            (-4.108_127_208, 47.989_444_44),
            (-2.756_360_424, 47.643_888_89),
            (138.119_493_5, 9.513_333_333),
            (-2.707_773_852, 51.498_888_89),
            (-0.285_100_118, 53.743_611_11),
            (-1.590_930_506, 54.965),
            (-1.440_930_506, 54.994_444_44),
            (0.685_806_832, 51.434_722_22),
            (0.007_420_495, 5.631_944_444),
            (25.889_163_72, 40.841_388_89),
            (27.289_870_44, 36.894_444_44),
            (25.150_706_71, 37.087_777_78),
            (26.966_666_67, 37.754_444_44),
            (-45.238_103_65, 60.139_722_22),
            (-69.235_100_12, 77.466_944_44),
            (-57.521_790_34, 6.237_777_778),
            (-86.756_890_46, 15.793_888_89),
            (-87.455_830_39, 15.783_888_89),
            (14.457_597_17, 44.538_333_33),
            (-72.535_983_51, 18.231_666_67),
            (-74.107_067_14, 18.643_611_11),
            (131.020_376_9, -1.3075),
            (97.142_873_97, 5.166_666_667),
            (117.654_947, 4.145_555_556),
            (105.307_067_1, -5.460_277_778),
            (106.890_400_5, -6.101_388_889),
            (74.807_420_49, 12.926_944_44),
            (69.587_396_94, 21.639_444_44),
            (73.271_790_34, 17.001_944_44),
            (-9.456_183_746, 51.680_555_56),
            (-6.456_183_746, 52.341_111_11),
            (48.185_453_47, 30.4325),
            (-14.286_513_55, 64.660_555_56),
            (-14.008_833_92, 64.929_722_22),
            (-15.941_460_54, 66.456_388_89),
            (11.937_926_97, 36.833_333_33),
            (15.090_930_51, 37.495_833_33),
            (12.235_453_47, 41.755_833_33),
            (12.602_473_5, 35.499_166_67),
            (15.925_323_91, 41.624_166_67),
            (-77.106_713_78, 18.410_277_78),
            (-76.451_766_78, 18.181_111_11),
            (130.985_630_2, 30.731_666_67),
            (140.039_870_4, 39.762_222_22),
            (141.523_557_1, 40.545_833_33),
            (124.151_236_7, 24.335_555_56),
            (132.693_404, 33.855_277_78),
            (139.803_533_6, 38.939_444_44),
            (134.053_356_9, 34.355_277_78),
            (141.689_163_7, 45.410_555_56),
            (134.187_927, 35.540_277_78),
            (40.901_590_11, -2.268_055_556),
            (126.588_103_7, 35.959_722_22),
            (129.402_826_9, 36.019_722_22),
            (129.188_280_3, 37.433_611_11),
            (128.592_343_9, 38.206_944_44),
            (129.385_630_2, 35.487_777_78),
            (35.819_140_16, 34.455_277_78),
            (-10.052_650_18, 5.858_055_556),
            (13.193_580_68, 32.9075),
            (-9.634_923_439, 30.423_888_89),
            (-3.919_846_879, 35.247_777_78),
            (-15.933_863_37, 23.682_222_22),
            (50.276_207_3, -14.9),
            (48.018_786_81, -22.1425),
            (48.326_207_3, -21.249_166_67),
            (44.271_967_02, -20.288_611_11),
            (49.421_967_02, -18.157_222_22),
            (-111.336_866_9, 26.014_722_22),
            (-90.590_400_47, 19.8125),
            (-86.957_420_49, 20.495),
            (-110.310_424, 24.169_444_44),
            (-87.067_726_74, 20.624_444_44),
            (-113.542_343_9, 31.306_111_11),
            (-105.240_930_5, 20.659_166_67),
            (-94.418_963_49, 18.129_722_22),
            (94.723_380_45, 16.779_722_22),
            (40.667_196_7, -14.541_388_89),
            (40.484_923_44, -12.966_666_67),
            (36.877_090_69, -17.881_666_67),
            (57.489_870_44, -20.148_055_56),
            (102.123_380_4, 2.248_888_889),
            (101.788_633_7, 2.536_111_111),
            (14.500_530_04, -22.940_555_56),
            (8.316_843_345, 4.986_388_889),
            (6.586_866_902, 53.218_333_33),
            (23.237_043_58, 69.967_777_78),
            (16.123_733_8, 69.324_166_67),
            (12.201_943_46, 65.4725),
            (6.789_870_436, 58.088_611_11),
            (5.002_826_855, 61.597_777_78),
            (7.986_336_867, 58.141_944_44),
            (11.490_047_11, 64.464_166_67),
            (11.226_914_02, 64.860_555_56),
            (12.618_786_81, 66.021_666_67),
            (14.909_893_99, 68.568_333_33),
            (5.501_413_428, 59.78),
            (174.004_417, -41.285_555_56),
            (170.519_316_8, -45.875_833_33),
            (54.003_180_21, 16.941_666_67),
            (62.338_633_69, 25.112_222_22),
            (-82.237_573_62, 9.34),
            (-78.151_060_07, 8.413_611_111),
            (-78.607_773_85, -9.076_666_667),
            (-71.343_757_36, -17.646_944_44),
            (-72.010_247_35, -17.031_111_11),
            (-76.221_967_02, -13.733_055_56),
            (123.308_127_2, 9.311_944_444),
            (125.154_240_3, 6.086_111_111),
            (154.671_967, -5.431_944_444),
            (143.206_537_1, -9.068_333_333),
            (146.984_746_8, -6.741_111_111),
            (145.800_353_4, -5.213_333_333),
            (18.668_786_81, 54.393_333_33),
            (18.538_457_01, 54.533_333_33),
            (-67.157_067_14, 18.429_444_44),
            (-66.700_353_36, 18.481_388_89),
            (-67.156_890_46, 18.213_888_89),
            (-7.926_030_624, 37.010_555_56),
            (-151.75, -16.516_666_67),
            (28.816_843_35, 45.191_111_11),
            (61.559_187_28, 69.756_111_11),
            (177.538_633_7, 64.744_166_67),
            (170.277_090_7, 69.701_666_67),
            (-16.071_967_02, 14.129_444_44),
            (-16.268_080_09, 12.589_722_22),
            (15.623_733_8, 78.226_111_11),
            (156.838_810_4, -8.101_388_889),
            (157.726_030_6, -8.499_722_222),
            (42.541_107_18, -0.374_166_667),
            (13.520_200_24, 59.375_277_78),
            (13.159_363_96, 58.509_166_67),
            (21.251_766_78, 64.678_333_33),
            (20.267_726_74, 63.816_666_67),
            (-72.256_007_07, 21.762_777_78),
            (100.569_670_2, 7.228_611_111),
            (27.957_243_82, 40.354_166_67),
            (36.340_400_47, 41.299_722_22),
            (35.142_873_97, 42.023_333_33),
            (39.736_513_55, 41.005_555_56),
            (33.541_460_54, 44.618_888_89),
            (-135.322_320_4, 59.450_833_33),
            (-146.357_067_1, 61.103_611_11),
            (-84.986_690_22, 29.7275),
            (-76.474_263_84, 38.965),
            (-76.071_083_63, 38.574_166_67),
            (-122.783_686_7, 45.605_833_33),
            (-122.637_750_3, 47.5625),
            (-69.102_120_14, 44.103_055_56),
            (-122.001_413_4, 36.966_388_89),
            (-76.239_517_08, 36.7225),
            (-64.187_750_29, 10.474_166_67),
            (106.675_147_2, 20.867_222_22),
            (19.910_572_060_487_087, 39.621_950_140_387_334),
            (30.167_657_186_346_13, 31.334_923_292_880_035),
            (-5.824_954_241_841_614, 35.796_113_535_694_27),
            (-79.85, 43.25),
            (-83.03, 42.31),
            (-83.11, 42.28),
            (-80.93, 44.58),
            (-84.35, 46.51),
            (-83.93, 43.43),
            (-84.46, 45.65),
            (-84.35, 46.5),
            (-86.26, 43.23),
            (-87.33, 41.61),
            (-87.81, 42.36),
            (-88.56, 47.11),
            (-132.38, 56.46),
            (-132.95, 56.81),
            (24.56, 65.73),
            (21.58, 63.1),
            (22.23, 60.45),
            (21.01, 56.51),
            (4.11, 51.95),
            (-6.36, 58.18),
            (-9.16, 38.7),
            (-6.33, 36.8),
            (1.45, 38.9),
            (39.71, 47.16),
            (34.76, 32.06),
            (10.25, 36.8),
            (5.08, 36.75),
            (3.06, 36.76),
            (-16.51, 16.01),
            (3.3, 6.41),
            (40.73, -15.03),
            (42.75, 13.0),
            (39.46, 15.61),
            (34.95, 29.55),
            (47.96, 29.38),
            (72.83, 18.83),
            (72.93, 18.95),
            (76.23, 9.96),
            (80.3, 13.1),
            (88.31, 22.55),
            (98.4, 7.83),
            (100.35, 5.41),
            (98.68, 3.78),
            (110.41, -6.95),
            (123.58, -10.16),
            (110.35, 1.56),
            (113.96, 4.38),
            (116.06, 5.98),
            (118.11, 5.83),
            (119.4, -5.13),
            (125.61, 7.06),
            (118.75, 32.08),
            (114.28, 30.58),
            (112.98, 28.2),
            (135.16, 48.5),
            (158.65, 53.05),
            (12.135_453_47, -15.193_611_11),
            (-62.270_906_95, -38.791_388_89),
            (-57.890_223_79, -34.855_277_78),
            (-68.300_883_39, -54.809_444_44),
            (-65.033_333_33, -42.736_111_11),
            (-170.687_927, -14.274_166_67),
            (149.223_733_8, -21.108_333_33),
            (135.869_493_5, -34.718_333_33),
            (138.007_243_8, -33.176_944_44),
            (117.886_690_2, -35.031_666_67),
            (115.470_906_9, -20.725_833_33),
            (115.651_236_7, -33.315_277_78),
            (145.908_127_2, -41.052_777_78),
            (144.387_396_9, -38.126_666_67),
            (151.251_766_8, -23.83),
            (151.769_140_2, -32.9075),
            (150.893_580_7, -34.4625),
            (141.609_364, -38.349_166_67),
            (142.218_963_5, -10.585_555_56),
            (146.824_793_9, -19.250_833_33),
            (141.866_666_7, -12.67),
            (137.590_753_8, -33.038_333_33),
            (128.100_883_4, -15.451_388_89),
            (4.386_160_188, 50.886_111_11),
            (3.767_373_38, 51.114_444_44),
            (-39.025_677_27, -14.780_277_78),
            (-35.722_320_38, -9.678_888_889),
            (-35.2, -5.783_055_556),
            (-52.076_207_3, -32.056_111_11),
            (-40.335_100_12, -20.323_333_33),
            (-127.688_810_4, 52.350_833_33),
            (-53.986_866_9, 47.293_055_56),
            (-65.65, 47.616_666_67),
            (-64.437_220_26, 48.824_444_44),
            (-124.819_316_8, 49.23),
            (-124.524_793_9, 49.835),
            (-66.383_863_37, 50.206_388_89),
            (-60.203_003_53, 46.141_944_44),
            (-72.538_987_04, 46.335_833_33),
            (-66.120_906_95, 43.835_555_56),
            (-68.508_127_21, 48.477_777_78),
            (-69.568_080_09, 47.846_666_67),
            (-55.577_090_69, 51.366_666_67),
            (-63.787_043_58, 46.388_611_11),
            (-70.321_967_02, -18.473_055_56),
            (-70.205_653_71, -22.086_111_11),
            (109.070_553_6, 21.458_611_11),
            (113.001_943_5, 22.5025),
            (120.884_746_8, 27.998_333_33),
            (121.388_987, 37.571_111_11),
            (113.584_216_7, 22.239_722_22),
            (-74.217_020_02, 11.251_944_44),
            (-77.053_356_89, 3.883_055_556),
            (-23.503_180_21, 14.944_722_22),
            (-83.166_666_67, 8.637_777_778),
            (-80.453_710_25, 22.14),
            (-77.133_510_01, 20.3375),
            (-81.537_043_58, 23.051_388_89),
            (-75.856_713_78, 20.001_666_67),
            (7.192_873_969, 53.346_111_11),
            (9.436_866_902, 54.803_333_33),
            (8.122_143_698, 53.53),
            (8.433_863_369, 55.465_555_56),
            (10.386_866_9, 55.4175),
            (9.776_737_338, 54.906_944_44),
            (14.687_750_29, 55.096_111_11),
            (-71.086_513_55, 18.212_222_22),
            (-71.654_947, 17.925_277_78),
            (-0.636_690_224, 35.7125),
            (-8.387_926_973, 43.363_055_56),
            (-2.471_260_306, 36.832_222_22),
            (-5.688_633_687, 43.558_611_11),
            (-1.986_866_902, 43.322_777_78),
            (-3.804_946_996, 43.442_222_22),
            (1.220_200_236, 41.096_111_11),
            (25.454_593_64_f64, 65.015_833_33),
            (-1.493_757_362, 43.519_722_22),
            (1.575_500_589, 50.723_611_11),
            (-4.471_436_985, 48.380_555_56),
            (7.009_187_279, 43.548_055_56),
            (-1.617_550_059, 49.652_222_22),
            (1.085_983_51, 49.926_388_89),
            (-1.601_060_071, 48.834_722_22),
            (-1.157_773_852, 46.146_666_67),
            (-3.352_120_141, 47.734_166_67),
            (-3.834_923_439, 48.585_277_78),
            (-0.953_003_534, 45.949_166_67),
            (5.904_770_318, 43.110_555_56),
            (9.500_883_392, 0.288_333_333),
            (-2.074_617_197, 57.142_222_22),
            (-3.007_243_816, 53.436_388_89),
            (-4.154_063_604, 50.364_722_22),
            (-1.1, 50.8075),
            (41.650_530_04, 41.648_888_89),
            (24.401_943_46, 40.931_388_89),
            (26.137_750_29, 38.377_222_22),
            (22.108_127_21, 37.022_777_78),
            (21.734_923_44, 38.254_444_44),
            (22.936_690_22, 39.352_777_78),
            (-52.119_140_16, 70.674_722_22),
            (-88.604_063_6, 15.728_333_33),
            (13.834_746_76, 44.871_666_67),
            (14.422_320_38, 45.3275),
            (16.425_500_59, 43.515),
            (114.555_300_4, -3.321_944_444),
            (107.627_090_7, -2.75),
            (92.723_910_48, 11.672_222_22),
            (70.222_320_38, 23.013_611_11),
            (79.822_320_38, 11.914_166_67),
            (-8.424_440_518, 51.901_111_11),
            (-9.043_757_362, 53.27),
            (50.836_866_9, 28.984_722_22),
            (13.492_343_93, 43.618_333_33),
            (16.855_300_35, 41.136_944_44),
            (9.107_597_173, 39.204_166_67),
            (9.517_903_416, 40.922_777_78),
            (14.223_733_8, 42.467_777_78),
            (15.643_757_36, 38.124_444_44),
            (12.503_886_93, 38.013_611_11),
            (140.742_874, 40.831_388_89),
            (140.709_010_6, 41.787_777_78),
            (130.568_786_8, 31.5925),
            (133.557_067_1, 33.524_166_67),
            (144.352_296_8, 42.988_611_11),
            (133.259_540_6, 33.970_833_33),
            (131.676_030_6, 33.272_222_22),
            (130.901_060_1, 33.934_444_44),
            (141.653_356_9, 42.6375),
            (137.222_673_7, 36.757_222_22),
            (131.237_750_3, 33.938_333_33),
            (40.123_027_09, -3.213_611_111),
            (81.203_886_93, 8.558_333_333),
            (21.134_216_73, 55.687_222_22),
            (-9.242_697_291, 32.3075),
            (-116.622_850_4, 31.850_833_33),
            (-110.868_786_8, 27.919_166_67),
            (-104.300_883_4, 19.070_555_56),
            (-95.201_943_46, 16.157_222_22),
            (145.734_923_4, 15.225_833_33),
            (-17.041_813_9, 20.9075),
            (15.153_710_25, -26.637_777_78),
            (-83.756_890_46, 12.011_944_44),
            (6.171_613_663, 62.4725),
            (29.717_726_74, 70.634_166_67),
            (14.372_673_73, 67.288_888_89),
            (23.671_083_63, 70.667_222_22),
            (5.255_653_71, 59.412_222_22),
            (30.055_653_71, 69.728_333_33),
            (7.733_333_333, 63.115),
            (14.123_733_8, 66.315),
            (7.157_420_495, 62.736_388_89),
            (13.186_866_9, 65.851_944_44),
            (17.418_610_13, 68.438_611_11),
            (5.737_220_259, 58.978_888_89),
            (29.737_573_62, 70.071_111_11),
            (31.103_886_93, 70.373_333_33),
            (174.037_750_3, -39.057_222_22),
            (178.022_497_1, -38.6725),
            (171.20053, -42.444_722_22),
            (176.918_080_1, -39.475),
            (176.174_087_2, -37.659_444_44),
            (174.989_517_1, -39.944_166_67),
            (171.591_283_9, -41.750_277_78),
            (174.342_697_3, -35.751_111_11),
            (171.254_240_3, -44.389_166_67),
            (-81.274_617_2, -4.573_055_556),
            (123.619_846_9, 12.368_055_56),
            (120.274_793_9, 14.808_333_33),
            (150.451_060_1, -10.312_777_78),
            (150.786_690_2, -2.584_722_222),
            (155.626_914, -6.215_277_778),
            (152.185_100_1, -4.241_111_111),
            (141.292_343_9, -2.683_611_111),
            (-66.610_247_35, 17.968_055_56),
            (-8.867_020_024, 37.942_777_78),
            (-28.621_967_02, 38.53),
            (40.556_360_42, 64.5425),
            (33.041_460_54, 68.9725),
            (128.872_850_4, 71.643_055_56),
            (42.535_276_8, 16.890_277_78),
            (45.0, 10.440_833_33),
            (-57.001_766_78, 5.951_666_667),
            (13.001_590_11, 55.625_555_56),
            (17.192_697_29, 60.684_444_44),
            (12.85, 56.655),
            (12.687_573_62, 56.033_333_33),
            (17.118_610_13, 61.721_666_67),
            (16.369_140_16, 56.659_166_67),
            (18.725_853_95, 63.276_111_11),
            (15.3, 56.174_722_22),
            (17.089_340_4, 61.308_333_33),
            (16.651_943_46, 57.755_833_33),
            (18.274_617_2, 57.635_555_56),
            (27.507_950_53, 40.9675),
            (39.103_180_21, -5.065_555_556),
            (32.607_420_49, 46.618_611_11),
            (-57.842_167_26, -34.471_666_67),
            (-145.756_360_4, 60.55),
            (-131.671_260_3, 55.346_944_44),
            (-68.769_846_88, 44.791_944_44),
            (-121.323_027_1, 37.950_555_56),
            (-73.176_383_98, 41.1725),
            (-72.909_540_64_f64, 41.286_388_89),
            (-80.108_657_24, 26.084_444_44),
            (-81.122_497_06, 32.110_833_33),
            (-70.239_517_08, 43.653_333_33),
            (-89.088_457_01, 30.358_611_11),
            (-70.768_610_13, 43.0875),
            (-74.137_220_26, 40.7),
            (-123.826_207_3, 46.189_444_44),
            (-80.108_657_24, 26.084_444_44),
            (-97.404_593_64_f64, 27.812_777_78),
            (-88.537_220_26, 30.351_388_89),
            (-122.404_240_3, 47.267_222_22),
            (-94.084_216_73, 30.077_777_78),
            (-122.900_706_7, 47.053_333_33),
            (-123.435_100_1, 48.130_833_33),
            (-122.753_180_2, 48.114_444_44),
            (-67.993_050_65, 10.477_222_22),
            (49.143_757_36, 14.522_222_22),
            (27.904_770_32_f64, -33.025_277_78),
            (21.418_256_77, -34.375_277_78),
            (32.056_183_75, -28.804_722_22),
            (-80.1, 42.15),
            (-83.86, 43.6),
            (-87.88, 43.03),
            (-124.21, 43.36),
            (-135.33, 57.05),
            (-152.4, 57.78),
            (13.569_316_84, -12.336_944_44),
            (54.372_673_73, 24.525),
            (-67.459_893_99, -45.855_833_33),
            (-57.533_333_33, -38.042_777_78),
            (145.775_500_6, -16.929_722_22),
            (122.209_187_3, -18.0025),
            (146.367_550_1, -41.167_777_78),
            (147.335_806_8, -42.880_277_78),
            (4.293_757_362, 51.297_777_78),
            (27.888_633_69, 43.194_444_44),
            (-88.201_943_46, 17.479_722_22),
            (-64.776_383_98, 32.291_111_11),
            (-48.484_746_76, -1.451_666_667),
            (-48.519_140_16, -25.500_833_33),
            (-51.224_087_16, -30.0175),
            (-34.868_963_49, -8.053_611_111),
            (-38.504_770_32_f64, -12.958_611_11),
            (-46.300_530_04, -23.968_888_89),
            (-63.118_963_49, 46.231_666_67),
            (-63.574_440_52, 44.656_944_44),
            (-123.926_384, 49.169_444_44),
            (-71.209_010_6, 46.805_833_33),
            (-123.406_537_1, 48.431_111_11),
            (-130.337_043_6, 54.309_722_22),
            (-66.059_893_99, 45.267_777_78),
            (-70.403_710_25, -23.650_277_78),
            (-70.151_766_78, -20.202_777_78),
            (-72.954_416_96, -41.483_611_11),
            (110.274_970_6, 20.027_777_78),
            (121.553_356_9, 29.866_666_67),
            (118.020_023_6, 24.45),
            (-3.966_666_667, 5.233_055_556),
            (9.685_630_153, 4.055),
            (11.826_914_02, -4.784_444_444),
            (-74.756_183_75, 10.967_222_22),
            (43.243_227_33, -11.701_388_89),
            (-82.756_007_07, 23.006_388_89),
            (33.639_340_4, 34.924_166_67),
            (33.017_726_74, 34.65),
            (8.751_060_071, 53.0975),
            (9.958_480_565, 53.524_722_22),
            (43.135_630_15, 11.601_944_44),
            (10.005_300_35, 57.061_111_11),
            (9.740_577_15, 55.558_055_56),
            (-0.488_280_33, 38.335_277_78),
            (-3.026_914_016, 43.3425),
            (-0.318_433_451, 39.444_166_67),
            (-60.070_730_27, -51.955_555_56),
            (8.738_810_365, 41.92),
            (9.451_943_463, 42.7),
            (-0.553_180_212, 44.865_277_78),
            (1.850_176_678, 50.965_833_33),
            (2.170_906_949, 51.021_666_67),
            (0.235_100_118, 49.422_222_22),
            (9.433_686_69, 0.400_833_333),
            (8.785_276_796, -0.711_944_444),
            (-5.891_107_185, 54.620_555_56),
            (1.322_143_698, 51.120_833_33),
            (-4.234_746_761, 57.486_666_67),
            (-1.738_457_008, 4.884_166_667),
            (-61.727_090_69, 15.996_388_89),
            (-61.538_457_01, 16.233_888_89),
            (-16.570_376_91, 13.444_444_44),
            (9.738_103_651, 1.824_166_667),
            (25.141_107_18, 35.345_555_56),
            (22.917_726_74, 40.635),
            (-52.336_160_19, 4.935_277_778),
            (-52.624_263_84, 5.158_888_889),
            (-58.167_196_7, 6.819_444_444),
            (-87.940_223_79, 15.833_333_33),
            (15.219_316_84, 44.1175),
            (113.916_666_7, -7.616_666_667),
            (124.825_853_9, 1.481_944_444),
            (117.216_666_7, -1.05),
            (72.206_007_07, 21.77),
            (83.287_220_26, 17.693_333_33),
            (-8.633_333_333, 52.662_777_78),
            (56.204_240_28, 27.140_833_33),
            (-21.837_926_97, 64.148_333_33),
            (-14.004_240_28, 65.262_777_78),
            (13.754_240_28, 45.645),
            (132.421_437, 34.365_277_78),
            (135.238_103_7, 34.684_166_67),
            (129.859_540_6, 32.740_277_78),
            (141.016_666_7, 43.196_944_44),
            (139.788_457, 35.624_722_22),
            (-10.793_757_36, 6.345_555_556),
            (79.850_176_68, 6.951_388_889),
            (-7.600_176_678, 33.608_888_89),
            (7.420_906_949, 43.731_944_44),
            (-99.902_473_5, 16.844_444_44),
            (-106.393_404, 23.192_222_22),
            (-97.833_686_69, 22.236_944_44),
            (34.833_510_01, -19.819_166_67),
            (32.557_067_14, -25.972_777_78),
            (-16.021_436_98, 18.035_277_78),
            (-62.219_846_88, 16.703_333_33),
            (-61.056_890_46, 14.599_166_67),
            (166.425_853_9, -22.259_722_22),
            (7.003_180_212, 4.769_166_667),
            (-87.169_140_16, 12.481_944_44),
            (4.292_873_969, 51.927_222_22),
            (5.319_670_2, 60.396_944_44),
            (18.968_786_81, 69.655_833_33),
            (10.388_987_04, 63.437_777_78),
            (173.271_967, -41.259_722_22),
            (58.409_010_6, 23.606_666_67),
            (123.922_673_7, 10.306_388_89),
            (122.071_437, 6.902_777_778),
            (147.151_060_1, -9.467_222_222),
            (143.65053, -3.57),
            (-8.618_610_13, 41.141_111_11),
            (-25.658_833_92, 37.736_944_44),
            (-149.569_670_2, -17.531_111_11),
            (51.555_653_71, 25.298_055_56),
            (20.455_477_03, 54.7),
            (131.887_750_3, 43.094_444_44),
            (28.726_383_98, 60.705),
            (37.221_613_66, 19.615_833_33),
            (45.340_930_51, 2.028_888_889),
            (-55.138_987_04, 5.82),
            (11.870_023_56, 57.689_166_67),
            (22.158_303_89, 65.578_888_89),
            (16.225_677_27, 58.610_277_78),
            (1.284_923_439, 6.139_166_667),
            (27.153_886_93, 38.443_888_89),
            (120.307_067_1, 22.565_277_78),
            (120.503_886_9, 24.258_333_33),
            (30.737_043_58, 46.501_111_11),
            (-56.204_240_28, -34.900_555_56),
            (-134.670_023_6, 58.379_166_67),
            (-165.425_147_2, 64.498_333_33),
            (-155.068_610_1, 19.733_333_33),
            (-122.500_353_4, 48.740_555_56),
            (-95.202_120_14, 29.739_166_67),
            (-97.384_570_08, 25.957_222_22),
            (-122.96702, 46.112_777_78),
            (-61.242_873_97, 13.169_166_67),
            (-66.940_223_79, 10.602_777_78),
            (-71.588_280_33, 10.683_611_11),
            (108.223_203_8, 16.082_222_22),
            (168.305_830_4, -17.747_222_22),
            (-171.757_950_5, -13.828_333_33),
            (42.935_276_8, 14.833_611_11),
            (-75.7, 45.43),
            (-78.88, 42.88),
            (-81.71, 41.5),
            (-88.01, 44.51),
            (-92.1, 46.76),
            (-124.18, 40.8),
            (13.250_883_39, -8.783_888_889),
            (-68.273_557_13, 12.148_333_33),
            (-63.043_757_36, 18.014_444_44),
            (138.507_950_5, -34.799_166_67),
            (153.168_433_5, -27.3825),
            (118.573_203_8, -20.3175),
            (91.825_853_95, 22.268_888_89),
            (-59.624_087_16, 13.106_944_44),
            (-73.524_617_2, 45.543_611_11),
            (-70.904_770_32_f64, -53.168_333_33),
            (-73.226_560_66, -39.816_666_67),
            (121.650_353_4, 38.933_611_11),
            (119.30053, 26.0475),
            (120.31702, 36.095_833_33),
            (121.487_220_3, 31.221_944_44),
            (116.704_063_6, 23.354_722_22),
            (-84.804_240_28, 9.981_666_667),
            (10.156_890_46, 54.330_833_33),
            (-61.385_983_51, 15.296_388_89),
            (-69.875_853_95, 18.475_277_78),
            (-0.985_983_51, 37.589_444_44),
            (-8.726_207_303, 42.2425),
            (-16.226_560_66, 28.474_722_22),
            (24.690_753_83, 59.46),
            (178.421_083_6, -18.1325),
            (6.633_333_333, 43.270_555_56),
            (-4.303_180_212, 55.863_055_56),
            (-1.424_440_518, 50.9025),
            (-5.357_243_816, 36.136_944_44),
            (-13.709_893_99, 9.516_388_889),
            (-15.572_673_73, 11.858_333_33),
            (24.009_717_31, 35.518_055_56),
            (21.318_610_13, 37.646_388_89),
            (-90.841_283_86, 13.915_555_56),
            (114.156_183_7, 22.321_111_11),
            (-72.339_693_76, 18.565_833_33),
            (112.723_910_5, -7.206_388_889),
            (-6.206_007_067, 53.344_444_44),
            (-7.118_786_808, 52.266_388_89),
            (35.018_256_77, 32.8225),
            (10.301_943_46, 43.555_833_33),
            (13.938_103_65, 40.745_277_78),
            (-76.824_793_88, 17.981_666_67),
            (130.854_240_3, 33.9225),
            (39.622_143_7, -4.053_055_556),
            (104.920_906_9, 11.583_055_56),
            (35.519_493_52, 33.905),
            (73.507_067_14, 4.175),
            (-96.133_686_69, 19.208_333_33),
            (96.168_786_81, 16.765_833_33),
            (174.789_870_4, -41.2775),
            (14.585_806_83, 53.430_555_56),
            (39.159_010_6, 21.458_611_11),
            (-17.425_147_23, 14.682_222_22),
            (103.722_143_7, 1.292_777_778),
            (-13.207_950_53, 8.494_166_667),
            (18.109_363_96, 59.335),
            (100.569_140_2, 13.606_944_44),
            (26.521_967_02, 40.264_722_22),
            (-149.887_750_3, 61.235_555_56),
            (-118.200_706_7, 33.748_888_89),
            (-122.301_236_7, 37.799_444_44),
            (-81.559_187_28, 30.380_833_33),
            (-75.134_570_08, 39.895_277_78),
            (-79.923_557_13, 32.822_222_22),
            (-71.324_440_52, 41.4825),
            (106.721_790_3, 10.793_888_89),
            (44.989_693_76, 12.795_555_56),
            (31.023_380_45, -29.881_111_11),
            (25.635_276_8, -33.961_388_89),
            (25.635_276_8, -33.961_388_89),
            (-83.03, 42.33),
            (-90.16, 35.06),
            (-90.2, 38.71),
            (24.96, 60.16),
            (30.3, 59.93),
            (-70.038_810_37, 12.52),
            (55.269_316_84, 25.267_222_22),
            (-58.369_670_2, -34.599_166_67),
            (130.854_947, -12.470_277_78),
            (144.917_196_7, -37.832_777_78),
            (115.855_477, -31.965_277_78),
            (151.189_163_7, -33.862_222_22),
            (2.422_320_377, 6.346_666_667),
            (-77.321_260_31, 25.076_666_67),
            (-43.191_813_9, -22.883_055_56),
            (-123.071_437, 49.297_777_78),
            (-64.703_710_25, 44.043_333_33),
            (-71.619_140_16, -33.035_277_78),
            (113.409_364, 23.094_166_67),
            (117.456_537_1, 39.009_444_44),
            (-4.021_260_306, 5.283_333_333),
            (8.553_003_534, 53.563_611_11),
            (12.117_903_42, 54.1525),
            (12.584_040_05, 55.726_111_11),
            (-79.902_473_5, -2.284_166_667),
            (32.306_007_07, 31.253_333_33),
            (2.168_786_808, 41.354_722_22),
            (-4.418_256_773, 36.709_444_44),
            (2.625_323_91, 39.551_111_11),
            (0.173_733_804, 49.466_944_44),
            (5.341_283_863, 43.329_444_44),
            (7.285_630_153, 43.693_888_89),
            (-0.067_196_702, 51.502_777_78),
            (22.567_550_06, 36.76),
            (28.233_510_01, 36.443_611_11),
            (18.076_383_98, 42.66),
            (11.775_677_27, 42.098_888_89),
            (13.369_846_88, 38.130_833_33),
            (8.489_517_079, 44.311_666_67),
            (12.240_577_15, 45.453_055_56),
            (-77.935_453_47, 18.4675),
            (135.433_863_4, 34.635_833_33),
            (139.667_373_4, 35.436_388_89),
            (24.088_103_65, 57.007_777_78),
            (-109.900_706_7, 22.883_888_89),
            (14.541_283_86, 35.826_111_11),
            (4.824_087_161, 52.413_055_56),
            (10.734_570_08, 59.897_222_22),
            (174.769_493_5, -36.836_388_89),
            (66.973_733_8, 24.835),
            (-79.885_100_12, 9.373_333_333),
            (120.943_404, 14.524_166_67),
            (-66.091_283_86, 18.436_111_11),
            (28.993_404, 41.012_777_78),
            (121.374_617_2, 25.151_944_44),
            (39.293_404, -6.834_444_444),
            (-88.037_750_29, 30.711_388_89),
            (-118.259_717_3, 33.731_944_44),
            (-117.157_243_8, 32.684_444_44),
            (-122.400_883_4, 37.788_611_11),
            (-80.167_020_02, 25.775),
            (-82.436_160_19, 27.93),
            (-157.873_733_8, 21.309_444_44),
            (-71.035_453_47, 42.363_611_11),
            (-76.555_653_71, 39.2325),
            (-74.024_263_84, 40.688_333_33),
            (-94.817_903_42, 29.304_166_67),
            (-76.292_520_61, 36.901_944_44),
            (-122.359_717_3, 47.602_222_22),
            (18.435_276_8, -33.909_166_67),
            (-79.38, 43.61),
            (-87.6, 41.88),
        ])?;

        assert_eq!(
            result[0],
            MultiPointCollection::from_data(
                coordinates,
                vec![Default::default(); 1081],
                HashMap::new(),
            )?
        );

        Ok(())
    }

    #[tokio::test]
    async fn plain_data() -> Result<()> {
        let dataset_information = OgrSourceDataset {
            file_name: test_data!("vector/data/plain_data.csv").into(),
            layer_name: "plain_data".to_string(),
            data_type: None,
            time: OgrSourceDatasetTimeType::None,
            default_geometry: None,
            columns: Some(OgrSourceColumnSpec {
                format_specifics: Some(Csv {
                    header: CsvHeader::Auto,
                }),
                x: "".to_string(),
                y: None,
                float: vec!["b".to_string()],
                int: vec!["a".to_string()],
                text: vec!["c".to_string()],
                rename: None,
            }),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Ignore,
            sql_query: None,
            attribute_query: None,
        };

        let info = StaticMetaData {
            loading_info: dataset_information,
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::Data,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: [
                    ("a".to_string(), FeatureDataType::Int),
                    ("b".to_string(), FeatureDataType::Float),
                    ("c".to_string(), FeatureDataType::Text),
                ]
                .iter()
                .cloned()
                .collect(),
            },
            phantom: Default::default(),
        };

        let query_processor = OgrSourceProcessor::<NoGeometry>::new(Box::new(info));

        let context = MockQueryContext::new(usize::MAX);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((0., 0.).into(), (1., 1.).into())?,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.)?,
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<DataCollection> = query.try_collect().await?;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0],
            DataCollection::from_data(
                vec![],
                vec![Default::default(); 2],
                [
                    (
                        "a".to_string(),
                        FeatureData::NullableInt(vec![Some(1), Some(2)])
                    ),
                    (
                        "b".to_string(),
                        FeatureData::NullableFloat(vec![Some(5.4), None])
                    ),
                    (
                        "c".to_string(),
                        FeatureData::NullableText(vec![
                            Some("foo".to_string()),
                            Some("bar".to_string())
                        ])
                    ),
                ]
                .iter()
                .cloned()
                .collect(),
            )?
        );

        Ok(())
    }

    #[tokio::test]
    async fn default_geometry() -> Result<()> {
        let dataset_information = OgrSourceDataset {
            file_name: test_data!("vector/data/plain_data.csv").into(),
            layer_name: "plain_data".to_string(),
            data_type: None,
            time: OgrSourceDatasetTimeType::None,
            default_geometry: Some(TypedGeometry::MultiPoint({
                MultiPoint::new(vec![Coordinate2D::new(1.0, 2.0)])?
            })),
            columns: Some(OgrSourceColumnSpec {
                format_specifics: Some(Csv {
                    header: CsvHeader::Auto,
                }),
                x: "".to_string(),
                y: None,
                float: vec!["b".to_string()],
                int: vec!["a".to_string()],
                text: vec!["c".to_string()],
                rename: None,
            }),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Ignore,
            sql_query: None,
            attribute_query: None,
        };

        let info = StaticMetaData {
            loading_info: dataset_information,
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: [
                    ("a".to_string(), FeatureDataType::Int),
                    ("b".to_string(), FeatureDataType::Float),
                    ("c".to_string(), FeatureDataType::Text),
                ]
                .iter()
                .cloned()
                .collect(),
            },
            phantom: Default::default(),
        };

        let query_processor = OgrSourceProcessor::<MultiPoint>::new(Box::new(info));

        let context = MockQueryContext::new(usize::MAX);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((0., 0.).into(), (1., 2.).into())?,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.)?,
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await?;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0],
            MultiPointCollection::from_data(
                MultiPoint::many(vec![vec![(1.0, 2.0)], vec![(1.0, 2.0)]]).unwrap(),
                vec![Default::default(); 2],
                [
                    (
                        "a".to_string(),
                        FeatureData::NullableInt(vec![Some(1), Some(2)])
                    ),
                    (
                        "b".to_string(),
                        FeatureData::NullableFloat(vec![Some(5.4), None])
                    ),
                    (
                        "c".to_string(),
                        FeatureData::NullableText(vec![
                            Some("foo".to_string()),
                            Some("bar".to_string())
                        ])
                    ),
                ]
                .iter()
                .cloned()
                .collect(),
            )?
        );

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn chunked() -> Result<()> {
        let id = DatasetId::Internal {
            dataset_id: InternalDatasetId::new(),
        };
        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            id.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: None,
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: Default::default(),
                },
                phantom: Default::default(),
            }),
        );

        let source = OgrSource {
            params: OgrSourceParameters {
                dataset: id.clone(),
                attribute_projection: None,
            },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await?;

        assert_eq!(
            source.result_descriptor().data_type,
            VectorDataType::MultiPoint
        );
        assert_eq!(
            source.result_descriptor().spatial_reference,
            SpatialReference::epsg_4326().into()
        );

        let query_processor = source.query_processor()?.multi_point().unwrap();

        let query_bbox = BoundingBox2D::new((-9.45, 47.64).into(), (10.00, 63.43).into())?;

        let expected_multipoints = MultiPoint::many(vec![
            (2.933_686_69, 51.23),
            (3.204_593_64_f64, 51.336_388_89),
            (8.707_243_816, 53.864_722_22),
            (9.835_806_832, 54.473_611_11),
            (8.489_517_079, 53.4875),
            (7.368_963_486, 53.096_388_89),
            (9.504_416_961, 55.251_111_11),
            (9.868_433_451, 55.854_166_67),
            (9.493_404_005, 55.492_777_78),
            (9.551_943_463, 55.705_555_56),
            (0.073_380_448, 49.365_833_33),
            (0.369_493_522, 49.761_944_44),
            (-2.022_850_412, 48.644_444_44),
            (-4.756_713_781, 55.954_722_22),
            (-1.986_513_545, 50.714_444_44),
            (-5.053_533_569, 50.152_777_78),
            (-3.171_790_342, 55.9825),
            (-2.467_903_416, 56.704_444_44),
            (-2.984_746_761, 51.558_888_89),
            (-1.356_183_746, 54.906_944_44),
            (-1.157_067_138, 54.608_055_56),
            (-8.471_260_306, 54.271_666_67),
            (4.783_686_69, 52.958_055_56),
            (4.651_413_428, 51.805_833_33),
            (6.955_477_032, 53.320_555_56),
            (8.757_420_495, 58.454_166_67),
            (1.73, 52.61),
            (-2.96, 56.46),
            (-3.83, 57.83),
            (-3.08, 58.43),
            (-5.46, 56.41),
            (-4.46, 54.15),
            (-7.31, 55.),
            (-3.4, 54.86),
            (7.891_283_863, 54.1775),
            (8.692_167_256, 56.952_777_78),
            (-1.620_376_914, 49.646_388_89),
            (-0.326_383_981, 49.190_555_56),
            (-4.108_127_208, 47.989_444_44),
            (-2.756_360_424, 47.643_888_89),
            (-2.707_773_852, 51.498_888_89),
            (-0.285_100_118, 53.743_611_11),
            (-1.590_930_506, 54.965),
            (-1.440_930_506, 54.994_444_44),
            (0.685_806_832, 51.434_722_22),
            (-6.456_183_746, 52.341_111_11),
            (6.586_866_902, 53.218_333_33),
            (6.789_870_436, 58.088_611_11),
            (5.002_826_855, 61.597_777_78),
            (7.986_336_867, 58.141_944_44),
            (5.501_413_428, 59.78),
            (4.11, 51.95),
            (-6.36, 58.18),
            (4.386_160_188, 50.886_111_11),
            (3.767_373_38, 51.114_444_44),
            (7.192_873_969, 53.346_111_11),
            (9.436_866_902, 54.803_333_33),
            (8.122_143_698, 53.53),
            (8.433_863_369, 55.465_555_56),
            (9.776_737_338, 54.906_944_44),
            (1.575_500_589, 50.723_611_11),
            (-4.471_436_985, 48.380_555_56),
            (-1.617_550_059, 49.652_222_22),
            (1.085_983_51, 49.926_388_89),
            (-1.601_060_071, 48.834_722_22),
            (-3.352_120_141, 47.734_166_67),
            (-3.834_923_439, 48.585_277_78),
            (-2.074_617_197, 57.142_222_22),
            (-3.007_243_816, 53.436_388_89),
            (-4.154_063_604, 50.364_722_22),
            (-1.1, 50.8075),
            (-8.424_440_518, 51.901_111_11),
            (-9.043_757_362, 53.27),
            (6.171_613_663, 62.4725),
            (5.255_653_71, 59.412_222_22),
            (7.733_333_333, 63.115),
            (7.157_420_495, 62.736_388_89),
            (5.737_220_259, 58.978_888_89),
            (4.293_757_362, 51.297_777_78),
            (8.751_060_071, 53.0975),
            (9.958_480_565, 53.524_722_22),
            (9.740_577_15, 55.558_055_56),
            (1.850_176_678, 50.965_833_33),
            (2.170_906_949, 51.021_666_67),
            (0.235_100_118, 49.422_222_22),
            (-5.891_107_185, 54.620_555_56),
            (1.322_143_698, 51.120_833_33),
            (-4.234_746_761, 57.486_666_67),
            (-8.633_333_333, 52.662_777_78),
            (4.292_873_969, 51.927_222_22),
            (5.319_670_2, 60.396_944_44),
            (-4.303_180_212, 55.863_055_56),
            (-1.424_440_518, 50.9025),
            (-6.206_007_067, 53.344_444_44),
            (-7.118_786_808, 52.266_388_89),
            (8.553_003_534, 53.563_611_11),
            (0.173_733_804, 49.466_944_44),
            (-0.067_196_702, 51.502_777_78),
            (4.824_087_161, 52.413_055_56),
        ])?;

        let context1 = MockQueryContext::new(0);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: query_bbox,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.)?,
                },
                &context1,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await?;

        assert_eq!(result.len(), 99);

        for (collection, expected_multi_point) in
            result.iter().zip(expected_multipoints.iter().cloned())
        {
            assert_eq!(collection.len(), 1);
            assert_eq!(
                collection,
                &MultiPointCollection::from_data(
                    vec![expected_multi_point],
                    vec![Default::default(); 1],
                    Default::default(),
                )?
            );
        }

        assert!(!result.last().unwrap().is_empty());

        // LARGER CHUNK
        let context = MockQueryContext::new(1_000);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: query_bbox,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.)?,
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await?;

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].len(), 25);
        assert_eq!(result[1].len(), 25);
        assert_eq!(result[2].len(), 25);
        assert_eq!(result[3].len(), 24);

        assert_eq!(
            result[0],
            MultiPointCollection::from_data(
                expected_multipoints[0..25].to_vec(),
                vec![Default::default(); result[0].len()],
                Default::default(),
            )?
        );
        assert_eq!(
            result[1],
            MultiPointCollection::from_data(
                expected_multipoints[25..50].to_vec(),
                vec![Default::default(); result[1].len()],
                Default::default(),
            )?
        );
        assert_eq!(
            result[2],
            MultiPointCollection::from_data(
                expected_multipoints[50..75].to_vec(),
                vec![Default::default(); result[2].len()],
                Default::default(),
            )?
        );
        assert_eq!(
            result[3],
            MultiPointCollection::from_data(
                expected_multipoints[75..99].to_vec(),
                vec![Default::default(); result[3].len()],
                Default::default(),
            )?
        );

        Ok(())
    }

    #[tokio::test]
    async fn empty() {
        let dataset = DatasetId::Internal {
            dataset_id: InternalDatasetId::new(),
        };
        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            dataset.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: None,
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: Default::default(),
                },
                phantom: Default::default(),
            }),
        );

        let source = OgrSource {
            params: OgrSourceParameters {
                dataset,
                attribute_projection: None,
            },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await
        .unwrap();

        assert_eq!(
            source.result_descriptor().data_type,
            VectorDataType::MultiPoint
        );
        assert_eq!(
            source.result_descriptor().spatial_reference,
            SpatialReference::epsg_4326().into()
        );

        let query_processor = source.query_processor().unwrap().multi_point().unwrap();

        let query_bbox =
            BoundingBox2D::new((-180.0, -90.0).into(), (-180.00, -90.0).into()).unwrap();

        let context = MockQueryContext::new(0);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: query_bbox,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.).unwrap(),
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await.unwrap();

        assert_eq!(result.len(), 1);

        assert!(result[0].is_empty());
    }

    #[tokio::test]
    async fn polygon_gpkg() {
        let dataset = DatasetId::Internal {
            dataset_id: InternalDatasetId::new(),
        };
        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            dataset.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/germany_polygon.gpkg").into(),
                    layer_name: "test_germany".to_owned(),
                    data_type: Some(VectorDataType::MultiPolygon),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: "".to_owned(),
                        y: None,
                        int: vec![],
                        float: vec![],
                        text: vec![],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Abort,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPolygon,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: Default::default(),
                },
                phantom: Default::default(),
            }),
        );

        let source = OgrSource {
            params: OgrSourceParameters {
                dataset,
                attribute_projection: None,
            },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await
        .unwrap();

        assert_eq!(
            source.result_descriptor().data_type,
            VectorDataType::MultiPolygon
        );
        assert_eq!(
            source.result_descriptor().spatial_reference,
            SpatialReference::epsg_4326().into()
        );

        let query_processor = source.query_processor().unwrap().multi_polygon().unwrap();

        let query_bbox = BoundingBox2D::new((-180.0, -90.0).into(), (180.00, 90.0).into()).unwrap();

        let context = MockQueryContext::new(1024 * 1024);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: query_bbox,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.).unwrap(),
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPolygonCollection> = query.try_collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let result = result.into_iter().next().unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result.feature_offsets().len(), 2);
        assert_eq!(result.polygon_offsets().len(), 23);
        assert_eq!(result.ring_offsets().len(), 23);
        assert_eq!(result.coordinates().len(), 3027);

        assert_eq!(
            result.coordinates()[0],
            (13.815_724_731_000_074, 48.766_430_156_000_055).into()
        );
    }

    #[tokio::test]
    async fn points_csv() {
        let dataset = DatasetId::Internal {
            dataset_id: InternalDatasetId::new(),
        };
        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            dataset.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/points.csv").into(),
                    layer_name: "points".to_owned(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: Some(Csv {
                            header: CsvHeader::Yes,
                        }),
                        x: "x".to_owned(),
                        y: Some("y".to_owned()),
                        int: vec!["num".to_owned()],
                        float: vec![],
                        text: vec!["txt".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Abort,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        ("num".to_string(), FeatureDataType::Int),
                        ("txt".to_string(), FeatureDataType::Text),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                },
                phantom: Default::default(),
            }),
        );

        let source = OgrSource {
            params: OgrSourceParameters {
                dataset,
                attribute_projection: None,
            },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await
        .unwrap();

        assert_eq!(
            source.result_descriptor().data_type,
            VectorDataType::MultiPoint
        );
        assert_eq!(
            source.result_descriptor().spatial_reference,
            SpatialReference::epsg_4326().into()
        );

        let query_processor = source.query_processor().unwrap().multi_point().unwrap();

        let query_bbox = BoundingBox2D::new((-180.0, -90.0).into(), (180.00, 90.0).into()).unwrap();

        let context = MockQueryContext::new(1024 * 1024);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: query_bbox,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.).unwrap(),
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let result = result.into_iter().next().unwrap();

        let pc = MultiPointCollection::from_data(
            MultiPoint::many(vec![vec![(1.1, 2.2)], vec![(3.3, 4.4)]]).unwrap(),
            vec![TimeInterval::default(), TimeInterval::default()],
            {
                let mut map = HashMap::new();
                map.insert("num".into(), FeatureData::Int(vec![42, 815]));
                map.insert(
                    "txt".into(),
                    FeatureData::Text(vec!["foo".to_owned(), "bar".to_owned()]),
                );
                map
            },
        )
        .unwrap();

        assert_eq!(result, pc);
    }

    #[tokio::test]
    async fn points_date_csv() {
        let dataset = DatasetId::Internal {
            dataset_id: InternalDatasetId::new(),
        };
        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            dataset.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/lonlat_date.csv").into(),
                    layer_name: "lonlat_date".to_owned(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::Start {
                        start_field: "Date".to_owned(),
                        start_format: OgrSourceTimeFormat::Custom {
                            custom_format: "%d.%m.%Y".to_owned(),
                        },
                        duration: OgrSourceDurationSpec::Value(TimeStep {
                            granularity: TimeGranularity::Seconds,
                            step: 84,
                        }),
                    },
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: Some(Csv {
                            header: CsvHeader::Yes,
                        }),
                        x: "Longitude".to_owned(),
                        y: Some("Latitude".to_owned()),
                        int: vec![],
                        float: vec![],
                        text: vec!["Name".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Abort,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [("Name".to_string(), FeatureDataType::Text)]
                        .iter()
                        .cloned()
                        .collect(),
                },
                phantom: Default::default(),
            }),
        );

        let source = OgrSource {
            params: OgrSourceParameters {
                dataset,
                attribute_projection: None,
            },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await
        .unwrap();

        assert_eq!(
            source.result_descriptor().data_type,
            VectorDataType::MultiPoint
        );
        assert_eq!(
            source.result_descriptor().spatial_reference,
            SpatialReference::epsg_4326().into()
        );

        let query_processor = source.query_processor().unwrap().multi_point().unwrap();

        let query_bbox = BoundingBox2D::new((-180.0, -90.0).into(), (180.00, 90.0).into()).unwrap();

        let context = MockQueryContext::new(1024 * 1024);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: query_bbox,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.).unwrap(),
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let result = result.into_iter().next().unwrap();

        let pc = MultiPointCollection::from_data(
            MultiPoint::many(vec![vec![(1.1, 2.2)]]).unwrap(),
            vec![TimeInterval::new(819_763_200_000, 819_763_284_000).unwrap()],
            {
                let mut map = HashMap::new();
                map.insert("Name".into(), FeatureData::Text(vec!["foo".to_owned()]));
                map
            },
        )
        .unwrap();

        assert_eq!(result, pc);
    }

    #[tokio::test]
    async fn points_date_time_csv() {
        let dataset = DatasetId::Internal {
            dataset_id: InternalDatasetId::new(),
        };
        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            dataset.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/lonlat_date_time.csv").into(),
                    layer_name: "lonlat_date_time".to_owned(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::Start {
                        start_field: "DateTime".to_owned(),
                        start_format: OgrSourceTimeFormat::Custom {
                            custom_format: "%d.%m.%Y %H:%M:%S".to_owned(),
                        },
                        duration: OgrSourceDurationSpec::Value(TimeStep {
                            granularity: TimeGranularity::Seconds,
                            step: 84,
                        }),
                    },
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: Some(Csv {
                            header: CsvHeader::Yes,
                        }),
                        x: "Longitude".to_owned(),
                        y: Some("Latitude".to_owned()),
                        int: vec![],
                        float: vec![],
                        text: vec!["Name".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Abort,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [("Name".to_string(), FeatureDataType::Text)]
                        .iter()
                        .cloned()
                        .collect(),
                },
                phantom: Default::default(),
            }),
        );

        let source = OgrSource {
            params: OgrSourceParameters {
                dataset,
                attribute_projection: None,
            },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await
        .unwrap();

        assert_eq!(
            source.result_descriptor().data_type,
            VectorDataType::MultiPoint
        );
        assert_eq!(
            source.result_descriptor().spatial_reference,
            SpatialReference::epsg_4326().into()
        );

        let query_processor = source.query_processor().unwrap().multi_point().unwrap();

        let query_bbox = BoundingBox2D::new((-180.0, -90.0).into(), (180.00, 90.0).into()).unwrap();

        let context = MockQueryContext::new(1024 * 1024);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: query_bbox,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.).unwrap(),
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let result = result.into_iter().next().unwrap();

        let pc = MultiPointCollection::from_data(
            MultiPoint::many(vec![vec![(1.1, 2.2)]]).unwrap(),
            vec![TimeInterval::new(819_828_000_000, 819_828_084_000).unwrap()],
            {
                let mut map = HashMap::new();
                map.insert("Name".into(), FeatureData::Text(vec!["foo".to_owned()]));
                map
            },
        )
        .unwrap();

        assert_eq!(result, pc);
    }

    #[tokio::test]
    async fn points_date_time_tz_csv() {
        let dataset = DatasetId::Internal {
            dataset_id: InternalDatasetId::new(),
        };
        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.add_meta_data::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>(
            dataset.clone(),
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/lonlat_date_time_tz.csv").into(),
                    layer_name: "lonlat_date_time_tz".to_owned(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::Start {
                        start_field: "DateTimeTz".to_owned(),
                        start_format: OgrSourceTimeFormat::Custom {
                            custom_format: "%d.%m.%Y %H:%M:%S %z".to_owned(),
                        },
                        duration: OgrSourceDurationSpec::Value(TimeStep {
                            granularity: TimeGranularity::Seconds,
                            step: 84,
                        }),
                    },
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: Some(Csv {
                            header: CsvHeader::Yes,
                        }),
                        x: "Longitude".to_owned(),
                        y: Some("Latitude".to_owned()),
                        int: vec![],
                        float: vec![],
                        text: vec!["Name".to_owned()],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Abort,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [("Name".to_string(), FeatureDataType::Text)]
                        .iter()
                        .cloned()
                        .collect(),
                },
                phantom: Default::default(),
            }),
        );

        let source = OgrSource {
            params: OgrSourceParameters {
                dataset,
                attribute_projection: None,
            },
        }
        .boxed()
        .initialize(&exe_ctx)
        .await
        .unwrap();

        assert_eq!(
            source.result_descriptor().data_type,
            VectorDataType::MultiPoint
        );
        assert_eq!(
            source.result_descriptor().spatial_reference,
            SpatialReference::epsg_4326().into()
        );

        let query_processor = source.query_processor().unwrap().multi_point().unwrap();

        let query_bbox = BoundingBox2D::new((-180.0, -90.0).into(), (180.00, 90.0).into()).unwrap();

        let context = MockQueryContext::new(1024 * 1024);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: query_bbox,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.).unwrap(),
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<MultiPointCollection> = query.try_collect().await.unwrap();

        assert_eq!(result.len(), 1);
        let result = result.into_iter().next().unwrap();

        let pc = MultiPointCollection::from_data(
            MultiPoint::many(vec![vec![(1.1, 2.2)]]).unwrap(),
            vec![TimeInterval::new(819_842_400_000, 819_842_484_000).unwrap()],
            {
                let mut map = HashMap::new();
                map.insert("Name".into(), FeatureData::Text(vec!["foo".to_owned()]));
                map
            },
        )
        .unwrap();

        assert_eq!(result, pc);
    }

    #[tokio::test]
    async fn rename() -> Result<()> {
        let dataset_information = OgrSourceDataset {
            file_name: test_data!("vector/data/plain_data.csv").into(),
            layer_name: "plain_data".to_string(),
            data_type: None,
            time: OgrSourceDatasetTimeType::None,
            default_geometry: None,
            columns: Some(OgrSourceColumnSpec {
                format_specifics: Some(Csv {
                    header: CsvHeader::Yes,
                }),
                x: "".to_string(),
                y: None,
                float: vec!["b".to_string()],
                int: vec!["a".to_string()],
                text: vec!["c".to_string()],
                rename: Some(
                    [("a".to_owned(), "foo".to_owned())]
                        .iter()
                        .cloned()
                        .collect(),
                ),
            }),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Ignore,
            sql_query: None,
            attribute_query: None,
        };

        let info = StaticMetaData {
            loading_info: dataset_information,
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: [
                    ("foo".to_string(), FeatureDataType::Int),
                    ("b".to_string(), FeatureDataType::Float),
                    ("c".to_string(), FeatureDataType::Text),
                ]
                .iter()
                .cloned()
                .collect(),
            },
            phantom: Default::default(),
        };

        let query_processor = OgrSourceProcessor::<NoGeometry>::new(Box::new(info));

        let context = MockQueryContext::new(usize::MAX);
        let query = query_processor
            .query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((0., 0.).into(), (1., 1.).into())?,
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::new(1., 1.)?,
                },
                &context,
            )
            .await
            .unwrap();

        let result: Vec<DataCollection> = query.try_collect().await?;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0],
            DataCollection::from_data(
                vec![],
                vec![Default::default(); 2],
                [
                    (
                        "foo".to_string(),
                        FeatureData::NullableInt(vec![Some(1), Some(2)])
                    ),
                    (
                        "b".to_string(),
                        FeatureData::NullableFloat(vec![Some(5.4), None])
                    ),
                    (
                        "c".to_string(),
                        FeatureData::NullableText(vec![
                            Some("foo".to_string()),
                            Some("bar".to_string())
                        ])
                    ),
                ]
                .iter()
                .cloned()
                .collect(),
            )?
        );

        Ok(())
    }
}
