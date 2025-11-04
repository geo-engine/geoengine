use crate::adapters::{QueryWrapper, RasterStackerAdapter, RasterStackerSource};
use crate::engine::{
    BoxRasterQueryProcessor, CanonicOperatorName, ExecutionContext, InitializedRasterOperator,
    InitializedSources, MultipleRasterSources, Operator, OperatorName, QueryContext,
    QueryProcessor, RasterBandDescriptor, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, TypedRasterQueryProcessor, WorkflowOperatorPath,
};
use crate::error::{
    InvalidNumberOfRasterStackerInputs, RasterInputsMustHaveSameSpatialReferenceAndDatatype,
};
use crate::optimization::OptimizationError;
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use geoengine_datatypes::primitives::{BandSelection, RasterQueryRectangle, SpatialResolution};
use geoengine_datatypes::raster::{
    DynamicRasterDataType, GridBoundingBox2D, Pixel, RasterTile2D, RenameBands,
};
use serde::{Deserialize, Serialize};
use snafu::ensure;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RasterStackerParams {
    pub rename_bands: RenameBands,
}

/// This `QueryProcessor` stacks all of it's inputs into a single raster time-series.
/// It does so by querying all of it's inputs outputting them by band, space and then time.
/// The tiles are automatically temporally aligned.
///
/// All inputs must have the same data type and spatial reference.
pub type RasterStacker = Operator<RasterStackerParams, MultipleRasterSources>;

impl OperatorName for RasterStacker {
    const TYPE_NAME: &'static str = "RasterStacker";
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for RasterStacker {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);

        ensure!(
            !self.sources.rasters.is_empty() && self.sources.rasters.len() <= 8,
            InvalidNumberOfRasterStackerInputs
        );

        let raster_sources = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?
            .rasters;

        let in_descriptors = raster_sources
            .iter()
            .map(InitializedRasterOperator::result_descriptor)
            .collect::<Vec<_>>();

        ensure!(
            in_descriptors.iter().all(|d| d.spatial_reference
                == in_descriptors[0].spatial_reference
                && d.data_type == in_descriptors[0].data_type),
            RasterInputsMustHaveSameSpatialReferenceAndDatatype {
                datatypes: in_descriptors
                    .iter()
                    .map(|d| d.data_type)
                    .collect::<Vec<_>>(),
                spatial_references: in_descriptors
                    .iter()
                    .map(|d| d.spatial_reference)
                    .collect::<Vec<_>>(),
            }
        );

        let first_spatial_grid = in_descriptors[0].spatial_grid;
        let result_spatial_grid = in_descriptors
            .iter()
            .skip(1)
            .map(|x| x.spatial_grid_descriptor())
            .try_fold(first_spatial_grid, |a, &b| {
                a.merge(&b)
                    .ok_or(crate::error::Error::CantMergeSpatialGridDescriptor { a, b })
            })?;

        let time = in_descriptors.iter().skip(1).map(|rd| rd.time).fold(
            in_descriptors
                .first()
                .expect("There must be at least one input")
                .time,
            |a, b| a.merge(b),
        );

        let data_type = in_descriptors[0].data_type;
        let spatial_reference = in_descriptors[0].spatial_reference;

        let bands_per_source = in_descriptors
            .iter()
            .map(|d| d.bands.count())
            .collect::<Vec<_>>();

        let band_names = self.params.rename_bands.apply(
            in_descriptors
                .iter()
                .map(|d| d.bands.iter().map(|b| b.name.clone()).collect())
                .collect(),
        )?;

        let output_band_descriptors = in_descriptors
            .into_iter()
            .flat_map(|d| d.bands.iter().cloned())
            .zip(band_names)
            .map(|(descriptor, name)| RasterBandDescriptor { name, ..descriptor })
            .collect::<Vec<_>>()
            .try_into()?;

        let result_descriptor = RasterResultDescriptor {
            data_type,
            spatial_reference,
            time,
            spatial_grid: result_spatial_grid,
            bands: output_band_descriptors,
        };

        Ok(Box::new(InitializedRasterStacker {
            name,
            path,
            result_descriptor,
            rename_bands: self.params.rename_bands.clone(),
            raster_sources,
            bands_per_source,
        }))
    }

    span_fn!(RasterStacker);
}

pub struct InitializedRasterStacker {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    result_descriptor: RasterResultDescriptor,
    rename_bands: RenameBands,
    raster_sources: Vec<Box<dyn InitializedRasterOperator>>,
    bands_per_source: Vec<u32>,
}

impl InitializedRasterOperator for InitializedRasterStacker {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let typed_raster_processors = self
            .raster_sources
            .iter()
            .map(InitializedRasterOperator::query_processor)
            .collect::<Result<Vec<_>>>()?;

        // unpack all processors
        let datatype = typed_raster_processors[0].raster_data_type();

        let bands_per_source = self.bands_per_source.clone();

        // TODO: use a macro to unpack all the input processor to the same datatype?
        Ok(match datatype {
            geoengine_datatypes::raster::RasterDataType::U8 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_u8().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::U8(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::U16 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_u16().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::U16(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::U32 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_u32().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::U32(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::U64 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_u64().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::U64(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::I8 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_i8().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::I8(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::I16 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_i16().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::I16(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::I32 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_i32().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::I32(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::I64 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_i64().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::I64(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::F32 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_f32().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::F32(Box::new(p))
            }
            geoengine_datatypes::raster::RasterDataType::F64 => {
                let inputs = typed_raster_processors.into_iter().map(|p| p.get_f64().expect("all inputs should have the same datatype because it was checked in the initialization of the operator")).collect();
                let p = RasterStackerProcessor::new(
                    inputs,
                    self.result_descriptor.clone(),
                    bands_per_source,
                );
                TypedRasterQueryProcessor::F64(Box::new(p))
            }
        })
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        RasterStacker::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }

    fn optimize(
        &self,
        resolution: SpatialResolution,
    ) -> Result<Box<dyn RasterOperator>, OptimizationError> {
        Ok(RasterStacker {
            params: RasterStackerParams {
                rename_bands: self.rename_bands.clone(),
            },
            sources: MultipleRasterSources {
                rasters: self
                    .raster_sources
                    .iter()
                    .map(|s| s.optimize(resolution))
                    .collect::<Result<Vec<_>, _>>()?,
            },
        }
        .boxed())
    }
}

pub(crate) struct RasterStackerProcessor<T> {
    sources: Vec<BoxRasterQueryProcessor<T>>,
    result_descriptor: RasterResultDescriptor,
    bands_per_source: Vec<u32>,
}

impl<T> RasterStackerProcessor<T> {
    pub fn new(
        sources: Vec<BoxRasterQueryProcessor<T>>,
        result_descriptor: RasterResultDescriptor,
        bands_per_source: Vec<u32>,
    ) -> Self {
        Self {
            sources,
            result_descriptor,
            bands_per_source,
        }
    }
}

/// compute the bands in the input source from the bands in a query that uses multiple sources
fn map_query_bands_to_source_bands(
    query_bands: &BandSelection,
    bands_per_source: &[u32],
    source_index: usize,
) -> Option<BandSelection> {
    let source_start: u32 = bands_per_source.iter().take(source_index).sum();
    let source_bands = bands_per_source[source_index];
    let source_end = source_start + source_bands;

    let bands = query_bands
        .as_slice()
        .iter()
        .filter(|output_band| **output_band >= source_start && **output_band < source_end)
        .map(|output_band| output_band - source_start)
        .collect::<Vec<_>>();

    if bands.is_empty() {
        return None;
    }

    Some(BandSelection::new_unchecked(bands))
}

#[async_trait]
impl<T> QueryProcessor for RasterStackerProcessor<T>
where
    T: Pixel,
{
    type Output = RasterTile2D<T>;
    type ResultDescription = RasterResultDescriptor;
    type Selection = BandSelection;
    type SpatialBounds = GridBoundingBox2D;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<T>>>> {
        let mut sources = vec![];

        for (idx, source) in self.sources.iter().enumerate() {
            let Some(bands) =
                map_query_bands_to_source_bands(query.attributes(), &self.bands_per_source, idx)
            else {
                continue;
            };

            sources.push(RasterStackerSource {
                queryable: QueryWrapper { p: source, ctx },
                band_idxs: bands.as_vec(),
            });
        }

        let output = RasterStackerAdapter::new(sources, query.into());

        Ok(Box::pin(output))
    }

    fn result_descriptor(&self) -> &Self::ResultDescription {
        &self.result_descriptor
    }
}

#[async_trait]
impl<T> RasterQueryProcessor for RasterStackerProcessor<T>
where
    T: Pixel,
{
    type RasterType = T;

    async fn _time_query<'a>(
        &'a self,
        query: geoengine_datatypes::primitives::TimeInterval,
        ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<futures::stream::BoxStream<'a, Result<geoengine_datatypes::primitives::TimeInterval>>>
    {
        let mut time_sources = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            let s = source.time_query(query, ctx).await?;
            time_sources.push(s);
        }
        let output = crate::adapters::TimeIntervalStreamMerge::new(time_sources);
        Ok(Box::pin(output))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::{CacheHint, TimeInstance, TimeInterval, TimeStep},
        raster::{
            GeoTransform, Grid, GridBoundingBox2D, GridShape, RasterDataType,
            TilesEqualIgnoringCacheHint,
        },
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use crate::{
        engine::{
            MockExecutionContext, RasterBandDescriptor, RasterBandDescriptors, SingleRasterSource,
            SpatialGridDescriptor,
        },
        mock::{MockRasterSource, MockRasterSourceParams},
        processing::{Expression, ExpressionParams},
        source::{GdalSource, GdalSourceParameters},
        util::gdal::add_ndvi_dataset,
    };

    use super::*;

    #[test]
    fn it_maps_query_bands_to_source_bands() {
        assert_eq!(
            map_query_bands_to_source_bands(&0.into(), &[2, 1], 0),
            Some(0.into())
        );
        assert_eq!(map_query_bands_to_source_bands(&0.into(), &[2, 1], 1), None);
        assert_eq!(
            map_query_bands_to_source_bands(&2.into(), &[2, 1], 1),
            Some(0.into())
        );

        assert_eq!(
            map_query_bands_to_source_bands(&[1, 2].try_into().unwrap(), &[2, 2], 0),
            Some(1.into())
        );
        assert_eq!(
            map_query_bands_to_source_bands(&[1, 2, 3].try_into().unwrap(), &[2, 2], 1),
            Some([0, 1].try_into().unwrap())
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_stacks() {
        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let data2: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: crate::engine::TimeDescriptor::new_regular_with_epoch(
                Some(TimeInterval::new_unchecked(0, 5)),
                TimeStep::millis(10),
            ),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor,
            },
        }
        .boxed();

        let stacker = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mrs1, mrs2],
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 10),
            [0, 1].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = stacker
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let expected: Vec<_> = data
            .into_iter()
            .zip(data2.into_iter().map(|mut tile| {
                tile.band = 1;
                tile
            }))
            .flat_map(|(a, b)| vec![a.clone(), b.clone()])
            .collect();

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_stacks_stacks() {
        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![3, 2, 1, 0]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 6, 5, 4]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![11, 10, 9, 8]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![15, 14, 13, 12])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let data2: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 18, 17, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![32, 22, 21, 20])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![27, 26, 25, 24])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 1,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![31, 30, 39, 28])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: crate::engine::TimeDescriptor::new_regular_with_epoch(
                Some(TimeInterval::new_unchecked(0, 10)),
                TimeStep::millis(10),
            ),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            ),
            bands: RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new_unitless("band_0".into()),
                RasterBandDescriptor::new_unitless("band_1".into()),
            ])
            .unwrap(),
        };

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor,
            },
        }
        .boxed();

        let stacker = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mrs1, mrs2],
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-1, 0], [-1, 2]).unwrap(),
            TimeInterval::new_unchecked(0, 10),
            [0, 1, 2, 3].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = stacker
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let expected: Vec<_> = data
            .chunks(2)
            .zip(
                data2
                    .into_iter()
                    .map(|mut tile| {
                        tile.band += 2;
                        tile
                    })
                    .collect::<Vec<_>>()
                    .chunks(2),
            )
            .flat_map(|(chunk1, chunk2)| chunk1.iter().chain(chunk2.iter()))
            .cloned()
            .collect();

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_selects_band_from_stack() {
        let data: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0, 1, 2, 3]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![4, 5, 6, 7]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![8, 9, 10, 11]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![12, 13, 14, 15])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let data2: Vec<RasterTile2D<u8>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![16, 17, 18, 19])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![20, 21, 22, 23])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![24, 25, 26, 27])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![28, 29, 30, 31])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: crate::engine::TimeDescriptor::new_regular_with_epoch(
                Some(TimeInterval::new_unchecked(0, 10)),
                TimeStep::millis(10),
            ),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor,
            },
        }
        .boxed();

        let stacker = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mrs1, mrs2],
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-1, 0], [-1, 2]).unwrap(),
            TimeInterval::new_unchecked(0, 10),
            1.into(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = stacker
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert!(data2.tiles_equal_ignoring_cache_hint(&result));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_stacks_ndvi() {
        let mut exe_ctx = MockExecutionContext::test_default();

        let ndvi_id = add_ndvi_dataset(&mut exe_ctx);

        let expression = Expression {
            params: ExpressionParams {
                expression: "if A > 100 { A } else { 0 }".into(),
                output_type: RasterDataType::U8,
                output_band: None,
                map_no_data: false,
            },
            sources: SingleRasterSource {
                raster: GdalSource {
                    params: GdalSourceParameters {
                        data: ndvi_id.clone(),
                        overview_level: None,
                    },
                }
                .boxed(),
            },
        }
        .boxed();

        let operator = RasterStacker {
            params: RasterStackerParams {
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![
                    GdalSource {
                        params: GdalSourceParameters::new(ndvi_id),
                    }
                    .boxed(),
                    expression,
                ],
            },
        }
        .boxed();

        let operator = operator
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let processor = operator.query_processor().unwrap().get_u8().unwrap();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_ctx = exe_ctx.mock_query_context_test_default();

        // query both bands
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
            ),
            [0, 1].try_into().unwrap(),
        );

        let result = processor
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert!(!result.is_empty());

        // query only first band
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
            ),
            [0].try_into().unwrap(),
        );

        let result = processor
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert!(!result.is_empty());

        // query only second band
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap(),
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
            ),
            [1].try_into().unwrap(),
        );

        let result = processor
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert!(!result.is_empty());
    }

    #[test]
    fn it_renames() {
        let names = vec![
            vec!["foo".to_string(), "bar".to_string()],
            vec!["foo".to_string(), "bla".to_string()],
            vec!["foo".to_string(), "baz".to_string()],
        ];

        assert_eq!(
            RenameBands::Default.apply(names.clone()).unwrap(),
            vec![
                "foo".to_string(),
                "bar".to_string(),
                "foo (1)".to_string(),
                "bla".to_string(),
                "foo (2)".to_string(),
                "baz".to_string()
            ]
        );

        assert_eq!(
            RenameBands::Suffix(vec![
                String::new(),
                " second".to_string(),
                " third".to_string()
            ])
            .apply(names.clone())
            .unwrap(),
            vec![
                "foo".to_string(),
                "bar".to_string(),
                "foo second".to_string(),
                "bla second".to_string(),
                "foo third".to_string(),
                "baz third".to_string()
            ]
        );

        assert_eq!(
            RenameBands::Rename(vec![
                "A".to_string(),
                "B".to_string(),
                "C".to_string(),
                "D".to_string(),
                "E".to_string(),
                "F".to_string()
            ])
            .apply(names.clone())
            .unwrap(),
            vec![
                "A".to_string(),
                "B".to_string(),
                "C".to_string(),
                "D".to_string(),
                "E".to_string(),
                "F".to_string()
            ]
        );
    }
}
