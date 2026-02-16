use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources, Operator,
    OperatorName, QueryContext, QueryProcessor, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, SingleRasterSource, TypedRasterQueryProcessor, WorkflowOperatorPath,
};
use crate::optimization::OptimizationError;
use crate::util::Result;
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use geoengine_datatypes::primitives::{BandSelection, RasterQueryRectangle, SpatialResolution};
use geoengine_datatypes::raster::{GridBoundingBox2D, RasterTile2D};
use serde::{Deserialize, Serialize};
use snafu::{Snafu, ensure};
use tracing::{debug, warn};

/// Filter bands of a raster
///
/// Removes all but the specified bands from a raster operator output.
/// The order of the remaining bands is preserved.
pub type BandFilter = Operator<BandFilterParams, SingleRasterSource>;

impl OperatorName for BandFilter {
    const TYPE_NAME: &'static str = "BandFilter";
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BandFilterParams {
    bands: BandsByNameOrIndex,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BandsByNameOrIndex {
    Name(Vec<String>),
    Index(Vec<usize>),
}

impl BandsByNameOrIndex {
    fn is_empty(&self) -> bool {
        match &self {
            BandsByNameOrIndex::Name(names) => names.is_empty(),
            BandsByNameOrIndex::Index(indices) => indices.is_empty(),
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(error))]
pub enum BandFilterError {
    #[snafu(display("No bands selected, must select at least one band."))]
    NoBandsSelected,

    #[snafu(display("Band with name '{}' not found in input raster.", name))]
    BandNotFound { name: String },

    #[snafu(display("Bands cannot be mapped to input raster bands."))]
    BandsCannotBeMappedToInput,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for BandFilter {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        ensure!(!self.params.bands.is_empty(), error::NoBandsSelected);

        let name = CanonicOperatorName::from(&self);

        let initialized_sources = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?;

        debug!("Initializing `BandFilter` with {:?}.", &self.params);

        let input_descriptor = initialized_sources.raster.result_descriptor().clone();

        let input_bands = input_descriptor.bands;

        let mut output_bands = vec![];
        let mut output_band_to_input_band_idx = vec![];

        match &self.params.bands {
            BandsByNameOrIndex::Name(names) => {
                let mut unresolved_names: std::collections::HashSet<_> = names.iter().collect();
                for (band_idx, band) in input_bands.iter().enumerate() {
                    if unresolved_names.remove(&band.name) {
                        output_bands.push(band.clone());
                        output_band_to_input_band_idx.push(band_idx as u32);
                    }
                }
                if let Some(name) = unresolved_names.into_iter().next() {
                    return Err(BandFilterError::BandNotFound { name: name.clone() }.into());
                }
            }
            BandsByNameOrIndex::Index(indices) => {
                let mut unresolved_indices: std::collections::HashSet<_> =
                    indices.iter().copied().collect();
                for (band_idx, band) in input_bands.iter().enumerate() {
                    if unresolved_indices.remove(&band_idx) {
                        output_bands.push(band.clone());
                        output_band_to_input_band_idx.push(band_idx as u32);
                    }
                }
                if let Some(idx) = unresolved_indices.into_iter().next() {
                    return Err(BandFilterError::BandNotFound {
                        name: format!("index {idx}"),
                    }
                    .into());
                }
            }
        }

        let result_descriptor = RasterResultDescriptor {
            data_type: input_descriptor.data_type,
            spatial_reference: input_descriptor.spatial_reference,
            bands: output_bands.try_into()?,
            time: input_descriptor.time,
            spatial_grid: input_descriptor.spatial_grid,
        };

        let initialized_operator = InitializedBandFilter {
            name,
            path,
            source: initialized_sources.raster,
            result_descriptor,
            bands: self.params.bands.clone(),
            output_band_to_input_band_idx,
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(BandFilter);
}

pub struct InitializedBandFilter {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    source: Box<dyn InitializedRasterOperator>,
    result_descriptor: RasterResultDescriptor,
    bands: BandsByNameOrIndex,
    output_band_to_input_band_idx: Vec<u32>,
}

impl InitializedRasterOperator for InitializedBandFilter {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source_processor = self.source.query_processor()?;

        Ok(
            call_on_generic_raster_processor!(source_processor, source => BandFilterProcessor {
                source,
                result_descriptor: self.result_descriptor.clone(),
                output_band_to_input_band_idx: self.output_band_to_input_band_idx.clone(),
            }.boxed().into()),
        )
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        BandFilter::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<Box<dyn RasterOperator>, OptimizationError> {
        Ok(BandFilter {
            params: BandFilterParams {
                bands: self.bands.clone(),
            },
            sources: SingleRasterSource {
                raster: self.source.optimize(target_resolution)?,
            },
        }
        .boxed())
    }
}

pub struct BandFilterProcessor<S> {
    source: S,
    result_descriptor: RasterResultDescriptor,
    output_band_to_input_band_idx: Vec<u32>,
}

#[async_trait]
impl<S> QueryProcessor for BandFilterProcessor<S>
where
    S: RasterQueryProcessor,
{
    type Output = RasterTile2D<S::RasterType>;
    type ResultDescription = RasterResultDescriptor;
    type Selection = BandSelection;
    type SpatialBounds = GridBoundingBox2D;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<S::RasterType>>>> {
        let output_attributes = query.attributes();

        let input_bands = output_attributes
            .as_slice()
            .iter()
            .map(|band| {
                self.output_band_to_input_band_idx
                    .get(*band as usize)
                    .copied()
            })
            .collect::<Option<Vec<_>>>()
            .ok_or(BandFilterError::BandsCannotBeMappedToInput)?;

        let query = query.select_attributes(BandSelection::new(input_bands.clone())?);

        // Create a mapping from input band index to output band index
        let mut input_to_output_band: std::collections::HashMap<u32, u32> =
            std::collections::HashMap::new();
        for (output_idx, input_idx) in input_bands.iter().enumerate() {
            input_to_output_band.insert(*input_idx, output_idx as u32);
        }

        let stream = self.source.query(query, ctx).await?;

        // Remap band indices in the returned tiles
        Ok(stream
            .filter_map(move |result| {
                let input_to_output_band = input_to_output_band.clone();
                async move {
                    match result {
                        Ok(mut tile) => {
                            if let Some(&output_band) = input_to_output_band.get(&tile.band) {
                                tile.band = output_band;
                                Some(Ok(tile))
                            } else {
                                warn!("BandFilter: Received tile for band {} which is not in the selected bands, skipping. This is a bug.", tile.band);
                                None
                            }
                        }
                        Err(e) => Some(Err(e)),
                    }
                }
            })
            .boxed())
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

#[async_trait]
impl<S> RasterQueryProcessor for BandFilterProcessor<S>
where
    S: RasterQueryProcessor,
{
    type RasterType = S::RasterType;

    async fn _time_query<'a>(
        &'a self,
        query: geoengine_datatypes::primitives::TimeInterval,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<geoengine_datatypes::primitives::TimeInterval>>> {
        self.source.time_query(query, ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        engine::{
            MockExecutionContext, RasterBandDescriptor, RasterBandDescriptors,
            SpatialGridDescriptor, TimeDescriptor,
        },
        mock::{MockRasterSource, MockRasterSourceParams},
    };
    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::{CacheHint, Coordinate2D, TimeInterval},
        raster::{
            BoundedGrid, GeoTransform, Grid2D, GridBoundingBox2D, GridShape2D, MaskedGrid2D,
            RasterDataType, TileInformation, TilesEqualIgnoringCacheHint,
        },
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_filters_bands() {
        let tile_size_in_pixels = GridShape2D::new_2d(2, 2);
        let time_interval = TimeInterval::default();

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(Some(time_interval)),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
                tile_size_in_pixels.bounding_box(),
            ),
            bands: RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new_unitless("red".into()),
                RasterBandDescriptor::new_unitless("green".into()),
                RasterBandDescriptor::new_unitless("blue".into()),
            ])
            .unwrap(),
        };

        let tile_info = TileInformation {
            global_geo_transform: TestDefault::test_default(),
            global_tile_position: [0, 0].into(),
            tile_size_in_pixels,
        };

        let band_0: MaskedGrid2D<u8> = Grid2D::new(tile_size_in_pixels, vec![1_u8, 2, 3, 4])
            .unwrap()
            .into();
        let band_1: MaskedGrid2D<u8> = Grid2D::new(tile_size_in_pixels, vec![5_u8, 6, 7, 8])
            .unwrap()
            .into();
        let band_2: MaskedGrid2D<u8> = Grid2D::new(tile_size_in_pixels, vec![9_u8, 10, 11, 12])
            .unwrap()
            .into();

        let data = vec![
            RasterTile2D::new_with_tile_info(
                time_interval,
                tile_info,
                0,
                band_0.into(),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                time_interval,
                tile_info,
                1,
                band_1.into(),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                time_interval,
                tile_info,
                2,
                band_2.into(),
                CacheHint::default(),
            ),
        ];

        let mock_source = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let band_filter: Box<dyn RasterOperator> = BandFilter {
            params: BandFilterParams {
                bands: BandsByNameOrIndex::Name(vec!["red".to_string(), "blue".to_string()]),
            },
            sources: SingleRasterSource {
                raster: mock_source,
            },
        }
        .boxed();

        let execution_context = MockExecutionContext::new_with_tiling_spec(
            geoengine_datatypes::raster::TilingSpecification::new(tile_size_in_pixels),
        );
        let query_context = execution_context.mock_query_context_test_default();

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([0, 0], [1, 1]).unwrap(),
            time_interval,
            [0, 1].try_into().unwrap(),
        );

        let initialized = band_filter
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let qp = initialized.query_processor().unwrap().get_u8().unwrap();

        let result = qp
            .raster_query(query_rect, &query_context)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        // Expected: band 0 (red) and band 1 (blue from original band 2)
        let mut expected_band_0 = data[0].clone();
        expected_band_0.band = 0;
        let mut expected_band_1 = data[2].clone();
        expected_band_1.band = 1;
        let expected = vec![expected_band_0, expected_band_1];

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));

        assert_eq!(
            initialized.result_descriptor().bands,
            vec![
                RasterBandDescriptor::new_unitless("red".into()),
                RasterBandDescriptor::new_unitless("blue".into()),
            ]
            .try_into()
            .unwrap()
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_filters_bands_by_index() {
        let tile_size_in_pixels = GridShape2D::new_2d(2, 2);
        let time_interval = TimeInterval::default();

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(Some(time_interval)),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
                tile_size_in_pixels.bounding_box(),
            ),
            bands: RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new_unitless("band0".into()),
                RasterBandDescriptor::new_unitless("band1".into()),
                RasterBandDescriptor::new_unitless("band2".into()),
            ])
            .unwrap(),
        };

        let tile_info = TileInformation {
            global_geo_transform: TestDefault::test_default(),
            global_tile_position: [0, 0].into(),
            tile_size_in_pixels,
        };

        let band_0: MaskedGrid2D<u8> = Grid2D::new(tile_size_in_pixels, vec![1_u8, 2, 3, 4])
            .unwrap()
            .into();
        let band_1: MaskedGrid2D<u8> = Grid2D::new(tile_size_in_pixels, vec![5_u8, 6, 7, 8])
            .unwrap()
            .into();
        let band_2: MaskedGrid2D<u8> = Grid2D::new(tile_size_in_pixels, vec![9_u8, 10, 11, 12])
            .unwrap()
            .into();

        let data = vec![
            RasterTile2D::new_with_tile_info(
                time_interval,
                tile_info,
                0,
                band_0.into(),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                time_interval,
                tile_info,
                1,
                band_1.into(),
                CacheHint::default(),
            ),
            RasterTile2D::new_with_tile_info(
                time_interval,
                tile_info,
                2,
                band_2.into(),
                CacheHint::default(),
            ),
        ];

        let mock_source = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let band_filter: Box<dyn RasterOperator> = BandFilter {
            params: BandFilterParams {
                bands: BandsByNameOrIndex::Index(vec![1, 2]),
            },
            sources: SingleRasterSource {
                raster: mock_source,
            },
        }
        .boxed();

        let execution_context = MockExecutionContext::new_with_tiling_spec(
            geoengine_datatypes::raster::TilingSpecification::new(tile_size_in_pixels),
        );
        let query_context = execution_context.mock_query_context_test_default();

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([0, 0], [1, 1]).unwrap(),
            time_interval,
            [0, 1].try_into().unwrap(),
        );

        let initialized = band_filter
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let qp = initialized.query_processor().unwrap().get_u8().unwrap();

        let result = qp
            .raster_query(query_rect, &query_context)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        // Expected: band 0 (from original band 1) and band 1 (from original band 2)
        let mut expected_band_0 = data[1].clone();
        expected_band_0.band = 0;
        let mut expected_band_1 = data[2].clone();
        expected_band_1.band = 1;
        let expected = vec![expected_band_0, expected_band_1];

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));

        assert_eq!(
            initialized.result_descriptor().bands,
            vec![
                RasterBandDescriptor::new_unitless("band1".into()),
                RasterBandDescriptor::new_unitless("band2".into()),
            ]
            .try_into()
            .unwrap()
        );
    }

    #[test]
    fn test_serde_by_name() {
        let params = BandFilterParams {
            bands: BandsByNameOrIndex::Name(vec!["red".to_string(), "green".to_string()]),
        };

        let json = serde_json::to_string(&params).unwrap();
        assert_eq!(json, r#"{"bands":["red","green"]}"#);

        let deserialized: BandFilterParams = serde_json::from_str(&json).unwrap();
        assert_eq!(params, deserialized);
    }

    #[test]
    fn test_serde_by_index() {
        let params = BandFilterParams {
            bands: BandsByNameOrIndex::Index(vec![0, 2, 4]),
        };

        let json = serde_json::to_string(&params).unwrap();
        assert_eq!(json, r#"{"bands":[0,2,4]}"#);

        let deserialized: BandFilterParams = serde_json::from_str(&json).unwrap();
        assert_eq!(params, deserialized);
    }
}
