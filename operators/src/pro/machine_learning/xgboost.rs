use std::mem;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use geoengine_datatypes::primitives::{
    partitions_extent, time_interval_extent, RasterQueryRectangle, SpatialPartition2D,
    SpatialResolution,
};
use geoengine_datatypes::primitives::{BandSelection, CacheHint};
use geoengine_datatypes::pro::MlModelId;
use geoengine_datatypes::raster::{
    BaseTile, Grid2D, GridOrEmpty, GridShape, GridShapeAccess, GridSize, RasterDataType,
    RasterTile2D,
};

use rayon::prelude::ParallelIterator;
use rayon::slice::ParallelSlice;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use xgboost_rs::{Booster, DMatrix, XGBError};

use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources,
    MultipleRasterSources, Operator, OperatorName, QueryContext, QueryProcessor,
    RasterBandDescriptors, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
    TypedRasterQueryProcessor, WorkflowOperatorPath,
};
use crate::pro::xg_error::error as XgModuleError;
use crate::util::stream_zip::StreamVectorZip;
use crate::util::Result;
use futures::stream::BoxStream;
use RasterDataType::F32 as RasterOut;

use TypedRasterQueryProcessor::F32 as QueryProcessorOut;

use super::xg_error::XGBoostModuleError;
use super::MlModelAccess;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct XgboostParams {
    pub model_id: MlModelId,
    pub no_data_value: f32,
}

pub type XgboostOperator = Operator<XgboostParams, MultipleRasterSources>;

impl OperatorName for XgboostOperator {
    const TYPE_NAME: &'static str = "XgboostOperator";
}

pub struct InitializedXgboostOperator {
    name: CanonicOperatorName,
    result_descriptor: RasterResultDescriptor,
    sources: Vec<Box<dyn InitializedRasterOperator>>,
    model: String,
    no_data_value: f32,
}

type PixelOut = f32;

#[typetag::serde]
#[async_trait]
impl RasterOperator for XgboostOperator {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);

        let model_access = context
            .extensions()
            .get::<MlModelAccess>()
            .expect("`MlModelAccess` extension should be set during `ProContext` creation");

        let model = model_access
            .load_ml_model_by_id(self.params.model_id)
            .await?;

        let initialized_sources = self.sources.initialize_sources(path, context).await?;

        let init_rasters = initialized_sources.rasters;

        let input = init_rasters.get(0).context(XgModuleError::NoInputData)?;

        let spatial_reference = input.result_descriptor().spatial_reference;

        let in_descriptors = init_rasters
            .iter()
            .map(InitializedRasterOperator::result_descriptor)
            .collect::<Vec<_>>();

        // TODO: implement multi-band functionality and remove this check
        ensure!(
            in_descriptors.iter().all(|r| r.bands.len() == 1),
            crate::error::OperatorDoesNotSupportMultiBandsSourcesYet {
                operator: XgboostOperator::TYPE_NAME
            }
        );

        for other_spatial_reference in in_descriptors.iter().skip(1).map(|rd| rd.spatial_reference)
        {
            ensure!(
                spatial_reference == other_spatial_reference,
                crate::error::InvalidSpatialReference {
                    expected: spatial_reference,
                    found: other_spatial_reference,
                }
            );
        }

        let time = time_interval_extent(in_descriptors.iter().map(|d| d.time));
        let bbox = partitions_extent(in_descriptors.iter().map(|d| d.bbox));

        let resolution = in_descriptors
            .iter()
            .map(|d| d.resolution)
            .reduce(|a, b| match (a, b) {
                (Some(a), Some(b)) => {
                    Some(SpatialResolution::new_unchecked(a.x.min(b.x), a.y.min(b.y)))
                }
                _ => None,
            })
            .flatten();

        let out_desc = RasterResultDescriptor {
            data_type: RasterOut,
            time,
            bbox,
            resolution,
            spatial_reference,
            bands: RasterBandDescriptors::new_single_band(),
        };

        let initialized_operator = InitializedXgboostOperator {
            name,
            result_descriptor: out_desc,
            sources: init_rasters,
            model,
            no_data_value: self.params.no_data_value,
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(XgboostOperator);
}

impl InitializedRasterOperator for InitializedXgboostOperator {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let vec_of_rqps: Vec<Box<dyn RasterQueryProcessor<RasterType = f32>>> = self
            .sources
            .iter()
            .map(
                |init_raster| -> Result<
                    Box<dyn RasterQueryProcessor<RasterType = f32>>,
                    crate::error::Error,
                > {
                    let typed_raster_qp = init_raster
                        .query_processor()
                        .map_err(|_| crate::error::Error::QueryProcessor)?;
                    let converted = typed_raster_qp.into_f32();
                    Ok(converted)
                },
            )
            .collect::<Result<Vec<_>, _>>()?;

        Ok(QueryProcessorOut(Box::new(XgboostProcessor::new(
            vec_of_rqps,
            self.model.clone(),
            self.no_data_value,
        ))))
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }
}

struct XgboostProcessor<Q>
where
    Q: RasterQueryProcessor<RasterType = f32>,
{
    sources: Vec<Q>,
    model: String,
    no_data_value: f32,
}

impl<Q> XgboostProcessor<Q>
where
    Q: RasterQueryProcessor<RasterType = f32>,
{
    pub fn new(sources: Vec<Q>, model_file_content: String, no_data_value: f32) -> Self {
        Self {
            sources,
            model: model_file_content,
            no_data_value,
        }
    }

    async fn process_tile_async(
        &self,
        bands_of_tile: Vec<RasterTile2D<f32>>,
        model_content: Arc<String>,
        pool: Arc<ThreadPool>,
    ) -> Result<BaseTile<GridOrEmpty<GridShape<[usize; 2]>, f32>>, XGBoostModuleError> {
        let tile = bands_of_tile.get(0).context(XgModuleError::BaseTile)?;

        // gather the data
        let grid_shape = tile.grid_shape();
        let n_bands = bands_of_tile.len() as u32;
        let props = tile.properties.clone(); // = &tile.properties;
        let time = tile.time;
        let tile_position = tile.tile_position;
        let global_geo_transform = tile.global_geo_transform;
        let ndv = self.no_data_value;

        let cache_hint = bands_of_tile
            .iter()
            .fold(CacheHint::max_duration(), |acc, bt| {
                acc.merged(&bt.cache_hint)
            });

        let predicted_grid = crate::util::spawn_blocking(move || {
            process_tile(
                bands_of_tile,
                &pool,
                &model_content,
                grid_shape,
                n_bands,
                ndv,
            )
        })
        .await
        .context(XgModuleError::TokioJoin)??;

        let rt: BaseTile<GridOrEmpty<GridShape<[usize; 2]>, f32>> =
            RasterTile2D::new_with_properties(
                time,
                tile_position,
                0,
                global_geo_transform,
                predicted_grid.into(),
                props.clone(),
                cache_hint,
            );

        Ok(rt)
    }
}

fn process_tile(
    bands_of_tile: Vec<RasterTile2D<f32>>,
    pool: &ThreadPool,
    model_file: &Arc<String>,
    grid_shape: GridShape<[usize; 2]>,
    n_bands: u32,
    nan_val: f32,
) -> Result<geoengine_datatypes::raster::Grid<GridShape<[usize; 2]>, f32>, XGBoostModuleError> {
    pool.install(|| {
        // get the actual tile data
        let band_data: Vec<_> = bands_of_tile
            .into_iter()
            .map(|band| {
                let mat_tile = band.into_materialized_tile();
                mat_tile.grid_array.inner_grid.data
            })
            .collect();

        let n_rows = grid_shape.axis_size_x() * grid_shape.axis_size_y();
        // we need to reshape the data and take the i-th element from each band.
        let i = (0..n_rows).map(|row_idx| band_data.iter().flatten().skip(row_idx).step_by(n_rows));
        let pixels: Vec<_> = i.flatten().copied().collect::<Vec<f32>>();

        // TODO: clarify: as of right now, this is not doing anything
        // because xgboost seems to be the fastest, when the chunks are big.
        let chunk_size = grid_shape.number_of_elements() * n_bands as usize;

        let res: Vec<Vec<f32>> = pixels
            .par_chunks(chunk_size)
            .map(|elem| {
                // get xgboost style matrices
                let xg_matrix = DMatrix::from_col_major_f32(
                    elem,
                    mem::size_of::<f32>() * n_bands as usize,
                    mem::size_of::<f32>(),
                    grid_shape.number_of_elements(),
                    n_bands as usize,
                    -1, // TODO: add this to settings.toml: # of threads for xgboost to use
                    nan_val,
                )
                .context(XgModuleError::CreateDMatrix)?;

                let mut out_dim: u64 = 0;

                let bst = Booster::load_buffer(model_file.as_bytes())
                    .context(XgModuleError::LoadBoosterFromModel)?;

                // measure time for prediction
                let predictions: Result<Vec<f32>, XGBError> = bst.predict_from_dmat(
                    &xg_matrix,
                    &[grid_shape.number_of_elements() as u64, u64::from(n_bands)],
                    &mut out_dim,
                );

                // TODO:
                // We now have a sequence of predicted values in numeric form.
                // If the original data had categorical classes with meaningful names,
                // we could map these numeric values back to their original names at this point.

                predictions.map_err(|xg_err| XGBoostModuleError::PredictionError { source: xg_err })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let predictions_flat: Vec<f32> = res.into_iter().flatten().collect();

        Grid2D::new(grid_shape, predictions_flat).context(XgModuleError::DataTypes)
    })
}

#[async_trait]
impl<Q> QueryProcessor for XgboostProcessor<Q>
where
    Q: QueryProcessor<
        Output = RasterTile2D<f32>,
        SpatialBounds = SpatialPartition2D,
        Selection = BandSelection,
    >,
{
    type Output = RasterTile2D<PixelOut>;
    type SpatialBounds = SpatialPartition2D;
    type Selection = BandSelection;
    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let model_content = Arc::new(self.model.clone());

        let mut band_buffer = Vec::new();

        for band in &self.sources {
            let stream = band.query(query.clone(), ctx).await?;
            band_buffer.push(stream);
        }

        let rs = StreamVectorZip::new(band_buffer).then(move |tile_vec| {
            let arc_clone = Arc::clone(&model_content);
            async move {
                let tile_vec = tile_vec.into_iter().collect::<Result<_, _>>()?;
                let processed_tile =
                    self.process_tile_async(tile_vec, arc_clone, ctx.thread_pool().clone());
                Ok(processed_tile.await?)
            }
        });

        Ok(rs.boxed())
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::{
        MockExecutionContext, MockQueryContext, MultipleRasterSources, QueryProcessor,
        RasterBandDescriptors, RasterOperator, RasterResultDescriptor, SourceOperator,
        WorkflowOperatorPath,
    };
    use crate::error::Error;
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use crate::pro::machine_learning::{LoadMlModel, MlModelAccess};
    use async_trait::async_trait;
    use geoengine_datatypes::pro::MlModelId;

    use futures::StreamExt;
    use geoengine_datatypes::primitives::{BandSelection, CacheHint};
    use geoengine_datatypes::primitives::{
        RasterQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval,
    };

    use geoengine_datatypes::raster::{
        Grid2D, GridShape, MaskedGrid2D, RasterDataType, RasterTile2D, TileInformation,
        TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::test::TestDefault;
    use ndarray::{arr2, ArrayBase, Dim, OwnedRepr};
    use xgboost_rs::{Booster, DMatrix};

    use crate::util::Result;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::str::FromStr;

    use super::{XgboostOperator, XgboostParams};

    struct MockModelAccess {
        models: HashMap<MlModelId, String>,
    }

    #[async_trait]
    impl LoadMlModel for MockModelAccess {
        async fn load_ml_model_by_id(&self, model_id: MlModelId) -> Result<String> {
            self.models
                .get(&model_id)
                .cloned()
                .ok_or(Error::MachineLearningModelNotFound)
        }
    }

    /// Initializes a machine learning model by reading its contents from a file specified by the given UUID-based `PathBuf`.
    /// The contents of the model file are expected to be in JSON format.
    pub fn mock_ml_model_persistance(
        exe_ctx: &mut MockExecutionContext,
        model_id: MlModelId,
    ) -> Result<()> {
        let model_defs_path = test_data!("pro/ml").to_owned();
        let model_path_uuid = PathBuf::from(model_id.to_string());
        let model_path = model_path_uuid.join("mock_model.json");

        let full_model_sub_path = model_defs_path.join(model_path);

        let full_model_path = test_data!(full_model_sub_path).to_owned();

        let model = std::fs::read_to_string(full_model_path)?;

        let mock_model_access: MlModelAccess = Box::new(MockModelAccess {
            models: [(model_id, model)].into_iter().collect(),
        });

        exe_ctx.extensions.insert(mock_model_access);

        Ok(())
    }

    /// Just a helper method to make the code less cluttery.
    fn zip_bands(b1: &[f32], b2: &[f32], b3: &[f32]) -> Vec<[f32; 3]> {
        b1.iter()
            .zip(b2.iter())
            .zip(b3.iter())
            .map(|((x, y), z)| [*x, *y, *z])
            .collect::<Vec<[f32; 3]>>()
    }

    /// Generates test data for a mock raster source.
    ///
    /// # Arguments
    ///
    /// * `data` - A 2D vector of integer data. Contains the data per tile
    /// * `tile_size_in_pixels` - A `GridShape` struct specifying the size of tiles to create.
    fn generate_raster_test_data_band_helper(
        data: Vec<Vec<i32>>,
        tile_size_in_pixels: GridShape<[usize; 2]>,
    ) -> SourceOperator<MockRasterSourceParams<i32>> {
        let n_pixels = data
            .get(0)
            .expect("could not access the first data element")
            .len();

        let n_tiles = data.len();

        let mut tiles = Vec::with_capacity(data.len());

        for (idx, values) in data.into_iter().enumerate() {
            let grid_data = Grid2D::new(tile_size_in_pixels, values).unwrap();
            let grid_mask = Grid2D::new(tile_size_in_pixels, vec![true; n_pixels]).unwrap();
            let masked_grid = MaskedGrid2D::new(grid_data, grid_mask).unwrap().into();

            let global_tile_position = match n_tiles {
                1 => [-1, 0].into(),
                _ => [-1, idx as isize].into(),
            };

            tiles.push(RasterTile2D::new_with_tile_info(
                TimeInterval::default(),
                TileInformation {
                    global_geo_transform: TestDefault::test_default(),
                    global_tile_position,
                    tile_size_in_pixels,
                },
                0,
                masked_grid,
                CacheHint::default(),
            ));
        }

        MockRasterSource {
            params: MockRasterSourceParams {
                data: tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
    }

    // Just a helper method to extract xgboost matrix generation code.
    fn make_xg_matrix(data_arr_2d: &ArrayBase<OwnedRepr<f32>, Dim<[usize; 2]>>) -> DMatrix {
        // define information needed for xgboost
        let strides_ax_0 = data_arr_2d.strides()[0] as usize;
        let strides_ax_1 = data_arr_2d.strides()[1] as usize;
        let byte_size_ax_0 = std::mem::size_of::<f32>() * strides_ax_0;
        let byte_size_ax_1 = std::mem::size_of::<f32>() * strides_ax_1;

        // get xgboost style matrices
        let xg_matrix = DMatrix::from_col_major_f32(
            data_arr_2d.as_slice_memory_order().unwrap(),
            byte_size_ax_0,
            byte_size_ax_1,
            25,
            3,
            -1,
            f32::NAN,
        )
        .unwrap();

        xg_matrix
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn multi_tile_test() {
        let red = generate_raster_test_data_band_helper(
            vec![
                vec![
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                ],
                vec![
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 0, 0, 0, //
                ],
            ],
            [5, 5].into(),
        )
        .boxed();

        let green = generate_raster_test_data_band_helper(
            vec![
                vec![
                    255, 255, 255, 255, 255, //
                    255, 0, 0, 0, 255, //
                    255, 0, 0, 0, 255, //
                    255, 0, 0, 0, 255, //
                    255, 255, 255, 255, 255, //
                ],
                vec![
                    0, 0, 255, 255, 255, //
                    0, 0, 255, 0, 0, //
                    0, 0, 0, 0, 0, //
                    0, 0, 255, 0, 0, //
                    255, 255, 255, 0, 0, //
                ],
            ],
            [5, 5].into(),
        )
        .boxed();

        let blue = generate_raster_test_data_band_helper(
            vec![
                vec![
                    0, 0, 0, 0, 0, //
                    0, 0, 255, 0, 0, //
                    0, 255, 255, 255, 0, //
                    0, 0, 255, 0, 0, //
                    0, 0, 0, 0, 0, //
                ],
                vec![
                    0, 255, 0, 0, 0, //
                    255, 0, 0, 255, 255, //
                    255, 255, 0, 255, 255, //
                    255, 255, 0, 0, 255, //
                    0, 0, 0, 255, 0, //
                ],
            ],
            [5, 5].into(),
        )
        .boxed();

        let temperature = generate_raster_test_data_band_helper(
            vec![
                vec![
                    15, 15, 15, 15, 15, //
                    15, 30, 5, 30, 15, //
                    15, 5, 5, 5, 15, //
                    15, 30, 5, 30, 15, //
                    15, 15, 15, 15, 15, //
                ],
                vec![
                    30, 5, 15, 15, 15, //
                    5, 30, 15, 5, 5, //
                    5, 5, 30, 5, 5, //
                    5, 5, 15, 30, 5, //
                    15, 15, 15, 5, 30, //
                ],
            ],
            [5, 5].into(),
        )
        .boxed();

        let srcs = vec![red, green, blue, temperature];

        let model_uuid_path = MlModelId::from_str("b764bf81-e21d-4eb8-bf01-fac9af13faee")
            .expect("Should have generated a ModelId from the given uuid string.");

        let xg = XgboostOperator {
            params: XgboostParams {
                model_id: model_uuid_path,
                no_data_value: f32::NAN,
            },
            sources: MultipleRasterSources { rasters: srcs },
        };

        let mut exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [5, 5].into(),
        ));

        mock_ml_model_persistance(&mut exe_ctx, model_uuid_path)
            .expect("The model file should be available.");

        let op = RasterOperator::boxed(xg)
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let processor = op.query_processor().unwrap().get_f32().unwrap();

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((0., 5.).into(), (10., 0.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(), // TODO
        };

        let query_ctx = MockQueryContext::test_default();

        let result_stream = processor.query(query_rect, &query_ctx).await.unwrap();

        let result: Vec<Result<RasterTile2D<f32>>> = result_stream.collect().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let mut all_pixels = Vec::new();

        for tile in result {
            let data_of_tile = tile.into_materialized_tile().grid_array.inner_grid.data;
            for pixel in &data_of_tile {
                all_pixels.push(pixel.round());
            }
        }

        // expected result tiles
        //    tile 1      ||      tile 2
        // --------------------------------
        // 1, 1, 1, 1, 1  ||  0, 2, 1, 1, 1
        // 1, 0, 2, 0, 1  ||  2, 0, 1, 2, 2
        // 1, 2, 2, 2, 1  ||  2, 2, 0, 2, 2
        // 1, 0, 2, 0, 1  ||  2, 2, 1, 0, 2
        // 1, 1, 1, 1, 1  ||  1, 1, 1, 2, 0

        let expected = vec![
            // tile 1
            1.0, 1.0, 1.0, 1.0, 1.0, //
            1.0, 0.0, 2.0, 0.0, 1.0, //
            1.0, 2.0, 2.0, 2.0, 1.0, //
            1.0, 0.0, 2.0, 0.0, 1.0, //
            1.0, 1.0, 1.0, 1.0, 1.0, //
            // tile 2
            0.0, 2.0, 1.0, 1.0, 1.0, //
            2.0, 0.0, 1.0, 2.0, 2.0, //
            2.0, 2.0, 0.0, 2.0, 2.0, //
            2.0, 2.0, 1.0, 0.0, 2.0, //
            1.0, 1.0, 1.0, 2.0, 0.0, //
        ];

        assert_eq!(all_pixels, expected);
    }

    /// This test is used to verify, that xgboost is updating a model on consecutive tiles.
    // TODO: This test should be more extensive
    #[test]
    #[allow(clippy::too_many_lines)]
    fn xg_train_increment() {
        // DATA USED FOR TRAINING -------------
        //       raster green       ||        raster blue        ||        raster temp
        // 255, 255,   0,   0,   0  ||    0,   0,   0, 255, 255  ||  15,  15,  60,   5,   5
        // 255, 255,   0,   0,   0  ||    0,   0,   0, 255, 255  ||  15,  15,  60,   5,   5
        //   0,   0,   0,   0,   0  ||    0,   0, 100,   0,   0  ||  60,  60,   0,  60,  60
        //   0,   0,   0, 255, 255  ||  255, 255,   0,   0,   0  ||   5,   5,  60,  15,  15
        //   0,   0,   0, 255, 255  ||  255, 255,   0,   0,   0  ||   5,   5,  60,  15,  15

        // Class layout looks like this:
        // 0, 0, 2, 1, 1
        // 0, 0, 2, 1, 1
        // 2, 2, 3, 2, 2
        // 1, 1, 2, 0, 0
        // 1, 1, 2, 0, 0

        let b1 = &[
            255., 255., 0., 0., 0., 255., 255., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 255.,
            255., 0., 0., 0., 255., 255.,
        ];

        let b2 = &[
            0., 0., 0., 255., 255., 0., 0., 0., 255., 255., 0., 0., 100., 0., 0., 255., 255., 0.,
            0., 0., 255., 255., 0., 0., 0.,
        ];

        let b3 = &[
            15., 15., 60., 5., 5., 15., 15., 60., 5., 5., 60., 60., 0., 60., 60., 5., 5., 60., 15.,
            15., 5., 5., 60., 15., 15.,
        ];

        let data_arr_2d = arr2(zip_bands(b1, b2, b3).as_slice());

        // specification of the class labels for each datapoint
        let target_vec = arr2(&[
            [0., 0., 2., 1., 1.],
            [0., 0., 2., 1., 1.],
            [2., 2., 3., 2., 2.],
            [1., 1., 2., 0., 0.],
            [1., 1., 2., 0., 0.],
        ]);

        //       raster green       ||     raster blue          ||     raster temp
        //   0,   0,   0,   0,   0  || 100, 100,   0,   0, 255  ||   0,  0, 15, 15,  5
        //   0,   0,   0,   0,   0  || 100, 100, 100,   0,   0  ||   0,  0,  0, 15, 60
        //   0,   0,   0,   0,   0  ||   0, 100, 100, 100,   0  ||  15,  0,  0,  0, 15
        //   0,   0,   0,   0,   0  ||   0,   0, 100, 100, 100  ||  15, 15,  0,  0,  0
        //   0,   0,   0,   0,   0  ||   0,   0,   0, 100, 100  ||  15, 15, 15,  0,  0

        // Class layout looks like this:
        // 3, 3, 0, 0, 1
        // 3, 3, 3, 0, 2
        // 0, 3, 3, 3, 0
        // 0, 0, 3, 3, 3
        // 0, 0, 0, 3, 3

        let b1 = &[
            0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
            0., 0., 0.,
        ];

        let b2 = &[
            100., 100., 0., 0., 255., 100., 100., 100., 0., 0., 0., 100., 100., 100., 0., 0., 0.,
            100., 100., 100., 0., 0., 0., 100., 100.,
        ];

        let b3 = &[
            0., 0., 15., 15., 5., 0., 0., 0., 15., 60., 15., 0., 0., 0., 15., 15., 15., 0., 0., 0.,
            15., 15., 15., 0., 0.,
        ];

        let data_arr_2d2 = arr2(zip_bands(b1, b2, b3).as_slice());

        let target_vec2 = arr2(&[
            [3., 3., 0., 0., 1.],
            [3., 3., 3., 0., 2.],
            [0., 3., 3., 3., 0.],
            [0., 0., 3., 3., 3.],
            [0., 0., 0., 3., 3.],
        ]);

        // how many rounds should be trained?
        let training_rounds = 2;

        // setup data/model cache
        let mut booster_vec: Vec<Booster> = Vec::new();
        let mut matrix_vec: Vec<DMatrix> = Vec::new();

        for _ in 0..training_rounds {
            let mut xg_matrix = make_xg_matrix(&data_arr_2d);

            // set labels for the dmatrix
            xg_matrix
                .set_labels(target_vec.as_slice().unwrap())
                .unwrap();

            let mut xg_matrix2 = make_xg_matrix(&data_arr_2d2);

            // set labels for the dmatrix
            xg_matrix2
                .set_labels(target_vec2.as_slice().unwrap())
                .unwrap();

            // start the training process
            if booster_vec.is_empty() {
                // in the first iteration, there is no model yet.
                matrix_vec.push(xg_matrix);

                let mut initial_training_config: HashMap<String, String> = HashMap::new();

                initial_training_config.insert("validate_parameters".into(), "1".into());
                initial_training_config.insert("process_type".into(), "default".into());
                initial_training_config.insert("tree_method".into(), "hist".into());
                initial_training_config.insert("max_depth".into(), "10".into());
                initial_training_config.insert("objective".into(), "multi:softmax".into());
                initial_training_config.insert("num_class".into(), "4".into());
                initial_training_config.insert("eta".into(), "0.75".into());

                let evals = &[(matrix_vec.get(0).unwrap(), "train")];
                let bst = Booster::train(
                    Some(evals),
                    matrix_vec.get(0).unwrap(),
                    initial_training_config,
                    None, // <- No old model yet
                )
                .unwrap();

                // store the first booster
                booster_vec.push(bst);
            }
            // update training rounds
            else {
                // this is a consecutive iteration, so we need the last booster instance
                // to update the model
                let bst = booster_vec.pop().unwrap();

                let mut update_training_config: HashMap<String, String> = HashMap::new();

                update_training_config.insert("validate_parameters".into(), "1".into());
                update_training_config.insert("process_type".into(), "update".into());
                update_training_config.insert("updater".into(), "refresh".into());
                update_training_config.insert("refresh_leaf".into(), "true".into());
                update_training_config.insert("objective".into(), "multi:softmax".into());
                update_training_config.insert("num_class".into(), "4".into());
                update_training_config.insert("max_depth".into(), "15".into());

                let evals = &[(matrix_vec.get(0).unwrap(), "orig"), (&xg_matrix2, "train")];
                let bst_updated = Booster::train(
                    Some(evals),
                    &xg_matrix2,
                    update_training_config,
                    Some(bst), // <- this contains the last model which is now being updated
                )
                .unwrap();

                // store the new booster instance
                booster_vec.push(bst_updated);
            }
        }

        // lets use the trained model now to predict something
        let bst = booster_vec.pop().unwrap();

        // test tile looks like this:
        //         GREEN            ||           BLUE             ||        TEMP
        //   0,   0,   0,   0,   0  ||   100, 100, 100, 100, 100  ||  0,  0,  0,  0,  0
        //   0,   0, 255,   0,   0  ||   100,   0,   0, 255, 100  ||  0, 60, 15,  5,  0
        //   0, 255,   0, 255,   0  ||   100,   0,   0,   0, 100  ||  0, 15, 60, 15,  0
        //   0,   0, 255,   0,   0  ||   100, 255,   0,   0, 100  ||  0,  5, 15, 60,  0
        //   0,   0,   0,   0,   0  ||   100, 100, 100, 100, 100  ||  0,  0,  0,  0,  0

        // with class layout like this:
        // 3, 3, 3, 3, 3
        // 3, 2, 0, 1, 3
        // 3, 0, 2, 0, 3
        // 3, 1, 0, 2, 3
        // 3, 3, 3, 3, 3

        let b1 = &[
            0., 0., 0., 0., 0., 0., 0., 255., 0., 0., 0., 255., 0., 255., 0., 0., 0., 255., 0., 0.,
            0., 0., 0., 0., 0.,
        ];

        let b2 = &[
            100., 100., 100., 100., 100., 100., 0., 0., 255., 100., 100., 0., 0., 0., 100., 100.,
            255., 0., 0., 100., 100., 100., 100., 100., 100.,
        ];

        let b3 = &[
            0., 0., 0., 0., 0., 0., 60., 15., 5., 0., 0., 15., 60., 15., 0., 0., 5., 15., 60., 0.,
            0., 0., 0., 0., 0.,
        ];

        let test_data_arr_2d = arr2(zip_bands(b1, b2, b3).as_slice());
        let test_data = make_xg_matrix(&test_data_arr_2d);
        let result = bst.predict(&test_data).unwrap();

        // this result is not 100% desired, but most likely just a matter of a better or bigger training set
        let expected = vec![
            3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 0.0, 0.0, 3.0, 3.0, 3.0, 0.0, 0.0, 0.0, 3.0, 3.0, 3.0,
            0.0, 0.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0,
        ];

        assert_eq!(expected, result);
    }

    /// This test verifies, that xgboost is creating meaningful result tiles from a learned model.
    #[tokio::test]
    async fn xg_single_tile_test() {
        let red = generate_raster_test_data_band_helper(
            vec![vec![
                0, 0, 0, 0, 0, //
                0, 0, 0, 0, 0, //
                0, 0, 0, 0, 0, //
                0, 0, 0, 0, 0, //
                0, 0, 0, 0, 0, //
            ]],
            [5, 5].into(),
        )
        .boxed();

        let green = generate_raster_test_data_band_helper(
            vec![vec![
                255, 255, 0, 0, 0, //
                255, 255, 0, 0, 0, //
                0, 0, 0, 0, 0, //
                0, 0, 0, 255, 255, //
                0, 0, 0, 255, 255, //
            ]],
            [5, 5].into(),
        )
        .boxed();

        let blue = generate_raster_test_data_band_helper(
            vec![vec![
                0, 0, 0, 255, 255, //
                0, 0, 0, 255, 255, //
                0, 0, 0, 0, 0, //
                255, 255, 0, 0, 0, //
                255, 255, 0, 0, 0, //
            ]],
            [5, 5].into(),
        )
        .boxed();

        let temp = generate_raster_test_data_band_helper(
            vec![vec![
                15, 15, 30, 5, 5, //
                15, 15, 30, 5, 5, //
                30, 30, 30, 30, 30, //
                5, 5, 30, 15, 15, //
                5, 5, 30, 15, 15, //
            ]],
            [5, 5].into(),
        )
        .boxed();

        let srcs = vec![red, green, blue, temp];

        let model_uuid_path = MlModelId::from_str("b764bf81-e21d-4eb8-bf01-fac9af13faee")
            .expect("Should have generated a ModelId from the given uuid string.");

        let xg = XgboostOperator {
            params: XgboostParams {
                model_id: model_uuid_path,
                no_data_value: f32::NAN,
            },
            sources: MultipleRasterSources { rasters: srcs },
        };

        let mut exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [5, 5].into(),
        ));

        mock_ml_model_persistance(&mut exe_ctx, model_uuid_path)
            .expect("The model file should be available.");

        let op = RasterOperator::boxed(xg)
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let processor = op.query_processor().unwrap().get_f32().unwrap();

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((0., 5.).into(), (5., 0.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::one(),
            attributes: BandSelection::first(),
        };

        let query_ctx = MockQueryContext::test_default();

        let result_stream = processor.query(query_rect, &query_ctx).await.unwrap();

        let result: Vec<Result<RasterTile2D<f32>>> = result_stream.collect().await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let mut all_pixels = Vec::new();

        for tile in result {
            let data_of_tile = tile.into_materialized_tile().grid_array.inner_grid.data;
            for pixel in &data_of_tile {
                all_pixels.push(pixel.round());
            }
        }

        let expected = vec![
            1., 1., 0., 2., 2., //
            1., 1., 0., 2., 2., //
            0., 0., 0., 0., 0., //
            2., 2., 0., 1., 1., //
            2., 2., 0., 1., 1., //
        ];

        assert_eq!(all_pixels, expected);
    }
}
