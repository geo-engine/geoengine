use std::mem;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{future, StreamExt};
use geoengine_datatypes::primitives::{
    partitions_extent, time_interval_extent, Measurement, RasterQueryRectangle, SpatialPartition2D,
    SpatialResolution,
};
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
    CreateSpan, ExecutionContext, InitializedRasterOperator, MultipleRasterSources, Operator,
    OperatorName, QueryContext, QueryProcessor, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, TypedRasterQueryProcessor,
};
use crate::util::stream_zip::StreamVectorZip;
use crate::util::Result;
use futures::stream::BoxStream;
use RasterDataType::F32 as RasterOut;

use snafu::Snafu;
use tracing::{span, Level};
use TypedRasterQueryProcessor::F32 as QueryProcessorOut;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(error))]
pub enum XGBoostModuleError {
    #[snafu(display("The XGBoost library could not complete the operation successfully.",))]
    LibraryError { source: XGBError },

    #[snafu(display("Couldn't parse the model file contents.",))]
    ModelFileParsingError { source: std::io::Error },

    #[snafu(display("Couldn't create a booster instance from the content of the model file.",))]
    LoadBoosterFromModelError { source: XGBError },

    #[snafu(display("Couldn't generate a xgboost dmatrix from the given data.",))]
    CreateDMatrixError { source: XGBError },

    #[snafu(display("Couldn't calculate predictions from the given data.",))]
    PredictionError { source: XGBError },

    #[snafu(display("Couldn't get a base tile.",))]
    BaseTileError,

    #[snafu(display("No input data error. At least one raster is required.",))]
    NoInputData,

    #[snafu(display("There was an error with the creation of a new grid.",))]
    DataTypesError {
        source: geoengine_datatypes::error::Error,
    },

    #[snafu(display("There was an error with the joining of tokio tasks.",))]
    TokioJoinError { source: tokio::task::JoinError },
}

impl From<std::io::Error> for XGBoostModuleError {
    fn from(source: std::io::Error) -> Self {
        Self::ModelFileParsingError { source }
    }
}

impl From<XGBError> for XGBoostModuleError {
    fn from(source: XGBError) -> Self {
        Self::LibraryError { source }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct XgboostParams {
    model_sub_path: PathBuf,
    no_data_value: f32,
}

pub type XgboostOperator = Operator<XgboostParams, MultipleRasterSources>;

impl OperatorName for XgboostOperator {
    const TYPE_NAME: &'static str = "XgboostOperator";
}

pub struct InitializedXgboostOperator {
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
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let model = context.read_ml_model(self.params.model_sub_path).await?;

        let init_rasters = future::try_join_all(
            self.sources
                .rasters
                .iter()
                .map(|raster| raster.clone().initialize(context)),
        )
        .await?;

        let input = init_rasters.get(0).context(self::error::NoInputData)?;

        let spatial_reference = input.result_descriptor().spatial_reference;

        let in_descriptors = init_rasters
            .iter()
            .map(InitializedRasterOperator::result_descriptor)
            .collect::<Vec<_>>();

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
            measurement: Measurement::Unitless,
        };

        let initialized_operator = InitializedXgboostOperator {
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
        let tile = bands_of_tile.get(0).context(self::error::BaseTile)?;

        // gather the data
        let grid_shape = tile.grid_shape();
        let n_bands = bands_of_tile.len() as i32;
        let props = tile.properties.clone(); // = &tile.properties;
        let time = tile.time;
        let tile_position = tile.tile_position;
        let global_geo_transform = tile.global_geo_transform;
        let ndv = self.no_data_value;

        let predicted_grid = crate::util::spawn_blocking(move || {
            process_tile(
                bands_of_tile,
                &pool,
                &model_content,
                grid_shape,
                n_bands as usize,
                ndv,
            )
        })
        .await
        .context(error::TokioJoin)??;

        let rt: BaseTile<GridOrEmpty<GridShape<[usize; 2]>, f32>> =
            RasterTile2D::new_with_properties(
                time,
                tile_position,
                global_geo_transform,
                predicted_grid.into(),
                props.clone(),
            );

        Ok(rt)
    }
}

fn process_tile(
    bands_of_tile: Vec<RasterTile2D<f32>>,
    pool: &ThreadPool,
    model_file: &Arc<String>,
    grid_shape: GridShape<[usize; 2]>,
    n_bands: usize,
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
        let chunk_size = grid_shape.number_of_elements() * n_bands;

        let res: Vec<Vec<f32>> = pixels
            .par_chunks(chunk_size)
            .map(|elem| {
                // get xgboost style matrices
                let xg_matrix = DMatrix::from_col_major_f32(
                    elem,
                    mem::size_of::<f32>() * n_bands,
                    mem::size_of::<f32>(),
                    grid_shape.number_of_elements(),
                    n_bands,
                    -1, // TODO: add this to settings.toml: # of threads for xgboost to use
                    nan_val,
                )
                .context(self::error::CreateDMatrix)?;

                let mut out_dim: u64 = 0;

                let bst = Booster::load_buffer(model_file.as_bytes())
                    .context(self::error::LoadBoosterFromModel)?;

                // measure time for prediction
                let predictions: Result<Vec<f32>, XGBError> = bst.predict_from_dmat(
                    &xg_matrix,
                    &[grid_shape.number_of_elements() as u64, n_bands as u64],
                    &mut out_dim,
                );

                predictions.map_err(|xg_err| XGBoostModuleError::PredictionError { source: xg_err })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let predictions_flat: Vec<f32> = res.into_iter().flatten().collect();

        Grid2D::new(grid_shape, predictions_flat).context(self::error::DataTypes)
    })
}

#[async_trait]
impl<Q> QueryProcessor for XgboostProcessor<Q>
where
    Q: QueryProcessor<Output = RasterTile2D<f32>, SpatialBounds = SpatialPartition2D>,
{
    type Output = RasterTile2D<PixelOut>;
    type SpatialBounds = SpatialPartition2D;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let model_content = Arc::new(self.model.clone());

        let mut band_buffer = Vec::new();

        for band in &self.sources {
            let stream = band.query(query, ctx).await?;
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
        RasterOperator, RasterResultDescriptor,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};

    use futures::StreamExt;
    use geoengine_datatypes::primitives::{
        Measurement, RasterQueryRectangle, SpatialPartition2D, SpatialResolution, TimeInterval,
    };

    use geoengine_datatypes::raster::{
        Grid2D, GridOrEmpty, RasterDataType, RasterTile2D, TileInformation, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::test::TestDefault;
    use ndarray::{arr2, ArrayBase, Dim, OwnedRepr};
    use xgboost_rs::{Booster, DMatrix};

    use crate::util::Result;
    use std::collections::HashMap;
    use std::path::PathBuf;

    use super::{XgboostOperator, XgboostParams};

    /// Just a helper method to make the code less cluttery.
    fn zip_bands(b1: &[f32], b2: &[f32], b3: &[f32]) -> Vec<[f32; 3]> {
        b1.iter()
            .zip(b2.iter())
            .zip(b3.iter())
            .map(|((x, y), z)| [*x, *y, *z])
            .collect::<Vec<[f32; 3]>>()
    }

    /// Helper method to generate a raster with two tiles.
    fn make_double_raster(t1: Vec<i32>, t2: Vec<i32>) -> Box<dyn RasterOperator> {
        let raster_tiles = vec![
            RasterTile2D::<i32>::new_with_tile_info(
                TimeInterval::new_unchecked(0, 1),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [5, 5].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([5, 5].into(), t1).unwrap()),
            ),
            RasterTile2D::<i32>::new_with_tile_info(
                TimeInterval::new_unchecked(0, 1),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [5, 5].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([5, 5].into(), t2).unwrap()),
            ),
        ];

        MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        }
        .boxed()
    }

    fn make_raster(tile: Vec<i32>) -> Box<dyn RasterOperator> {
        // green raster:
        // 255, 255,   0,   0,   0
        // 255, 255,   0,   0,   0
        //   0,   0,   0,   0,   0
        //   0,   0,   0, 255, 255
        //   0,   0,   0, 255, 255
        let raster_tiles = vec![RasterTile2D::<i32>::new_with_tile_info(
            TimeInterval::new_unchecked(0, 1),
            TileInformation {
                global_tile_position: [-1, 0].into(),
                tile_size_in_pixels: [5, 5].into(),
                global_geo_transform: TestDefault::test_default(),
            },
            GridOrEmpty::from(Grid2D::new([5, 5].into(), tile).unwrap()),
        )];

        MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        }
        .boxed()
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
    async fn multi_tile_test() {
        // 255, 255,   0,   0,   0  ||  0,   0,   0, 255, 255
        // 255, 255,   0,   0,   0  ||  0,   0,   0, 255, 255
        // 255, 255,   0,   0,   0  ||  0,   0,   0, 255, 255
        // 255, 255,   0,   0,   0  ||  0,   0,   0, 255, 255
        // 255, 255,   0,   0,   0  ||  0,   0,   0, 255, 255
        let green = make_double_raster(
            vec![
                255, 255, 0, 0, 0, 255, 255, 0, 0, 0, 255, 255, 0, 0, 0, 255, 255, 0, 0, 0, 255,
                255, 0, 0, 0,
            ],
            vec![
                0, 0, 0, 255, 255, 0, 0, 0, 255, 255, 0, 0, 0, 255, 255, 0, 0, 0, 255, 255, 0, 0,
                0, 255, 255,
            ],
        );

        //   0,   0,   0, 255, 255  ||  255, 255,   0,   0,   0
        //   0,   0,   0, 255, 255  ||  255, 255,   0,   0,   0
        //   0,   0,   0,   0,   0  ||    0,   0,   0,   0,   0
        //   0,   0,   0, 255, 255  ||  255, 255,   0,   0,   0
        //   0,   0,   0, 255, 255  ||  255, 255,   0,   0,   0
        let blue = make_double_raster(
            vec![
                0, 0, 0, 255, 255, 0, 0, 0, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 0, 0, 0,
                255, 255,
            ],
            vec![
                255, 255, 0, 0, 0, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 0, 0, 0, 255, 255,
                0, 0, 0,
            ],
        );

        //   0,   0, 255,   0,   0  ||     0,   0, 255,   0,   0
        //   0,   0, 255,   0,   0  ||     0,   0, 255,   0,   0
        //   0,   0, 255, 255, 255  ||   255, 255,   0,   0,   0
        //   0,   0, 255,   0,   0  ||     0,   0, 255,   0,   0
        //   0,   0, 255,   0,   0  ||     0,   0, 255,   0,   0
        let temp = make_double_raster(
            vec![
                0, 0, 255, 0, 0, 0, 0, 255, 0, 0, 0, 0, 255, 255, 255, 0, 0, 255, 0, 0, 0, 0, 255,
                0, 0,
            ],
            vec![
                0, 0, 255, 0, 0, 0, 0, 255, 0, 0, 255, 255, 255, 0, 0, 0, 0, 255, 0, 0, 0, 0, 255,
                0, 0,
            ],
        );

        let srcs = vec![green, blue, temp];

        let model_path = PathBuf::from(test_data!("pro/ml/xgboost/s2_10m_de_marburg/model.json"));

        let xg = XgboostOperator {
            params: XgboostParams {
                model_sub_path: model_path.clone(),
                no_data_value: f32::NAN,
            },
            sources: MultipleRasterSources { rasters: srcs },
        };

        let mut exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [5, 5].into(),
        ));

        exe_ctx
            .initialize_ml_model(model_path)
            .expect("The model file should be available.");

        let op = RasterOperator::boxed(xg)
            .initialize(&exe_ctx)
            .await
            .unwrap();

        let processor = op.query_processor().unwrap().get_f32().unwrap();

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((0., 5.).into(), (10., 0.).into()).unwrap(),
            time_interval: TimeInterval::new_unchecked(0, 1),
            spatial_resolution: SpatialResolution::one(),
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
        // 0, 0, 2, 1, 1  ||  1, 1, 2, 0, 0
        // 0, 0, 2, 1, 1  ||  1, 1, 2, 0, 0
        // 0, 0, 2, 2, 2  ||  2, 2, 2, 2, 2
        // 0, 0, 2, 1, 1  ||  1, 1, 2, 0, 0
        // 0, 0, 2, 1, 1  ||  1, 1, 2, 0, 0

        let expected = vec![
            0.0, 0.0, 2.0, 1.0, 1.0, 0.0, 0.0, 2.0, 1.0, 1.0, 0.0, 0.0, 2.0, 2.0, 2.0, 0.0, 0.0,
            2.0, 1.0, 1.0, 0.0, 0.0, 2.0, 1.0, 1.0, 1.0, 1.0, 2.0, 0.0, 0.0, 1.0, 1.0, 2.0, 0.0,
            0.0, 2.0, 2.0, 2.0, 0.0, 0.0, 1.0, 1.0, 2.0, 0.0, 0.0, 1.0, 1.0, 2.0, 0.0, 0.0,
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

                let mut initial_training_config: HashMap<&str, &str> = HashMap::new();

                initial_training_config.insert("validate_parameters", "1");
                initial_training_config.insert("process_type", "default");
                initial_training_config.insert("tree_method", "hist");
                initial_training_config.insert("max_depth", "10");
                initial_training_config.insert("objective", "multi:softmax");
                initial_training_config.insert("num_class", "4");
                initial_training_config.insert("eta", "0.75");

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

                let mut update_training_config: HashMap<&str, &str> = HashMap::new();

                update_training_config.insert("validate_parameters", "1");
                update_training_config.insert("process_type", "update");
                update_training_config.insert("updater", "refresh");
                update_training_config.insert("refresh_leaf", "true");
                update_training_config.insert("objective", "multi:softmax");
                update_training_config.insert("num_class", "4");
                update_training_config.insert("max_depth", "15");

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
        //          GREEN           ||           BLUE             ||        TEMP
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
        // 255, 255,   0,   0,   0
        // 255, 255,   0,   0,   0
        //   0,   0,   0,   0,   0
        //   0,   0,   0, 255, 255
        //   0,   0,   0, 255, 255
        let green = make_raster(vec![
            255, 255, 0, 0, 0, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 0, 0, 0, 255,
            255,
        ]);

        //   0,   0,   0, 255, 255
        //   0,   0,   0, 255, 255
        //   0,   0,   0,   0,   0
        // 255, 255,   0,   0,   0
        // 255, 255,   0,   0,   0
        let blue = make_raster(vec![
            0, 0, 0, 255, 255, 0, 0, 0, 255, 255, 0, 0, 0, 0, 0, 255, 255, 0, 0, 0, 255, 255, 0, 0,
            0,
        ]);

        //   15,  15,  60,   5,   5
        //   15,  15,  60,   5,   5
        //   60,  60,  60,  60,  60
        //    5,   5,  60,  15,  15
        //    5,   5,  60,  15,  15
        let temp = make_raster(vec![
            15, 15, 60, 5, 5, 15, 15, 60, 5, 5, 60, 60, 60, 60, 60, 5, 5, 60, 15, 15, 5, 5, 60, 15,
            15,
        ]);

        let srcs = vec![green, blue, temp];

        let model_path = PathBuf::from(test_data!("pro/ml/xgboost/s2_10m_de_marburg/model.json"));

        let xg = XgboostOperator {
            params: XgboostParams {
                model_sub_path: model_path.clone(),
                no_data_value: -1_000.,
            },
            sources: MultipleRasterSources { rasters: srcs },
        };

        let mut exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [5, 5].into(),
        ));

        exe_ctx
            .initialize_ml_model(model_path)
            .expect("The model file should be available.");

        let op = RasterOperator::boxed(xg)
            .initialize(&exe_ctx)
            .await
            .unwrap();

        let processor = op.query_processor().unwrap().get_f32().unwrap();

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((0., 5.).into(), (5., 0.).into()).unwrap(),
            time_interval: TimeInterval::new_unchecked(0, 1),
            spatial_resolution: SpatialResolution::one(),
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
            0., 0., 2., 1., 1., 0., 0., 2., 1., 1., 2., 2., 2., 2., 2., 1., 1., 2., 0., 0., 1., 1.,
            2., 0., 0.,
        ];

        assert_eq!(all_pixels, expected);
    }
}
