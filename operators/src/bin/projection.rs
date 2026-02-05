use std::str::FromStr;

use futures::StreamExt;
use geoengine_datatypes::{
    dataset::{DataId, DatasetId, NamedData},
    hashmap,
    primitives::{
        BandSelection, CacheTtlSeconds, Coordinate2D, DateTimeParseFormat, RasterQueryRectangle,
        TimeGranularity, TimeInstance, TimeInterval, TimeStep,
    },
    raster::{
        GeoTransform, GridBoundingBox2D, GridSize, RasterDataType, RasterTile2D,
        TilingSpecification,
    },
    spatial_reference::{SpatialReference, SpatialReferenceAuthority},
    test_data,
    util::{Identifier, test::TestDefault},
};
use geoengine_operators::{
    engine::{
        MockExecutionContext, QueryContext, RasterBandDescriptors, RasterOperator,
        RasterResultDescriptor, SingleRasterOrVectorSource, SpatialGridDescriptor, TimeDescriptor,
        WorkflowOperatorPath,
    },
    processing::{DeriveOutRasterSpecsSource, Reprojection, ReprojectionParams},
    source::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalMetaDataRegular,
        GdalSource, GdalSourceParameters, GdalSourceTimePlaceholder, TimeReference,
    },
};

#[allow(clippy::too_many_lines)]
async fn actual() -> Result<Vec<RasterTile2D<u8>>, geoengine_operators::error::Error> {
    let tile_size_in_pixels = [200, 200].into();
    let data_geo_transform = GeoTransform::new(
        Coordinate2D::new(-20_037_508.342_789_244, 19_971_868.880_408_562),
        14_052.950_258_048_738,
        -14_057.881_117_788_405,
    );
    let data_bounds = GridBoundingBox2D::new([0, 0], [2840, 2850]).unwrap();
    let result_descriptor = RasterResultDescriptor {
        data_type: RasterDataType::U8,
        spatial_reference: SpatialReference::new(SpatialReferenceAuthority::Epsg, 3857).into(),
        time: TimeDescriptor::new_regular_with_epoch(
            Some(TimeInterval::new_unchecked(
                TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
                TimeInstance::from_str("2014-07-01T00:00:00.000Z").unwrap(),
            )),
            TimeStep::months(1).unwrap(),
        ),
        spatial_grid: SpatialGridDescriptor::source_from_parts(data_geo_transform, data_bounds),
        bands: RasterBandDescriptors::new_single_band(),
    };

    let m = GdalMetaDataRegular {
        data_time: TimeInterval::new_unchecked(
            TimeInstance::from_str("2014-01-01T00:00:00.000Z").unwrap(),
            TimeInstance::from_str("2014-07-01T00:00:00.000Z").unwrap(),
        ),
        step: TimeStep {
            granularity: TimeGranularity::Months,
            step: 1,
        },
        time_placeholders: hashmap! {
            "%_START_TIME_%".to_string() => GdalSourceTimePlaceholder {
                format: DateTimeParseFormat::custom("%Y-%m-%d".to_string()),
                reference: TimeReference::Start,
            },
        },
        params: GdalDatasetParameters {
            file_path: test_data!(
                "raster/modis_ndvi/projected_3857/MOD13A2_M_NDVI_%_START_TIME_%.TIFF"
            )
            .into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: data_geo_transform.origin_coordinate,
                x_pixel_size: data_geo_transform.x_pixel_size(),
                y_pixel_size: data_geo_transform.y_pixel_size(),
            },
            width: data_bounds.axis_size_x(),
            height: data_bounds.axis_size_y(),
            file_not_found_handling: FileNotFoundHandling::Error,
            no_data_value: Some(0.),
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        },
        result_descriptor: result_descriptor.clone(),
        cache_ttl: CacheTtlSeconds::default(),
    };

    let mut exe_ctx =
        MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(tile_size_in_pixels));

    let id: DataId = DatasetId::new().into();
    let name = NamedData::with_system_name("ndvi");
    exe_ctx.add_meta_data(id.clone(), name.clone(), Box::new(m));

    let time_interval = TimeInterval::new_unchecked(1_396_310_400_000, 1_396_310_400_000); // 2014-04-01

    let gdal_op = GdalSource {
        params: GdalSourceParameters::new(name),
    }
    .boxed();

    let initialized_operator = RasterOperator::boxed(Reprojection {
        params: ReprojectionParams {
            target_spatial_reference: SpatialReference::epsg_4326(),
            derive_out_spec: DeriveOutRasterSpecsSource::DataBounds,
        },
        sources: SingleRasterOrVectorSource {
            source: gdal_op.into(),
        },
    })
    .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
    .await?;

    let qp = initialized_operator
        .query_processor()
        .unwrap()
        .get_u8()
        .unwrap();

    let qr = qp.result_descriptor();
    let query_ctx = exe_ctx.mock_query_context(TestDefault::test_default());

    let qs = qp
        .raster_query(
            RasterQueryRectangle::new(
                qr.spatial_grid_descriptor()
                    .tiling_grid_definition(query_ctx.tiling_specification())
                    .tiling_grid_bounds(),
                time_interval,
                BandSelection::first(),
            ),
            &query_ctx,
        )
        .await
        .unwrap();

    let tiles = qs
        .map(Result::unwrap)
        .collect::<Vec<RasterTile2D<u8>>>()
        .await;

    Ok(tiles)
}

#[tokio::main]
async fn main() {
    let _x = actual().await;
}
