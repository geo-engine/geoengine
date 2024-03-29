use geoengine_datatypes::raster::RasterPropertiesKey;

mod radiance;
mod reflectance;
mod satellite;
mod temperature;

fn new_slope_key() -> RasterPropertiesKey {
    RasterPropertiesKey {
        domain: Some("msg".into()),
        key: "calibration_slope".into(),
    }
}

fn new_offset_key() -> RasterPropertiesKey {
    RasterPropertiesKey {
        domain: Some("msg".into()),
        key: "calibration_offset".into(),
    }
}

fn new_channel_key() -> RasterPropertiesKey {
    RasterPropertiesKey {
        domain: Some("msg".into()),
        key: "channel_number".into(),
    }
}

fn new_satellite_key() -> RasterPropertiesKey {
    RasterPropertiesKey {
        domain: Some("msg".into()),
        key: "satellite_number".into(),
    }
}

#[cfg(test)]
mod test_util {
    use std::str::FromStr;

    use futures::StreamExt;
    use geoengine_datatypes::dataset::{DataId, DatasetId, NamedData};
    use geoengine_datatypes::hashmap;
    use geoengine_datatypes::primitives::{
        BandSelection, CacheHint, CacheTtlSeconds, Coordinate2D,
    };
    use geoengine_datatypes::primitives::{
        ContinuousMeasurement, DateTime, DateTimeParseFormat, Measurement, RasterQueryRectangle,
        TimeGranularity, TimeInstance, TimeInterval, TimeStep,
    };
    use geoengine_datatypes::raster::{
        BoundedGrid, GeoTransform, Grid2D, GridBoundingBox2D, GridOrEmpty, GridOrEmpty2D,
        GridShape2D, MaskedGrid2D, Pixel, RasterDataType, RasterProperties, RasterPropertiesEntry,
        RasterPropertiesEntryType, RasterTile2D, TileInformation,
    };
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceAuthority};
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_datatypes::util::Identifier;
    use num_traits::AsPrimitive;

    use crate::engine::{
        MockExecutionContext, MockQueryContext, QueryProcessor, RasterBandDescriptor,
        RasterBandDescriptors, RasterOperator, RasterResultDescriptor, WorkflowOperatorPath,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use crate::processing::meteosat::{
        new_channel_key, new_offset_key, new_satellite_key, new_slope_key,
    };
    use crate::source::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalMetaDataRegular,
        GdalMetadataMapping, GdalSource, GdalSourceParameters, GdalSourceTimePlaceholder,
        TimeReference,
    };
    use crate::test_data;
    use crate::util::Result;

    pub(crate) async fn process<T>(
        make_op: T,
        query: RasterQueryRectangle,
        ctx: &MockExecutionContext,
    ) -> Result<RasterTile2D<f32>>
    where
        T: FnOnce() -> Box<dyn RasterOperator>,
    {
        // let input = make_raster(props, custom_data, measurement);

        let op = make_op()
            .initialize(WorkflowOperatorPath::initialize_root(), ctx)
            .await?;

        let processor = op.query_processor().unwrap().get_f32().unwrap();

        let ctx = MockQueryContext::test_default();
        let result_stream = processor.query(query, &ctx).await.unwrap();
        let mut result: Vec<Result<RasterTile2D<f32>>> = result_stream.collect().await;
        assert_eq!(1, result.len());
        result.pop().unwrap()
    }

    pub(crate) fn create_properties(
        channel: Option<u8>,
        satellite: Option<u8>,
        offset: Option<f64>,
        slope: Option<f64>,
    ) -> RasterProperties {
        let mut props = RasterProperties::default();

        if let Some(v) = channel {
            props.insert_property(new_channel_key(), RasterPropertiesEntry::Number(v.as_()));
        }

        if let Some(v) = satellite {
            props.insert_property(new_satellite_key(), RasterPropertiesEntry::Number(v.as_()));
        }

        if let Some(v) = slope {
            props.insert_property(new_slope_key(), RasterPropertiesEntry::Number(v));
        }

        if let Some(v) = offset {
            props.insert_property(new_offset_key(), RasterPropertiesEntry::Number(v));
        }
        props
    }

    pub(crate) fn _create_gdal_query() -> RasterQueryRectangle {
        RasterQueryRectangle::new_with_grid_bounds(
            GridBoundingBox2D::new_min_max(0, 599, 0, 599).unwrap(),
            TimeInterval::new_unchecked(
                TimeInstance::from(DateTime::new_utc(2012, 12, 12, 12, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2012, 12, 12, 12, 15, 0)),
            ),
            BandSelection::first(),
        )
    }

    pub(crate) fn create_mock_query() -> RasterQueryRectangle {
        RasterQueryRectangle::new_with_grid_bounds(
            GridBoundingBox2D::new_min_max(0, 2, 0, 1).unwrap(),
            Default::default(),
            BandSelection::first(),
        )
    }

    pub(crate) fn create_mock_source<P: Pixel>(
        props: RasterProperties,
        custom_data: Option<GridOrEmpty2D<P>>,
        measurement: Option<Measurement>,
    ) -> MockRasterSource<P> {
        let raster = match custom_data {
            Some(g) => g,
            None => GridOrEmpty::Grid(
                MaskedGrid2D::new(
                    Grid2D::<P>::new(
                        [3, 2].into(),
                        vec![
                            P::from_(1),
                            P::from_(2),
                            P::from_(3),
                            P::from_(4),
                            P::from_(5),
                            P::from_(0),
                        ],
                    )
                    .unwrap(),
                    Grid2D::new([3, 2].into(), vec![true, true, true, true, true, false]).unwrap(),
                )
                .unwrap(),
            ),
        };

        let raster_tile = RasterTile2D::new_with_tile_info_and_properties(
            TimeInterval::default(),
            TileInformation {
                global_tile_position: [-1, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
                global_geo_transform: TestDefault::test_default(),
            },
            0,
            raster,
            props,
            CacheHint::default(),
        );

        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    geo_transform_x: GeoTransform::new(Coordinate2D::new(0., -3.), 1., -1.),
                    pixel_bounds_x: GridBoundingBox2D::new([-3, 0], [0, 2]).unwrap(),
                    bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                        "band".into(),
                        measurement.unwrap_or_else(|| {
                            Measurement::Continuous(ContinuousMeasurement {
                                measurement: "raw".to_string(),
                                unit: None,
                            })
                        }),
                    )])
                    .unwrap(),
                },
            },
        }
    }

    pub(crate) fn _create_gdal_src(ctx: &mut MockExecutionContext) -> GdalSource {
        let dataset_id: DataId = DatasetId::new().into();
        let dataset_name = NamedData::with_system_name("gdal-ds");

        let no_data_value = Some(0.);
        let origin_coordinate: Coordinate2D =
            (-5_570_248.477_339_745, 5_570_248.477_339_745).into();
        let x_pixel_size = 3_000.403_165_817_261;
        let y_pixel_size = -3_000.403_165_817_261;

        let meta = GdalMetaDataRegular {
            data_time: TimeInterval::new_unchecked(
                TimeInstance::from_str("2012-12-12T12:00:00.000Z").unwrap(),
                TimeInstance::from_str("2020-12-12T12:00:00.000Z").unwrap(),
            ),
            step: TimeStep {
                granularity: TimeGranularity::Minutes,
                step: 15,
            },
            time_placeholders: hashmap! {
                "%_START_TIME_%".to_string() => GdalSourceTimePlaceholder {
                    format: DateTimeParseFormat::custom("%Y%m%d_%H%M".to_string()),
                    reference: TimeReference::Start,
                },
            },
            params: GdalDatasetParameters {
                file_path: test_data!("raster/msg/%_START_TIME_%.tif").into(),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate,
                    x_pixel_size,
                    y_pixel_size,
                },
                width: 3712,
                height: 3712,
                file_not_found_handling: FileNotFoundHandling::Error,
                no_data_value,
                properties_mapping: Some(vec![
                    GdalMetadataMapping::identity(
                        new_satellite_key(),
                        RasterPropertiesEntryType::Number,
                    ),
                    GdalMetadataMapping::identity(
                        new_channel_key(),
                        RasterPropertiesEntryType::Number,
                    ),
                    GdalMetadataMapping::identity(
                        new_offset_key(),
                        RasterPropertiesEntryType::Number,
                    ),
                    GdalMetadataMapping::identity(
                        new_slope_key(),
                        RasterPropertiesEntryType::Number,
                    ),
                ]),
                gdal_open_options: None,
                gdal_config_options: None,
                allow_alphaband_as_mask: true,
                retry: None,
            },
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::I16,
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::SrOrg, 81)
                    .into(),
                time: None,
                geo_transform_x: GeoTransform::new(origin_coordinate, x_pixel_size, y_pixel_size),
                pixel_bounds_x: GridShape2D::new_2d(3712, 3712).bounding_box(),
                bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                    "band".into(),
                    Measurement::Continuous(ContinuousMeasurement {
                        measurement: "raw".to_string(),
                        unit: None,
                    }),
                )])
                .unwrap(),
            },
            cache_ttl: CacheTtlSeconds::default(),
        };
        ctx.add_meta_data(dataset_id, dataset_name.clone(), Box::new(meta));

        GdalSource {
            params: GdalSourceParameters::new(dataset_name),
        }
    }
}
