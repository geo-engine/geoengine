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
    use futures::StreamExt;
    use geoengine_datatypes::hashmap;
    use geoengine_datatypes::util::test::TestDefault;
    use num_traits::AsPrimitive;

    use geoengine_datatypes::dataset::{DatasetId, InternalDatasetId};
    use geoengine_datatypes::primitives::{
        ContinuousMeasurement, DateTime, DateTimeParseFormat, Measurement, RasterQueryRectangle,
        SpatialPartition2D, SpatialResolution, TimeGranularity, TimeInstance, TimeInterval,
        TimeStep,
    };
    use geoengine_datatypes::raster::{
        EmptyGrid2D, Grid2D, GridOrEmpty, Pixel, RasterDataType, RasterProperties,
        RasterPropertiesEntry, RasterPropertiesEntryType, RasterTile2D, TileInformation,
    };
    use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceAuthority};
    use geoengine_datatypes::util::Identifier;

    use crate::engine::{
        MockExecutionContext, MockQueryContext, QueryProcessor, RasterOperator,
        RasterResultDescriptor,
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

        let op = make_op().initialize(ctx).await?;

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
            props
                .properties_map
                .insert(new_channel_key(), RasterPropertiesEntry::Number(v.as_()));
        }

        if let Some(v) = satellite {
            props
                .properties_map
                .insert(new_satellite_key(), RasterPropertiesEntry::Number(v.as_()));
        }

        if let Some(v) = slope {
            props
                .properties_map
                .insert(new_slope_key(), RasterPropertiesEntry::Number(v));
        }

        if let Some(v) = offset {
            props
                .properties_map
                .insert(new_offset_key(), RasterPropertiesEntry::Number(v));
        }
        props
    }

    pub(crate) fn _create_gdal_query() -> RasterQueryRectangle {
        let sr = SpatialResolution::new_unchecked(3_000.403_165_817_261, 3_000.403_165_817_261);
        let ul = (0., 0.).into();
        let lr = (599. * sr.x, -599. * sr.y).into();
        RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked(ul, lr),
            time_interval: TimeInterval::new_unchecked(
                TimeInstance::from(DateTime::new_utc(2012, 12, 12, 12, 0, 0)),
                TimeInstance::from(DateTime::new_utc(2012, 12, 12, 12, 15, 0)),
            ),
            spatial_resolution: sr,
        }
    }

    pub(crate) fn create_mock_query() -> RasterQueryRectangle {
        RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (2., 0.).into()),
            time_interval: Default::default(),
            spatial_resolution: SpatialResolution::one(),
        }
    }

    pub(crate) fn create_mock_source<P: Pixel>(
        props: RasterProperties,
        custom_data: Option<Vec<P>>,
        measurement: Option<Measurement>,
    ) -> MockRasterSource<P> {
        let raster = match custom_data {
            Some(v) if v.is_empty() => GridOrEmpty::Empty(EmptyGrid2D::new([3, 2].into())),
            Some(v) => GridOrEmpty::Grid(Grid2D::new([3, 2].into(), v).unwrap()),
            None => GridOrEmpty::Grid(
                Grid2D::<P>::new(
                    [3, 2].into(),
                    vec![
                        P::from_(1),
                        P::from_(2),
                        P::from_(3),
                        P::from_(4),
                        P::from_(5),
                    ],
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
            raster,
            props,
        );

        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: measurement.unwrap_or_else(|| {
                        Measurement::Continuous(ContinuousMeasurement {
                            measurement: "raw".to_string(),
                            unit: None,
                        })
                    }),
                    time: None,
                    bbox: None,
                },
            },
        }
    }

    pub(crate) fn _create_gdal_src(ctx: &mut MockExecutionContext) -> GdalSource {
        let dataset_id: DatasetId = InternalDatasetId::new().into();

        let timestamp = DateTime::new_utc(2012, 12, 12, 12, 0, 0);

        let no_data_value = Some(0.);
        let meta = GdalMetaDataRegular {
            start: TimeInstance::from(timestamp),
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
                    origin_coordinate: (-5_570_248.477_339_745, 5_570_248.477_339_745).into(),
                    x_pixel_size: 3_000.403_165_817_261,
                    y_pixel_size: -3_000.403_165_817_261,
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
            },
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::I16,
                spatial_reference: SpatialReference::new(SpatialReferenceAuthority::SrOrg, 81)
                    .into(),
                measurement: Measurement::Continuous(ContinuousMeasurement {
                    measurement: "raw".to_string(),
                    unit: None,
                }),
                time: None,
                bbox: None,
            },
        };
        ctx.add_meta_data(dataset_id.clone(), Box::new(meta));

        GdalSource {
            params: GdalSourceParameters {
                dataset: dataset_id,
            },
        }
    }
}
