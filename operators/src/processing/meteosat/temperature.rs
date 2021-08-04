use crate::engine::{
    ExecutionContext, InitializedRasterOperator, Operator, QueryContext, QueryProcessor,
    RasterOperator, RasterQueryProcessor, RasterQueryRectangle, RasterResultDescriptor,
    SingleRasterSource, TypedRasterQueryProcessor,
};
use crate::util::Result;
use async_trait::async_trait;
use TypedRasterQueryProcessor::F32 as QueryProcessorOut;

use crate::error::Error;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::primitives::{Measurement, SpatialPartition2D};
use geoengine_datatypes::raster::{
    EmptyGrid, Grid2D, GridOrEmpty, GridShapeAccess, GridSize, NoDataValue, Pixel, RasterDataType,
    RasterPropertiesKey, RasterTile2D,
};
use serde::{Deserialize, Serialize};

// Output type is always f32
type PixelOut = f32;
use crate::processing::meteosat::satellite::{Channel, Satellite};
use RasterDataType::F32 as RasterOut;

const OUT_NO_DATA_VALUE: PixelOut = PixelOut::NAN;

/// Parameters for the `Temperature` operator.
/// * `force_satellite` forces the use of the satellite with the given name.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TemperatureParams {
    force_satellite: String,
}

impl Default for TemperatureParams {
    fn default() -> Self {
        TemperatureParams {
            force_satellite: "".into(),
        }
    }
}

/// The temperature operator approximates BT from
/// the raw MSG rasters.
pub type Temperature = Operator<TemperatureParams, SingleRasterSource>;

pub struct InitializedTemperature {
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    params: TemperatureParams,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for Temperature {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let input = self.sources.raster.initialize(context).await?;

        let in_desc = input.result_descriptor();

        match &in_desc.measurement {
            Measurement::Continuous {
                measurement: m,
                unit: _,
            } if m != "raw" => {
                return Err(Error::InvalidMeasurement {
                    expected: "raw".into(),
                    found: m.clone(),
                })
            }
            Measurement::Classification {
                measurement: m,
                classes: _,
            } => {
                return Err(Error::InvalidMeasurement {
                    expected: "raw".into(),
                    found: m.clone(),
                })
            }
            Measurement::Unitless => {
                return Err(Error::InvalidMeasurement {
                    expected: "raw".into(),
                    found: "unitless".into(),
                })
            }
            _ => {}
        }

        let out_desc = RasterResultDescriptor {
            spatial_reference: in_desc.spatial_reference,
            data_type: RasterOut,
            measurement: Measurement::Continuous {
                measurement: "temperature".into(),
                unit: Some("k".into()),
            },
            no_data_value: Some(f64::from(OUT_NO_DATA_VALUE)),
        };

        let initialized_operator = InitializedTemperature {
            result_descriptor: out_desc,
            source: input,
            params: self.params,
        };

        Ok(initialized_operator.boxed())
    }
}

impl InitializedRasterOperator for InitializedTemperature {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor, Error> {
        let q = self.source.query_processor()?;

        Ok(match q {
            TypedRasterQueryProcessor::U8(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::U16(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::U32(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::U64(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::I8(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::I16(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::I32(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::I64(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::F32(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
            TypedRasterQueryProcessor::F64(p) => {
                QueryProcessorOut(Box::new(TemperatureProcessor::new(p, self.params.clone())))
            }
        })
    }
}

struct TemperatureProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    source: Q,
    params: TemperatureParams,
    satellite_key: RasterPropertiesKey,
    channel_key: RasterPropertiesKey,
    offset_key: RasterPropertiesKey,
    slope_key: RasterPropertiesKey,
}

impl<Q, P> TemperatureProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    pub fn new(source: Q, params: TemperatureParams) -> Self {
        Self {
            source,
            params,
            satellite_key: RasterPropertiesKey {
                domain: Some("msg".into()),
                key: "Satellite".into(),
            },
            channel_key: RasterPropertiesKey {
                domain: Some("msg".into()),
                key: "Channel".into(),
            },
            offset_key: RasterPropertiesKey {
                domain: Some("msg".into()),
                key: "CalibrationOffset".into(),
            },
            slope_key: RasterPropertiesKey {
                domain: Some("msg".into()),
                key: "CalibrationSlope".into(),
            },
        }
    }

    fn get_satellite(&self, tile: &RasterTile2D<P>) -> Result<&'static Satellite> {
        if self.params.force_satellite.is_empty() {
            let satellite_id = tile.properties.get_number_property(&self.satellite_key)?;
            Satellite::satellite_by_msg_id(satellite_id)
        } else {
            Satellite::satellite_by_name(self.params.force_satellite.as_str())
        }
    }

    fn get_channel<'a>(
        &self,
        tile: &RasterTile2D<P>,
        satellite: &'a Satellite,
    ) -> Result<&'a Channel> {
        let channel_id = tile
            .properties
            .get_number_property::<usize>(&self.channel_key)?
            - 1;
        if channel_id < 3 || channel_id > 10 {
            Err(Error::InvalidChannel {
                channel: channel_id,
            })
        } else {
            satellite.get_channel(channel_id)
        }
    }

    fn create_lookup_table(&self, tile: &RasterTile2D<P>) -> Result<Vec<f32>> {
        let satellite = self.get_satellite(&tile)?;
        let channel = self.get_channel(&tile, satellite)?;
        let offset = tile
            .properties
            .get_number_property::<f64>(&self.offset_key)?;
        let slope = tile
            .properties
            .get_number_property::<f64>(&self.slope_key)?;

        let mut lut = Vec::with_capacity(1024);
        for i in 0..1024 {
            let radiance = offset + i as f64 * slope;
            let temp = channel.calculate_temperature_from_radiance(radiance);
            lut.push(temp as f32);
        }
        Ok(lut)
    }
}
#[async_trait]
impl<Q, P> QueryProcessor for TemperatureProcessor<Q, P>
where
    Q: QueryProcessor<Output = RasterTile2D<P>, SpatialBounds = SpatialPartition2D>,
    P: Pixel,
{
    type Output = RasterTile2D<PixelOut>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let src = self.source.query(query, ctx).await?;

        let rs = src.map(move |tile| {
            let tile = tile?;
            match &tile.grid_array {
                GridOrEmpty::Empty(_) => Ok(RasterTile2D::new_with_properties(
                    tile.time,
                    tile.tile_position,
                    tile.global_geo_transform,
                    EmptyGrid::new(tile.grid_array.grid_shape(), OUT_NO_DATA_VALUE).into(),
                    tile.properties,
                )),
                GridOrEmpty::Grid(grid) => {
                    let lut = self.create_lookup_table(&tile)?;

                    // Create result raster
                    let mut out = Grid2D::new(
                        grid.grid_shape(),
                        vec![OUT_NO_DATA_VALUE; grid.number_of_elements()],
                        Some(OUT_NO_DATA_VALUE),
                    )
                    .expect("raster creation must succeed");

                    for (idx, &pixel) in grid.data.iter().enumerate() {
                        if !grid.is_no_data(pixel) {
                            let lut_idx: u64 = pixel.as_();
                            out.data[idx] = *lut
                                .get(lut_idx as usize)
                                .ok_or(Error::UnsupportedRasterValue)?;
                        }
                    }

                    Ok(RasterTile2D::new_with_properties(
                        tile.time,
                        tile.tile_position,
                        tile.global_geo_transform,
                        out.into(),
                        tile.properties,
                    ))
                }
            }
        });
        Ok(rs.boxed())
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::{
        MockExecutionContext, MockQueryContext, QueryProcessor, RasterOperator,
        RasterQueryRectangle, RasterResultDescriptor, SingleRasterSource,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use crate::processing::meteosat::temperature::{
        PixelOut, Temperature, TemperatureParams, OUT_NO_DATA_VALUE,
    };
    use crate::util::Result;
    use futures::StreamExt;
    use geoengine_datatypes::primitives::{
        Measurement, SpatialPartition2D, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::{
        EmptyGrid2D, Grid2D, GridOrEmpty, RasterDataType, RasterProperties, RasterPropertiesEntry,
        RasterPropertiesKey, RasterTile2D, TileInformation,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use num_traits::AsPrimitive;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_empty_ok() {
        let props = create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
        let params = TemperatureParams::default();
        let res = process(props, params, None, Some(vec![])).await.unwrap();

        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &res.grid_array,
            &EmptyGrid2D::new([3, 2].into(), OUT_NO_DATA_VALUE,).into()
        ));
    }

    #[tokio::test]
    async fn test_ok() {
        let props = create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
        let params = TemperatureParams::default();
        let res = process(props, params, None, None).await.unwrap();

        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &res.grid_array,
            &Grid2D::new(
                [3, 2].into(),
                vec![
                    300.34143,
                    318.61765,
                    330.36514,
                    339.23364,
                    346.44394,
                    OUT_NO_DATA_VALUE,
                ],
                Some(OUT_NO_DATA_VALUE),
            )
            .unwrap()
            .into()
        ));
    }

    #[tokio::test]
    async fn test_ok_force_satellite() {
        let props = create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
        let mut params = TemperatureParams::default();
        params.force_satellite = "Meteosat-11".into();
        let res = process(props, params, None, None).await.unwrap();

        assert!(geoengine_datatypes::util::test::eq_with_no_data(
            &res.grid_array,
            &Grid2D::new(
                [3, 2].into(),
                vec![
                    300.9428,
                    319.25015,
                    331.01904,
                    339.9044,
                    347.12878,
                    OUT_NO_DATA_VALUE
                ],
                Some(OUT_NO_DATA_VALUE),
            )
            .unwrap()
            .into()
        ));
    }

    // TODO: Cannot be tested, as the whole u8 range is a valid input. Think about making mock source u16
    // #[tokio::test]
    // async fn test_fail_illegal_input() {
    //     let props = create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
    //     let mut params = TemperatureParams::default();
    //     let res = process(props, params, None, Some(vec![1, 2, 3, 4, 1024, 0])).await;
    //     assert!(res.is_err());
    // }

    #[tokio::test]
    async fn test_invalid_force_satellite() {
        let props = create_properties(Some(4), Some(1), Some(0.0), Some(1.0));
        let mut params = TemperatureParams::default();
        params.force_satellite = "Meteosat-42".into();
        let res = process(props, params, None, None).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_missing_satellite() {
        let props = create_properties(Some(4), None, Some(0.0), Some(1.0));
        let params = TemperatureParams::default();
        let res = process(props, params, None, None).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_satellite() {
        let props = create_properties(Some(4), Some(42), Some(0.0), Some(1.0));
        let params = TemperatureParams::default();
        let res = process(props, params, None, None).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_missing_channel() {
        let props = create_properties(None, Some(1), Some(0.0), Some(1.0));
        let params = TemperatureParams::default();
        let res = process(props, params, None, None).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_channel() {
        let props = create_properties(Some(1), Some(1), Some(0.0), Some(1.0));
        let params = TemperatureParams::default();
        let res = process(props, params, None, None).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_missing_slope() {
        let props = create_properties(Some(4), Some(1), Some(0.0), None);
        let params = TemperatureParams::default();
        let res = process(props, params, None, None).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_missing_offset() {
        let props = create_properties(Some(4), Some(1), None, Some(1.0));
        let params = TemperatureParams::default();
        let res = process(props, params, None, None).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_measurement_unitless() {
        let props = create_properties(Some(1), Some(1), Some(0.0), Some(1.0));
        let params = TemperatureParams::default();
        let res = process(props, params, Some(Measurement::Unitless), None).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_measurement_continuous() {
        let props = create_properties(Some(1), Some(1), Some(0.0), Some(1.0));
        let params = TemperatureParams::default();
        let res = process(
            props,
            params,
            Some(Measurement::Continuous {
                measurement: "invalid".into(),
                unit: None,
            }),
            None,
        )
        .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_invalid_measurement_classification() {
        let props = create_properties(Some(1), Some(1), Some(0.0), Some(1.0));
        let params = TemperatureParams::default();
        let res = process(
            props,
            params,
            Some(Measurement::Classification {
                measurement: "invalid".into(),
                classes: HashMap::new(),
            }),
            None,
        )
        .await;
        assert!(res.is_err());
    }

    fn create_properties(
        channel: Option<u8>,
        satellite: Option<u8>,
        offset: Option<f64>,
        slope: Option<f64>,
    ) -> RasterProperties {
        let mut props = RasterProperties::default();

        if let Some(v) = channel {
            props.properties_map.insert(
                RasterPropertiesKey {
                    domain: Some("msg".into()),
                    key: "Channel".into(),
                },
                RasterPropertiesEntry::Number(v.as_()),
            );
        }

        if let Some(v) = satellite {
            props.properties_map.insert(
                RasterPropertiesKey {
                    domain: Some("msg".into()),
                    key: "Satellite".into(),
                },
                RasterPropertiesEntry::Number(v.as_()),
            );
        }

        if let Some(v) = slope {
            props.properties_map.insert(
                RasterPropertiesKey {
                    domain: Some("msg".into()),
                    key: "CalibrationSlope".into(),
                },
                RasterPropertiesEntry::Number(v),
            );
        }

        if let Some(v) = offset {
            props.properties_map.insert(
                RasterPropertiesKey {
                    domain: Some("msg".into()),
                    key: "CalibrationOffset".into(),
                },
                RasterPropertiesEntry::Number(v),
            );
        }
        props
    }

    async fn process(
        props: RasterProperties,
        params: TemperatureParams,
        measurement: Option<Measurement>,
        custom_data: Option<Vec<u8>>,
    ) -> Result<RasterTile2D<PixelOut>> {
        let input = make_raster(props, custom_data, measurement);

        let op = Temperature {
            sources: SingleRasterSource { raster: input },
            params: params,
        }
        .boxed()
        .initialize(&MockExecutionContext::default())
        .await?;

        let processor = op.query_processor().unwrap().get_f32().unwrap();

        let ctx = MockQueryContext::new(1);
        let result_stream = processor
            .query(
                RasterQueryRectangle {
                    spatial_bounds: SpatialPartition2D::new_unchecked(
                        (0., 4.).into(),
                        (3., 0.).into(),
                    ),
                    time_interval: Default::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &ctx,
            )
            .await
            .unwrap();

        let mut result: Vec<Result<RasterTile2D<PixelOut>>> = result_stream.collect().await;
        result.pop().unwrap()
    }

    fn make_raster(
        props: RasterProperties,
        custom_data: Option<Vec<u8>>,
        measurement: Option<Measurement>,
    ) -> Box<dyn RasterOperator> {
        let no_data_value = Some(0);

        let raster = match custom_data {
            Some(v) if v.is_empty() => {
                GridOrEmpty::Empty(EmptyGrid2D::new([3, 2].into(), no_data_value.unwrap()))
            }
            Some(v) => GridOrEmpty::Grid(Grid2D::new([3, 2].into(), v, no_data_value).unwrap()),
            None => GridOrEmpty::Grid(
                Grid2D::new(
                    [3, 2].into(),
                    vec![1, 2, 3, 4, 5, no_data_value.unwrap()],
                    no_data_value,
                )
                .unwrap(),
            ),
        };

        let raster_tile = RasterTile2D::new_with_tile_info_and_properties(
            TimeInterval::default(),
            TileInformation {
                global_tile_position: [-1, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
                global_geo_transform: Default::default(),
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
                    measurement: measurement.unwrap_or_else(|| Measurement::Continuous {
                        measurement: "raw".into(),
                        unit: None,
                    }),
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed()
    }
}
