use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use geoengine_datatypes::primitives::SpatialPartition2D;
use geoengine_datatypes::raster::{EmptyGrid, GeoTransformAccess, GridShapeAccess};
use geoengine_datatypes::{
    raster::{Grid2D, Pixel, RasterTile2D, GridOrEmpty},
};
use crate::{
    engine::{
        ExecutionContext, InitializedRasterOperator,
        InitializedVectorOperator, QueryContext, QueryRectangle, RasterOperator,
        RasterQueryProcessor, RasterResultDescriptor, TypedRasterQueryProcessor, VectorOperator,
        OperatorDatasets, QueryProcessor
    },
    error::Error as GeoengineOperatorsError,
    util::Result,
};
use geoengine_datatypes::dataset::DatasetId;

use serde::{Deserialize, Serialize};

use async_trait::async_trait;

use ndarray::{Array2, stack, Axis, ArrayBase, OwnedRepr, Dim};
use numpy::{PyArray, PyArray4};
use pyo3::prelude::*;
use pyo3::{types::{PyModule, PyUnicode}, Py, Python };


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ImsegOperator {
    pub raster_sources: Vec<Box<dyn RasterOperator>>,
    pub vector_sources: Vec<Box<dyn VectorOperator>>,
}

impl OperatorDatasets for ImsegOperator {

    fn datasets_collect(&self, datasets: &mut Vec<DatasetId>) {

    }
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for ImsegOperator {
    async fn initialize(
        mut self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        if !self.vector_sources.is_empty() {
            return Err(GeoengineOperatorsError::InvalidNumberOfVectorInputs {
                expected: 0..1,
                found: self.vector_sources.len(),
            });
        }

        if self.raster_sources.len() != 1 {
            return Err(GeoengineOperatorsError::InvalidNumberOfRasterInputs {
                expected: 1..2,
                found: self.raster_sources.len(),
            });
        }

        let initialized_raster = self
            .raster_sources
            .pop()
            .expect("checked")
            .initialize(context).await.unwrap();
        let result_descriptor = initialized_raster.result_descriptor().clone();

        let initialized_operator = InitializedImsegOperator {
            raster_sources: vec![initialized_raster],
            vector_sources: vec![],
            result_descriptor,
            state: (),
        };

        Ok(initialized_operator.boxed())
    }
}

pub struct InitializedImsegOperator {
    pub raster_sources: Vec<Box<dyn InitializedRasterOperator>>,
    pub vector_sources: Vec<Box<dyn InitializedVectorOperator>>,
    pub result_descriptor: RasterResultDescriptor,
    pub state: (),
}

impl InitializedRasterOperator for InitializedImsegOperator {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let typed_raster_processor_ir016 = self.raster_sources[0].query_processor()?;
        let typed_raster_processor_ir039 = self.raster_sources[1].query_processor()?;
        let typed_raster_processor_ir087 = self.raster_sources[2].query_processor()?;
        let typed_raster_processor_ir097 = self.raster_sources[3].query_processor()?;
        let typed_raster_processor_ir108 = self.raster_sources[4].query_processor()?;
        let typed_raster_processor_ir120 = self.raster_sources[5].query_processor()?;
        let typed_raster_processor_ir134 = self.raster_sources[6].query_processor()?;

        match (typed_raster_processor_ir016, typed_raster_processor_ir039, typed_raster_processor_ir087, typed_raster_processor_ir097, typed_raster_processor_ir108, typed_raster_processor_ir120, typed_raster_processor_ir134) {
            (TypedRasterQueryProcessor::F32(ir_016),TypedRasterQueryProcessor::F32(ir_039), TypedRasterQueryProcessor::F32(ir_087), TypedRasterQueryProcessor::F32(ir_097), TypedRasterQueryProcessor::F32(ir_108), TypedRasterQueryProcessor::F32(ir_120), TypedRasterQueryProcessor::F32(ir_134)) => {
                Ok(TypedRasterQueryProcessor::U8(ImsegProcessor::new(vec![ir_016, ir_039, ir_087, ir_097, ir_108, ir_120, ir_134], vec![0,1,2,3], 99).boxed()))
            }, 
            _ => {
                panic!("Something went terribly wrong...")
            }
        }
    }
}

pub struct ImsegProcessor<T, U> {
    rasters: Vec<Box<dyn RasterQueryProcessor<RasterType = T>>>,
    classes: Vec<U>,
    ndv: U,
    pymod_tf: Py<PyModule>,
}

impl<T, U> ImsegProcessor<T, U>
where
T: Pixel + numpy::Element,
U: Pixel + numpy::Element,

{
    pub fn new(rasters: Vec<Box<dyn RasterQueryProcessor<RasterType = T>>>, classes: Vec<U>, ndv: U) -> Self {
        //temporary py stuff
        let gil = Python::acquire_gil();
        let py = gil.python();
        
        
        // saving your python script file as a struct field
        // using this, we can access python functions and objects without loss of memory state
        // on successive iterations
        let name = PyUnicode::new(py, "second");
        
        let py_mod: Py<PyModule> =
            PyModule::from_code(py, include_str!("tf_v2.py"), "tf_v2.py", "tf_v2")
                .unwrap()
                .into_py(py);
        let _load = py_mod.as_ref(py).call("load", (name, ), None).unwrap();
        

        
        Self {
            rasters: rasters,
            pymod_tf: py_mod,
            classes: classes,
            ndv: ndv,
        }
        
    }

    pub fn predict(&self, ir_016: Result<RasterTile2D<T>>, ir_039: Result<RasterTile2D<T>>, ir_087: Result<RasterTile2D<T>>, ir_097: Result<RasterTile2D<T>>, ir_108: Result<RasterTile2D<T>>, ir_120: Result<RasterTile2D<T>>, ir_137: Result<RasterTile2D<T>>) -> Result<RasterTile2D<U>> {
        println!("started prediction!");
        let (ir016, ir039, ir087, ir097, ir108, ir120, ir134) = (ir_016?, ir_039?, ir_087?, ir_097?, ir_108?, ir_120?, ir_137?);
        
        let time = ir016.time;
        let position = ir016.tile_position;
        let geo = ir016.geo_transform();
        let shape = ir016.grid_array.grid_shape();
        match (ir016.grid_array, ir039.grid_array, ir087.grid_array, ir097.grid_array, ir108.grid_array, ir120.grid_array, ir134.grid_array) {
        
            (GridOrEmpty::Grid(grid_016), GridOrEmpty::Grid(grid_039),  GridOrEmpty::Grid(grid_087),  GridOrEmpty::Grid(grid_097), GridOrEmpty::Grid(grid_108), GridOrEmpty::Grid(grid_120), GridOrEmpty::Grid(grid_137)) => {
                
                let gil = Python::acquire_gil();
                let py = gil.python();

                let data_016 = grid_016.data;
                let data_039 = grid_039.data;
                let data_087 = grid_087.data;
                let data_097 = grid_097.data;
                let data_108 = grid_108.data;
                let data_120 = grid_120.data;
                let data_137 = grid_137.data;

                let tile_size = grid_016.shape.shape_array;
                let arr_img_batch = create_arrays_from_data(data_016, data_039, data_087, data_097, data_108, data_120, data_137, tile_size);
                let py_img_batch = PyArray::from_owned_array(py, arr_img_batch);

                let result_img = self.pymod_tf.as_ref(py).call("predict", (py_img_batch,1), None)
                .unwrap()
                .downcast::<PyArray4<f32>>()
                .unwrap()
                .to_owned_array();

                let mut segmap = Array2::<U>::from_elem((512,512), self.classes[0]);
                let result = result_img.slice(ndarray::s![0,..,..,..]);
                for i in 0..512 {
                    for j in 0..512 {
                        let view = result.slice(ndarray::s![i,j,..]);
                        let mut max: f32 = 0.0;
                        let mut max_class = self.classes[0]; 
                            for t in 0..3 {
                                //println!("predited: {:?}", view[t as usize]);
                                
                                if max <= view[t as usize] {
                                    max = view[t as usize];
                                    max_class = self.classes[t];
                                }
                            }
                            segmap[[i as usize, j as usize]] = max_class;
                        }
                }
                //println!("{:?}", segmap);
                
                Ok(RasterTile2D::new(
                            time,
                position,
            geo,
                         GridOrEmpty::Grid(Grid2D::new(
                        shape,
                        segmap.into_raw_vec(),
                        Some(self.ndv),
                        )?),
                    ))
                },
            _ => {
                Ok(RasterTile2D::new(
                    time,
                    position,
                    geo,
                    GridOrEmpty::Empty(EmptyGrid::new(
                        shape,
                        self.ndv
                    ))
                ))
            }
        }
    }
}
#[async_trait]
impl<T, U> RasterQueryProcessor for ImsegProcessor<T, U>
where  
T: Pixel + numpy::Element,
U: Pixel + numpy::Element, 
{
    type RasterType = U;

    async fn raster_query<'a>(
        &'a self,
        query: QueryRectangle<SpatialPartition2D>,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<Self::RasterType>>>> {
        let stream_ir_016 = self.rasters[0].raster_query(query, ctx).await?;
        let stream_ir_039 = self.rasters[1].raster_query(query, ctx).await?;
        let stream_ir_087 = self.rasters[2].raster_query(query, ctx).await?;
        let stream_ir_097 = self.rasters[3].raster_query(query, ctx).await?;
        let stream_ir_108 = self.rasters[4].raster_query(query, ctx).await?;
        let stream_ir_120 = self.rasters[5].raster_query(query, ctx).await?;
        let stream_ir_134 = self.rasters[6].raster_query(query, ctx).await?;

        let final_stream = stream_ir_016.zip(stream_ir_039.zip(stream_ir_087.zip(stream_ir_097.zip(stream_ir_108.zip(stream_ir_120.zip(stream_ir_134))))));
        println!("Query started");
        

        Ok(final_stream.map(move |(ir_016, (ir_039, (ir_087, (ir_097, (ir_108, (ir_120, ir_137))))))| self.predict(ir_016, ir_039, ir_087, ir_097, ir_108, ir_120, ir_137)).boxed())

    }
}
/// Creates batches used for training the model from the vectors of the rasterbands
fn create_arrays_from_data<T>(data_1: Vec<T>, data_2: Vec<T>, data_3: Vec<T>, data_4: Vec<T>, data_5: Vec<T>, data_6: Vec<T>, data_7: Vec<T>, tile_size: [usize;2]) -> (ArrayBase<OwnedRepr<T>, Dim<[usize; 4]>>) 
where 
T: Clone + std::marker::Copy{

    let arr_1: ndarray::Array2<T> = 
            Array2::from_shape_vec((tile_size[0], tile_size[1]), data_1)
            .unwrap();
            let arr_2: ndarray::Array2<T> = 
            Array2::from_shape_vec((tile_size[0], tile_size[1]), data_2)
            .unwrap();
            let arr_3: ndarray::Array2<T> = 
            Array2::from_shape_vec((tile_size[0], tile_size[1]), data_3)
            .unwrap();
            let arr_4: ndarray::Array2<T> = 
            Array2::from_shape_vec((tile_size[0], tile_size[1]), data_4)
            .unwrap()
            .to_owned();
            let arr_5: ndarray::Array2<T> = 
            Array2::from_shape_vec((tile_size[0], tile_size[1]), data_5)
            .unwrap()
            .to_owned();
            let arr_6: ndarray::Array2<T> = 
            Array2::from_shape_vec((tile_size[0], tile_size[1]), data_6)
            .unwrap()
            .to_owned();
            let arr_7: ndarray::Array2<T> = 
            Array2::from_shape_vec((tile_size[0], tile_size[1]), data_7)
            .unwrap()
            .to_owned();
            
            let arr_res: ndarray::Array<T, _> = stack(Axis(2), &[arr_1.view(),arr_2.view(),arr_3.view(), arr_4.view(), arr_5.view(), arr_6.view(), arr_7.view()]).unwrap().insert_axis(Axis(0));

            (arr_res)
}
#[cfg(test)]
mod tests {
    use geoengine_datatypes::{dataset::{DatasetId, InternalDatasetId}, hashmap, primitives::{Coordinate2D, TimeInterval, SpatialResolution,  Measurement, TimeGranularity, TimeInstance, TimeStep}, raster::{GeoTransform, RasterDataType}, raster::{TilingSpecification}, spatial_reference::{SpatialReference, SpatialReferenceAuthority, SpatialReferenceOption}};
 

    use geoengine_datatypes::{util::Identifier, raster::{RasterPropertiesEntryType}};
    use std::{path::PathBuf};
    use crate::{engine::{MockExecutionContext,MockQueryContext, RasterOperator, RasterResultDescriptor}, source::{FileNotFoundHandling, GdalDatasetParameters, GdalMetaDataRegular, GdalSource, GdalSourceParameters,GdalMetadataMapping, GdalSourceTimePlaceholder, TimeReference, GdalDatasetGeoTransform}};
    use crate::processing::{expression::{Expression, ExpressionParams, ExpressionSources}, meteosat::{offset_key, slope_key, satellite_key, channel_key, radiance::{Radiance, RadianceParams}, temperature::{Temperature, TemperatureParams}, reflectance::{Reflectance, ReflectanceParams}}};
    use crate::engine::SingleRasterSource;
    use super::*;

    #[tokio::test]
    async fn predict_test()  {
        let ctx = MockQueryContext::default();
        //tile size has to be a power of 2
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [512, 512].into());

        let query_spatial_resolution = SpatialResolution::new(3000.4, 3000.4).unwrap();

        let query_bbox = SpatialPartition2D::new((-802607.8468561172485352, 5108186.3898038864135742).into(), (1498701.3813257217407227, 3577980.7752370834350586).into()).unwrap();
        let no_data_value = Some(0.);
        let ir_016 = GdalMetaDataRegular{
            time_placeholders: hashmap! {
                "%%%_TIME_FORMATED_%%%".to_string() => GdalSourceTimePlaceholder {
                    format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_016___-000001___-%Y%m%d%H%M-C_".to_string(),
                    reference: TimeReference::Start,

                }
            },
            result_descriptor: RasterResultDescriptor{
                data_type: RasterDataType::I16,
                spatial_reference: SpatialReferenceOption::
                    SpatialReference(SpatialReference{
                        authority: SpatialReferenceAuthority::SrOrg,
                        code: 81,
                    }),
                measurement: Measurement::Continuous{
                    measurement: "raw".to_string(),
                    unit: None
                },
                no_data_value,
            },
            params: GdalDatasetParameters{
                file_path: PathBuf::from("/mnt/panq/dbs_geo_data/satellite_data/msg_seviri/%%%_TIME_FORMATED_%%%"),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform{
                    origin_coordinate: (-5570248.477339744567871,5570248.477339744567871).into(),
                    x_pixel_size: 3000.403165817260742,
                    y_pixel_size: -3000.403165817260742, 
                },
                width: 3712,
                height: 3712,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: Some(vec![GdalMetadataMapping::identity(offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(satellite_key(), RasterPropertiesEntryType::Number)]),
                gdal_open_options: None,
                gdal_config_options: None,
            },
            //placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
            //time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_016___-000001___-%Y%m%d%H%M-C_".to_string(),
            start: TimeInstance::from_millis(1072917000000).unwrap(),
            step: TimeStep{
                granularity: TimeGranularity::Minutes,
                step: 15,
            }
        };
        let ir_039 = GdalMetaDataRegular{
            time_placeholders: hashmap! {
                "%%%_TIME_FORMATED_%%%".to_string() => GdalSourceTimePlaceholder {
                    format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_039___-000001___-%Y%m%d%H%M-C_".to_string(),
                    reference: TimeReference::Start,

                }
            },
             result_descriptor: RasterResultDescriptor{
                 data_type: RasterDataType::I16,
                 spatial_reference: SpatialReferenceOption::
                     SpatialReference(SpatialReference{
                         authority: SpatialReferenceAuthority::SrOrg,
                         code: 81,
                     }),
                 measurement: Measurement::Continuous{
                    measurement: "raw".to_string(),
                    unit: None
                },
                 no_data_value,
             },
             params: GdalDatasetParameters{
                 file_path: PathBuf::from("/mnt/panq/dbs_geo_data/satellite_data/msg_seviri/%%%_TIME_FORMATED_%%%"),
                 rasterband_channel: 1,
                 geo_transform: GdalDatasetGeoTransform{
                     origin_coordinate: (-5570248.477339744567871,5570248.477339744567871).into(),
                     x_pixel_size: 3000.403165817260742,
                     y_pixel_size: -3000.403165817260742, 
                 },
                 width: 3712,
                 height: 3712,
                 file_not_found_handling: FileNotFoundHandling::NoData,
                 no_data_value,
                 properties_mapping: Some(vec![GdalMetadataMapping::identity(offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(satellite_key(), RasterPropertiesEntryType::Number)]),
                 gdal_open_options: None,
                 gdal_config_options: None,
             },
             //placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
             //time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_039___-000001___-%Y%m%d%H%M-C_".to_string(),
             start: TimeInstance::from_millis(1072917000000).unwrap(),
             step: TimeStep{
                 granularity: TimeGranularity::Minutes,
                step: 15,
            },

        };

        let ir_087 = GdalMetaDataRegular{
            time_placeholders: hashmap! {
                "%%%_TIME_FORMATED_%%%".to_string() => GdalSourceTimePlaceholder {
                    format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_087___-000001___-%Y%m%d%H%M-C_".to_string(),
                    reference: TimeReference::Start,

                }
            },
            result_descriptor: RasterResultDescriptor{
                data_type: RasterDataType::I16,
                spatial_reference: SpatialReferenceOption::
                    SpatialReference(SpatialReference{
                        authority: SpatialReferenceAuthority::SrOrg,
                        code: 81,
                    }),
                measurement: Measurement::Continuous{
                    measurement: "raw".to_string(),
                    unit: None
                },
                no_data_value,
            },
            params: GdalDatasetParameters{
                file_path: PathBuf::from("/mnt/panq/dbs_geo_data/satellite_data/msg_seviri/%%%_TIME_FORMATED_%%%"),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform{
                    origin_coordinate: (-5570248.477, 5570248.477).into(),
                    x_pixel_size: 3000.403165817260742,
                    y_pixel_size: -3000.403165817260742, 
                },
                width: 3712,
                height: 3712,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: Some(vec![GdalMetadataMapping::identity(offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(satellite_key(), RasterPropertiesEntryType::Number)]),
                gdal_open_options: None,
                gdal_config_options: None,
            },
            //placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
            //time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_087___-000001___-%Y%m%d%H%M-C_".to_string(),
            start: TimeInstance::from_millis(1072917000000).unwrap(),
            step: TimeStep{
                granularity: TimeGranularity::Minutes,
                step: 15,
            }

        };

        let ir_097 = GdalMetaDataRegular{
            time_placeholders: hashmap! {
                "%%%_TIME_FORMATED_%%%".to_string() => GdalSourceTimePlaceholder {
                    format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_097___-000001___-%Y%m%d%H%M-C_".to_string(),
                    reference: TimeReference::Start,

                }
            },
            result_descriptor: RasterResultDescriptor{
                data_type: RasterDataType::I16,
                spatial_reference: SpatialReferenceOption::
                    SpatialReference(SpatialReference{
                        authority: SpatialReferenceAuthority::SrOrg,
                        code: 81,
                    }),
                measurement: Measurement::Continuous{
                    measurement: "raw".to_string(),
                    unit: None
                },
                no_data_value,
            },
            params: GdalDatasetParameters{
                file_path: PathBuf::from("/mnt/panq/dbs_geo_data/satellite_data/msg_seviri/%%%_TIME_FORMATED_%%%"),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform{
                    origin_coordinate: (-5570248.477, 5570248.477).into(),
                    x_pixel_size: 3000.403165817260742,
                    y_pixel_size: -3000.403165817260742, 
                },
                width: 3712,
                height: 3712,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: Some(vec![GdalMetadataMapping::identity(offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(satellite_key(), RasterPropertiesEntryType::Number)]),
                gdal_open_options: None,
                gdal_config_options: None,
            },
            //placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
            //time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_097___-000001___-%Y%m%d%H%M-C_".to_string(),
            start: TimeInstance::from_millis(1072917000000).unwrap(),
            step: TimeStep{
                granularity: TimeGranularity::Minutes,
                step: 15,
            }

        };


        let ir_108 = GdalMetaDataRegular{
            time_placeholders: hashmap! {
                "%%%_TIME_FORMATED_%%%".to_string() => GdalSourceTimePlaceholder {
                    format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_108___-000001___-%Y%m%d%H%M-C_".to_string(),
                    reference: TimeReference::Start,

                }
            },
            result_descriptor: RasterResultDescriptor{
                data_type: RasterDataType::I16,
                spatial_reference: SpatialReferenceOption::
                    SpatialReference(SpatialReference{
                        authority: SpatialReferenceAuthority::SrOrg,
                        code: 81,
                    }),
                measurement: Measurement::Continuous{
                    measurement: "raw".to_string(),
                    unit: None
                },
                no_data_value,
            },
            params: GdalDatasetParameters{
                file_path: PathBuf::from("/mnt/panq/dbs_geo_data/satellite_data/msg_seviri/%%%_TIME_FORMATED_%%%"),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform{
                    origin_coordinate: (-5570248.477, 5570248.477).into(),
                    x_pixel_size: 3000.403165817260742,
                    y_pixel_size: -3000.403165817260742, 
                },
                width: 3712,
                height: 3712,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: Some(vec![GdalMetadataMapping::identity(offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(satellite_key(), RasterPropertiesEntryType::Number)]),
                gdal_open_options: None,
                gdal_config_options: None,
            },
            //placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
            //time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_108___-000001___-%Y%m%d%H%M-C_".to_string(),
            start: TimeInstance::from_millis(1072917000000).unwrap(),
            step: TimeStep{
                granularity: TimeGranularity::Minutes,
                step: 15,
            }

        };

        let ir_120 = GdalMetaDataRegular{
            time_placeholders: hashmap! {
                "%%%_TIME_FORMATED_%%%".to_string() => GdalSourceTimePlaceholder {
                    format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_120___-000001___-%Y%m%d%H%M-C_".to_string(),
                    reference: TimeReference::Start,

                }
            },
            result_descriptor: RasterResultDescriptor{
                data_type: RasterDataType::I16,
                spatial_reference: SpatialReferenceOption::
                    SpatialReference(SpatialReference{
                        authority: SpatialReferenceAuthority::SrOrg,
                        code: 81,
                    }),
                measurement: Measurement::Continuous{
                    measurement: "raw".to_string(),
                    unit: None
                },
                no_data_value,
            },
            params: GdalDatasetParameters{
                file_path: PathBuf::from("/mnt/panq/dbs_geo_data/satellite_data/msg_seviri/%%%_TIME_FORMATED_%%%"),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform{
                    origin_coordinate: (-5570248.477, 5570248.477).into(),
                    x_pixel_size: 3000.403165817260742,
                    y_pixel_size: -3000.403165817260742, 
                },
                width: 3712,
                height: 3712,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: Some(vec![GdalMetadataMapping::identity(offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(satellite_key(), RasterPropertiesEntryType::Number)]),
                gdal_open_options: None,
                gdal_config_options: None,
            },
            
            //placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
            //time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_120___-000001___-%Y%m%d%H%M-C_".to_string(),
            start: TimeInstance::from_millis(1072917000000).unwrap(),
            step: TimeStep{
                granularity: TimeGranularity::Minutes,
                step: 15,
            }

        };

        let ir_134 = GdalMetaDataRegular{
            time_placeholders: hashmap! {
                "%%%_TIME_FORMATED_%%%".to_string() => GdalSourceTimePlaceholder {
                    format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_134___-000001___-%Y%m%d%H%M-C_".to_string(),
                    reference: TimeReference::Start,

                }
            },
            result_descriptor: RasterResultDescriptor{
                data_type: RasterDataType::I16,
                spatial_reference: SpatialReferenceOption::
                    SpatialReference(SpatialReference{
                        authority: SpatialReferenceAuthority::SrOrg,
                        code: 81,
                    }),
                measurement: Measurement::Continuous{
                    measurement: "raw".to_string(),
                    unit: None
                },
                no_data_value,
            },
            params: GdalDatasetParameters{
                file_path: PathBuf::from("/mnt/panq/dbs_geo_data/satellite_data/msg_seviri/%%%_TIME_FORMATED_%%%"),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform{
                    origin_coordinate: (-5570248.477, 5570248.477).into(),
                    x_pixel_size: 3000.403165817260742,
                    y_pixel_size: -3000.403165817260742, 
                },
                width: 3712,
                height: 3712,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: Some(vec![GdalMetadataMapping::identity(offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(satellite_key(), RasterPropertiesEntryType::Number)]),
                gdal_open_options: None,
                gdal_config_options: None,
            },
            //placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
            //time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_134___-000001___-%Y%m%d%H%M-C_".to_string(),
            start: TimeInstance::from_millis(1072917000000).unwrap(),
            step: TimeStep{
                granularity: TimeGranularity::Minutes,
                step: 15,
            }

        };

       

        let mut mc = MockExecutionContext::default();
        mc.tiling_specification = tiling_specification;

        let id_ir_016 = DatasetId::Internal{
            dataset_id: InternalDatasetId::new(),
        };

        let op_ir_016 = Box::new(GdalSource{
            params: GdalSourceParameters{
                dataset: id_ir_016.clone(),
            }
        });

        
        mc.add_meta_data(id_ir_016, Box::new(ir_016.clone()));

        let rad_ir_016 = RasterOperator::boxed(Radiance{
            params: RadianceParams{
                
            },
            sources: SingleRasterSource{
                raster: op_ir_016
            }
        });
        let ref_ir_016 = RasterOperator::boxed(Reflectance {
            params: ReflectanceParams::default(),
            sources: SingleRasterSource{
                raster: rad_ir_016,
            }
        });

        let exp_ir_016 = RasterOperator::boxed(Expression{
            params: ExpressionParams { expression: "(A-0.15282721917305925)/0.20047640300325603".to_string(), output_type: RasterDataType::F32, output_no_data_value: no_data_value.unwrap(), output_measurement: Some(Measurement::Continuous{
                measurement: "raw".to_string(),
                unit: None,
            })
        },
        sources: ExpressionSources{
            a: ref_ir_016.clone(),
            b: Some(ref_ir_016),
            c: None,
        }
        });
        
        let proc_ir_016 = exp_ir_016
        .initialize(&mc).await.unwrap().query_processor().unwrap().get_f32().unwrap();

        let id_ir_039 = DatasetId::Internal{
            dataset_id: InternalDatasetId::new(),
        };

        let op_ir_039 = Box::new(GdalSource{
            params: GdalSourceParameters{
                dataset: id_ir_039.clone(),
            }
        });

        
        mc.add_meta_data(id_ir_039, Box::new(ir_039.clone()));

        let temp_ir_039 = RasterOperator::boxed(Temperature{
            params: TemperatureParams::default(),
            sources: SingleRasterSource{
                raster: op_ir_039
            }
        });

        let exp_ir_039 = RasterOperator::boxed(Expression{
            params: ExpressionParams { expression: "(A-276.72667474831303)/15.982918482298778".to_string(), output_type: RasterDataType::F32, output_no_data_value: no_data_value.unwrap(), output_measurement: Some(Measurement::Continuous{
                measurement: "raw".to_string(),
                unit: None,
            })
        },
        sources: ExpressionSources{
            a: temp_ir_039.clone(),
            b: Some(temp_ir_039),
            c: None,
        }
        });
        let proc_ir_039 = exp_ir_039
        .initialize(&mc).await.unwrap().query_processor().unwrap().get_f32().unwrap();

        let id_ir_087 = DatasetId::Internal{
            dataset_id: InternalDatasetId::new(),
        };

        let op_ir_087 = Box::new(GdalSource{
            params: GdalSourceParameters{
                dataset: id_ir_087.clone(),
            }
        });

        
        mc.add_meta_data(id_ir_087, Box::new(ir_087.clone()));

        let temp_ir_087 = RasterOperator::boxed(Temperature{
            params: TemperatureParams::default(),
            sources: SingleRasterSource{
                raster: op_ir_087
            }
        });

        let exp_ir_087 = RasterOperator::boxed(Expression{
            params: ExpressionParams { expression: "(A-267.92274094012157)/15.763409602725156".to_string(), output_type: RasterDataType::F32, output_no_data_value: no_data_value.unwrap(), output_measurement: Some(Measurement::Continuous{
                measurement: "raw".to_string(),
                unit: None,
            })
        },
        sources: ExpressionSources{
            a: temp_ir_087.clone(),
            b: Some(temp_ir_087),
            c: None,
        }
        });
        let proc_ir_087 = exp_ir_087
        .initialize(&mc).await.unwrap().query_processor().unwrap().get_f32().unwrap();

        let id_ir_097 = DatasetId::Internal{
            dataset_id: InternalDatasetId::new(),
        };

        let op_ir_097 = Box::new(GdalSource{
            params: GdalSourceParameters{
                dataset: id_ir_097.clone(),
            }
        });

        
        mc.add_meta_data(id_ir_097, Box::new(ir_097.clone()));

        let temp_ir_097 = RasterOperator::boxed(Temperature{
            params: TemperatureParams::default(),
            sources: SingleRasterSource{
                raster: op_ir_097
            }
        });

        let exp_ir_097 = RasterOperator::boxed(Expression{
            params: ExpressionParams { expression: "(A-245.4006454137375)/9.644958714922186".to_string(), output_type: RasterDataType::F32, output_no_data_value: no_data_value.unwrap(), output_measurement: Some(Measurement::Continuous{
                measurement: "raw".to_string(),
                unit: None,
            })
        },
        sources: ExpressionSources{
            a: temp_ir_097.clone(),
            b: Some(temp_ir_097),
            c: None,
        }
        });
        let proc_ir_097 = exp_ir_097
        .initialize(&mc).await.unwrap().query_processor().unwrap().get_f32().unwrap();
        
        let id_ir_108 = DatasetId::Internal{
            dataset_id: InternalDatasetId::new(),
        };

        let op_ir_108 = Box::new(GdalSource{
            params: GdalSourceParameters{
                dataset: id_ir_108.clone(),
            }
        });

        
        mc.add_meta_data(id_ir_108, Box::new(ir_108.clone()));

        let temp_ir_108 = RasterOperator::boxed(Temperature{
            params: TemperatureParams::default(),
            sources: SingleRasterSource{
                raster: op_ir_108
            }
        });

        let exp_ir_108 = RasterOperator::boxed(Expression{
            params: ExpressionParams { expression: "(A-269.95727803541155)/16.92469947004928".to_string(), output_type: RasterDataType::F32, output_no_data_value: no_data_value.unwrap(), output_measurement: Some(Measurement::Continuous{
                measurement: "raw".to_string(),
                unit: None,
            })
        },
        sources: ExpressionSources{
            a: temp_ir_108.clone(),
            b: Some(temp_ir_108),
            c: None,
        }
        });
        let proc_ir_108 = exp_ir_108
        .initialize(&mc).await.unwrap().query_processor().unwrap().get_f32().unwrap();


        let id_ir_120 = DatasetId::Internal{
            dataset_id: InternalDatasetId::new(),
        };

        let op_ir_120 = Box::new(GdalSource{
            params: GdalSourceParameters{
                dataset: id_ir_120.clone(),
            }
        });

        
        mc.add_meta_data(id_ir_120, Box::new(ir_120.clone()));

        let temp_ir_120 = RasterOperator::boxed(Temperature{
            params: TemperatureParams::default(),
            sources: SingleRasterSource{
                raster: op_ir_120
            }
        });
        let exp_ir_120 = RasterOperator::boxed(Expression{
            params: ExpressionParams { expression: "(A-268.69063154766155)/16.963088951563815".to_string(), output_type: RasterDataType::F32, output_no_data_value: no_data_value.unwrap(), output_measurement: Some(Measurement::Continuous{
                measurement: "raw".to_string(),
                unit: None,
            })
        },
        sources: ExpressionSources{
            a: temp_ir_120.clone(),
            b: Some(temp_ir_120),
            c: None,
        }
        });
        let proc_ir_120 = exp_ir_120
        .initialize(&mc).await.unwrap().query_processor().unwrap().get_f32().unwrap();

        let id_ir_134 = DatasetId::Internal{
            dataset_id: InternalDatasetId::new(),
        };

        let op_ir_134 = Box::new(GdalSource{
            params: GdalSourceParameters{
                dataset: id_ir_134.clone(),
            }
        });

        
        mc.add_meta_data(id_ir_134, Box::new(ir_134.clone()));

        let temp_ir_134 = RasterOperator::boxed(Temperature{
            params: TemperatureParams::default(),
            sources: SingleRasterSource{
                raster: op_ir_134
            }
        });
        let exp_ir_134 = RasterOperator::boxed(Expression{
            params: ExpressionParams { expression: "(A-252.1465931705522)/11.171453493090551".to_string(), output_type: RasterDataType::F32, output_no_data_value: no_data_value.unwrap(), output_measurement: Some(Measurement::Continuous{
                measurement: "raw".to_string(),
                unit: None,
            })
        },
        sources: ExpressionSources{
            a: temp_ir_134.clone(),
            b: Some(temp_ir_134),
            c: None,
        }
        });
        let proc_ir_134 = exp_ir_134
        .initialize(&mc).await.unwrap().query_processor().unwrap().get_f32().unwrap();

        let classes: Vec<u8> = vec![0,1,2,3];


        pyo3::prepare_freethreaded_python();
        let gil = pyo3::Python::acquire_gil();
        let py = gil.python();

        let name = PyUnicode::new(py, "second");
        let py_mod = PyModule::from_code(py, include_str!("tf_v2.py"), "tf_v2.py", "tf_v2")
        .unwrap();
        let _load = py_mod.call("load", (name, ), None).unwrap();

        let processor = ImsegProcessor {
            rasters: vec![proc_ir_016, proc_ir_039, proc_ir_087, proc_ir_097, proc_ir_108, proc_ir_120, proc_ir_134],
            ndv: 255,
            classes: classes,
            pymod_tf: py_mod
            .into_py(py),
        };

        

        let mut x = processor.raster_query(QueryRectangle {
            spatial_bounds: query_bbox,
            time_interval: TimeInterval::new(1_388_536_200_000, 1_388_536_200_000 + 1000)
                .unwrap(),
            spatial_resolution: query_spatial_resolution,
        }, 
    &ctx).await.unwrap();

    while let Some(result) = x.try_next().await.unwrap() {
        match result.grid_array {
            GridOrEmpty::Grid(g) => {
                println!("{:?}", g.data[0]);
                
            },
            _ => {
                println!("Something went wrong");
                
            }
        }
    }

    }
    
}