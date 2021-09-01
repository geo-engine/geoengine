// use std::future;
// use futures::stream::BoxStream;
// use futures::{StreamExt, TryStreamExt};
// use chrono::NaiveDate;
// use futures::TryFutureExt;
// use geoengine_datatypes::primitives::SpatialPartition2D;
// use geoengine_datatypes::raster::{EmptyGrid, GeoTransformAccess, GridShapeAccess};
// use geoengine_datatypes::raster::NoDataValue;
// use geoengine_datatypes::{
//     primitives::TimeInterval,
//     raster::{Grid2D, Pixel, Raster, RasterTile2D, GridOrEmpty},
// };
// use crate::{
//     engine::{
//         ExecutionContext, InitializedRasterOperator,
//         InitializedVectorOperator, QueryContext, QueryProcessor, QueryRectangle, RasterOperator,
//         RasterQueryProcessor, RasterResultDescriptor, TypedRasterQueryProcessor, VectorOperator,
//     },
//     error::Error as GeoengineOperatorsError,
//     util::Result,
// };

// use serde::{Deserialize, Serialize};

// use async_trait::async_trait;

// use ndarray::{Array2, stack, Axis};
// use numpy::{PyArray, PyArray4};
// use pyo3::prelude::*;
// use pyo3::{types::{PyModule, PyUnicode}, Py, Python };

// use std::{error::Error, fmt};


// #[derive(Debug, Serialize, Deserialize, Clone)]
// pub struct ImsegOperator {
//     pub raster_sources: Vec<Box<dyn RasterOperator>>,
//     pub vector_sources: Vec<Box<dyn VectorOperator>>,
// }

// #[typetag::serde]
// #[async_trait]
// impl RasterOperator for ImsegOperator {
//     async fn initialize(
//         mut self: Box<Self>,
//         context: &dyn ExecutionContext,
//     ) -> Result<Box<InitializedRasterOperator>> {
//         if !self.vector_sources.is_empty() {
//             return Err(GeoengineOperatorsError::InvalidNumberOfVectorInputs {
//                 expected: 0..1,
//                 found: self.vector_sources.len(),
//             });
//         }

//         if self.raster_sources.len() != 1 {
//             return Err(GeoengineOperatorsError::InvalidNumberOfRasterInputs {
//                 expected: 1..2,
//                 found: self.raster_sources.len(),
//             });
//         }

//         let initialized_raster = self
//             .raster_sources
//             .pop()
//             .expect("checked")
//             .initialize(context).await.unwrap();
//         let result_descriptor = initialized_raster.result_descriptor().clone();

//         let initialized_operator = InitializedImsegOperator {
//             raster_sources: vec![initialized_raster],
//             vector_sources: vec![],
//             result_descriptor,
//             state: (),
//         };

//         Ok(initialized_operator.boxed())
//     }
// }

// pub struct InitializedImsegOperator {
//     pub raster_sources: Vec<Box<dyn InitializedRasterOperator>>,
//     pub vector_sources: Vec<Box<dyn InitializedVectorOperator>>,
//     pub result_descriptor: RasterResultDescriptor,
//     pub state: (),
// }

// impl InitializedRasterOperator for InitializedImsegOperator {
//     fn result_descriptor(&self) -> &RasterResultDescriptor {
//         &self.result_descriptor
//     }

//     fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
//         let number_of_processors = self.raster_sources.len();
//         let typed_raster_processor_ir016 = self.raster_sources[0].query_processor()?;
//         let typed_raster_processor_ir039 = self.raster_sources[1].query_processor()?;
//         let typed_raster_processor_ir087 = self.raster_sources[2].query_processor()?;
//         let typed_raster_processor_ir097 = self.raster_sources[3].query_processor()?;
//         let typed_raster_processor_ir108 = self.raster_sources[4].query_processor()?;
//         let typed_raster_processor_ir120 = self.raster_sources[5].query_processor()?;
//         let typed_raster_processor_ir134 = self.raster_sources[6].query_processor()?;

//         match (typed_raster_processor_ir016, typed_raster_processor_ir039, typed_raster_processor_ir087, typed_raster_processor_ir097, typed_raster_processor_ir108, typed_raster_processor_ir120, typed_raster_processor_ir134) {
//             (TypedRasterQueryProcessor::I16(ir_016),TypedRasterQueryProcessor::I16(ir_039), TypedRasterQueryProcessor::I16(ir_087), TypedRasterQueryProcessor::I16(ir_097), TypedRasterQueryProcessor::I16(ir_108), TypedRasterQueryProcessor::I16(ir_120), TypedRasterQueryProcessor::I16(ir_134)) => {
//                 Ok(TypedRasterQueryProcessor::I16(ImsegProcessor::new(vec![ir_016,ir_039,ir_087,ir_097,ir_108,ir_120,ir_134], vec![0,1,2,3], -999).boxed()))
//             }, 
//             _ => {
//                 panic!("Something went terribly wrong...")
//             }
//         }
//     }
// }

// pub struct ImsegProcessor<T, U> {
//     rasters: Vec<Box<dyn RasterQueryProcessor<RasterType = T>>>,
//     classes: Vec<U>,
//     ndv: U,
//     pymod_tf: Py<PyModule>,
// }

// impl<T, U> ImsegProcessor<T, U>
// where
// T: Pixel + numpy::Element,
// U: Pixel + numpy::Element,

// {
//     pub fn new(rasters: Vec<Box<dyn RasterQueryProcessor<RasterType = T>>>, classes: Vec<U>, ndv: U) -> Self {
//         //temporary py stuff
//         let gil = Python::acquire_gil();
//         let py = gil.python();
        
        
//         // saving your python script file as a struct field
//         // using this, we can access python functions and objects without loss of memory state
//         // on successive iterations
//         let name = PyUnicode::new(py, "first");
        
//         let py_mod: Py<PyModule> =
//             PyModule::from_code(py, include_str!("tf_v2.py"), "tf_v2.py", "tf_v2")
//                 .unwrap()
//                 .into_py(py);
//         let _load = py_mod.as_ref(py).call("load", (name, ), None).unwrap();
        

        
//         Self {
//             rasters: rasters,
//             pymod_tf: py_mod,
//             classes: classes,
//             ndv: ndv,
//         }
        
//     }

//     pub fn predict(&self, ir_016: Result<RasterTile2D<T>>, ir_039: Result<RasterTile2D<T>>, ir_087: Result<RasterTile2D<T>>, ir_097: Result<RasterTile2D<T>>, ir_108: Result<RasterTile2D<T>>, ir_120: Result<RasterTile2D<T>>, ir_137: Result<RasterTile2D<T>>) -> Result<RasterTile2D<U>> {
//         println!("started prediction!");
//         let (ir016, ir039, ir087, ir097, ir108, ir120, ir134) = (ir_016?, ir_039?, ir_087?, ir_097?, ir_108?, ir_120?, ir_137?);
        
//         let time = ir016.time;
//         let position = ir016.tile_position;
//         let geo = ir016.geo_transform();
//         //let ndv = ir_016.grid_array.no_data_value();
//         let tile_size = ir016.grid_array.grid_shape_array();
//         let shape = ir016.grid_array.grid_shape();
//         match (ir016.grid_array, ir039.grid_array, ir087.grid_array, ir097.grid_array, ir108.grid_array, ir120.grid_array, ir134.grid_array) {
        
//             (GridOrEmpty::Grid(grid_016), GridOrEmpty::Grid(grid_039),  GridOrEmpty::Grid(grid_087),  GridOrEmpty::Grid(grid_097), GridOrEmpty::Grid(grid_108), GridOrEmpty::Grid(grid_120), GridOrEmpty::Grid(grid_137)) => {
                
//                 let gil = Python::acquire_gil();
//                 let py = gil.python();

//                 let data_016 = grid_016.data;
//                 let data_039 = grid_039.data;
//                 let data_087 = grid_087.data;
//                 let data_097 = grid_097.data;
//                 let data_108 = grid_108.data;
//                 let data_120 = grid_120.data;
//                 let data_137 = grid_137.data;

//                 let tile_size = grid_016.shape.shape_array;

//                 let arr_016: ndarray::Array2<T> = 
//                 Array2::from_shape_vec((tile_size[0], tile_size[1]), data_016)
//                 .unwrap();
//                 let arr_039: ndarray::Array2<T> = 
//                 Array2::from_shape_vec((tile_size[0], tile_size[1]), data_039)
//                 .unwrap();
//                 let arr_087: ndarray::Array2<T> = 
//                 Array2::from_shape_vec((tile_size[0], tile_size[1]), data_087)
//                 .unwrap();
//                 let arr_097: ndarray::Array2<T> = 
//                 Array2::from_shape_vec((tile_size[0], tile_size[1]), data_097)
//                 .unwrap()
//                 .to_owned();
//                 let arr_108: ndarray::Array2<T> = 
//                 Array2::from_shape_vec((tile_size[0], tile_size[1]), data_108)
//                 .unwrap()
//                 .to_owned();
//                 let arr_120: ndarray::Array2<T> = 
//                 Array2::from_shape_vec((tile_size[0], tile_size[1]), data_120)
//                 .unwrap()
//                 .to_owned();
//                 let arr_134: ndarray::Array2<T> = 
//                 Array2::from_shape_vec((tile_size[0], tile_size[1]), data_137)
//                 .unwrap()
//                 .to_owned();

//                 let arr_img: ndarray::Array<T, _> = stack(Axis(2), &[arr_016.view(),arr_039.view(),arr_087.view(), arr_097.view(), arr_108.view(), arr_120.view(), arr_134.view()]).unwrap();
                                        
//                 let arr_img_batch = arr_img.insert_axis(Axis(0)); // add a leading axis for the batches!

//                 dbg!(&arr_img_batch.shape());
                        
//                 let py_img_batch = PyArray::from_owned_array(py, arr_img_batch);

//                 let result_img = self.pymod_tf.as_ref(py).call("predict", (py_img_batch,), None)
//                 .unwrap()
//                 .downcast::<PyArray4<f32>>()
//                 .unwrap()
//                 .to_owned_array();

//                 let mut segmap = Array2::<U>::from_elem((512,512), self.classes[0]);
//                 let result = result_img.slice(ndarray::s![0,..,..,..]);
//                 for i in 0..512 {
//                     for j in 0..512 {
//                         let view = result.slice(ndarray::s![i,j,..]);
//                         let mut max: f32 = 0.0;
//                         let mut max_class = self.classes[0];
                                
//                             for t in 0..3 {
//                                 //println!("predited: {:?}", view[t as usize]);
                                
//                                 if max <= view[t as usize] {
//                                     max = view[t as usize];
//                                     max_class = self.classes[t];
//                                 }
//                             }
//                             segmap[[i as usize, j as usize]] = max_class;
//                         }
//                 }
//                 println!("{:?}", segmap);
                
//                 Ok(RasterTile2D::new(
//                             time,
//                 position,
//             geo,
//                          GridOrEmpty::Grid(Grid2D::new(
//                         shape,
//                         segmap.into_raw_vec(),
//                         Some(self.ndv),
//                         )?),
//                     ))
//                 },
//             _ => {
//                 Ok(RasterTile2D::new(
//                     time,
//                     position,
//                     geo,
//                     GridOrEmpty::Empty(EmptyGrid::new(
//                         shape,
//                         self.ndv
//                     ))
//                 ))
//             }
//         }
//     }
// }
// #[async_trait]
// impl<T, U> RasterQueryProcessor for ImsegProcessor<T, U>
// where  
// T: Pixel + numpy::Element,
// U: Pixel + numpy::Element, 
// {
//     type RasterType = U;

//     async fn raster_query<'a>(
//         &'a self,
//         query: QueryRectangle<SpatialPartition2D>,
//         ctx: &'a dyn QueryContext,
//     ) -> Result<BoxStream<'a, Result<RasterTile2D<Self::RasterType>>>> {
//         let stream_ir_016 = self.rasters[0].raster_query(query, ctx).await?;
//         let stream_ir_039 = self.rasters[1].raster_query(query, ctx).await?;
//         let stream_ir_087 = self.rasters[2].raster_query(query, ctx).await?;
//         let stream_ir_097 = self.rasters[3].raster_query(query, ctx).await?;
//         let stream_ir_108 = self.rasters[4].raster_query(query, ctx).await?;
//         let stream_ir_120 = self.rasters[5].raster_query(query, ctx).await?;
//         let stream_ir_134 = self.rasters[6].raster_query(query, ctx).await?;

//         let final_stream = stream_ir_016.zip(stream_ir_039.zip(stream_ir_087.zip(stream_ir_097.zip(stream_ir_108.zip(stream_ir_120.zip(stream_ir_134))))));
//         println!("Query started");
        

//         Ok(final_stream.map(move |(ir_016, (ir_039, (ir_087, (ir_097, (ir_108, (ir_120, ir_137))))))| self.predict(ir_016, ir_039, ir_087, ir_097, ir_108, ir_120, ir_137)).boxed())

//     }
// }

// #[cfg(test)]
// mod tests {
//     use std::{borrow::Borrow, fmt::Result};

//     use futures::TryStreamExt;
//     use geoengine_datatypes::{
//         primitives::{BoundingBox2D, Coordinate2D, SpatialResolution},
//         raster::TilingSpecification, dataset::*,
//     };
//     use pyo3::prelude::*;
//     use pyo3::{types::{PyModule, PyUnicode}, Py, Python };

//     use crate::{
//         engine::{MockQueryContext,  MockExecutionContext, QueryContext, QueryRectangle, RasterOperator,
//             RasterQueryProcessor, RasterResultDescriptor,}, source::GdalSourceProcessor, util::gdal::create_ndvi_meta_data,
//     };
//     use geoengine_datatypes::util::Identifier;
//     use crate::source::{FileNotFoundHandling, GdalSourceParameters,GdalMetaDataRegular, GdalSource,};

//     use geoengine_datatypes::{raster::{GeoTransform, RasterDataType}, spatial_reference::{SpatialReference, SpatialReferenceAuthority, SpatialReferenceOption}, 
//         primitives::{TimeInterval,  Measurement, TimeGranularity, TimeInstance, TimeStep},
//     };
//     use std::path::PathBuf;
//     use crate::{source::{GdalDatasetParameters}};
//     use super::*;

//     #[tokio::test]
//     async fn predict_test() -> Result  {
//         let ctx = MockQueryContext::default();
//         //tile size has to be a power of 2
//         let tiling_specification =
//             TilingSpecification::new(Coordinate2D::default(), [512, 512].into());

//         let query_spatial_resolution = SpatialResolution::new(3000.4, 3000.4).unwrap();

//         let query_bbox = SpatialPartition2D::new((0.0, 30000.0).into(), (30000.0, 0.0).into()).unwrap();
//         let no_data_value = Some(0.);
        
//         let ir_016 = GdalMetaDataRegular{
//             result_descriptor: RasterResultDescriptor{
//                 data_type: RasterDataType::I16,
//                 spatial_reference: SpatialReferenceOption::
//                     SpatialReference(SpatialReference{
//                         authority: SpatialReferenceAuthority::SrOrg,
//                         code: 81,
//                     }),
//                 measurement: Measurement::Unitless,
//                 no_data_value,
//             },
//             params: GdalDatasetParameters{
//                 file_path: PathBuf::from("/mnt/panq/dbs_geo_data/satellite_data/msg_seviri/%%%_TIME_FORMATED_%%%"),
//                 rasterband_channel: 1,
//                 geo_transform: GeoTransform{
//                     origin_coordinate: (-5570248.477339744567871,5570248.477339744567871).into(),
//                     x_pixel_size: 3000.403165817260742,
//                     y_pixel_size: -3000.403165817260742, 
//                 },
//                 width: 11136,
//                 height: 11136,
//                 file_not_found_handling: FileNotFoundHandling::NoData,
//                 no_data_value,
//                 properties_mapping: None,
//                 gdal_open_options: None,
//             },
//             placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
//             time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_016___-000001___-%Y%m%d%H%M-C_".to_string(),
//             start: TimeInstance::from_millis(1072917000000).unwrap(),
//             step: TimeStep{
//                 granularity: TimeGranularity::Minutes,
//                 step: 15,
//             }
//         };
//         let ir_039 = GdalMetaDataRegular{
//              result_descriptor: RasterResultDescriptor{
//                  data_type: RasterDataType::I16,
//                  spatial_reference: SpatialReferenceOption::
//                      SpatialReference(SpatialReference{
//                          authority: SpatialReferenceAuthority::SrOrg,
//                          code: 81,
//                      }),
//                  measurement: Measurement::Unitless,
//                  no_data_value,
//              },
//              params: GdalDatasetParameters{
//                  file_path: PathBuf::from("/mnt/panq/dbs_geo_data/satellite_data/msg_seviri/%%%_TIME_FORMATED_%%%"),
//                  rasterband_channel: 1,
//                  geo_transform: GeoTransform{
//                      origin_coordinate: (-5570248.477339744567871,5570248.477339744567871).into(),
//                      x_pixel_size: 3000.403165817260742,
//                      y_pixel_size: -3000.403165817260742, 
//                  },
//                  width: 11136,
//                  height: 11136,
//                  file_not_found_handling: FileNotFoundHandling::NoData,
//                  no_data_value,
//                  properties_mapping: None,
//                  gdal_open_options: None
//              },
//              placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
//              time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_039___-000001___-%Y%m%d%H%M-C_".to_string(),
//              start: TimeInstance::from_millis(1072917000000).unwrap(),
//              step: TimeStep{
//                  granularity: TimeGranularity::Minutes,
//                 step: 15,
//             }

//         };

//         let ir_087 = GdalMetaDataRegular{
//             result_descriptor: RasterResultDescriptor{
//                 data_type: RasterDataType::I16,
//                 spatial_reference: SpatialReferenceOption::
//                     SpatialReference(SpatialReference{
//                         authority: SpatialReferenceAuthority::SrOrg,
//                         code: 81,
//                     }),
//                 measurement: Measurement::Unitless,
//                 no_data_value,
//             },
//             params: GdalDatasetParameters{
//                 file_path: PathBuf::from("/mnt/panq/dbs_geo_data/satellite_data/msg_seviri/%%%_TIME_FORMATED_%%%"),
//                 rasterband_channel: 1,
//                 geo_transform: GeoTransform{
//                     origin_coordinate: (-5570248.477, 5570248.477).into(),
//                     x_pixel_size: 3000.403165817260742,
//                     y_pixel_size: -3000.403165817260742, 
//                 },
//                 width: 11136,
//                 height: 11136,
//                 file_not_found_handling: FileNotFoundHandling::NoData,
//                 no_data_value,
//                 properties_mapping: None,
//                 gdal_open_options: None,
//             },
//             placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
//             time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_087___-000001___-%Y%m%d%H%M-C_".to_string(),
//             start: TimeInstance::from_millis(1072917000000).unwrap(),
//             step: TimeStep{
//                 granularity: TimeGranularity::Minutes,
//                 step: 15,
//             }

//         };

//         let ir_097 = GdalMetaDataRegular{
//             result_descriptor: RasterResultDescriptor{
//                 data_type: RasterDataType::I16,
//                 spatial_reference: SpatialReferenceOption::
//                     SpatialReference(SpatialReference{
//                         authority: SpatialReferenceAuthority::SrOrg,
//                         code: 81,
//                     }),
//                 measurement: Measurement::Unitless,
//                 no_data_value,
//             },
//             params: GdalDatasetParameters{
//                 file_path: PathBuf::from("/mnt/panq/dbs_geo_data/satellite_data/msg_seviri/%%%_TIME_FORMATED_%%%"),
//                 rasterband_channel: 1,
//                 geo_transform: GeoTransform{
//                     origin_coordinate: (-5570248.477, 5570248.477).into(),
//                     x_pixel_size: 3000.403165817260742,
//                     y_pixel_size: -3000.403165817260742, 
//                 },
//                 width: 11136,
//                 height: 11136,
//                 file_not_found_handling: FileNotFoundHandling::NoData,
//                 no_data_value,
//                 properties_mapping: None,
//                 gdal_open_options: None,
//             },
//             placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
//             time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_097___-000001___-%Y%m%d%H%M-C_".to_string(),
//             start: TimeInstance::from_millis(1072917000000).unwrap(),
//             step: TimeStep{
//                 granularity: TimeGranularity::Minutes,
//                 step: 15,
//             }

//         };


//         let ir_108 = GdalMetaDataRegular{
//             result_descriptor: RasterResultDescriptor{
//                 data_type: RasterDataType::I16,
//                 spatial_reference: SpatialReferenceOption::
//                     SpatialReference(SpatialReference{
//                         authority: SpatialReferenceAuthority::SrOrg,
//                         code: 81,
//                     }),
//                 measurement: Measurement::Unitless,
//                 no_data_value,
//             },
//             params: GdalDatasetParameters{
//                 file_path: PathBuf::from("/mnt/panq/dbs_geo_data/satellite_data/msg_seviri/%%%_TIME_FORMATED_%%%"),
//                 rasterband_channel: 1,
//                 geo_transform: GeoTransform{
//                     origin_coordinate: (-5570248.477, 5570248.477).into(),
//                     x_pixel_size: 3000.403165817260742,
//                     y_pixel_size: -3000.403165817260742, 
//                 },
//                 width: 11136,
//                 height: 11136,
//                 file_not_found_handling: FileNotFoundHandling::NoData,
//                 no_data_value,
//                 properties_mapping: None,
//                 gdal_open_options: None,
//             },
//             placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
//             time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_108___-000001___-%Y%m%d%H%M-C_".to_string(),
//             start: TimeInstance::from_millis(1072917000000).unwrap(),
//             step: TimeStep{
//                 granularity: TimeGranularity::Minutes,
//                 step: 15,
//             }

//         };

//         let ir_120 = GdalMetaDataRegular{
//             result_descriptor: RasterResultDescriptor{
//                 data_type: RasterDataType::I16,
//                 spatial_reference: SpatialReferenceOption::
//                     SpatialReference(SpatialReference{
//                         authority: SpatialReferenceAuthority::SrOrg,
//                         code: 81,
//                     }),
//                 measurement: Measurement::Unitless,
//                 no_data_value,
//             },
//             params: GdalDatasetParameters{
//                 file_path: PathBuf::from("/mnt/panq/dbs_geo_data/satellite_data/msg_seviri/%%%_TIME_FORMATED_%%%"),
//                 rasterband_channel: 1,
//                 geo_transform: GeoTransform{
//                     origin_coordinate: (-5570248.477, 5570248.477).into(),
//                     x_pixel_size: 3000.403165817260742,
//                     y_pixel_size: -3000.403165817260742, 
//                 },
//                 width: 11136,
//                 height: 11136,
//                 file_not_found_handling: FileNotFoundHandling::NoData,
//                 no_data_value,
//                 properties_mapping: None,
//                 gdal_open_options: None,
//             },
//             placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
//             time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_120___-000001___-%Y%m%d%H%M-C_".to_string(),
//             start: TimeInstance::from_millis(1072917000000).unwrap(),
//             step: TimeStep{
//                 granularity: TimeGranularity::Minutes,
//                 step: 15,
//             }

//         };

//         let ir_134 = GdalMetaDataRegular{
//             result_descriptor: RasterResultDescriptor{
//                 data_type: RasterDataType::I16,
//                 spatial_reference: SpatialReferenceOption::
//                     SpatialReference(SpatialReference{
//                         authority: SpatialReferenceAuthority::SrOrg,
//                         code: 81,
//                     }),
//                 measurement: Measurement::Unitless,
//                 no_data_value,
//             },
//             params: GdalDatasetParameters{
//                 file_path: PathBuf::from("/mnt/panq/dbs_geo_data/satellite_data/msg_seviri/%%%_TIME_FORMATED_%%%"),
//                 rasterband_channel: 1,
//                 geo_transform: GeoTransform{
//                     origin_coordinate: (-5570248.477, 5570248.477).into(),
//                     x_pixel_size: 3000.403165817260742,
//                     y_pixel_size: -3000.403165817260742, 
//                 },
//                 width: 11136,
//                 height: 11136,
//                 file_not_found_handling: FileNotFoundHandling::NoData,
//                 no_data_value,
//                 properties_mapping: None,
//                 gdal_open_options: None,
//             },
//             placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
//             time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_134___-000001___-%Y%m%d%H%M-C_".to_string(),
//             start: TimeInstance::from_millis(1072917000000).unwrap(),
//             step: TimeStep{
//                 granularity: TimeGranularity::Minutes,
//                 step: 15,
//             }

//         };

//         let claas = GdalMetaDataRegular{
//             result_descriptor: RasterResultDescriptor{
//                 data_type: RasterDataType::U8,
//                 spatial_reference: SpatialReferenceOption::
//                     SpatialReference(SpatialReference{
//                         authority: SpatialReferenceAuthority::SrOrg,
//                         code: 81,
//                     }),
//                 measurement: Measurement::Unitless,
//                 no_data_value,
//             },
//             params: GdalDatasetParameters{
//                 file_path: PathBuf::from("NETCDF:\"/mnt/panq/dbs_geo_data/satellite_data/CLAAS-2/level2/%%%_TIME_FORMATED_%%%.nc\":cma"),
//                 rasterband_channel: 1,
//                 geo_transform: GeoTransform{
//                     origin_coordinate: ( -5456233.41938636, 5456233.41938636).into(),
//                     x_pixel_size: 3000.403165817260742,
//                     y_pixel_size: -3000.403165817260742, 
//                 },
//                 width: 11136,
//                 height: 11136,
//                 file_not_found_handling: FileNotFoundHandling::NoData,
//                 no_data_value,
//                 properties_mapping: None,
//                 gdal_open_options: None,
//             },
//             placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
//             time_format: "%Y/%m/%d/CMAin%Y%m%d%H%M00305SVMSG01MD".to_string(),
//             start: TimeInstance::from_millis(1072917000000).unwrap(),
//             step: TimeStep{
//                 granularity: TimeGranularity::Minutes,
//                 step: 15,
//             }

//         };


//         let source_a = GdalSourceProcessor::<i16>{
//             tiling_specification,
//             meta_data: Box::new(ir_016),
//             phantom_data: Default::default(),
//         };
//         let source_b = GdalSourceProcessor::<i16>{
//             tiling_specification,
//             meta_data: Box::new(ir_039),
//             phantom_data: Default::default(),
//          };
//          let source_c = GdalSourceProcessor::<i16>{
//            tiling_specification,
//            meta_data: Box::new(ir_087),
//            phantom_data: Default::default(),
//          };
//         let source_d = GdalSourceProcessor::<i16>{
//             tiling_specification,
//             meta_data: Box::new(ir_097),
//             phantom_data: Default::default(),
//         };

//         let source_e = GdalSourceProcessor::<i16>{
//             tiling_specification,
//             meta_data: Box::new(ir_108),
//             phantom_data: Default::default(),
//         };

//         let source_f = GdalSourceProcessor::<i16>{
//             tiling_specification,
//             meta_data: Box::new(ir_120),
//             phantom_data: Default::default(),
//         };
   
//         let source_g = GdalSourceProcessor::<i16>{
//             tiling_specification,
//             meta_data: Box::new(ir_134),
//             phantom_data: Default::default(),
//         };

//         let source_h = GdalSourceProcessor::<u8>{
//             tiling_specification,
//             meta_data: Box::new(claas),
//             phantom_data: Default::default(),
//         };

//         let classes: Vec<u8> = vec![0,1,2,3];


//         pyo3::prepare_freethreaded_python();
//         let gil = pyo3::Python::acquire_gil();
//         let py = gil.python();

//         let name = PyUnicode::new(py, "first");
//         let py_mod = PyModule::from_code(py, include_str!("tf_v2.py"), "tf_v2.py", "tf_v2")
//         .unwrap();
//         let _load = py_mod.call("load", (name, ), None).unwrap();

//         let processor = ImsegProcessor {
//             rasters: vec![source_a.boxed(), source_b.boxed(), source_c.boxed(), source_d.boxed(), source_e.boxed(), source_f.boxed(), source_g.boxed()],
//             ndv: 255,
//             classes: classes,
//             pymod_tf: py_mod
//             .into_py(py),
//         };

        

//         let mut x = processor.raster_query(QueryRectangle {
//             spatial_bounds: query_bbox,
//             time_interval: TimeInterval::new(1_388_536_200_000, 1_388_536_200_000 + 1000)
//                 .unwrap(),
//             spatial_resolution: query_spatial_resolution,
//         }, 
//     &ctx).await.unwrap();

//     while let Some(result) = x.try_next().await.unwrap() {
//         match result.grid_array {
//             GridOrEmpty::Grid(g) => {
//                 println!("{:?}", g.data[0]);
                
//             },
//             _ => {
//                 println!("Something went wrong");
                
//             }
//         }
//     }
//     Ok(())
//     }
    
// }