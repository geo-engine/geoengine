use futures::{StreamExt};
use futures::SinkExt;
use geoengine_datatypes::{primitives::{SpatialPartition2D, TimeInstance, TimeInterval}, raster::{GridOrEmpty, Pixel}};
use crate::engine::{QueryContext, QueryRectangle, RasterQueryProcessor};
use pyo3::{Python, types::{PyModule, PyUnicode}};
use ndarray::{Array2, Axis,concatenate, stack, ArrayBase, OwnedRepr, Dim};
use numpy::{PyArray};
use rand::prelude::*;
use crate::util::Result;
use std::time::{Instant};
use futures::Stream;
use core::pin::Pin;
use geoengine_datatypes::raster::BaseTile;
use geoengine_datatypes::raster::GridShape;
use crate::error::Error;
use futures::channel::mpsc::{channel, Sender, Receiver};
use futures::stream::Zip;

#[allow(clippy::too_many_arguments)]
pub async fn imseg_fit<T, C: QueryContext>(
    processors: Vec<Box<dyn RasterQueryProcessor<RasterType=T>>>,
    processor_truth: Box<dyn RasterQueryProcessor<RasterType = u8>>,
    query_rect: QueryRectangle<SpatialPartition2D>,
    query_ctx: C,
    batch_size: usize,
    batches_per_query: usize,
    no_data_value: u8,
) -> Result<()>
where
    T: Pixel + numpy::Element,
{

    //For some reason we need that now...
    pyo3::prepare_freethreaded_python();
    let gil = pyo3::Python::acquire_gil();
    let py = gil.python();

    let mut rng = rand::thread_rng();

    let py_mod = PyModule::from_code(py, include_str!("tf_v2.py"),"filename.py", "modulename").unwrap();
    let name = PyUnicode::new(py, "test_model_1");
    //TODO change depreciated function
    let _init = py_mod.call("initUnet", (4,name, batch_size), None).unwrap();
    let _check_model_size = py_mod.call("get_model_memory_usage", (batch_size, ), None).unwrap();
    //since every 15 minutes an image is available...
    let step: i64 = (batch_size as i64 * 900_000) * batches_per_query as i64;
    println!("Step: {}", step);
    
    let mut queries = split_time_intervals(query_rect, step)?;

    let mut rand_index: usize; 
    
    let mut checkoint = 0;
    let mut buffer: Vec<(Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<u8>)> = Vec::new();
    while !queries.is_empty() {
        let queries_left = queries.len();
        println!("queries left: {:?}", queries_left);
        
        if queries_left > 1 {
            rand_index = rng.gen_range(0..queries_left-1);
        } else {
            rand_index = 0;
        }           
        let the_chosen_one = queries.remove(rand_index);
        checkoint = checkoint + 1;

        println!("{:?}", the_chosen_one.time_interval.start().as_naive_date_time().unwrap());

        let mut bffr: Vec<Pin<Box<dyn Stream<Item = Result<BaseTile<GridOrEmpty<GridShape<[usize; 2]>, T>>, Error>> + Send, >>> = Vec::new();           
        for element in processors.iter() {
           bffr.push(element.raster_query(the_chosen_one, &query_ctx).await?)
        }
                                                    
        let tile_stream_truth = processor_truth.raster_query(the_chosen_one, &query_ctx).await?;
        
        let mut final_stream = bffr.remove(0).zip(bffr.remove(0).zip(bffr.remove(0).zip(bffr.remove(0).zip(bffr.remove(0).zip(bffr.remove(0).zip(bffr.remove(0).zip(tile_stream_truth)))))));

        // let (sx, rx) = channel(10);
        // let send = sx.send_all(&mut bffr.remove(0));
        // rx.for_each(|x| {
            
        // });
        
        while let Some((ir_016, (ir_039, (ir_087, (ir_097, (ir_108, (ir_120, (ir_137, truth)))))))) = final_stream.next().await {

            let mut tile_size: [usize;2] = [0,0];
            let missing_elements: usize;
            

            buffer = extract_data(ir_016, ir_039, ir_087, ir_097, ir_108, ir_120, ir_137, truth, buffer, no_data_value).await;

            if buffer.len() == batch_size {
                buffer = training(buffer, tile_size, batch_size, &py, py_mod);
            }
            
            if checkoint == 15000 {
                let _checkpoint_save = py_mod.call("save", (name, ), None).unwrap();
                checkoint = 0;
            }
            
        }

   
    }
       


    
    
    //TODO change depreciated function
    let _save = py_mod.call("save", (name, ), None).unwrap();
    
    
    
    
    
    
    

    Ok(())
    
}

/// Creates batches used for training the model from the vectors of the rasterbands
fn create_arrays_from_data<T>(data_1: Vec<T>, data_2: Vec<T>, data_3: Vec<T>, data_4: Vec<T>, data_5: Vec<T>, data_6: Vec<T>, data_7: Vec<T>, data_8: Vec<u8>, tile_size: [usize;2]) -> (ArrayBase<OwnedRepr<T>, Dim<[usize; 4]>>, ArrayBase<OwnedRepr<u8>, Dim<[usize; 4]>>) 
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
            let arr_8: ndarray::Array4<u8> = 
            Array2::from_shape_vec((tile_size[0], tile_size[1]), data_8)
            .unwrap()
            .to_owned().insert_axis(Axis(0)).insert_axis(Axis(3));
            let arr_res: ndarray::Array<T, _> = stack(Axis(2), &[arr_1.view(),arr_2.view(),arr_3.view(), arr_4.view(), arr_5.view(), arr_6.view(), arr_7.view()]).unwrap().insert_axis(Axis(0));

            (arr_res, arr_8)
}
/// Splits time intervals into smaller intervalls of length step.
fn split_time_intervals(query_rect: QueryRectangle<SpatialPartition2D>, step: i64 ) -> Result<Vec<QueryRectangle<SpatialPartition2D>>> {
    let mut start = query_rect.time_interval.start();
    let mut inter_end = query_rect.time_interval.start();
    let end = query_rect.time_interval.end();
    let mut queries: Vec<QueryRectangle<SpatialPartition2D>> = Vec::new();
    while start.as_utc_date_time().unwrap().timestamp() * 1000 + step<= (end.as_utc_date_time().unwrap().timestamp() * 1000){
        let end_time_new = (inter_end.as_utc_date_time().unwrap().timestamp() * 1000) + step;
        inter_end = TimeInstance::from_millis(end_time_new)?;
        
        let new_rect = QueryRectangle{
            spatial_bounds: query_rect.spatial_bounds,
            time_interval: TimeInterval::new(start, inter_end)?,
            spatial_resolution: query_rect.spatial_resolution
        };
        queries.push(new_rect);
        let start_time_new = (start.as_utc_date_time().unwrap().timestamp() * 1000) + step;
        start = TimeInstance::from_millis(start_time_new)?;
        
    }
    Ok(queries)
}

async fn extract_data<T>(ir_016: Result<BaseTile<GridOrEmpty<GridShape<[usize; 2]>, T>>, Error>, ir_039: Result<BaseTile<GridOrEmpty<GridShape<[usize; 2]>, T>>, Error>, ir_087: Result<BaseTile<GridOrEmpty<GridShape<[usize; 2]>, T>>, Error>, ir_097: Result<BaseTile<GridOrEmpty<GridShape<[usize; 2]>, T>>, Error>, ir_108: Result<BaseTile<GridOrEmpty<GridShape<[usize; 2]>, T>>, Error>, ir_120: Result<BaseTile<GridOrEmpty<GridShape<[usize; 2]>, T>>, Error>, ir_137: Result<BaseTile<GridOrEmpty<GridShape<[usize; 2]>, T>>, Error>, truth: Result<BaseTile<GridOrEmpty<GridShape<[usize; 2]>, u8>>, Error>,mut buffer: Vec<(Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<u8>)>, no_data_value: u8) -> Vec<(Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<u8>)> {
    

        let mut tile_size: [usize;2] = [0,0];
        let missing_elements: usize;
        

        match (ir_016, ir_039, ir_087, ir_097, ir_108, ir_120, ir_137, truth) {
            (Ok(ir_016), Ok(ir_039), Ok(ir_087), Ok(ir_097), Ok(ir_108), Ok(ir_120), Ok(ir_137), Ok(truth)) => {
                match (ir_016.grid_array, ir_039.grid_array, ir_087.grid_array, ir_097.grid_array, ir_108.grid_array, ir_120.grid_array, ir_137.grid_array, truth.grid_array) {
                    (GridOrEmpty::Grid(grid_016), GridOrEmpty::Grid(grid_039), GridOrEmpty::Grid(grid_087), GridOrEmpty::Grid(grid_097), GridOrEmpty::Grid(grid_108), GridOrEmpty::Grid(grid_120), GridOrEmpty::Grid(grid_134), GridOrEmpty::Grid(grid_truth)) => {

                            let ndv = grid_truth.data.contains(&no_data_value);
                            //let ndv = false;
                            if !ndv {
                                tile_size = grid_016.shape.shape_array;
                                buffer.push((grid_016.data, grid_039.data, grid_087.data, grid_097.data, grid_108.data, grid_120.data, grid_134.data, grid_truth.data.iter().map(|x| x-1).collect()));
                            } else {
                                
                            }
                    },
                    _ => {
                        
                    }
                }
            },
            _ => {
                
            }
        }
        buffer
    }
fn training<T>(mut buffer: Vec<(Vec<T, >, Vec<T, >, Vec<T, >, Vec<T, >, Vec<T, >, Vec<T, >, Vec<T, >, Vec<u8, >), >, tile_size: [usize;2], batch_size: usize,  py: &Python, py_mod: &PyModule) ->   Vec<(Vec<T, >, Vec<T, >, Vec<T, >, Vec<T, >, Vec<T, >, Vec<T, >, Vec<T, >, Vec<u8, >,), >
where T: Clone + std::marker::Copy + numpy::Element{
    let (data_016_init, data_039_init, data_087_init, data_097_init, data_108_init, data_120_init, data_134_init, data_truth_init) = buffer.remove(0);

    let (mut arr_img_batch, mut arr_truth_batch) = create_arrays_from_data(data_016_init, data_039_init, data_087_init, data_097_init, data_108_init, data_120_init, data_134_init, data_truth_init, tile_size);
                    
    let num_elements = buffer.len();
                
    let mut rng = rand::thread_rng();     
                    
    let mut rand_index: usize;

    for i in 0..(batch_size - 1) {
                            
        rand_index = 0;
        if num_elements - i > 0 {
            rand_index = rng.gen_range(0..num_elements-i);
        }
        let (data_016, data_039, data_087, data_097, data_108, data_120, data_134, data_truth) = buffer.remove(rand_index);
        let (arr_img, arr_truth) = create_arrays_from_data(data_016, data_039, data_087, data_097, data_108, data_120, data_134, data_truth, tile_size);
    
        arr_img_batch = concatenate(Axis(0), &[arr_img_batch.view(), arr_img.view()]).unwrap();
               
        arr_truth_batch = concatenate(Axis(0), &[arr_truth_batch.view(), arr_truth.view()]).unwrap();
    }

    let pool = unsafe {py.new_pool()};
    let py = pool.python();
                
    let py_img = PyArray::from_owned_array(py, arr_img_batch);
    let py_truth = PyArray::from_owned_array(py, arr_truth_batch );

    let _result = py_mod.call("fit", (py_img, py_truth, batch_size), None).unwrap();
    
    buffer
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
    async fn imseg_fit_test() {
        let ctx = MockQueryContext::default();
        //tile size has to be a power of 2
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [512, 512].into());

        let query_spatial_resolution = SpatialResolution::new(3000.4, 3000.4).unwrap();

        let query_bbox = SpatialPartition2D::new((0.0, 3102413.6).into(), (30004.0, 3096412.8).into()).unwrap();
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

        let claas = GdalMetaDataRegular{
            time_placeholders: hashmap! {
                "%%%_TIME_FORMATED_%%%".to_string() => GdalSourceTimePlaceholder {
                    format: "%Y/CFCin%Y%m%d%H%M".to_string(),
                    reference: TimeReference::Start,

                }
            },
            result_descriptor: RasterResultDescriptor{
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReferenceOption::
                    SpatialReference(SpatialReference{
                        authority: SpatialReferenceAuthority::SrOrg,
                        code: 81,
                    }),
                measurement: Measurement::Unitless,
                no_data_value,
            },
            params: GdalDatasetParameters{
                file_path: PathBuf::from("hdf5:/mnt/panq/dbs_geo_data/satellite_data/CLAAS-2/pre_2013/ORD19112/%%%_TIME_FORMATED_%%%002050016001MA.hdf://CMa"),
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
                properties_mapping: None,
                gdal_open_options: None,
                gdal_config_options: None,
            },
            //placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
            //time_format: "%Y/%m/%d/CMAin%Y%m%d%H%M00305SVMSG01MD".to_string(),
            start: TimeInstance::from_millis(1072911600000).unwrap(),
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

        let id_claas = DatasetId::Internal{
            dataset_id: InternalDatasetId::new(),
        };

        let op_claas = Box::new(GdalSource{
            params: GdalSourceParameters{
                dataset: id_claas.clone(),
            }
        });

        mc.add_meta_data(id_claas, Box::new(claas.clone()));

        
        let proc_claas = op_claas
        .initialize(&mc).await.unwrap().query_processor().unwrap().get_u8().unwrap();

        
        


        // let source_h = GdalSourceProcessor::<u8>{
        //     tiling_specification,
        //     meta_data: Box::new(claas),
        //     phantom_data: Default::default(),
        // };

        let x = imseg_fit(vec![proc_ir_016, proc_ir_039, proc_ir_087, proc_ir_097, proc_ir_108, proc_ir_120, proc_ir_134],proc_claas, QueryRectangle {
            spatial_bounds: query_bbox,
            time_interval: TimeInterval::new(1104534000000, 1104534000000 + 45_000_000 )
                .unwrap(),
            spatial_resolution: query_spatial_resolution,
        }, ctx,
    10 as usize,
    1 as usize,
0 as u8).await.unwrap();
    }
}
//1_388_444_400_000