use futures::{StreamExt, Stream};
use geoengine_datatypes::{primitives::{SpatialPartition2D, TimeInstance, TimeInterval}, raster::{GridOrEmpty, Pixel}};
use crate::engine::{QueryContext, RasterQueryProcessor};
use pyo3::{types::{PyModule, PyUnicode}};
use ndarray::{Array2};
use numpy::{PyArray};
use rand::prelude::*;
use crate::util::Result;
use core::pin::Pin;
use geoengine_datatypes::{primitives::QueryRectangle, raster::{BaseTile,GridShape}};
use crate::error::Error;
use crate::pro::datatypes::{RasterResult, Zip};
use std::time::{Instant};

#[allow(clippy::too_many_arguments)]
pub async fn imseg_fit<T, C: QueryContext>(
    processors: Vec<Box<dyn RasterQueryProcessor<RasterType=T>>>,
    processor_truth: Box<dyn RasterQueryProcessor<RasterType = u8>>,
    query_rect: QueryRectangle<SpatialPartition2D>,
    query_ctx: C,
    batch_size: usize,
    batches_per_query: usize,
    no_data_value: u8,
    tile_size: [usize;2],
    default_value: T,
) -> Result<()>
where
    T: Pixel + numpy::Element,
    C: 'static,
{

    //For some reason we need that now...
    pyo3::prepare_freethreaded_python();
    let gil = pyo3::Python::acquire_gil();
    let py = gil.python();

    let mut rng = rand::thread_rng();
    let mut time_general_processing: u128 = 0;
    let mut time_waiting: u128 = 0;
    let mut time_data_processing: u128 = 0;
    let mut time_python: u128 = 0;
    let mut time = Instant::now();

    let py_mod = PyModule::from_code(py, include_str!("tf_v2.py"),"filename.py", "modulename").unwrap();
    let name = PyUnicode::new(py, "big_model_1");
    //TODO change depreciated function
    let _init = py_mod.call("initUnet2", (5,name, batch_size), None).unwrap();
    let _check_model_size = py_mod.call("get_model_memory_usage", (batch_size, ), None).unwrap();
    //since every 15 minutes an image is available...
    let step: i64 = (batch_size as i64 * 900_000) * batches_per_query as i64;
    println!("Step: {}", step);
    
    let mut queries = split_time_intervals(query_rect, step)?;

    let mut rand_index: usize; 
    
    let mut checkoint = 0;
   
    let nop = processors.len();
    let mut final_buff_proc: Vec<Vec<Vec<T>>> = Vec::new();
    let mut final_buff_truth: Vec<Vec<u8>> = Vec::new();
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
           bffr.push(element.raster_query(the_chosen_one, &query_ctx).await?);
        }

        let mut truth_stream = processor_truth.raster_query(the_chosen_one, &query_ctx).await?;


        

        //let x = bffr[0].next().await;

        
            let mut zip = Zip::new(bffr);
            

            let mut buffer_proc: Vec<RasterResult<T>> = Vec::with_capacity(nop);
            let mut truth_int: RasterResult<u8>;
            time_general_processing = time_general_processing + time.elapsed().as_millis();
            time = Instant::now();
            while let (Some(proc), Some(truth)) = (zip.next().await, truth_stream.next().await) {
                time_waiting = time_waiting + time.elapsed().as_millis();
                time = Instant::now();
                for processor in proc {
                    match processor {
                        Ok(processor) => {
                            match processor.grid_array {
                                GridOrEmpty::Grid(processor) => {
                                    buffer_proc.push(RasterResult::Some(processor.data));
                                },
                                _ => {
                                    buffer_proc.push(RasterResult::Empty);
                                }
                            }
                        },
                        _ => {
                            buffer_proc.push(RasterResult::Error);
                        }
                    }
                }
                match truth {
                    Ok(truth) => {
                        match truth.grid_array {
                            GridOrEmpty::Grid(truth) => {
                                truth_int = RasterResult::Some(truth.data);
                            },
                            _ => {
                                truth_int = RasterResult::Empty;
                            }
                        }
                    },
                    _ => {
                        truth_int = RasterResult::Error;
                    }
                }
                if buffer_proc.iter().all(|x| matches!(x, &RasterResult::Some(_))) && buffer_proc.len() == nop && matches!(truth_int, RasterResult::Some(_)) {
                    final_buff_proc.push(buffer_proc.clone().into_iter().map(|x| {
                        if let RasterResult::Some(x) = x {
                            return x;
                        } else {
                            panic!("!!!");
                        }
                    }).collect());
                    if let RasterResult::Some(x) = truth_int{
                        final_buff_truth.push(x);
                    } else {
                        panic!("!!!");
                    }            
                }
                time_general_processing = time_general_processing + time.elapsed().as_millis();
                time = Instant::now();
                
                
                if final_buff_proc.len() == batch_size {
                    let mut arr_proc_final = ndarray::Array::from_elem((batch_size, tile_size[0], tile_size[1], nop), default_value);
                    let mut arr_truth_final = ndarray::Array::from_elem((batch_size, tile_size[0], tile_size[1],1), no_data_value);
                    assert!(final_buff_truth.len() == batch_size);
                    for j in 0..batch_size {
                        let mut proc = final_buff_proc.remove(0);
                        //println!("{:?}", proc);
                        
                        let truth = final_buff_truth.remove(0);
                        //println!("{:?}", truth);
                        
                        let mut arr_proc = ndarray::Array::from_elem((tile_size[0], tile_size[1], nop), default_value);
                        for i in 0 .. proc.len() {
                            let arr_x: ndarray::Array<T, _> = 
                            Array2::from_shape_vec((tile_size[0], tile_size[1]), proc.remove(0))
                            .unwrap(); 
                            arr_proc.slice_mut(ndarray::s![..,..,i]).assign(&arr_x);
                            
                            //slice = arr_x.view_mut();
                        }
                        arr_proc_final.slice_mut(ndarray::s![j,..,..,..]).assign(&arr_proc);
    
                        let arr_t = Array2::from_shape_vec((tile_size[0], tile_size[1]), truth).unwrap();
                        arr_truth_final.slice_mut(ndarray::s![j,..,..,0]).assign(&arr_t);
                        
    
                    }
                    
                    let pool = unsafe {py.new_pool()};
                    let py = pool.python();
                    let py_img = PyArray::from_owned_array(py, arr_proc_final);
                    let py_truth = PyArray::from_owned_array(py, arr_truth_final);

                    time_data_processing = time_data_processing + time.elapsed().as_millis();
                    time = Instant::now();
                    let _result = py_mod.call("fit", (py_img, py_truth, batch_size), None).unwrap();
        
                    time_python = time_python + time.elapsed().as_millis();
                    time = Instant::now();
                }
                buffer_proc.drain(..);
                time_general_processing = time_general_processing + time.elapsed().as_millis();
                time = Instant::now();
            }
            
                
                
       
        
    }

    println!("Time general processing: {}", time_general_processing);
    println!("Time waiting for data: {}", time_waiting);
    println!("Time data processing: {}", time_data_processing);
    println!("Time python: {}", time_python);
    //TODO change depreciated function
    let _save = py_mod.call("save", (name, ), None).unwrap();
    Ok(())
    
    
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

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{dataset::{DatasetId, InternalDatasetId}, hashmap, primitives::{Coordinate2D, TimeInterval, SpatialResolution,  Measurement, TimeGranularity, TimeInstance, TimeStep}, raster::{RasterDataType}, raster::{TilingSpecification}, spatial_reference::{SpatialReference, SpatialReferenceAuthority, SpatialReferenceOption}};
    use geoengine_datatypes::{util::Identifier, raster::{RasterPropertiesEntryType}};
    use std::{path::PathBuf};
    use crate::{engine::{MockExecutionContext,MockQueryContext, RasterOperator, RasterResultDescriptor}, source::{FileNotFoundHandling, GdalDatasetParameters, GdalMetaDataRegular, GdalSource, GdalSourceParameters,GdalMetadataMapping, GdalSourceTimePlaceholder, TimeReference, GdalDatasetGeoTransform}};
    use crate::processing::{expression::{Expression, ExpressionParams, ExpressionSources}, meteosat::{new_offset_key, new_slope_key, new_satellite_key, new_channel_key, radiance::{Radiance, RadianceParams}, temperature::{Temperature, TemperatureParams}, reflectance::{Reflectance, ReflectanceParams}}};
    use crate::engine::SingleRasterSource;
    use geoengine_datatypes::util::test::TestDefault;

    use super::*;

    #[tokio::test]
    async fn imseg_fit_test() {
        let ctx = MockQueryContext::test_default();
        //tile size has to be a power of 2
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [512, 512].into());

        let query_spatial_resolution = SpatialResolution::new(3000.4, 3000.4).unwrap();
        //(-802607.8468561172485352, 5108186.3898038864135742).into(), (802607.8468561172485352, 3577980.7752370834350586).into())
        let query_bbox = SpatialPartition2D::new((-802607.8468561172485352, 5108186.3898038864135742).into(), (802607.8468561172485352, 3577980.7752370834350586).into()).unwrap();
        let no_data_value = Some(0.);
        let wv62 = GdalMetaDataRegular{
            time_placeholders: hashmap! {
                "%%%_TIME_FORMATED_%%%".to_string() => GdalSourceTimePlaceholder {
                    format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-WV_062___-000001___-%Y%m%d%H%M-C_".to_string(),
                    reference: TimeReference::Start,

                }
            },
            result_descriptor: RasterResultDescriptor{
                data_type: RasterDataType::I16,
                spatial_reference: SpatialReferenceOption::
                    SpatialReference(
                        SpatialReference::new(SpatialReferenceAuthority::SrOrg, 
                            81)),
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
                properties_mapping: Some(vec![GdalMetadataMapping::identity(new_offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_satellite_key(), RasterPropertiesEntryType::Number)]),
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
        let wv73 = GdalMetaDataRegular{
            time_placeholders: hashmap! {
                "%%%_TIME_FORMATED_%%%".to_string() => GdalSourceTimePlaceholder {
                    format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-WV_073___-000001___-%Y%m%d%H%M-C_".to_string(),
                    reference: TimeReference::Start,

                }
            },
            result_descriptor: RasterResultDescriptor{
                data_type: RasterDataType::I16,
                spatial_reference: SpatialReferenceOption::
                    SpatialReference(
                        SpatialReference::new(SpatialReferenceAuthority::SrOrg, 
                            81)),
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
                properties_mapping: Some(vec![GdalMetadataMapping::identity(new_offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_satellite_key(), RasterPropertiesEntryType::Number)]),
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
        let vis_06 = GdalMetaDataRegular{
            time_placeholders: hashmap! {
                "%%%_TIME_FORMATED_%%%".to_string() => GdalSourceTimePlaceholder {
                    format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-VIS006___-000001___-%Y%m%d%H%M-C_".to_string(),
                    reference: TimeReference::Start,

                }
            },
            result_descriptor: RasterResultDescriptor{
                data_type: RasterDataType::I16,
                spatial_reference: SpatialReferenceOption::
                    SpatialReference(
                        SpatialReference::new(SpatialReferenceAuthority::SrOrg, 
                            81)),
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
                properties_mapping: Some(vec![GdalMetadataMapping::identity(new_offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_satellite_key(), RasterPropertiesEntryType::Number)]),
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
        let vis_08 = GdalMetaDataRegular{
            time_placeholders: hashmap! {
                "%%%_TIME_FORMATED_%%%".to_string() => GdalSourceTimePlaceholder {
                    format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-VIS008___-000001___-%Y%m%d%H%M-C_".to_string(),
                    reference: TimeReference::Start,

                }
            },
            result_descriptor: RasterResultDescriptor{
                data_type: RasterDataType::I16,
                spatial_reference: SpatialReferenceOption::
                    SpatialReference(
                        SpatialReference::new(SpatialReferenceAuthority::SrOrg, 
                            81)),
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
                properties_mapping: Some(vec![GdalMetadataMapping::identity(new_offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_satellite_key(), RasterPropertiesEntryType::Number)]),
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
                    SpatialReference(
                        SpatialReference::new(SpatialReferenceAuthority::SrOrg, 
                            81)),
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
                properties_mapping: Some(vec![GdalMetadataMapping::identity(new_offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_satellite_key(), RasterPropertiesEntryType::Number)]),
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
                    SpatialReference(
                        SpatialReference::new(SpatialReferenceAuthority::SrOrg, 
                            81)),
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
                 properties_mapping: Some(vec![GdalMetadataMapping::identity(new_offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_satellite_key(), RasterPropertiesEntryType::Number)]),
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
                    SpatialReference(
                        SpatialReference::new(SpatialReferenceAuthority::SrOrg, 
                            81)),
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
                properties_mapping: Some(vec![GdalMetadataMapping::identity(new_offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_satellite_key(), RasterPropertiesEntryType::Number)]),
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
                    SpatialReference(
                        SpatialReference::new(SpatialReferenceAuthority::SrOrg, 
                            81)),
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
                properties_mapping: Some(vec![GdalMetadataMapping::identity(new_offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_satellite_key(), RasterPropertiesEntryType::Number)]),
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
                    SpatialReference(
                        SpatialReference::new(SpatialReferenceAuthority::SrOrg, 
                            81)),
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
                properties_mapping: Some(vec![GdalMetadataMapping::identity(new_offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_satellite_key(), RasterPropertiesEntryType::Number)]),
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
                    SpatialReference(
                        SpatialReference::new(SpatialReferenceAuthority::SrOrg, 
                            81)),
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
                properties_mapping: Some(vec![GdalMetadataMapping::identity(new_offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_satellite_key(), RasterPropertiesEntryType::Number)]),
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
                    SpatialReference(
                        SpatialReference::new(SpatialReferenceAuthority::SrOrg, 
                            81)),
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
                properties_mapping: Some(vec![GdalMetadataMapping::identity(new_offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(new_satellite_key(), RasterPropertiesEntryType::Number)]),
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
                    SpatialReference(
                        SpatialReference::new(SpatialReferenceAuthority::SrOrg, 
                            81)),
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

        let mut mc = MockExecutionContext::test_default();
        mc.tiling_specification = tiling_specification;

        let id_vis_06 =DatasetId::Internal{
            dataset_id:InternalDatasetId::new(),
        };

        let op_vis_06 = Box::new(GdalSource{
            params: GdalSourceParameters{
                dataset: id_vis_06.clone(),
            }
        });

        mc.add_meta_data(id_vis_06, Box::new(vis_06.clone()));

        let rad_vis_06 = RasterOperator::boxed(Radiance{
            params: RadianceParams{

            },
            sources: SingleRasterSource{
                raster: op_vis_06
            }
        });

        let ref_vis_06 = RasterOperator::boxed(Reflectance{
            params: ReflectanceParams::default(),
            sources: SingleRasterSource{
                raster: rad_vis_06
            }
        });

        let exp_vis_06 = RasterOperator::boxed(Expression{
            params: ExpressionParams { expression: "(A-0.1561391201478538)/0.21198859799405942".to_string(), output_type: RasterDataType::F32, output_no_data_value: no_data_value.unwrap(), output_measurement: Some(Measurement::Continuous{
                measurement: "raw".to_string(),
                unit: None,
            }), map_no_data: false
        },
        sources: ExpressionSources::new_a(ref_vis_06)
        });

        let proc_vis_06 = exp_vis_06
        .initialize(&mc).await.unwrap().query_processor().unwrap().get_f32().unwrap();

        let id_wv_62 = DatasetId::Internal{
            dataset_id: InternalDatasetId::new(),
        };

        let op_wv_62 = Box::new(GdalSource{
            params: GdalSourceParameters{
                dataset: id_wv_62.clone(),
            }
        });

        
        mc.add_meta_data(id_wv_62, Box::new(wv62.clone()));

        let temp_wv_62 = RasterOperator::boxed(Temperature{
            params: TemperatureParams::default(),
            sources: SingleRasterSource{
                raster: op_wv_62
            }
        });

        let exp_wv_62 = RasterOperator::boxed(Expression{
            params: ExpressionParams { expression: "(A-232.1600623038471)/5.042135465497473".to_string(), output_type: RasterDataType::F32, output_no_data_value: no_data_value.unwrap(), output_measurement: Some(Measurement::Continuous{
                measurement: "raw".to_string(),
                unit: None,
            }), map_no_data: false
        },
        sources: ExpressionSources::new_a(temp_wv_62)
        });
        let proc_wv_62 = exp_wv_62
        .initialize(&mc).await.unwrap().query_processor().unwrap().get_f32().unwrap();

        let id_wv_73 = DatasetId::Internal{
            dataset_id: InternalDatasetId::new(),
        };

        let op_wv_73 = Box::new(GdalSource{
            params: GdalSourceParameters{
                dataset: id_wv_73.clone(),
            }
        });

        
        mc.add_meta_data(id_wv_73, Box::new(wv73.clone()));

        let temp_wv_73 = RasterOperator::boxed(Temperature{
            params: TemperatureParams::default(),
            sources: SingleRasterSource{
                raster: op_wv_73
            }
        });

        let exp_wv_73 = RasterOperator::boxed(Expression{
            params: ExpressionParams { expression: "(A-248.22318052333853)/8.607845891333238".to_string(), output_type: RasterDataType::F32, output_no_data_value: no_data_value.unwrap(), output_measurement: Some(Measurement::Continuous{
                measurement: "raw".to_string(),
                unit: None,
            }), map_no_data: false
        },
        sources: ExpressionSources::new_a(temp_wv_73)
        });
        let proc_wv_73 = exp_wv_73
        .initialize(&mc).await.unwrap().query_processor().unwrap().get_f32().unwrap();

        

        let id_vis_08 =DatasetId::Internal{
            dataset_id:InternalDatasetId::new(),
        };

        let op_vis_08 = Box::new(GdalSource{
            params: GdalSourceParameters{
                dataset: id_vis_08.clone(),
            }
        });

        mc.add_meta_data(id_vis_08, Box::new(vis_08.clone()));

        let rad_vis_08 = RasterOperator::boxed(Radiance{
            params: RadianceParams{

            },
            sources: SingleRasterSource{
                raster: op_vis_08
            }
        });

        let ref_vis_08 = RasterOperator::boxed(Reflectance{
            params: ReflectanceParams::default(),
            sources: SingleRasterSource{
                raster: rad_vis_08
            }
        });

        let exp_vis_08 = RasterOperator::boxed(Expression{
            params: ExpressionParams { expression: "(A-0.18973477651638973)/0.2436566309022752".to_string(), output_type: RasterDataType::F32, output_no_data_value: no_data_value.unwrap(), output_measurement: Some(Measurement::Continuous{
                measurement: "raw".to_string(),
                unit: None,
            }), map_no_data: false
        },
        sources: ExpressionSources::new_a(ref_vis_08)
        });

        let proc_vis_08 = exp_vis_08
        .initialize(&mc).await.unwrap().query_processor().unwrap().get_f32().unwrap();

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
            }), map_no_data: false
        },
        sources: ExpressionSources::new_a(ref_ir_016)
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
            }), map_no_data: false
        },
        sources: ExpressionSources::new_a(temp_ir_039)
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
            }), map_no_data: false
        },
        sources: ExpressionSources::new_a(temp_ir_087)
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
            }), map_no_data: false
        },
        sources: ExpressionSources::new_a(temp_ir_097)
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
            }), map_no_data: false
        },
        sources: ExpressionSources::new_a(temp_ir_108)
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
            }), map_no_data: false
        },
        sources: ExpressionSources::new_a(temp_ir_120)
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
            }), map_no_data: false
        },
        sources: ExpressionSources::new_a(temp_ir_134)
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

        let x = imseg_fit(vec![proc_vis_06, proc_vis_08, proc_ir_016, proc_ir_039, proc_wv_62, proc_wv_73, proc_ir_087, proc_ir_097, proc_ir_108, proc_ir_120, proc_ir_134],proc_claas, QueryRectangle {
            spatial_bounds: query_bbox,
            time_interval: TimeInterval::new(1072936800000, 1167631200000 )
                .unwrap(),
            spatial_resolution: query_spatial_resolution,
        }, ctx,
    24 as usize,
    32 as usize,
0 as u8,
[512,512],
0.0).await.unwrap();
    }
}
//1136005200000