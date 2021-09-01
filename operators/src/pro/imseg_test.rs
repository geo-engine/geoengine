use futures::{StreamExt, Stream};
use geoengine_datatypes::{primitives::{SpatialPartition2D, TimeInstance, TimeInterval}, raster::{GridOrEmpty, Pixel, BaseTile, GridShape}};
use crate::engine::{QueryContext, QueryRectangle, RasterQueryProcessor};
use pyo3::{types::{PyModule, PyUnicode, PyList}};
use ndarray::{Array2, Axis,concatenate, stack, ArrayBase, OwnedRepr, Dim};
use numpy::{PyArray};
use rand::prelude::*;
use crate::util::Result;

#[allow(clippy::too_many_arguments)]
pub async fn imseg_fit<T, U, C: QueryContext>(
    processor_ir_016: Box<dyn RasterQueryProcessor<RasterType = T>>,
    processor_ir_039: Box<dyn RasterQueryProcessor<RasterType = T>>,
    processor_ir_087: Box<dyn RasterQueryProcessor<RasterType = T>>,
    processor_ir_097: Box<dyn RasterQueryProcessor<RasterType = T>>,
    processor_ir_108: Box<dyn RasterQueryProcessor<RasterType = T>>,
    processor_ir_120: Box<dyn RasterQueryProcessor<RasterType = T>>,
    processor_ir_134: Box<dyn RasterQueryProcessor<RasterType = T>>,
    processor_truth: Box<dyn RasterQueryProcessor<RasterType = U>>,
    query_rect: QueryRectangle<SpatialPartition2D>,
    query_ctx: C,
    batch_size: usize,
    batches_per_query: usize,
) -> Result<()>
where
    T: Pixel + numpy::Element,
    U: Pixel + numpy::Element,
{

    //For some reason we need that now...
    pyo3::prepare_freethreaded_python();
    let gil = pyo3::Python::acquire_gil();
    let py = gil.python();

    let mut rng = rand::thread_rng();

    let py_mod = PyModule::from_code(py, include_str!("tf_v2.py"),"filename.py", "modulename").unwrap();
    let name = PyUnicode::new(py, "first");
    //TODO change depreciated function
    let _init = py_mod.call("initUnet", (4,name, batch_size), None).unwrap();
    //since every 15 minutes an image is available...
    let step: i64 = (batch_size as i64 * 900_000) * batches_per_query as i64;
    println!("Step: {}", step);
    
    let mut start = query_rect.time_interval.start();
    let mut inter_end = query_rect.time_interval.start();
    let end = query_rect.time_interval.end();
    
    let mut queries: Vec<QueryRectangle<SpatialPartition2D>> = Vec::new();
    while start.as_utc_date_time().unwrap().timestamp() * 1000 <= (end.as_utc_date_time().unwrap().timestamp() * 1000) + step {
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
    let mut rand_index: usize; 
    while !queries.is_empty() {
        let queries_left = queries.len();
        println!("queries left: {:?}", queries_left);
        rand_index = 0;
        if queries_left > 1 {
            println!("{:?}", queries_left - 1);
            rand_index = rng.gen_range(0..queries_left-1);
        }
        
        let the_chosen_one = queries.remove(rand_index);
        let tile_stream_ir_016 = processor_ir_016.raster_query(the_chosen_one, &query_ctx).await?;
        let tile_stream_ir_039 = processor_ir_039.raster_query(the_chosen_one, &query_ctx).await?;
        let tile_stream_ir_087 = processor_ir_087.raster_query(the_chosen_one, &query_ctx).await?;
        let tile_stream_ir_097 = processor_ir_097.raster_query(the_chosen_one, &query_ctx).await?;
        let tile_stream_ir_108 = processor_ir_108.raster_query(the_chosen_one, &query_ctx).await?;
        let tile_stream_ir_120 = processor_ir_120.raster_query(the_chosen_one, &query_ctx).await?;
        let tile_stream_ir_134 = processor_ir_134.raster_query(the_chosen_one, &query_ctx).await?;
        let tile_stream_truth = processor_truth.raster_query(the_chosen_one, &query_ctx).await?;

        let final_stream = tile_stream_ir_016.zip(tile_stream_ir_039.zip(tile_stream_ir_087.zip(tile_stream_ir_097.zip(tile_stream_ir_108.zip(tile_stream_ir_120.zip(tile_stream_ir_134.zip(tile_stream_truth)))))));

        let mut chunked_stream = final_stream.chunks(batch_size);
        while let Some(mut vctr) = chunked_stream.next().await {
        let mut buffer: Vec<(Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<U>)> = Vec::new();
        let mut tile_size: [usize;2] = [0,0];
        let mut missing_elements: usize = 0;
        
             //TODO What if number of elements less than batch size?
        
        for _ in 0..batch_size {
            let (ir_016, (ir_039, (ir_087, (ir_097, (ir_108, (ir_120, (ir_137, truth))))))) = vctr.remove(0);
            // let a = ir_016.unwrap();
            // let b = ir_039.unwrap();
            // let c = ir_087.unwrap();
            // let d = ir_097.unwrap();
            // let e = ir_108.unwrap();
            // let f = ir_120.unwrap();
            // let g = ir_137.unwrap();
            match (ir_016, ir_039, ir_087, ir_097, ir_108, ir_120, ir_137, truth) {
                (Ok(ir_016), Ok(ir_039), Ok(ir_087), Ok(ir_097), Ok(ir_108), Ok(ir_120), Ok(ir_134), Ok(truth)) => {
                    match (ir_016.grid_array, ir_039.grid_array, ir_087.grid_array, ir_097.grid_array, ir_108.grid_array, ir_120.grid_array, ir_134.grid_array, truth.grid_array) {
                        (GridOrEmpty::Grid(grid_016), GridOrEmpty::Grid(grid_039),  GridOrEmpty::Grid(grid_087),  GridOrEmpty::Grid(grid_097), GridOrEmpty::Grid(grid_108), GridOrEmpty::Grid(grid_120), GridOrEmpty::Grid(grid_134), GridOrEmpty::Grid(grid_truth)) => {
                            println!("016: {:?}", grid_016.data[131000]);
                            println!("039: {:?}", grid_039.data[131000]);
                            println!("087: {:?}", grid_087.data[131000]);
                            println!("097: {:?}", grid_097.data[131000]);
                            println!("108: {:?}", grid_108.data[131000]);
                            println!("120: {:?}", grid_120.data[131000]);
                            println!("134: {:?}", grid_134.data[131000]);
                            
                            
                            tile_size = grid_016.shape.shape_array;
                            buffer.push((grid_016.data, grid_039.data, grid_087.data, grid_097.data, grid_108.data, grid_120.data, grid_134.data, grid_truth.data));
                            
                        }, 
                        _ => {
                            println!("SOME ARE EMPTY");
                            
                            //if batch is not full we fill it with copies of randomnly selected elements already present(not ideal, just first idea I had)
                            // buffer.push(buffer[rng.gen_range(0..buffer.len() - 1)].clone());
                            // missing_elements = missing_elements + 1;
                            
                        }
                    }
                },
                (Err(e1), Err(e2), Err(e3), Err(e4), Err(e5), Err(e6), Err(e7), Ok(truth)) => {
                    dbg!(e1);
                },
                (Err(e1), Err(e2), Err(e3), Err(e4), Err(e5), Err(e6), Err(e7), Err(truth)) => {
                    dbg!(e1);
                },
                (Ok(e1), Ok(e2), Ok(e3), Ok(e4), Ok(e5), Ok(e6), Ok(e7), Err(truth)) => {
                    dbg!(truth);
                },
                (Err(e1), Ok(e2), Ok(e3), Ok(e4), Ok(e5), Ok(e6), Ok(e7), Ok(truth)) => {
                    dbg!(e1);
                },
                _ => {
                    println!("AN ERROR OCCURED");
                            
                    //if batch is not full we fill it with copies of randomnly selected elements already  present(not ideal, just first idea I had)
                    // buffer.push(buffer[rng.gen_range(0..buffer.len() - 1)].clone());
                    // missing_elements = missing_elements + 1;
                }
            }
        }
    
        let number_of_elements = buffer.len();

        if number_of_elements != batch_size {
            
            //Filling up missing elements
            let diff = batch_size - number_of_elements;
            for _ in 0..diff {
                
                buffer.push(buffer[rng.gen_range(0..number_of_elements - 1)].clone());
                missing_elements = missing_elements + 1;
            }
        }
        //if more than 10% of the batch are copies we discard it alltogether
        if missing_elements <= batch_size/10 {
            let (data_016_init, data_039_init, data_087_init, data_097_init, data_108_init, data_120_init, data_134_init, data_truth_init) = buffer.remove(0);

        let (mut arr_img_batch, mut arr_truth_batch) = create_arrays_from_data(data_016_init, data_039_init, data_087_init, data_097_init, data_108_init, data_120_init, data_134_init, data_truth_init, tile_size);
        
        let num_elements = buffer.len();

        
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

        dbg!(&arr_img_batch.shape());
        dbg!(&arr_truth_batch.shape());

        let py_img = PyArray::from_owned_array(py, arr_img_batch);
        let py_truth = PyArray::from_owned_array(py, arr_truth_batch );
        //TODO change depreciated function
        let _result = py_mod.call("fit", (py_img, py_truth, batch_size), None).unwrap();
        
        } else {
            println!("Too many missing values!");
        }
        
        
        
       }
    }
       


    
    
    //TODO change depreciated function
    let _save = py_mod.call("save", (name, ), None).unwrap();

    Ok(())
    
}

/// Creates batches used for training the model from the vectors of the rasterbands
fn create_arrays_from_data<T, U>(data_1: Vec<T>, data_2: Vec<T>, data_3: Vec<T>, data_4: Vec<T>, data_5: Vec<T>, data_6: Vec<T>, data_7: Vec<T>, data_8: Vec<U>, tile_size: [usize;2]) -> (ArrayBase<OwnedRepr<T>, Dim<[usize; 4]>>, ArrayBase<OwnedRepr<U>, Dim<[usize; 4]>>) 
where 
T: Clone + std::marker::Copy, 
U: Clone {

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
            let arr_8: ndarray::Array4<U> = 
            Array2::from_shape_vec((tile_size[0], tile_size[1]), data_8)
            .unwrap()
            .to_owned().insert_axis(Axis(0)).insert_axis(Axis(3));
            let arr_res: ndarray::Array<T, _> = stack(Axis(2), &[arr_1.view(),arr_2.view(),arr_3.view(), arr_4.view(), arr_5.view(), arr_6.view(), arr_7.view()]).unwrap().insert_axis(Axis(0));

            (arr_res, arr_8)
}



#[cfg(test)]
mod tests {
    use geoengine_datatypes::{dataset::{DatasetId, InternalDatasetId},raster::{GeoTransform, RasterDataType}, spatial_reference::{SpatialReference, SpatialReferenceAuthority, SpatialReferenceOption}, 
        primitives::{Coordinate2D, TimeInterval, SpatialResolution,  Measurement, TimeGranularity, TimeInstance, TimeStep},
        raster::{TilingSpecification}
    };
 

    use geoengine_datatypes::{util::Identifier, raster::{RasterPropertiesEntryType, RasterPropertiesEntry}};
    use std::{borrow::Borrow, path::PathBuf};
    use crate::{engine::{ExecutionContext, MockExecutionContext,MockQueryContext, RasterOperator, RasterResultDescriptor}, source::{FileNotFoundHandling, GdalDatasetParameters, GdalMetaDataRegular, GdalSource, GdalSourceParameters, GdalSourceProcessor, GdalMetadataMapping}};
    use crate::processing::meteosat::radiance::{Radiance, RadianceProcessor};
    use crate::processing::meteosat::{offset_key, slope_key, satellite_key, channel_key};
    use crate::processing::meteosat::temperature::{Temperature, TemperatureProcessor};
    use crate::processing::meteosat::radiance::RadianceParams;
    use crate::processing::meteosat::temperature::TemperatureParams;
    use crate::processing::meteosat::reflectance::{Reflectance, ReflectanceParams};
    use crate::processing::expression::{Expression, ExpressionParams, ExpressionSources};
    use crate::engine::SingleRasterSource;

    use super::*;

    #[tokio::test]
    async fn imseg_meaningless() {
        let ctx = MockQueryContext::default();
        //tile size has to be a power of 2
        let tiling_specification =
            TilingSpecification::new(Coordinate2D::default(), [512, 512].into());

        let query_spatial_resolution = SpatialResolution::new(3000.4, 3000.4).unwrap();

        let query_bbox = SpatialPartition2D::new((0.0, 30000.0).into(), (30000.0, 0.0).into()).unwrap();
        let no_data_value = Some(0.);
        let ir_016 = GdalMetaDataRegular{
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
                geo_transform: GeoTransform{
                    origin_coordinate: (-5570248.477339744567871,5570248.477339744567871).into(),
                    x_pixel_size: 3000.403165817260742,
                    y_pixel_size: -3000.403165817260742, 
                },
                width: 11136,
                height: 11136,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: Some(vec![GdalMetadataMapping::identity(offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(satellite_key(), RasterPropertiesEntryType::Number)]),
                gdal_open_options: None,
            },
            placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
            time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_016___-000001___-%Y%m%d%H%M-C_".to_string(),
            start: TimeInstance::from_millis(1072917000000).unwrap(),
            step: TimeStep{
                granularity: TimeGranularity::Minutes,
                step: 15,
            }
        };
        let ir_039 = GdalMetaDataRegular{
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
                 geo_transform: GeoTransform{
                     origin_coordinate: (-5570248.477339744567871,5570248.477339744567871).into(),
                     x_pixel_size: 3000.403165817260742,
                     y_pixel_size: -3000.403165817260742, 
                 },
                 width: 11136,
                 height: 11136,
                 file_not_found_handling: FileNotFoundHandling::NoData,
                 no_data_value,
                 properties_mapping: Some(vec![GdalMetadataMapping::identity(offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(satellite_key(), RasterPropertiesEntryType::Number)]),
                 gdal_open_options: None
             },
             placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
             time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_039___-000001___-%Y%m%d%H%M-C_".to_string(),
             start: TimeInstance::from_millis(1072917000000).unwrap(),
             step: TimeStep{
                 granularity: TimeGranularity::Minutes,
                step: 15,
            }

        };

        let ir_087 = GdalMetaDataRegular{
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
                geo_transform: GeoTransform{
                    origin_coordinate: (-5570248.477, 5570248.477).into(),
                    x_pixel_size: 3000.403165817260742,
                    y_pixel_size: -3000.403165817260742, 
                },
                width: 11136,
                height: 11136,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: Some(vec![GdalMetadataMapping::identity(offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(satellite_key(), RasterPropertiesEntryType::Number)]),
                gdal_open_options: None,
            },
            placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
            time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_087___-000001___-%Y%m%d%H%M-C_".to_string(),
            start: TimeInstance::from_millis(1072917000000).unwrap(),
            step: TimeStep{
                granularity: TimeGranularity::Minutes,
                step: 15,
            }

        };

        let ir_097 = GdalMetaDataRegular{
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
                geo_transform: GeoTransform{
                    origin_coordinate: (-5570248.477, 5570248.477).into(),
                    x_pixel_size: 3000.403165817260742,
                    y_pixel_size: -3000.403165817260742, 
                },
                width: 11136,
                height: 11136,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: Some(vec![GdalMetadataMapping::identity(offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(satellite_key(), RasterPropertiesEntryType::Number)]),
                gdal_open_options: None,
            },
            placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
            time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_097___-000001___-%Y%m%d%H%M-C_".to_string(),
            start: TimeInstance::from_millis(1072917000000).unwrap(),
            step: TimeStep{
                granularity: TimeGranularity::Minutes,
                step: 15,
            }

        };


        let ir_108 = GdalMetaDataRegular{
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
                geo_transform: GeoTransform{
                    origin_coordinate: (-5570248.477, 5570248.477).into(),
                    x_pixel_size: 3000.403165817260742,
                    y_pixel_size: -3000.403165817260742, 
                },
                width: 11136,
                height: 11136,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: Some(vec![GdalMetadataMapping::identity(offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(satellite_key(), RasterPropertiesEntryType::Number)]),
                gdal_open_options: None,
            },
            placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
            time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_108___-000001___-%Y%m%d%H%M-C_".to_string(),
            start: TimeInstance::from_millis(1072917000000).unwrap(),
            step: TimeStep{
                granularity: TimeGranularity::Minutes,
                step: 15,
            }

        };

        let ir_120 = GdalMetaDataRegular{
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
                geo_transform: GeoTransform{
                    origin_coordinate: (-5570248.477, 5570248.477).into(),
                    x_pixel_size: 3000.403165817260742,
                    y_pixel_size: -3000.403165817260742, 
                },
                width: 11136,
                height: 11136,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: Some(vec![GdalMetadataMapping::identity(offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(satellite_key(), RasterPropertiesEntryType::Number)]),
                gdal_open_options: None,
            },
            placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
            time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_120___-000001___-%Y%m%d%H%M-C_".to_string(),
            start: TimeInstance::from_millis(1072917000000).unwrap(),
            step: TimeStep{
                granularity: TimeGranularity::Minutes,
                step: 15,
            }

        };

        let ir_134 = GdalMetaDataRegular{
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
                geo_transform: GeoTransform{
                    origin_coordinate: (-5570248.477, 5570248.477).into(),
                    x_pixel_size: 3000.403165817260742,
                    y_pixel_size: -3000.403165817260742, 
                },
                width: 11136,
                height: 11136,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: Some(vec![GdalMetadataMapping::identity(offset_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(slope_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(channel_key(), RasterPropertiesEntryType::Number), GdalMetadataMapping::identity(satellite_key(), RasterPropertiesEntryType::Number)]),
                gdal_open_options: None,
            },
            placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
            time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_134___-000001___-%Y%m%d%H%M-C_".to_string(),
            start: TimeInstance::from_millis(1072917000000).unwrap(),
            step: TimeStep{
                granularity: TimeGranularity::Minutes,
                step: 15,
            }

        };

        let claas = GdalMetaDataRegular{
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
                file_path: PathBuf::from("NETCDF:\"/mnt/panq/dbs_geo_data/satellite_data/CLAAS-2/level2/%%%_TIME_FORMATED_%%%.nc\":cma"),
                rasterband_channel: 1,
                geo_transform: GeoTransform{
                    origin_coordinate: ( -5456233.41938636, 5456233.41938636).into(),
                    x_pixel_size: 3000.403165817260742,
                    y_pixel_size: -3000.403165817260742, 
                },
                width: 11136,
                height: 11136,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: None,
                gdal_open_options: None,
            },
            placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
            time_format: "%Y/%m/%d/CMAin%Y%m%d%H%M00305SVMSG01MD".to_string(),
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
            params: ExpressionParams { expression: "A-0.15282721917305925/0.040190788161123925".to_string(), output_type: RasterDataType::F32, output_no_data_value: no_data_value.unwrap(), output_measurement: Some(Measurement::Continuous{
                measurement: "raw".to_string(),
                unit: None,
            })
        },
        sources: ExpressionSources{
            a: ref_ir_016,
            b: None,
            c: None,
        }
        });
        
        let proc_ir_016 = ref_ir_016
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
        let proc_ir_039 = temp_ir_039
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
        let proc_ir_087 = temp_ir_087
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
        let proc_ir_097 = temp_ir_097
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
        let proc_ir_108 = temp_ir_108
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
        let proc_ir_120 = temp_ir_120
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
        let proc_ir_134 = temp_ir_134
        .initialize(&mc).await.unwrap().query_processor().unwrap().get_f32().unwrap();


        // let source_a = GdalSourceProcessor::<i16>{
        //     tiling_specification,
        //     meta_data: Box::new(ir_016),
        //     phantom_data: Default::default(),
        // };

        

        // let source_b = GdalSourceProcessor::<i16>{
        //     tiling_specification,
        //     meta_data: Box::new(ir_039),
        //     phantom_data: Default::default(),
        //  };
        //  let source_c = GdalSourceProcessor::<i16>{
        //    tiling_specification,
        //    meta_data: Box::new(ir_087),
        //    phantom_data: Default::default(),
        //  };
        // let source_d = GdalSourceProcessor::<i16>{
        //     tiling_specification,
        //     meta_data: Box::new(ir_097),
        //     phantom_data: Default::default(),
        // };

        // let source_e = GdalSourceProcessor::<i16>{
        //     tiling_specification,
        //     meta_data: Box::new(ir_108),
        //     phantom_data: Default::default(),
        // };

        // let source_f = GdalSourceProcessor::<i16>{
        //     tiling_specification,
        //     meta_data: Box::new(ir_120),
        //     phantom_data: Default::default(),
        // };
   
        // let source_g = GdalSourceProcessor::<i16>{
        //     tiling_specification,
        //     meta_data: Box::new(ir_134),
        //     phantom_data: Default::default(),
        // };

        let source_h = GdalSourceProcessor::<u8>{
            tiling_specification,
            meta_data: Box::new(claas),
            phantom_data: Default::default(),
        };
        

        // let source_radiance_a = RasterQueryProcessor::boxed(RadianceProcessor::new(
        //     source_a.boxed(),
        //     no_data_value.unwrap() as f32
        // ));

        // let source_radiance_b = RasterQueryProcessor::boxed(RadianceProcessor::new(
        //     source_b.boxed(),
        //     no_data_value.unwrap() as f32
        // ));

        // let source_radiance_c = RasterQueryProcessor::boxed(RadianceProcessor::new(
        //     source_c.boxed(),
        //     no_data_value.unwrap() as f32
        // ));

        // let source_radiance_d = RasterQueryProcessor::boxed(RadianceProcessor::new(
        //     source_d.boxed(),
        //     no_data_value.unwrap() as f32
        // ));

        // let source_radiance_e = RasterQueryProcessor::boxed(RadianceProcessor::new(
        //     source_e.boxed(),
        //     no_data_value.unwrap() as f32
        // ));

        // let source_radiance_f = RasterQueryProcessor::boxed(RadianceProcessor::new(
        //     source_f.boxed(),
        //     no_data_value.unwrap() as f32
        // ));

        // let source_radiance_g = RasterQueryProcessor::boxed(RadianceProcessor::new(
        //     source_g.boxed(),
        //     no_data_value.unwrap() as f32
        // ));

        
        // let source_temperature_a = RasterQueryProcessor::boxed(TemperatureProcessor::new(
        //     source_radiance_a,
        //     TemperatureParams::default(),
        // ));

        // let source_temperature_b = RasterQueryProcessor::boxed(TemperatureProcessor::new(
        //     source_radiance_b,
        //     TemperatureParams::default(),
        // ));

        // let source_temperature_c = RasterQueryProcessor::boxed(TemperatureProcessor::new(
        //     source_radiance_c,
        //     TemperatureParams::default(),
        // ));

        // let source_temperature_d = RasterQueryProcessor::boxed(TemperatureProcessor::new(
        //     source_radiance_d,
        //     TemperatureParams::default(),
        // ));

        // let source_temperature_e = RasterQueryProcessor::boxed(TemperatureProcessor::new(
        //     source_radiance_e,
        //     TemperatureParams::default(),
        // ));

        // let source_temperature_f = RasterQueryProcessor::boxed(TemperatureProcessor::new(
        //     source_radiance_f,
        //     TemperatureParams::default(),
        // ));

        // let source_temperature_g = RasterQueryProcessor::boxed(TemperatureProcessor::new(
        //     source_radiance_g,
        //     TemperatureParams::default(),
        // ));


        let x = imseg_fit(proc_ir_016, proc_ir_039, proc_ir_087, proc_ir_097, proc_ir_108, proc_ir_120, proc_ir_134,source_h.boxed(), QueryRectangle {
            spatial_bounds: query_bbox,
            time_interval: TimeInterval::new(1_388_536_200_000, 1_388_536_200_000 + 90_000_000)
                .unwrap(),
            spatial_resolution: query_spatial_resolution,
        }, ctx,
    10 as usize,
10 as usize).await.unwrap();
    }
}
