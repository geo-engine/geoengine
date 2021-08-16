use futures::StreamExt;
use geoengine_datatypes::{primitives::{SpatialPartition2D, TimeInstance, TimeInterval}, raster::{GridOrEmpty, Pixel}};
use crate::engine::{QueryContext, QueryRectangle, RasterQueryProcessor};
use crate::util::Result;
use pyo3::{types::{PyModule, PyUnicode, PyList}};
use ndarray::{Array2, Axis,concatenate, stack, ArrayBase, OwnedRepr, Dim};
use numpy::{PyArray};
use rand::prelude::*;

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
    let step: i64 = batch_size as i64 * 900_000;
    let mut start = query_rect.time_interval.start();
    let mut inter_end = query_rect.time_interval.start();
    let end = query_rect.time_interval.end();
    let mut queries: Vec<QueryRectangle<SpatialPartition2D>> = Vec::new();
    while start.as_utc_date_time().unwrap().timestamp() <= end.as_utc_date_time().unwrap().timestamp() {
        inter_end = TimeInstance::from_millis(inter_end.as_utc_date_time().unwrap().timestamp() + step)?;
        let new_rect = QueryRectangle{
            spatial_bounds: query_rect.spatial_bounds,
            time_interval: TimeInterval::new(start, inter_end)?,
            spatial_resolution: query_rect.spatial_resolution
        };
        queries.push(new_rect);
        start = TimeInstance::from_millis(start.as_utc_date_time().unwrap().timestamp() + step)?;
    }
    
    let tile_stream_ir_016 = processor_ir_016.raster_query(query_rect, &query_ctx).await?;
    let tile_stream_ir_039 = processor_ir_039.raster_query(query_rect, &query_ctx).await?;
    let tile_stream_ir_087 = processor_ir_087.raster_query(query_rect, &query_ctx).await?;
    let tile_stream_ir_097 = processor_ir_097.raster_query(query_rect, &query_ctx).await?;
    let tile_stream_ir_108 = processor_ir_108.raster_query(query_rect, &query_ctx).await?;
    let tile_stream_ir_120 = processor_ir_120.raster_query(query_rect, &query_ctx).await?;
    let tile_stream_ir_134 = processor_ir_134.raster_query(query_rect, &query_ctx).await?;
    let tile_stream_truth = processor_truth.raster_query(query_rect, &query_ctx).await?;
    
    let final_stream = tile_stream_ir_016.zip(tile_stream_ir_039.zip(tile_stream_ir_087.zip(tile_stream_ir_097.zip(tile_stream_ir_108.zip(tile_stream_ir_120.zip(tile_stream_ir_134.zip(tile_stream_truth)))))));

    let mut chunked_stream = final_stream.chunks(batch_size);
    while let Some(mut vctr) = chunked_stream.next().await {
        let mut buffer: Vec<(Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<T>, Vec<U>)> = Vec::new();
        let mut tile_size: [usize;2] = [0,0];
        
        for _ in 0..batch_size {
            let (ir_016, (ir_039, (ir_087, (ir_097, (ir_108, (ir_120, (ir_137, truth))))))) = vctr.remove(0);
            
            match (ir_016, ir_039, ir_087, ir_097, ir_108, ir_120, ir_137, truth) {
                (Ok(ir_016), Ok(ir_039), Ok(ir_087), Ok(ir_097), Ok(ir_108), Ok(ir_120), Ok(ir_134), Ok(truth)) => {
                    match (ir_016.grid_array, ir_039.grid_array, ir_087.grid_array, ir_097.grid_array, ir_108.grid_array, ir_120.grid_array, ir_134.grid_array, truth.grid_array) {
                        (GridOrEmpty::Grid(grid_016), GridOrEmpty::Grid(grid_039),  GridOrEmpty::Grid(grid_087),  GridOrEmpty::Grid(grid_097), GridOrEmpty::Grid(grid_108), GridOrEmpty::Grid(grid_120), GridOrEmpty::Grid(grid_134), GridOrEmpty::Grid(grid_truth)) => {
                            tile_size = grid_016.shape.shape_array;
                            buffer.push((grid_016.data, grid_039.data, grid_087.data, grid_097.data, grid_108.data, grid_120.data, grid_134.data, grid_truth.data));
                            
                        }, 
                        _ => {

                        }
                    }
                },
                _ => {
                    
                }
            }
        }
        
        let (data_016_init, data_039_init, data_087_init, data_097_init, data_108_init, data_120_init, data_134_init, data_truth_init) = buffer.remove(0);

        let (mut arr_img_batch, mut arr_truth_batch) = create_arrays_from_data(data_016_init, data_039_init, data_087_init, data_097_init, data_108_init, data_120_init, data_134_init, data_truth_init, tile_size);
        
        let num_elements = buffer.len();

        

        for i in 0..(batch_size - 1) {
            let rand_index: usize = rng.gen_range(0..num_elements-i);
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
    use geoengine_datatypes::{raster::{GeoTransform, RasterDataType}, spatial_reference::{SpatialReference, SpatialReferenceAuthority, SpatialReferenceOption}, 
        primitives::{Coordinate2D, TimeInterval, SpatialResolution,  Measurement, TimeGranularity, TimeInstance, TimeStep},
        raster::{TilingSpecification}
    };
    use std::path::PathBuf;
    use crate::{engine::{MockQueryContext, RasterResultDescriptor}, source::{GdalMetaDataRegular, GdalSourceProcessor,FileNotFoundHandling, GdalDatasetParameters}};


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
                measurement: Measurement::Unitless,
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
                properties_mapping: None,
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
                 measurement: Measurement::Unitless,
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
                 properties_mapping: None,
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
                measurement: Measurement::Unitless,
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
                properties_mapping: None,
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
                measurement: Measurement::Unitless,
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
                properties_mapping: None,
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
                measurement: Measurement::Unitless,
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
                properties_mapping: None,
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
                measurement: Measurement::Unitless,
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
                properties_mapping: None,
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
                measurement: Measurement::Unitless,
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
                properties_mapping: None,
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

        let source_a = GdalSourceProcessor::<i16>{
            tiling_specification,
            meta_data: Box::new(ir_016),
            phantom_data: Default::default(),
        };
        let source_b = GdalSourceProcessor::<i16>{
            tiling_specification,
            meta_data: Box::new(ir_039),
            phantom_data: Default::default(),
         };
         let source_c = GdalSourceProcessor::<i16>{
           tiling_specification,
           meta_data: Box::new(ir_087),
           phantom_data: Default::default(),
         };
        let source_d = GdalSourceProcessor::<i16>{
            tiling_specification,
            meta_data: Box::new(ir_097),
            phantom_data: Default::default(),
        };

        let source_e = GdalSourceProcessor::<i16>{
            tiling_specification,
            meta_data: Box::new(ir_108),
            phantom_data: Default::default(),
        };

        let source_f = GdalSourceProcessor::<i16>{
            tiling_specification,
            meta_data: Box::new(ir_120),
            phantom_data: Default::default(),
        };
   
        let source_g = GdalSourceProcessor::<i16>{
            tiling_specification,
            meta_data: Box::new(ir_134),
            phantom_data: Default::default(),
        };

        let source_h = GdalSourceProcessor::<u8>{
            tiling_specification,
            meta_data: Box::new(claas),
            phantom_data: Default::default(),
        };

        let x = imseg_fit(source_a.boxed(), source_b.boxed(), source_c.boxed(), source_d.boxed(), source_e.boxed(), source_f.boxed(), source_g.boxed(),source_h.boxed(), QueryRectangle {
            spatial_bounds: query_bbox,
            time_interval: TimeInterval::new(1_388_536_200_000, 1_388_536_200_000 + 45_000_000)
                .unwrap(),
            spatial_resolution: query_spatial_resolution,
        }, ctx,
    10 as usize).await.unwrap();
    }
}
