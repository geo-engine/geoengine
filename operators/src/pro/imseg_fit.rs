use futures::StreamExt;
use geoengine_datatypes::{primitives::{SpatialPartition2D}, raster::{GridOrEmpty, Pixel}};
use crate::engine::{QueryContext, QueryRectangle, RasterQueryProcessor};
use crate::util::Result;
use pyo3::{types::{PyModule, PyUnicode}};
use ndarray::{Array2, Axis, stack};
use numpy::{PyArray};

#[allow(clippy::too_many_arguments)]
pub async fn imseg_fit<T, U, C: QueryContext>(
    processor_red: Box<dyn RasterQueryProcessor<RasterType = T>>,
    processor_green: Box<dyn RasterQueryProcessor<RasterType = T>>,
    //processor_blue: Box<dyn RasterQueryProcessor<RasterType = T>>,
    processor_truth: Box<dyn RasterQueryProcessor<RasterType = U>>,
    query_rect: QueryRectangle<SpatialPartition2D>,
    query_ctx: C,
) -> Result<()>
where
    T: Pixel + numpy::Element,
    U: Pixel + numpy::Element,
{


    let gil = pyo3::Python::acquire_gil();
    let py = gil.python();
    println!("started");
    

    let py_mod = PyModule::from_code(py, include_str!("tf_v2.py"),"filename.py", "modulename").unwrap();
    let name = PyUnicode::new(py, "first");

    let _init = py_mod.call("initUnet", (4, name), None).unwrap();

    
    let tile_stream_red = processor_red.raster_query(query_rect, &query_ctx).await?;
    let tile_stream_green = processor_green.raster_query(query_rect, &query_ctx).await?;
    //let tile_stream_blue = processor_blue.raster_query(query_rect, &query_ctx).await?;
    
    let tile_stream_truth = processor_truth.raster_query(query_rect, &query_ctx).await?;
    
    //let mut final_stream = tile_stream_red.zip(tile_stream_green.zip(tile_stream_blue.zip(tile_stream_truth)));
    let mut fin_stream = tile_stream_red.zip(tile_stream_green).zip(tile_stream_truth);
    
    while let Some(((red, green), truth)) = fin_stream.next().await {
        match (red, green, truth) {
            (Ok(red), Ok(green), Ok(truth)) => {
                match (red.grid_array, green.grid_array, truth.grid_array) {
                                    (GridOrEmpty::Grid(red_grid), GridOrEmpty::Grid(green_grid),  GridOrEmpty::Grid(truth_grid)) => {
    
                                        let r_data = red_grid.data.clone();
                                        let g_data = green_grid.data;
                                        let b_data = red_grid.data;
                                        let t_data = truth_grid.data;
                                        let tile_size = red_grid.shape.shape_array;
                                        let arr_red: ndarray::Array2<T> = 
                                        Array2::from_shape_vec((tile_size[0], tile_size[1]), r_data)
                                        .unwrap();
                                        let arr_green: ndarray::Array2<T> = 
                                        Array2::from_shape_vec((tile_size[0], tile_size[1]), g_data)
                                        .unwrap();
                                        let arr_blue: ndarray::Array2<T> = 
                                        Array2::from_shape_vec((tile_size[0], tile_size[1]), b_data)
                                        .unwrap();
                                        let arr_truth: ndarray::Array2<U> = 
                                        Array2::from_shape_vec((tile_size[0], tile_size[1]), t_data)
                                        .unwrap()
                                        .to_owned();
                        
                                        let arr_img: ndarray::Array3<T> = stack(Axis(2), &[arr_red.view(),arr_green.view(),arr_blue.view()]).unwrap();
                                        
                                        let arr_img_batch = arr_img.insert_axis(Axis(0)); // add a leading axis for the batches!
                                        let arr_truth_batch = arr_truth.insert_axis(Axis(0)); // add a leading axis for the batches!

                                        dbg!(&arr_img_batch.shape());
                
                                        let py_img = PyArray::from_owned_array(py, arr_img_batch);
                                        let py_truth = PyArray::from_owned_array(py, arr_truth_batch );
                
                                        let _result = py_mod.call("fit", (py_img, py_truth), None).unwrap();
                        
                                    },
                                    _ => {
                                        println!("some are empty");
                                    }
                                }
            }, 
            _ => {
                println!("Something went wrong");
                
            }
        }
    }

    let _save = py_mod.call("save", (name, ), None).unwrap();



    Ok(())
    
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
        let gdal_108 = GdalMetaDataRegular{
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
                height: 464,
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
        let gdal_016 = GdalMetaDataRegular{
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
                 height: 464,
                 file_not_found_handling: FileNotFoundHandling::NoData,
                 no_data_value,
                 properties_mapping: None,
                 gdal_open_options: None
             },
             placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
             time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_120___-000001___-%Y%m%d%H%M-C_".to_string(),
             start: TimeInstance::from_millis(1072917000000).unwrap(),
             step: TimeStep{
                 granularity: TimeGranularity::Minutes,
                step: 15,
            }

        };

        // let gdal_108_b = GdalMetaDataRegular{
        //     result_descriptor: RasterResultDescriptor{
        //         data_type: RasterDataType::I16,
        //         spatial_reference: SpatialReferenceOption::
        //             SpatialReference(SpatialReference{
        //                 authority: SpatialReferenceAuthority::SrOrg,
        //                 code: 81,
        //             }),
        //         measurement: Measurement::Unitless,
        //         no_data_value,
        //     },
        //     params: GdalDatasetParameters{
        //         file_path: PathBuf::from("/mnt/panq/dbs_geo_data/satellite_data/msg_seviri/%%%_TIME_FORMATED_%%%"),
        //         rasterband_channel: 1,
        //         geo_transform: GeoTransform{
        //             origin_coordinate: (-5570248.477, 5570248.477).into(),
        //             x_pixel_size: 3000.403165817260742,
        //             y_pixel_size: -3000.403165817260742, 
        //         },
        //         partition: SpatialPartition2D::new((-5570248.477, 5570248.477).into(), (5567248.074, -5567248.074).into()).unwrap(),
        //         file_not_found_handling: FileNotFoundHandling::NoData,
        //         no_data_value,
        //         properties_mapping: None,
        //     },
        //     placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
        //     time_format: "%Y/%m/%d/%Y%m%d_%H%M/H-000-MSG3__-MSG3________-IR_016___-000001___-%Y%m%d%H%M-C_".to_string(),
        //     start: TimeInstance::from_millis(1072917000000).unwrap(),
        //     step: TimeStep{
        //         granularity: TimeGranularity::Months,
        //         step: 15,
        //     }

        // };

        let gdal_claas = GdalMetaDataRegular{
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
                height: 464,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value,
                properties_mapping: None,
                gdal_open_options: None,
            },
            placeholder: "%%%_TIME_FORMATED_%%%".to_string(),
            time_format: "%Y/%m/%d/CMAin%Y%m%d%H%M00305SVMSG01MD".to_string(),
            start: TimeInstance::from_millis(1072917000000).unwrap(),
            step: TimeStep{
                granularity: TimeGranularity::Months,
                step: 15,
            }

        };

        let source_a = GdalSourceProcessor::<i16>{
            tiling_specification,
            meta_data: Box::new(gdal_016.clone()),
            phantom_data: Default::default(),
        };
        let source_b = GdalSourceProcessor::<i16>{
            tiling_specification,
            meta_data: Box::new(gdal_108.clone()),
            phantom_data: Default::default(),
         };
         //let source_c = GdalSourceProcessor::<i16>{
         //   tiling_specification,
         //   meta_data: Box::new(gdal_039.clone()),
         //   phantom_data: Default::default(),
         //};
        let source_d = GdalSourceProcessor::<u8>{
            tiling_specification,
            meta_data: Box::new(gdal_claas),
            phantom_data: Default::default(),
        };
   
        let x = imseg_fit(source_a.boxed(), source_b.boxed(), source_d.boxed(), QueryRectangle {
            spatial_bounds: query_bbox,
            time_interval: TimeInterval::new(1_388_536_200_000, 1_388_536_200_000 + 180_000)
                .unwrap(),
            spatial_resolution: query_spatial_resolution,
        }, ctx).await.unwrap();
    }
}
