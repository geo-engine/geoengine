use futures::{StreamExt, Stream};
use geoengine_datatypes::{primitives::{SpatialPartition2D, TimeInstance, TimeInterval}, raster::{GridOrEmpty, Pixel, BaseTile, GridShape}};
use crate::engine::{QueryContext, QueryRectangle, RasterQueryProcessor};
use pyo3::{types::{PyModule, PyUnicode, PyList}};
use ndarray::{Array2, Axis,concatenate, stack, ArrayBase, OwnedRepr, Dim};
use numpy::{PyArray};
use rand::prelude::*;
use crate::util::Result;

#[allow(clippy::too_many_arguments)]
pub async fn imseg_fit<C: QueryContext>(
    processor_truth: Box<dyn RasterQueryProcessor<RasterType = u8>>,
    query_rect: QueryRectangle<SpatialPartition2D>,
    query_ctx: C,
) -> Result<()>
{

    
    
        
    let mut tile_stream_truth = processor_truth.raster_query(query_rect, &query_ctx).await?;

    let mut x: Vec<u32> = vec![0,0,0,0,0];
    let mut time: TimeInterval = TimeInterval::default();
    let mut count: usize = 0;
    while let Some(truth) = tile_stream_truth.next().await {
        match truth {
            Ok(truth) => {
                time = truth.time;
                match truth.grid_array {
                    GridOrEmpty::Grid(g) => {
                        count = count + 1;
                        let nd = g.data.contains(&0);
                        if !nd {
                            for element in g.data.iter(){
                                x[(*element - 1) as usize] = x[(*element - 1) as usize] + 1;
                                
                            }
                        } else {
                            println!("{}: {:?}", count, time);
                            
                        }
                        
                        
                    }, 
                    _ => {
                        println!("It was empty");
                        
                    }
                }
            }, 
            _ => {
                println!("Error");
                
            }
        }

    }
        
    println!("end: {:?}", x);
    
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

        let x = imseg_fit(proc_claas, QueryRectangle {
            spatial_bounds: query_bbox,
            time_interval: TimeInterval::new(1_388_574_000_000, 1_388_574_000_000 + 900_000_000)
                .unwrap(),
            spatial_resolution: query_spatial_resolution,
        }, ctx,
    ).await.unwrap();
    }
}
//[20992, 85395640, 108417253, 67523052, 631]
//[85416632, 108417253, 67523052, 631, 0]
//[85318247, 108293235, 67483311, 631, 0]