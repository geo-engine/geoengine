# GdalDatasetParameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**file_path** | **String** |  | 
**rasterband_channel** | **i32** |  | 
**geo_transform** | [**models::GeoTransform**](GeoTransform.md) |  | 
**width** | **i32** |  | 
**height** | **i32** |  | 
**file_not_found_handling** | [**models::FileNotFoundHandling**](FileNotFoundHandling.md) |  | 
**no_data_value** | Option<**f64**> |  | [optional]
**properties_mapping** | Option<[**Vec<models::GdalMetadataMapping>**](GdalMetadataMapping.md)> |  | [optional]
**gdal_open_options** | Option<**Vec<String>**> |  | [optional]
**gdal_config_options** | Option<[**Vec<Vec<String>>**](Vec.md)> |  | [optional]
**allow_alphaband_as_mask** | Option<**bool**> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


