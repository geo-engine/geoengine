# OgrSourceDataset

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**file_name** | **String** |  | 
**layer_name** | **String** |  | 
**data_type** | Option<[**models::VectorDataType**](VectorDataType.md)> |  | [optional]
**time** | Option<[**models::OgrSourceDatasetTimeType**](OgrSourceDatasetTimeType.md)> |  | [optional]
**default_geometry** | Option<[**models::TypedGeometry**](TypedGeometry.md)> |  | [optional]
**columns** | Option<[**models::OgrSourceColumnSpec**](OgrSourceColumnSpec.md)> |  | [optional]
**force_ogr_time_filter** | Option<**bool**> |  | [optional]
**force_ogr_spatial_filter** | Option<**bool**> |  | [optional]
**on_error** | [**models::OgrSourceErrorSpec**](OgrSourceErrorSpec.md) |  | 
**sql_query** | Option<**String**> |  | [optional]
**attribute_query** | Option<**String**> |  | [optional]
**cache_ttl** | Option<**i32**> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


