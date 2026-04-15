# OgrSourceDataset

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**attribute_query** | Option<**String**> |  | [optional]
**cache_ttl** | Option<**i32**> |  | [optional]
**columns** | Option<[**models::OgrSourceColumnSpec**](OgrSourceColumnSpec.md)> |  | [optional]
**data_type** | Option<[**models::VectorDataType**](VectorDataType.md)> |  | [optional]
**default_geometry** | Option<[**models::TypedGeometry**](TypedGeometry.md)> |  | [optional]
**file_name** | **String** |  | 
**force_ogr_spatial_filter** | Option<**bool**> |  | [optional]
**force_ogr_time_filter** | Option<**bool**> |  | [optional]
**layer_name** | **String** |  | 
**on_error** | [**models::OgrSourceErrorSpec**](OgrSourceErrorSpec.md) |  | 
**sql_query** | Option<**String**> |  | [optional]
**time** | Option<[**models::OgrSourceDatasetTimeType**](OgrSourceDatasetTimeType.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


