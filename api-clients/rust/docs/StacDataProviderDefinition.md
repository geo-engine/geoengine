# StacDataProviderDefinition

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**r#type** | **Type** |  (enum: StacProviderDefinition) | 
**name** | **String** |  | 
**id** | **uuid::Uuid** |  | 
**description** | **String** |  | 
**priority** | Option<**i32**> |  | [optional]
**api_url** | **String** |  | 
**collection_name** | **String** |  | 
**s3_config** | Option<[**models::StacProviderS3Config**](StacProviderS3Config.md)> |  | [optional]
**authentication** | Option<[**models::StacProviderAuthentication**](StacProviderAuthentication.md)> |  | [optional]
**time_dimension** | [**models::TimeDimension**](TimeDimension.md) |  | 
**datasets** | [**Vec<models::StacProviderDataset>**](StacProviderDataset.md) |  | 
**query_timeout_secs** | Option<**i64**> | Timeout in seconds for outgoing STAC API HTTP requests. | [optional]
**page_limit** | Option<**i64**> |  | [optional]
**cache_ttl_secs** | Option<**i32**> | Optional output cache lifetime; omitted values use the global cache default. | [optional]
**stac_grid** | Option<[**models::StacGrid**](StacGrid.md)> | Target number of square search cells per dataset CRS projected area of use; omitted values use 512. | [optional]
**max_concurrent_cell_searches** | Option<**i32**> | STAC searches one loading info request runs in parallel; omitted values use 8. | [optional]
**datetime_frequency_aggregation** | Option<**bool**> | Whether the STAC server has the `datetime_frequency` aggregation and answers in the bucket size it is asked for with `datetime_frequency_interval`. Footprint requests then ask it which time steps have items instead of searching them. Omitted values do not use it. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


