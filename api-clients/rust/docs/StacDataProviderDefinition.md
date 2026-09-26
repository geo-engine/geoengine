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
**cache_ttl_secs** | Option<**i32**> | Optional cache lifetime for tiles of this provider; an omitted value inherits the query default. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


