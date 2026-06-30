# StacDataProviderDefinition

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**api_url** | **String** |  | 
**collection_name** | **String** |  | 
**datasets** | [**Vec<models::StacProviderDataset>**](StacProviderDataset.md) |  | 
**description** | **String** |  | 
**id** | **uuid::Uuid** |  | 
**name** | **String** |  | 
**priority** | Option<**i32**> |  | [optional]
**s3_config** | Option<[**models::StacProviderS3Config**](StacProviderS3Config.md)> |  | [optional]
**time_dimension** | [**models::TimeDimension**](TimeDimension.md) |  | 
**r#type** | **Type** |  (enum: StacProviderDefinition) | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


