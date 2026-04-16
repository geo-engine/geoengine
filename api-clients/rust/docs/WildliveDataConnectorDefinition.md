# WildliveDataConnectorDefinition

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**description** | **String** |  | 
**expiry_date** | Option<**String**> |  | [optional]
**id** | **uuid::Uuid** |  | 
**name** | **String** |  | 
**priority** | Option<**i32**> |  | [optional]
**refresh_token** | Option<**String**> | A wrapper type that serializes to \"*****\" and can be deserialized from any string. If the inner value is \"*****\", it is considered unknown and `as_option` returns `None`. This is useful for secrets that should not be exposed in API responses, but can be set in API requests. | [optional]
**r#type** | **Type** |  (enum: WildLIVE!) | 
**user** | Option<**String**> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


