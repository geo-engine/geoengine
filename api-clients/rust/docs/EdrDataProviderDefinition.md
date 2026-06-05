# EdrDataProviderDefinition

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**r#type** | **Type** |  (enum: Edr) | 
**name** | **String** |  | 
**description** | **String** |  | 
**priority** | Option<**i32**> |  | [optional]
**id** | **uuid::Uuid** |  | 
**base_url** | **String** |  | 
**vector_spec** | Option<[**models::EdrVectorSpec**](EdrVectorSpec.md)> |  | [optional]
**cache_ttl** | Option<**i32**> |  | [optional]
**discrete_vrs** | Option<**Vec<String>**> | List of vertical reference systems with a discrete scale | [optional]
**provenance** | Option<[**Vec<models::Provenance>**](Provenance.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


