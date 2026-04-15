# EdrDataProviderDefinition

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**base_url** | **String** |  | 
**cache_ttl** | Option<**i32**> |  | [optional]
**description** | **String** |  | 
**discrete_vrs** | Option<**Vec<String>**> | List of vertical reference systems with a discrete scale | [optional]
**id** | **uuid::Uuid** |  | 
**name** | **String** |  | 
**priority** | Option<**i32**> |  | [optional]
**provenance** | Option<[**Vec<models::Provenance>**](Provenance.md)> |  | [optional]
**r#type** | **Type** |  (enum: Edr) | 
**vector_spec** | Option<[**models::EdrVectorSpec**](EdrVectorSpec.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


