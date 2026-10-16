# GdalMdMetaData

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**r#type** | **Type** |  (enum: GdalMdMetaData) | 
**result_descriptor** | [**models::RasterResultDescriptor**](RasterResultDescriptor.md) |  | 
**variables** | [**Vec<models::MdVariable>**](MdVariable.md) | The variables of the dataset; the index of a variable is its output band. | 
**wrap** | **bool** | whether the stored 0..360 degree coverage is re-presented as -180..180 | 
**cache_ttl** | Option<**i32**> | Dataset-level TTL fallback used when no tile-level TTL is provided. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


