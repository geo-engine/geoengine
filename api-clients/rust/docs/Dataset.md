# Dataset

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **uuid::Uuid** |  | 
**name** | **String** |  | 
**display_name** | **String** |  | 
**description** | **String** |  | 
**result_descriptor** | [**models::TypedResultDescriptor**](TypedResultDescriptor.md) |  | 
**source_operator** | **String** |  | 
**symbology** | Option<[**models::Symbology**](Symbology.md)> |  | [optional]
**provenance** | Option<[**Vec<models::Provenance>**](Provenance.md)> |  | [optional]
**tags** | Option<**Vec<String>**> |  | [optional]
**data_path** | Option<[**models::DataPath**](DataPath.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


