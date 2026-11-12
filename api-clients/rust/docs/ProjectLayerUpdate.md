# ProjectLayerUpdate

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | Option<**String**> | The id of an existing layer of the project, or none for a new layer. | [optional]
**name** | **String** |  | 
**description** | Option<**String**> |  | [optional]
**processing_graph** | **uuid::Uuid** | The id of a registered processing graph. | 
**symbology** | Option<[**models::Symbology**](Symbology.md)> |  | [optional]
**properties** | Option<[**Vec<Vec<String>>**](Vec.md)> |  | [optional]
**metadata** | Option<**std::collections::HashMap<String, String>**> |  | [optional]
**visibility** | Option<[**models::LayerVisibility**](LayerVisibility.md)> |  | [optional]
**source** | Option<[**models::ProviderLayerId**](ProviderLayerId.md)> | The catalog layer this layer was copied from, if any. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


