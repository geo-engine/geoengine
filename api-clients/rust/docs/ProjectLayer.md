# ProjectLayer

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **String** | The id of the layer, stable across project versions. | 
**name** | **String** |  | 
**description** | **String** |  | 
**processing_graph** | **uuid::Uuid** | The id of the processing graph of the layer. | 
**symbology** | Option<[**models::Symbology**](Symbology.md)> | The symbology of the layer, which plots do not have. | [optional]
**properties** | [**Vec<Vec<String>>**](Vec.md) | properties, for instance, to be rendered in the UI | 
**metadata** | **std::collections::HashMap<String, String>** | metadata used for loading the data | 
**visibility** | [**models::LayerVisibility**](LayerVisibility.md) |  | 
**source** | Option<[**models::ProviderLayerId**](ProviderLayerId.md)> | The catalog layer this layer was copied from, if any. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


