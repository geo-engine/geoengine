# GdalMdMetaData

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**r#type** | **Type** |  (enum: GdalMdMetaData) | 
**result_descriptor** | [**models::RasterResultDescriptor**](RasterResultDescriptor.md) |  | 
**z_role** | [**models::ZRole**](ZRole.md) |  | 
**wrap** | **bool** | whether the stored 0..360 degree coverage is re-presented as -180..180 | 
**max_z_batch_size** | Option<**i64**> | upper bound on the z slices a single worker read may request; `None` means the operator's default. A dataset property, since a batch is sized against the slice size of the data itself. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


