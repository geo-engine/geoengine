# GdalMdMetaData

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**r#type** | **Type** |  (enum: GdalMdMetaData) | 
**result_descriptor** | [**models::RasterResultDescriptor**](RasterResultDescriptor.md) |  | 
**z_role** | [**models::ZRole**](ZRole.md) |  | 
**wrap** | **bool** | whether the stored 0..360 degree coverage is re-presented as -180..180 | 
**max_z_batch_size** | Option<**i64**> | upper bound on the z slices a single worker read may request; `None` means the operator's default. A dataset property, since a batch is sized against the slice size of the data itself. | [optional]
**cache_ttl** | Option<**i32**> | Dataset-level TTL fallback used when no tile-level TTL is provided. | [optional]
**leading_prefix** | Option<**Vec<i64>**> | Fixed index into each dimension between z and (y, x), so one dataset is one slice of a 4D array - `[depth]` for `(time, depth, y, x)`. Empty for 3D. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


