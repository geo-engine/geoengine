# GdalMetadataNetCdfCf

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**r#type** | **Type** |  (enum: GdalMetaDataNetCdfCf) | 
**result_descriptor** | [**models::RasterResultDescriptor**](RasterResultDescriptor.md) |  | 
**params** | [**models::GdalDatasetParameters**](GdalDatasetParameters.md) |  | 
**start** | **i64** |  | 
**end** | **i64** | We use the end to specify the last, non-inclusive valid time point. Queries behind this point return no data. TODO: Alternatively, we could think about using the number of possible time steps in the future. | 
**step** | [**models::TimeStep**](TimeStep.md) |  | 
**band_offset** | **i32** | A band offset specifies the first band index to use for the first point in time. All other time steps are added to this offset. | 
**cache_ttl** | Option<**i32**> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


