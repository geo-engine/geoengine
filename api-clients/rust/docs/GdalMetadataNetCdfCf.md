# GdalMetadataNetCdfCf

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**band_offset** | **i32** | A band offset specifies the first band index to use for the first point in time. All other time steps are added to this offset. | 
**cache_ttl** | Option<**i32**> |  | [optional]
**end** | **i64** | We use the end to specify the last, non-inclusive valid time point. Queries behind this point return no data. TODO: Alternatively, we could think about using the number of possible time steps in the future. | 
**params** | [**models::GdalDatasetParameters**](GdalDatasetParameters.md) |  | 
**result_descriptor** | [**models::RasterResultDescriptor**](RasterResultDescriptor.md) |  | 
**start** | **i64** |  | 
**step** | [**models::TimeStep**](TimeStep.md) |  | 
**r#type** | **Type** |  (enum: GdalMetaDataNetCdfCf) | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


