# StatisticsParameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**column_names** | Option<**Vec<String>**> | # Vector data The names of the attributes to generate statistics for.  # Raster data _Optional_: An alias for each input source. The operator will automatically name the rasters `Raster-1`, `Raster-2`, … if this parameter is empty. If aliases are given, the number of aliases must match the number of input rasters. Otherwise an error is returned. | [optional]
**percentiles** | Option<**Vec<f64>**> | The percentiles to compute for each attribute. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


