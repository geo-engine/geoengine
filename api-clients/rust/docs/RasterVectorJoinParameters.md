# RasterVectorJoinParameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**feature_aggregation** | [**models::FeatureAggregationMethod**](FeatureAggregationMethod.md) | The aggregation function to use for features covering multiple pixels. | 
**feature_aggregation_ignore_no_data** | Option<**bool**> | Whether to ignore no data values in the aggregation. Defaults to `false`. | [optional]
**names** | [**models::ColumnNames**](ColumnNames.md) | Specify how the new column names are derived from the raster band names.  The `ColumnNames` type is used to specify how the new column names are derived from the raster band names.  - **default**: Appends \" (n)\" to the band name with the smallest `n` that avoids a conflict. - **suffix**: Specifies a suffix for each input, to be appended to the band names. - **rename**: A list of names for each new column.  | 
**temporal_aggregation** | [**models::TemporalAggregationMethod**](TemporalAggregationMethod.md) | The aggregation function to use for features covering multiple (raster) time steps. | 
**temporal_aggregation_ignore_no_data** | Option<**bool**> | Whether to ignore no data values in the aggregation. Defaults to `false`. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


