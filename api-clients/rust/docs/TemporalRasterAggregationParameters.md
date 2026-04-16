# TemporalRasterAggregationParameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**aggregation** | [**models::Aggregation**](Aggregation.md) | Aggregation method for values within each time window.  Encountering NO DATA makes the aggregation result NO DATA unless `ignoreNoData` is `true` for the selected aggregation variant. | 
**output_type** | Option<[**models::RasterDataType**](RasterDataType.md)> | Optional output raster data type. | [optional]
**window** | [**models::TimeStep**](TimeStep.md) | Window size and granularity for the output time series. | 
**window_reference** | Option<**i64**> | Optional reference timestamp used as the anchor for window boundaries.  If omitted, windows are anchored at `1970-01-01T00:00:00Z`. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


