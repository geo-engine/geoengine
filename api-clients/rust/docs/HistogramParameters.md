# HistogramParameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**bounds** | [**models::HistogramBounds**](HistogramBounds.md) | If `data`, it computes the bounds of the underlying data. If `{ \"min\": ..., \"max\": ... }`, one can specify custom bounds. | 
**buckets** | [**models::HistogramBuckets**](HistogramBuckets.md) | The number of buckets. The value can be specified or calculated. | 
**column_name** | **String** | Name of the (numeric) vector attribute or raster band to compute the histogram on. | 
**interactive** | Option<**bool**> | Flag, if the histogram should have user interactions for a range selection. It is `false` by default. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


