# PolygonRasterizationValueAttribute

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**r#type** | **Type** |  (enum: attribute) | 
**column** | **String** | The name of a numeric (int, float or category) column. | 
**aggregation** | [**models::PolygonRasterizationAggregation**](PolygonRasterizationAggregation.md) | How the values of overlapping polygons are combined. | 
**ignore_no_data** | Option<**bool**> | If `true` (default), features with a null value are skipped. If `false`, they make all pixels they cover NO DATA. | [optional][default to true]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


