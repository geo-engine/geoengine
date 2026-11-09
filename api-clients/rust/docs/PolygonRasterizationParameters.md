# PolygonRasterizationParameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**spatial_resolution** | [**models::SpatialResolution**](SpatialResolution.md) | Spatial resolution of the output grid in x/y direction. | 
**origin_coordinate** | Option<[**models::Coordinate2D**](Coordinate2D.md)> | Origin coordinate to which the raster grid is aligned.  If omitted, the grid is aligned to `(0, 0)`. | [optional]
**value** | Option<[**models::PolygonRasterizationValue**](PolygonRasterizationValue.md)> | The value that a polygon burns into the pixels it covers.  If omitted, each covered pixel gets the value `1`. | [optional]
**output_type** | Option<[**models::RasterDataType**](RasterDataType.md)> | Optional output raster data type.  If omitted, it is `U8` for a constant, `U32` for a count and `F64` for an attribute. | [optional]
**output_measurement** | Option<[**models::Measurement**](Measurement.md)> | Optional measurement of the output band.  If omitted, it is derived from the value: unitless for a constant, `count` for a count, and the measurement of the column for an attribute (unitless for a classification that is summed or averaged). | [optional]
**time** | Option<[**models::PolygonRasterizationTime**](PolygonRasterizationTime.md)> | How the time slices of the output raster are derived from the features.  If omitted, the slices are bounded by the start and end times of the features. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


