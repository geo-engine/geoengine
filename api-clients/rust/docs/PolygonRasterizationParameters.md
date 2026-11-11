# PolygonRasterizationParameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**spatial_resolution** | [**models::SpatialResolution**](SpatialResolution.md) | Spatial resolution of the output grid, i.e., the positive pixel size in x and y direction in units of the spatial reference of the source.  The columns of the grid run from west to east (increasing x) and the rows from north to south (decreasing y), i.e., the grid has the geo transform `(originCoordinate, x, -y)`. This holds for every spatial reference, since coordinates are always given as x = easting/longitude and y = northing/latitude. | 
**origin_coordinate** | Option<[**models::Coordinate2D**](Coordinate2D.md)> | Origin coordinate to which the raster grid is aligned.  It is a pixel corner, and the grid continues from it in all directions to cover the polygons. If omitted, the grid is aligned to `(0, 0)`. | [optional]
**value** | Option<[**models::PolygonRasterizationValue**](PolygonRasterizationValue.md)> | The value that a polygon burns into the pixels it covers.  If omitted, each covered pixel gets the value `1`. | [optional]
**output_type** | Option<[**models::RasterDataType**](RasterDataType.md)> | Optional output raster data type.  If omitted, it is `U8` for a constant, `U32` for a count and `F64` for an attribute. | [optional]
**output_measurement** | Option<[**models::Measurement**](Measurement.md)> | Optional measurement of the output band.  If omitted, it is derived from the value: unitless for a constant, `count` for a count, and the measurement of the column for an attribute (unitless for a classification that is summed or averaged). | [optional]
**time** | Option<[**models::PolygonRasterizationTime**](PolygonRasterizationTime.md)> | How the time slices of the output raster are derived from the features.  If omitted, every start and end time of a feature is a slice boundary (`featureTimes`). | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


