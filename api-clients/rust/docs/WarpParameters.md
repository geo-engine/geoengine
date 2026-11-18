# WarpParameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**target_spatial_reference** | Option<**String**> | Target spatial reference system. Defaults to the spatial reference of the input. | [optional]
**output_resolution** | Option<[**models::SpatialResolution**](SpatialResolution.md)> | Output resolution (`x`, `y`) in units of the target spatial reference. Defaults to the resolution of the (reprojected) input. | [optional]
**output_origin_reference** | Option<[**models::Coordinate2D**](Coordinate2D.md)> | Optional reference point used to align the output grid origin. | [optional]
**method** | Option<[**models::WarpMethod**](WarpMethod.md)> | Resampling method. | [optional]
**derive_out_spec** | Option<[**models::DeriveOutRasterSpecsSource**](DeriveOutRasterSpecsSource.md)> | How the output grid is derived when reprojecting, as in `Reprojection`: `projectionBounds` (default) derives it from the area of use of the input's spatial reference, `dataBounds` from the data bounds. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


