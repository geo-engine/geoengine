# WarpParameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**target_spatial_reference** | Option<**String**> | Target spatial reference system. Defaults to the spatial reference of the input. | [optional]
**output_resolution** | Option<[**models::SpatialResolution**](SpatialResolution.md)> | Output resolution (`x`, `y`) in units of the target spatial reference. Defaults to the resolution of the (reprojected) input. | [optional]
**output_origin_reference** | Option<[**models::Coordinate2D**](Coordinate2D.md)> | Optional reference point used to align the output grid origin. | [optional]
**downsampling** | Option<[**models::WarpDownsamplingMethod**](WarpDownsamplingMethod.md)> | How to compute an output pixel that is coarser than the input in at least one axis. Defaults to `nearest`. | [optional]
**upsampling** | Option<[**models::WarpUpsamplingMethod**](WarpUpsamplingMethod.md)> | How to compute an output pixel that is finer than the input in both axes. Defaults to `nearest`. | [optional]
**output_data_type** | Option<[**models::RasterDataType**](RasterDataType.md)> | The data type of the output. Defaults to the data type of the input. Integer types round half up and clip values to their range. | [optional]
**derive_out_spec** | Option<[**models::DeriveOutRasterSpecsSource**](DeriveOutRasterSpecsSource.md)> | How the output grid is derived when reprojecting, as in `Reprojection`: `projectionBounds` (default) derives it from the area of use of the input's spatial reference, `dataBounds` from the data bounds. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


