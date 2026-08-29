# RasterStackerParameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**rename_bands** | [**models::RenameBands**](RenameBands.md) | Strategy for deriving output band names.  - `default`: appends ` (n)` with the smallest `n` that avoids a conflict. - `suffix`: appends one suffix per input. - `rename`: explicitly provides names for all resulting bands. | 
**output_origin** | Option<[**models::Coordinate2D**](Coordinate2D.md)> | Override the origin of the stacked output grid. If `None`, the first input's origin is used. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


