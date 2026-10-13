# MeanRasterPixelValuesOverTimeParameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**time_position** | [**models::MeanRasterPixelValuesOverTimePosition**](MeanRasterPixelValuesOverTimePosition.md) | Where should the x-axis (time) tick be positioned? At either time start, time end or in the center. | 
**area** | Option<**bool**> | Whether to fill the area under the curve. Defaults to `true`. Only applies to a single band, multiple bands are drawn as lines. | [optional]
**band_names** | Option<**Vec<String>**> | _Optional_: The names of the bands to compute the means for. The operator uses all bands if this parameter is empty. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


