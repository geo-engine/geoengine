# StacProviderDatasetBand

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**asset_band** | [**models::StacAssetBand**](StacAssetBand.md) | The band inside the STAC asset that this dataset band reads from (addressing: which asset file + which raster channel within it). | 
**band_descriptor** | [**models::RasterBandDescriptor**](RasterBandDescriptor.md) | The band descriptor of the resulting geo engine dataset layer. | 
**coarser_assets** | Option<[**Vec<models::StacCoarserAsset>**](StacCoarserAsset.md)> | Coarser assets with the same band, finest first.  When the source reads an overview level, it reads the coarsest of these assets that is not coarser than the overview level, so it transfers less data. Their resolution must be an integer multiple of the dataset's resolution. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


