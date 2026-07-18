# TileMatrixSetItem

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | Option<[**models::TileMatrixSetId**](TileMatrixSetId.md)> | Optional local tile matrix set identifier, e.g. for use as unspecified `{tileMatrixSetId}` parameter. Implementation of 'identifier' | [optional]
**title** | Option<**String**> | Title of this tile matrix set, normally used for display to a human | [optional]
**uri** | Option<**String**> | Reference to an official source for this tileMatrixSet | [optional]
**crs** | Option<[**models::TilesCrs**](TilesCrs.md)> |  | [optional]
**links** | [**Vec<models::Link>**](Link.md) | Links to related resources. A 'self' link to the tile matrix set definition is required. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


