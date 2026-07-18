# TileMatrixSet

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | [**models::TileMatrixSetId**](TileMatrixSetId.md) | Tile matrix set identifier | 
**title** | Option<**String**> | Title of a tile matrix set, normally used for display to a human | [optional]
**description** | Option<**String**> | Brief narrative description of a tile matrix set, normally available for display to a human | [optional]
**keywords** | Option<**Vec<String>**> | Unordered list of one or more commonly used or formalized word(s) or phrase(s) used to describe this resource entity | [optional]
**uri** | Option<**String**> | Reference to an official source for this tile matrix set | [optional]
**crs** | [**models::TilesCrs**](TilesCrs.md) | Coordinate Reference System (CRS) | 
**ordered_axes** | Option<**Vec<String>**> | Ordered list of names of the dimensions defined in the CRS | [optional]
**well_known_scale_set** | Option<**String**> | Reference to a well-known scale set | [optional]
**bounding_box** | Option<[**models::BoundingBox2D**](BoundingBox2D.md)> | Minimum bounding rectangle surrounding the tile matrix set, in the supported CRS | [optional]
**tile_matrices** | [**Vec<models::TileMatrix>**](TileMatrix.md) | Describes scale levels and its tile matrices | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


