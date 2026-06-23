# TileSetItem

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**title** | Option<**String**> | A title for this tileset | [optional]
**data_type** | [**models::GeospatialDataDataType**](GeospatialDataDataType.md) |  | 
**crs** | [**models::TilesCrs**](TilesCrs.md) | Coordinate Reference System (CRS) | 
**tile_matrix_set_uri** | Option<**String**> | Reference to a Tile Matrix Set on an offical source for Tile Matrix Sets such as the OGC NA definition server (http://www.opengis.net/def/tms/). Required if the tile matrix set is registered on an open official source. | [optional]
**links** | [**Vec<models::Link>**](Link.md) | Links to related resources. A 'self' link to the tileset as well as a 'http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme' link to a definition of the TileMatrixSet are required. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


