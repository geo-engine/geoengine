# TileSet

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**title** | Option<**String**> | A title for this tileset | [optional]
**description** | Option<**String**> | Brief narrative description of this tile set | [optional]
**keywords** | Option<**Vec<String>**> | Unordered list of one or more commonly used or formalized word(s) or phrase(s) used to describe a TileSet | [optional]
**data_type** | [**models::GeospatialDataDataType**](GeospatialDataDataType.md) |  | 
**tile_matrix_set_uri** | Option<**String**> | Reference to a Tile Matrix Set on the OGC NA definition server (<http://www.opengis.net/def/tms/>). Required if the tile matrix set is registered on the definition server. | [optional]
**tile_matrix_set_limits** | Option<[**Vec<models::TileMatrixLimits>**](TileMatrixLimits.md)> | Limits for the TileRow and TileCol values for each TileMatrix in the TileMatrixSet. If missing, there are no limits other that the ones imposed by the TileMatrixSet. If present the TileMatrices listed are limited and the rest not available at all | [optional]
**crs** | [**models::TilesCrs**](TilesCrs.md) | Coordinate Reference System (CRS) | 
**epoch** | Option<**f64**> | Epoch of the Coordinate Reference System (CRS) | [optional]
**links** | [**Vec<models::Link>**](Link.md) | Links to related resources. Possible link 'rel' values are: 'dataset' for a URL pointing to the dataset, 'tiles' for a URL template to get the tiles; 'alternate' for a URL pointing to another representation of the TileSetMetadata (e.g a TileJSON file); 'tiling-scheme' for a definition of the TileMatrixSet | 
**layers** | Option<[**Vec<models::GeospatialData>**](GeospatialData.md)> |  | [optional]
**bounding_box** | Option<[**models::BoundingBox2D**](BoundingBox2D.md)> | Minimum bounding rectangle surrounding the tile matrix set, in the supported CRS | [optional]
**center_point** | Option<[**models::TilePoint**](TilePoint.md)> | Location of a tile that nicely represents the tileset. Implementations may use this center value to set the default location or to present a representative tile in a user interface | [optional]
**style** | Option<[**models::Style**](Style.md)> | Style involving all layers used to generate the tileset | [optional]
**attribution** | Option<**String**> | Short reference to recognize the author or provider | [optional]
**license** | Option<**String**> | License applicable to the tiles | [optional]
**access_constraints** | Option<[**models::AccessConstraints**](AccessConstraints.md)> | Restrictions on the availability of the Tile Set that the user needs to be aware of before using or redistributing the Tile Set | [optional]
**version** | Option<**String**> | Version of the Tile Set. Changes if the data behind the tiles has been changed | [optional]
**created** | Option<**String**> | When the Tile Set was first produced | [optional]
**updated** | Option<**String**> | Last Tile Set change/revision | [optional]
**point_of_contact** | Option<**String**> | Useful information to contact the authors or custodians for the Tile Set | [optional]
**media_types** | Option<**Vec<String>**> | Media types available for the tiles | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


