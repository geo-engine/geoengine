# GeospatialData

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **String** | Unique identifier of the Layer. | 
**title** | Option<**String**> | Title for this layer | [optional]
**description** | Option<**String**> | Brief narrative description of this layer | [optional]
**keywords** | Option<**Vec<String>**> | Unordered list of one or more commonly used or formalized word(s) or phrase(s) used to describe this layer | [optional]
**data_type** | [**models::GeospatialDataDataType**](GeospatialDataDataType.md) |  | 
**geometry_dimension** | Option<[**models::GeometryDimension**](GeometryDimension.md)> | The geometry dimension of the features shown in this layer (0: points, 1: curves, 2: surfaces, 3: solids), unspecified: mixed or unknown | [optional]
**feature_type** | Option<**String**> | Feature type identifier. Only applicable to layers of datatype 'geometries' | [optional]
**attribution** | Option<**String**> | Short reference to recognize the author or provider | [optional]
**license** | Option<**String**> | License applicable to the tiles | [optional]
**point_of_contact** | Option<**String**> | Useful information to contact the authors or custodians for the layer (e.g. e-mail address, a physical address,  phone numbers, etc) | [optional]
**publisher** | Option<**String**> | Organization or individual responsible for making the layer available | [optional]
**theme** | Option<**String**> | Category where the layer can be grouped | [optional]
**crs** | Option<[**models::TilesCrs**](TilesCrs.md)> | Coordinate Reference System (CRS) | [optional]
**epoch** | Option<**f64**> | Epoch of the Coordinate Reference System (CRS) | [optional]
**min_scale_denominator** | Option<**f64**> | Minimum scale denominator for usage of the layer | [optional]
**max_scale_denominator** | Option<**f64**> | aximum scale denominator for usage of the layer | [optional]
**min_cell_size** | Option<**f64**> | Minimum cell size for usage of the layer | [optional]
**max_cell_size** | Option<**f64**> | Maximum cell size for usage of the layer | [optional]
**max_tile_matrix** | Option<**String**> | TileMatrix identifier associated with the minScaleDenominator | [optional]
**min_tile_matrix** | Option<**String**> | TileMatrix identifier associated with the maxScaleDenominator | [optional]
**bounding_box** | Option<[**models::BoundingBox2D**](BoundingBox2D.md)> | Minimum bounding rectangle surrounding the layer | [optional]
**created** | Option<**String**> | When the layer was first produced | [optional]
**updated** | Option<**String**> | Last layer change/revision | [optional]
**style** | Option<[**models::Style**](Style.md)> | Style used to generate the layer in the tileset | [optional]
**geo_data_classes** | Option<**Vec<String>**> | URI identifying a class of data contained in this layer (useful to determine compatibility with styles or processes) | [optional]
**properties_schema** | Option<**serde_json::Value**> |  | [optional]
**links** | Option<[**Vec<models::Link>**](Link.md)> | Links related to this layer. Possible link 'rel' values are: 'geodata' for a URL pointing to the collection of geospatial data. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


