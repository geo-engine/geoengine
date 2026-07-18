# TileMatrix

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **String** | Identifier selecting one of the scales defined in the [TileMatrixSet] and representing the scaleDenominator the tile. | 
**title** | Option<**String**> | Title of a tile matrix, normally used for display to a human | [optional]
**description** | Option<**String**> | Brief narrative description of a tile matrix, normally available for display to a human | [optional]
**keywords** | Option<**Vec<String>**> | Unordered list of one or more commonly used or formalized word(s) or phrase(s) used to describe this tile set | [optional]
**scale_denominator** | **f64** | Scale denominator of this tile matrix | 
**cell_size** | **f64** | Cell size of this tile matrix | 
**corner_of_origin** | Option<[**models::CornerOfOrigin**](CornerOfOrigin.md)> | The corner of the tile matrix (_topLeft_ or _bottomLeft_) used as the origin for numbering tile rows and columns. This corner is also a corner of the (0, 0) tile. | [optional]
**point_of_origin** | **Vec<f64>** | Precise position in CRS coordinates of the corner of origin (e.g. the top-left corner) for this tile matrix. This position is also a corner of the (0, 0) tile. | 
**tile_width** | **i32** | Width of each tile of this tile matrix in pixels | 
**tile_height** | **i32** | Height of each tile of this tile matrix in pixels | 
**matrix_width** | **i32** | Width of the matrix (number of tiles in width) | 
**matrix_height** | **i32** | Height of the matrix (number of tiles in height) | 
**variable_matrix_widths** | Option<[**Vec<models::VariableMatrixWidth>**](VariableMatrixWidth.md)> | Describes the rows that has variable matrix width | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


