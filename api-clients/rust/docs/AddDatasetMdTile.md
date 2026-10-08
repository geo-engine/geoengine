# AddDatasetMdTile

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**spatial_partition** | [**models::SpatialPartition2D**](SpatialPartition2D.md) | the presented footprint (wrap-around aware) | 
**band** | **i32** |  | 
**z_index** | **i64** | position of this file in the concatenated z axis of `band` | 
**array_name** | **String** |  | 
**array_group** | Option<**String**> | \"/\"-separated path to the MD group below the root group; `None` = root group | [optional]
**time_descriptor** | [**models::TimeDescriptor**](TimeDescriptor.md) |  | 
**time_steps** | [**Vec<models::TimeInterval>**](TimeInterval.md) | One interval per z slice, always required.  Both forms are stored on purpose: `time_descriptor` is the compact form the API advertises, `time_steps` says which slices exist and is what the read path derives the file's slice count from. Omitting it would make the file contribute zero slices and every later file come back at the wrong time. | 
**params** | [**models::GdalDatasetParameters**](GdalDatasetParameters.md) |  | 
**leading_prefix** | Option<**Vec<i64>**> | Fixed index into each dimension between z and (y, x), so one row is one slice of a 4D array - `[depth]` for `(time, depth, y, x)`. Empty for 3D.  Per row rather than per dataset, so the bands of one dataset can each select a different slice: a `(time, depth, y, x)` file with one row per depth becomes one dataset whose band `b` is depth `b`. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


