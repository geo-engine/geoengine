# MdProbeRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**data_path** | [**models::DataPath**](DataPath.md) |  | 
**files** | **Vec<String>** | The files to probe, relative to `data_path` (or absolute GDAL VSI paths when `data_path` is `External`). | 
**array_name** | Option<**String**> | The MD array to read. Required when a file has more than one array with at least three dimensions, which is the normal case for netCDF. | [optional]
**group** | Option<**String**> | \"/\"-separated path to the MD group below the root group; `None` = root group. | [optional]
**variables_as_bands** | Option<**bool**> | Probe several data variables as separate Geo Engine bands instead of one. Each variable must be a time series. | [optional]
**max_z_batch_size** | Option<**i32**> | Upper bound on how many consecutive z slices one GDAL read may request. Carried into the dataset metadata, because a batch is sized against the slice size of the data and not against the workflow that reads it. `None` means the operator's default. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


