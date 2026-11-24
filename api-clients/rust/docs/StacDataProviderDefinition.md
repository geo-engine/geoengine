# StacDataProviderDefinition

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**r#type** | **Type** |  (enum: StacProviderDefinition) | 
**name** | **String** |  | 
**id** | **uuid::Uuid** |  | 
**description** | **String** |  | 
**priority** | Option<**i32**> |  | [optional]
**api_url** | **String** |  | 
**collection_name** | **String** |  | 
**s3_config** | Option<[**models::StacProviderS3Config**](StacProviderS3Config.md)> |  | [optional]
**authentication** | Option<[**models::StacProviderAuthentication**](StacProviderAuthentication.md)> |  | [optional]
**time_dimension** | [**models::TimeDimension**](TimeDimension.md) |  | 
**datasets** | [**Vec<models::StacProviderDataset>**](StacProviderDataset.md) |  | 
**query_timeout_secs** | Option<**i64**> | Timeout in seconds for outgoing STAC API HTTP requests. | [optional]
**page_limit** | Option<**i64**> |  | [optional]
**cache_ttl_secs** | Option<**i32**> | Optional output cache lifetime; omitted values use the global cache default. | [optional]
**stac_grid** | Option<[**models::StacGrid**](StacGrid.md)> | Target number of square search cells per dataset CRS projected area of use; omitted values use 512. | [optional]
**max_concurrent_cell_searches** | Option<**i32**> | STAC searches one loading info request runs in parallel; omitted values use 8. | [optional]
**datetime_frequency_aggregation** | Option<**bool**> | Whether the STAC server has the `datetime_frequency` aggregation and answers in the bucket size it is asked for with `datetime_frequency_interval` (e.g. CODE-DE; Earth Search and the Copernicus Data Space cannot).  If set, the availability of a calendar comes from one aggregation over the view per chunk of time steps, at any zoom, instead of one search per grid cell and page, which is much faster. Days without items are stored as empty for the grid cells inside the view, so their map tiles need no search. Map tiles and the boxes of a date on the map still search. Omitted values do not use it. | [optional]
**time_bounds** | Option<[**models::TimeInterval**](TimeInterval.md)> | The time of the datasets. Omitted values come from the temporal extent of the collection, so this is only needed where that is missing or wrong. Apps use it to ask no dates before the data starts. | [optional]
**item_time** | Option<[**models::StacItemTime**](StacItemTime.md)> | Which item time decides the layer time steps an item belongs to; omitted values use `datetime`. | [optional]
**max_concurrent_asset_probes** | Option<**i32**> | Asset file headers one STAC search reads in parallel to complete grid metadata the STAC items lack, as for `NetCDF` assets; omitted values use 8. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


