# ProcessingGraphReadStatus

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**processing_graph_id** | **uuid::Uuid** |  | 
**throttled_reads** | **i64** | The number of reads that are currently rate limited. | 
**throttled_for_seconds** | **f64** | How long the first of these reads has been rate limited, in seconds. | 
**http_status** | **i32** | The HTTP status of the latest rate limit, 429 or 503. | 
**retry_in_seconds** | **f64** | The pause before the next attempt of the latest rate limited read, in seconds. 0 if it is retried at once. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


