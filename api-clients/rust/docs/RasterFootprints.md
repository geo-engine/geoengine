# RasterFootprints

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**spatial_reference** | **String** | The spatial reference of the `bounds`. | 
**steps** | [**Vec<models::TimeStepFootprint>**](TimeStepFootprint.md) |  | 
**next** | Option<[**models::TimeInterval**](TimeInterval.md)> | The next time step to ask for, if more steps exist within the requested time interval. | [optional]
**too_large** | **bool** | Answering needs too many catalog searches. Query a smaller area or time range. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


