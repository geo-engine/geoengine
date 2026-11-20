# RasterFootprints

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**spatial_reference** | **String** | The spatial reference of the `bounds`, which is the one of the processing graph. It is part of the answer so that a client can project the boxes onto its map without asking for the metadata of the graph first. | 
**steps** | [**Vec<models::TimeStepFootprint>**](TimeStepFootprint.md) |  | 
**next** | Option<[**models::TimeInterval**](TimeInterval.md)> | The rest of the requested time interval, if it has more time steps than were answered. Send it as `timeInterval` to get the next steps. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


