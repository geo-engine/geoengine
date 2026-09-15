# VisualPointClusteringParameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**min_radius_px** | **f64** | Minimum circle radius in pixels. | 
**delta_px** | **f64** | Minimum circle-to-circle distance in pixels. | 
**resolution** | **f64** | Spatial resolution used during clustering. | 
**radius_column** | **String** | Column name used to store the cluster radius. | 
**count_column** | **String** | Column name used to store the number of clustered points. | 
**column_aggregates** | [**std::collections::HashMap<String, models::AttributeAggregateDef>**](AttributeAggregateDef.md) | Map of source columns to aggregation definitions used for clustered output attributes. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


