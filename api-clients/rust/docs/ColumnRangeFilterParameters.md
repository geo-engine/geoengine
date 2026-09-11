# ColumnRangeFilterParameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**column** | **String** | Column name to filter on. | 
**ranges** | [**Vec<models::StringOrNumberRange>**](StringOrNumberRange.md) | One or more inclusive ranges for the selected column values. Numeric ranges use `[start, end]`; string ranges use `[\"a\", \"k\"]`. | 
**keep_nulls** | **bool** | Whether rows whose column value is NULL should be retained. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


