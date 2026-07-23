# VectorExpressionParameters

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**input_columns** | **Vec<String>** | The columns to use as variables in the expression.  For usage in the expression, all special characters are replaced by underscores. E.g., `precipitation.cm` becomes `precipitation_cm`. If the column name starts with a number, an underscore is prepended. E.g., `1column` becomes `_1column`. | 
**expression** | **String** | The expression to evaluate. | 
**output_column** | [**models::OutputColumn**](OutputColumn.md) | The type and name of the new column. | 
**geometry_column_name** | Option<**String**> | The variable name of the geometry column. The default is `geom`. | [optional]
**output_measurement** | [**models::Measurement**](Measurement.md) | The measurement of the new column. The default is unitless. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


