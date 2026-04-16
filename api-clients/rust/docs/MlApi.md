# \MlApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_ml_model**](MlApi.md#add_ml_model) | **POST** /ml/models | Create a new ml model.
[**get_ml_model**](MlApi.md#get_ml_model) | **GET** /ml/models/{model_name} | Get ml model by name.
[**list_ml_models**](MlApi.md#list_ml_models) | **GET** /ml/models | List ml models.



## add_ml_model

> models::MlModelNameResponse add_ml_model(ml_model)
Create a new ml model.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**ml_model** | [**MlModel**](MlModel.md) |  | [required] |

### Return type

[**models::MlModelNameResponse**](MlModelNameResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_ml_model

> models::MlModel get_ml_model(model_name)
Get ml model by name.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**model_name** | **String** | Ml Model Name | [required] |

### Return type

[**models::MlModel**](MlModel.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_ml_models

> Vec<models::MlModel> list_ml_models()
List ml models.

### Parameters

This endpoint does not need any parameter.

### Return type

[**Vec<models::MlModel>**](MlModel.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

