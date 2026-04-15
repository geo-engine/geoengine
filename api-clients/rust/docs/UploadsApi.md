# \UploadsApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**list_upload_file_layers_handler**](UploadsApi.md#list_upload_file_layers_handler) | **GET** /uploads/{upload_id}/files/{file_name}/layers | List the layers of on uploaded file.
[**list_upload_files_handler**](UploadsApi.md#list_upload_files_handler) | **GET** /uploads/{upload_id}/files | List the files of on upload.
[**upload_handler**](UploadsApi.md#upload_handler) | **POST** /upload | Uploads files.



## list_upload_file_layers_handler

> models::UploadFileLayersResponse list_upload_file_layers_handler(upload_id, file_name)
List the layers of on uploaded file.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**upload_id** | **uuid::Uuid** | Upload id | [required] |
**file_name** | **String** | File name | [required] |

### Return type

[**models::UploadFileLayersResponse**](UploadFileLayersResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_upload_files_handler

> models::UploadFilesResponse list_upload_files_handler(upload_id)
List the files of on upload.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**upload_id** | **uuid::Uuid** | Upload id | [required] |

### Return type

[**models::UploadFilesResponse**](UploadFilesResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## upload_handler

> models::IdResponse upload_handler(files_left_square_bracket_right_square_bracket)
Uploads files.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**files_left_square_bracket_right_square_bracket** | [**Vec<std::path::PathBuf>**](Std__path__PathBuf.md) |  | [required] |

### Return type

[**models::IdResponse**](IdResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: multipart/form-data
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

