# \PermissionsApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_permission_handler**](PermissionsApi.md#add_permission_handler) | **PUT** /permissions | Adds a new permission.
[**get_resource_permissions_handler**](PermissionsApi.md#get_resource_permissions_handler) | **GET** /permissions/resources/{resource_type}/{resource_id} | Lists permission for a given resource.
[**remove_permission_handler**](PermissionsApi.md#remove_permission_handler) | **DELETE** /permissions | Removes an existing permission.



## add_permission_handler

> add_permission_handler(permission_request)
Adds a new permission.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**permission_request** | [**PermissionRequest**](PermissionRequest.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_resource_permissions_handler

> Vec<models::PermissionListing> get_resource_permissions_handler(resource_type, resource_id, limit, offset)
Lists permission for a given resource.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**resource_type** | **String** | Resource Type | [required] |
**resource_id** | **String** | Resource Id | [required] |
**limit** | **i32** |  | [required] |
**offset** | **i32** |  | [required] |

### Return type

[**Vec<models::PermissionListing>**](PermissionListing.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## remove_permission_handler

> remove_permission_handler(permission_request)
Removes an existing permission.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**permission_request** | [**PermissionRequest**](PermissionRequest.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

