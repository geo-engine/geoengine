# \UserApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_role_handler**](UserApi.md#add_role_handler) | **PUT** /roles | Add a new role. Requires admin privilige.
[**assign_role_handler**](UserApi.md#assign_role_handler) | **POST** /users/{user}/roles/{role} | Assign a role to a user. Requires admin privilige.
[**computation_quota_handler**](UserApi.md#computation_quota_handler) | **GET** /quota/computations/{computation} | Retrieves the quota used by computation with the given computation id
[**computations_quota_handler**](UserApi.md#computations_quota_handler) | **GET** /quota/computations | Retrieves the quota used by computations
[**data_usage_handler**](UserApi.md#data_usage_handler) | **GET** /quota/dataUsage | Retrieves the data usage
[**data_usage_summary_handler**](UserApi.md#data_usage_summary_handler) | **GET** /quota/dataUsage/summary | Retrieves the data usage summary
[**get_role_by_name_handler**](UserApi.md#get_role_by_name_handler) | **GET** /roles/byName/{name} | Get role by name
[**get_role_descriptions**](UserApi.md#get_role_descriptions) | **GET** /user/roles/descriptions | Query roles for the current user.
[**get_user_quota_handler**](UserApi.md#get_user_quota_handler) | **GET** /quotas/{user} | Retrieves the available and used quota of a specific user.
[**quota_handler**](UserApi.md#quota_handler) | **GET** /quota | Retrieves the available and used quota of the current user.
[**remove_role_handler**](UserApi.md#remove_role_handler) | **DELETE** /roles/{role} | Remove a role. Requires admin privilige.
[**revoke_role_handler**](UserApi.md#revoke_role_handler) | **DELETE** /users/{user}/roles/{role} | Revoke a role from a user. Requires admin privilige.
[**update_user_quota_handler**](UserApi.md#update_user_quota_handler) | **POST** /quotas/{user} | Update the available quota of a specific user.



## add_role_handler

> models::IdResponse add_role_handler(add_role)
Add a new role. Requires admin privilige.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**add_role** | [**AddRole**](AddRole.md) |  | [required] |

### Return type

[**models::IdResponse**](IdResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## assign_role_handler

> assign_role_handler(user, role)
Assign a role to a user. Requires admin privilige.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user** | **uuid::Uuid** | User id | [required] |
**role** | **uuid::Uuid** | Role id | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## computation_quota_handler

> Vec<models::OperatorQuota> computation_quota_handler(computation)
Retrieves the quota used by computation with the given computation id

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**computation** | **uuid::Uuid** | Computation id | [required] |

### Return type

[**Vec<models::OperatorQuota>**](OperatorQuota.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## computations_quota_handler

> Vec<models::ComputationQuota> computations_quota_handler(offset, limit)
Retrieves the quota used by computations

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**offset** | **i32** |  | [required] |
**limit** | **i32** |  | [required] |

### Return type

[**Vec<models::ComputationQuota>**](ComputationQuota.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## data_usage_handler

> Vec<models::DataUsage> data_usage_handler(offset, limit)
Retrieves the data usage

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**offset** | **i64** |  | [required] |
**limit** | **i64** |  | [required] |

### Return type

[**Vec<models::DataUsage>**](DataUsage.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## data_usage_summary_handler

> Vec<models::DataUsageSummary> data_usage_summary_handler(granularity, offset, limit, dataset)
Retrieves the data usage summary

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**granularity** | [**UsageSummaryGranularity**](UsageSummaryGranularity.md) |  | [required] |
**offset** | **i64** |  | [required] |
**limit** | **i64** |  | [required] |
**dataset** | Option<**String**> |  |  |

### Return type

[**Vec<models::DataUsageSummary>**](DataUsageSummary.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_role_by_name_handler

> models::IdResponse get_role_by_name_handler(name)
Get role by name

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**name** | **String** | Role Name | [required] |

### Return type

[**models::IdResponse**](IdResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_role_descriptions

> Vec<models::RoleDescription> get_role_descriptions()
Query roles for the current user.

### Parameters

This endpoint does not need any parameter.

### Return type

[**Vec<models::RoleDescription>**](RoleDescription.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_user_quota_handler

> models::Quota get_user_quota_handler(user)
Retrieves the available and used quota of a specific user.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user** | **uuid::Uuid** | User id | [required] |

### Return type

[**models::Quota**](Quota.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## quota_handler

> models::Quota quota_handler()
Retrieves the available and used quota of the current user.

### Parameters

This endpoint does not need any parameter.

### Return type

[**models::Quota**](Quota.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## remove_role_handler

> remove_role_handler(role)
Remove a role. Requires admin privilige.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**role** | **uuid::Uuid** | Role id | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## revoke_role_handler

> revoke_role_handler(user, role)
Revoke a role from a user. Requires admin privilige.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user** | **uuid::Uuid** | User id | [required] |
**role** | **uuid::Uuid** | Role id | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## update_user_quota_handler

> update_user_quota_handler(user, update_quota)
Update the available quota of a specific user.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user** | **uuid::Uuid** | User id | [required] |
**update_quota** | [**UpdateQuota**](UpdateQuota.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

