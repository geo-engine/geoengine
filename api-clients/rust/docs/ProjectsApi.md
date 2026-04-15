# \ProjectsApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_project_handler**](ProjectsApi.md#create_project_handler) | **POST** /project | Create a new project for the user.
[**delete_project_handler**](ProjectsApi.md#delete_project_handler) | **DELETE** /project/{project} | Deletes a project.
[**list_projects_handler**](ProjectsApi.md#list_projects_handler) | **GET** /projects | List all projects accessible to the user that match the selected criteria.
[**load_project_latest_handler**](ProjectsApi.md#load_project_latest_handler) | **GET** /project/{project} | Retrieves details about the latest version of a project.
[**load_project_version_handler**](ProjectsApi.md#load_project_version_handler) | **GET** /project/{project}/{version} | Retrieves details about the given version of a project.
[**project_versions_handler**](ProjectsApi.md#project_versions_handler) | **GET** /project/{project}/versions | Lists all available versions of a project.
[**update_project_handler**](ProjectsApi.md#update_project_handler) | **PATCH** /project/{project} | Updates a project. This will create a new version.



## create_project_handler

> models::IdResponse create_project_handler(create_project)
Create a new project for the user.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**create_project** | [**CreateProject**](CreateProject.md) |  | [required] |

### Return type

[**models::IdResponse**](IdResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## delete_project_handler

> delete_project_handler(project)
Deletes a project.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**project** | **uuid::Uuid** | Project id | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_projects_handler

> Vec<models::ProjectListing> list_projects_handler(order, offset, limit)
List all projects accessible to the user that match the selected criteria.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**order** | [**OrderBy**](OrderBy.md) |  | [required] |
**offset** | **i32** |  | [required] |
**limit** | **i32** |  | [required] |

### Return type

[**Vec<models::ProjectListing>**](ProjectListing.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## load_project_latest_handler

> models::Project load_project_latest_handler(project)
Retrieves details about the latest version of a project.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**project** | **uuid::Uuid** | Project id | [required] |

### Return type

[**models::Project**](Project.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## load_project_version_handler

> models::Project load_project_version_handler(project, version)
Retrieves details about the given version of a project.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**project** | **uuid::Uuid** | Project id | [required] |
**version** | **uuid::Uuid** | Version id | [required] |

### Return type

[**models::Project**](Project.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## project_versions_handler

> Vec<models::ProjectVersion> project_versions_handler(project)
Lists all available versions of a project.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**project** | **uuid::Uuid** | Project id | [required] |

### Return type

[**Vec<models::ProjectVersion>**](ProjectVersion.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## update_project_handler

> update_project_handler(project, update_project)
Updates a project. This will create a new version.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**project** | **uuid::Uuid** | Project id | [required] |
**update_project** | [**UpdateProject**](UpdateProject.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

