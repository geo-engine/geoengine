# \SessionApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**anonymous_handler**](SessionApi.md#anonymous_handler) | **POST** /anonymous | Creates session for anonymous user. The session's id serves as a Bearer token for requests.
[**login_handler**](SessionApi.md#login_handler) | **POST** /login | Creates a session by providing user credentials. The session's id serves as a Bearer token for requests.
[**logout_handler**](SessionApi.md#logout_handler) | **POST** /logout | Ends a session.
[**oidc_init**](SessionApi.md#oidc_init) | **POST** /oidcInit | Initializes the Open Id Connect login procedure by requesting a parametrized url to the configured Id Provider.
[**oidc_login**](SessionApi.md#oidc_login) | **POST** /oidcLogin | Creates a session for a user via a login with Open Id Connect. This call must be preceded by a call to oidcInit and match the parameters of that call.
[**register_user_handler**](SessionApi.md#register_user_handler) | **POST** /user | Registers a user.
[**session_handler**](SessionApi.md#session_handler) | **GET** /session | Retrieves details about the current session.
[**session_project_handler**](SessionApi.md#session_project_handler) | **POST** /session/project/{project} | Sets the active project of the session.
[**session_view_handler**](SessionApi.md#session_view_handler) | **POST** /session/view | 



## anonymous_handler

> models::UserSession anonymous_handler()
Creates session for anonymous user. The session's id serves as a Bearer token for requests.

### Parameters

This endpoint does not need any parameter.

### Return type

[**models::UserSession**](UserSession.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## login_handler

> models::UserSession login_handler(user_credentials)
Creates a session by providing user credentials. The session's id serves as a Bearer token for requests.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_credentials** | [**UserCredentials**](UserCredentials.md) |  | [required] |

### Return type

[**models::UserSession**](UserSession.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## logout_handler

> logout_handler()
Ends a session.

### Parameters

This endpoint does not need any parameter.

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## oidc_init

> models::AuthCodeRequestUrl oidc_init(redirect_uri)
Initializes the Open Id Connect login procedure by requesting a parametrized url to the configured Id Provider.

# Errors  This call fails if Open ID Connect is disabled, misconfigured or the Id Provider is unreachable.  

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**redirect_uri** | **String** |  | [required] |

### Return type

[**models::AuthCodeRequestUrl**](AuthCodeRequestURL.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## oidc_login

> models::UserSession oidc_login(redirect_uri, auth_code_response)
Creates a session for a user via a login with Open Id Connect. This call must be preceded by a call to oidcInit and match the parameters of that call.

# Errors  This call fails if the [`AuthCodeResponse`] is invalid, if a previous oidcLogin call with the same state was already successfully or unsuccessfully resolved, if the Open Id Connect configuration is invalid, or if the Id Provider is unreachable.  

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**redirect_uri** | **String** |  | [required] |
**auth_code_response** | [**AuthCodeResponse**](AuthCodeResponse.md) |  | [required] |

### Return type

[**models::UserSession**](UserSession.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## register_user_handler

> uuid::Uuid register_user_handler(user_registration)
Registers a user.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**user_registration** | [**UserRegistration**](UserRegistration.md) |  | [required] |

### Return type

[**uuid::Uuid**](uuid::Uuid.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## session_handler

> models::UserSession session_handler()
Retrieves details about the current session.

### Parameters

This endpoint does not need any parameter.

### Return type

[**models::UserSession**](UserSession.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## session_project_handler

> session_project_handler(project)
Sets the active project of the session.

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


## session_view_handler

> session_view_handler(st_rectangle)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**st_rectangle** | [**StRectangle**](StRectangle.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

