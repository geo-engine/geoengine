# SessionApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**anonymousHandler**](SessionApi.md#anonymoushandler) | **POST** /anonymous | Creates session for anonymous user. The session\&#39;s id serves as a Bearer token for requests. |
| [**loginHandler**](SessionApi.md#loginhandler) | **POST** /login | Creates a session by providing user credentials. The session\&#39;s id serves as a Bearer token for requests. |
| [**logoutHandler**](SessionApi.md#logouthandler) | **POST** /logout | Ends a session. |
| [**oidcInit**](SessionApi.md#oidcinit) | **POST** /oidcInit | Initializes the Open Id Connect login procedure by requesting a parametrized url to the configured Id Provider. |
| [**oidcLogin**](SessionApi.md#oidclogin) | **POST** /oidcLogin | Creates a session for a user via a login with Open Id Connect. This call must be preceded by a call to oidcInit and match the parameters of that call. |
| [**registerUserHandler**](SessionApi.md#registeruserhandler) | **POST** /user | Registers a user. |
| [**sessionHandler**](SessionApi.md#sessionhandler) | **GET** /session | Retrieves details about the current session. |
| [**sessionProjectHandler**](SessionApi.md#sessionprojecthandler) | **POST** /session/project/{project} | Sets the active project of the session. |
| [**sessionViewHandler**](SessionApi.md#sessionviewhandler) | **POST** /session/view |  |



## anonymousHandler

> UserSession anonymousHandler()

Creates session for anonymous user. The session\&#39;s id serves as a Bearer token for requests.

### Example

```ts
import {
  Configuration,
  SessionApi,
} from '@geoengine/api-client';
import type { AnonymousHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const api = new SessionApi();

  try {
    const data = await api.anonymousHandler();
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters

This endpoint does not need any parameter.

### Return type

[**UserSession**](UserSession.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The created session |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## loginHandler

> UserSession loginHandler(userCredentials)

Creates a session by providing user credentials. The session\&#39;s id serves as a Bearer token for requests.

### Example

```ts
import {
  Configuration,
  SessionApi,
} from '@geoengine/api-client';
import type { LoginHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const api = new SessionApi();

  const body = {
    // UserCredentials
    userCredentials: ...,
  } satisfies LoginHandlerRequest;

  try {
    const data = await api.loginHandler(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **userCredentials** | [UserCredentials](UserCredentials.md) |  | |

### Return type

[**UserSession**](UserSession.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The created session |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## logoutHandler

> logoutHandler()

Ends a session.

### Example

```ts
import {
  Configuration,
  SessionApi,
} from '@geoengine/api-client';
import type { LogoutHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new SessionApi(config);

  try {
    const data = await api.logoutHandler();
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters

This endpoint does not need any parameter.

### Return type

`void` (Empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The Session was deleted. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## oidcInit

> AuthCodeRequestURL oidcInit(redirectUri)

Initializes the Open Id Connect login procedure by requesting a parametrized url to the configured Id Provider.

# Errors  This call fails if Open ID Connect is disabled, misconfigured or the Id Provider is unreachable.  

### Example

```ts
import {
  Configuration,
  SessionApi,
} from '@geoengine/api-client';
import type { OidcInitRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const api = new SessionApi();

  const body = {
    // string
    redirectUri: redirectUri_example,
  } satisfies OidcInitRequest;

  try {
    const data = await api.oidcInit(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **redirectUri** | `string` |  | [Defaults to `undefined`] |

### Return type

[**AuthCodeRequestURL**](AuthCodeRequestURL.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** |  |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## oidcLogin

> UserSession oidcLogin(redirectUri, authCodeResponse)

Creates a session for a user via a login with Open Id Connect. This call must be preceded by a call to oidcInit and match the parameters of that call.

# Errors  This call fails if the [&#x60;AuthCodeResponse&#x60;] is invalid, if a previous oidcLogin call with the same state was already successfully or unsuccessfully resolved, if the Open Id Connect configuration is invalid, or if the Id Provider is unreachable.  

### Example

```ts
import {
  Configuration,
  SessionApi,
} from '@geoengine/api-client';
import type { OidcLoginRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const api = new SessionApi();

  const body = {
    // string
    redirectUri: redirectUri_example,
    // AuthCodeResponse
    authCodeResponse: ...,
  } satisfies OidcLoginRequest;

  try {
    const data = await api.oidcLogin(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **redirectUri** | `string` |  | [Defaults to `undefined`] |
| **authCodeResponse** | [AuthCodeResponse](AuthCodeResponse.md) |  | |

### Return type

[**UserSession**](UserSession.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** |  |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## registerUserHandler

> string registerUserHandler(userRegistration)

Registers a user.

### Example

```ts
import {
  Configuration,
  SessionApi,
} from '@geoengine/api-client';
import type { RegisterUserHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const api = new SessionApi();

  const body = {
    // UserRegistration
    userRegistration: ...,
  } satisfies RegisterUserHandlerRequest;

  try {
    const data = await api.registerUserHandler(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **userRegistration** | [UserRegistration](UserRegistration.md) |  | |

### Return type

**string**

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The id of the created user |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## sessionHandler

> UserSession sessionHandler()

Retrieves details about the current session.

### Example

```ts
import {
  Configuration,
  SessionApi,
} from '@geoengine/api-client';
import type { SessionHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new SessionApi(config);

  try {
    const data = await api.sessionHandler();
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters

This endpoint does not need any parameter.

### Return type

[**UserSession**](UserSession.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The current session |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## sessionProjectHandler

> sessionProjectHandler(project)

Sets the active project of the session.

### Example

```ts
import {
  Configuration,
  SessionApi,
} from '@geoengine/api-client';
import type { SessionProjectHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new SessionApi(config);

  const body = {
    // string | Project id
    project: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies SessionProjectHandlerRequest;

  try {
    const data = await api.sessionProjectHandler(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **project** | `string` | Project id | [Defaults to `undefined`] |

### Return type

`void` (Empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The project of the session was updated. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## sessionViewHandler

> sessionViewHandler(sTRectangle)



### Example

```ts
import {
  Configuration,
  SessionApi,
} from '@geoengine/api-client';
import type { SessionViewHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new SessionApi(config);

  const body = {
    // STRectangle
    sTRectangle: ...,
  } satisfies SessionViewHandlerRequest;

  try {
    const data = await api.sessionViewHandler(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **sTRectangle** | [STRectangle](STRectangle.md) |  | |

### Return type

`void` (Empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The view of the session was updated. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)

