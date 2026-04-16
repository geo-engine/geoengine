# UserApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**addRoleHandler**](UserApi.md#addrolehandler) | **PUT** /roles | Add a new role. Requires admin privilige. |
| [**assignRoleHandler**](UserApi.md#assignrolehandler) | **POST** /users/{user}/roles/{role} | Assign a role to a user. Requires admin privilige. |
| [**computationQuotaHandler**](UserApi.md#computationquotahandler) | **GET** /quota/computations/{computation} | Retrieves the quota used by computation with the given computation id |
| [**computationsQuotaHandler**](UserApi.md#computationsquotahandler) | **GET** /quota/computations | Retrieves the quota used by computations |
| [**dataUsageHandler**](UserApi.md#datausagehandler) | **GET** /quota/dataUsage | Retrieves the data usage |
| [**dataUsageSummaryHandler**](UserApi.md#datausagesummaryhandler) | **GET** /quota/dataUsage/summary | Retrieves the data usage summary |
| [**getRoleByNameHandler**](UserApi.md#getrolebynamehandler) | **GET** /roles/byName/{name} | Get role by name |
| [**getRoleDescriptions**](UserApi.md#getroledescriptions) | **GET** /user/roles/descriptions | Query roles for the current user. |
| [**getUserQuotaHandler**](UserApi.md#getuserquotahandler) | **GET** /quotas/{user} | Retrieves the available and used quota of a specific user. |
| [**quotaHandler**](UserApi.md#quotahandler) | **GET** /quota | Retrieves the available and used quota of the current user. |
| [**removeRoleHandler**](UserApi.md#removerolehandler) | **DELETE** /roles/{role} | Remove a role. Requires admin privilige. |
| [**revokeRoleHandler**](UserApi.md#revokerolehandler) | **DELETE** /users/{user}/roles/{role} | Revoke a role from a user. Requires admin privilige. |
| [**updateUserQuotaHandler**](UserApi.md#updateuserquotahandler) | **POST** /quotas/{user} | Update the available quota of a specific user. |



## addRoleHandler

> IdResponse addRoleHandler(addRole)

Add a new role. Requires admin privilige.

### Example

```ts
import {
  Configuration,
  UserApi,
} from '@geoengine/api-client';
import type { AddRoleHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UserApi(config);

  const body = {
    // AddRole
    addRole: ...,
  } satisfies AddRoleHandlerRequest;

  try {
    const data = await api.addRoleHandler(body);
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
| **addRole** | [AddRole](AddRole.md) |  | |

### Return type

[**IdResponse**](IdResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Id of generated resource |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## assignRoleHandler

> assignRoleHandler(user, role)

Assign a role to a user. Requires admin privilige.

### Example

```ts
import {
  Configuration,
  UserApi,
} from '@geoengine/api-client';
import type { AssignRoleHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UserApi(config);

  const body = {
    // string | User id
    user: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | Role id
    role: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies AssignRoleHandlerRequest;

  try {
    const data = await api.assignRoleHandler(body);
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
| **user** | `string` | User id | [Defaults to `undefined`] |
| **role** | `string` | Role id | [Defaults to `undefined`] |

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
| **200** | Role was assigned |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## computationQuotaHandler

> Array&lt;OperatorQuota&gt; computationQuotaHandler(computation)

Retrieves the quota used by computation with the given computation id

### Example

```ts
import {
  Configuration,
  UserApi,
} from '@geoengine/api-client';
import type { ComputationQuotaHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UserApi(config);

  const body = {
    // string | Computation id
    computation: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies ComputationQuotaHandlerRequest;

  try {
    const data = await api.computationQuotaHandler(body);
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
| **computation** | `string` | Computation id | [Defaults to `undefined`] |

### Return type

[**Array&lt;OperatorQuota&gt;**](OperatorQuota.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The quota used by computation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## computationsQuotaHandler

> Array&lt;ComputationQuota&gt; computationsQuotaHandler(offset, limit)

Retrieves the quota used by computations

### Example

```ts
import {
  Configuration,
  UserApi,
} from '@geoengine/api-client';
import type { ComputationsQuotaHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UserApi(config);

  const body = {
    // number
    offset: 56,
    // number
    limit: 56,
  } satisfies ComputationsQuotaHandlerRequest;

  try {
    const data = await api.computationsQuotaHandler(body);
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
| **offset** | `number` |  | [Defaults to `undefined`] |
| **limit** | `number` |  | [Defaults to `undefined`] |

### Return type

[**Array&lt;ComputationQuota&gt;**](ComputationQuota.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The quota used by computations |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## dataUsageHandler

> Array&lt;DataUsage&gt; dataUsageHandler(offset, limit)

Retrieves the data usage

### Example

```ts
import {
  Configuration,
  UserApi,
} from '@geoengine/api-client';
import type { DataUsageHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UserApi(config);

  const body = {
    // number
    offset: 789,
    // number
    limit: 789,
  } satisfies DataUsageHandlerRequest;

  try {
    const data = await api.dataUsageHandler(body);
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
| **offset** | `number` |  | [Defaults to `undefined`] |
| **limit** | `number` |  | [Defaults to `undefined`] |

### Return type

[**Array&lt;DataUsage&gt;**](DataUsage.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The quota used on data |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## dataUsageSummaryHandler

> Array&lt;DataUsageSummary&gt; dataUsageSummaryHandler(granularity, offset, limit, dataset)

Retrieves the data usage summary

### Example

```ts
import {
  Configuration,
  UserApi,
} from '@geoengine/api-client';
import type { DataUsageSummaryHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UserApi(config);

  const body = {
    // UsageSummaryGranularity
    granularity: ...,
    // number
    offset: 789,
    // number
    limit: 789,
    // string (optional)
    dataset: dataset_example,
  } satisfies DataUsageSummaryHandlerRequest;

  try {
    const data = await api.dataUsageSummaryHandler(body);
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
| **granularity** | `UsageSummaryGranularity` |  | [Defaults to `undefined`] [Enum: minutes, hours, days, months, years] |
| **offset** | `number` |  | [Defaults to `undefined`] |
| **limit** | `number` |  | [Defaults to `undefined`] |
| **dataset** | `string` |  | [Optional] [Defaults to `undefined`] |

### Return type

[**Array&lt;DataUsageSummary&gt;**](DataUsageSummary.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The quota used on data |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## getRoleByNameHandler

> IdResponse getRoleByNameHandler(name)

Get role by name

### Example

```ts
import {
  Configuration,
  UserApi,
} from '@geoengine/api-client';
import type { GetRoleByNameHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UserApi(config);

  const body = {
    // string | Role Name
    name: name_example,
  } satisfies GetRoleByNameHandlerRequest;

  try {
    const data = await api.getRoleByNameHandler(body);
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
| **name** | `string` | Role Name | [Defaults to `undefined`] |

### Return type

[**IdResponse**](IdResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Id of generated resource |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## getRoleDescriptions

> Array&lt;RoleDescription&gt; getRoleDescriptions()

Query roles for the current user.

### Example

```ts
import {
  Configuration,
  UserApi,
} from '@geoengine/api-client';
import type { GetRoleDescriptionsRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UserApi(config);

  try {
    const data = await api.getRoleDescriptions();
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

[**Array&lt;RoleDescription&gt;**](RoleDescription.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The description for roles of the current user |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## getUserQuotaHandler

> Quota getUserQuotaHandler(user)

Retrieves the available and used quota of a specific user.

### Example

```ts
import {
  Configuration,
  UserApi,
} from '@geoengine/api-client';
import type { GetUserQuotaHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UserApi(config);

  const body = {
    // string | User id
    user: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies GetUserQuotaHandlerRequest;

  try {
    const data = await api.getUserQuotaHandler(body);
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
| **user** | `string` | User id | [Defaults to `undefined`] |

### Return type

[**Quota**](Quota.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The available and used quota of the user |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## quotaHandler

> Quota quotaHandler()

Retrieves the available and used quota of the current user.

### Example

```ts
import {
  Configuration,
  UserApi,
} from '@geoengine/api-client';
import type { QuotaHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UserApi(config);

  try {
    const data = await api.quotaHandler();
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

[**Quota**](Quota.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The available and used quota of the user |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## removeRoleHandler

> removeRoleHandler(role)

Remove a role. Requires admin privilige.

### Example

```ts
import {
  Configuration,
  UserApi,
} from '@geoengine/api-client';
import type { RemoveRoleHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UserApi(config);

  const body = {
    // string | Role id
    role: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies RemoveRoleHandlerRequest;

  try {
    const data = await api.removeRoleHandler(body);
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
| **role** | `string` | Role id | [Defaults to `undefined`] |

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
| **200** | Role was removed |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## revokeRoleHandler

> revokeRoleHandler(user, role)

Revoke a role from a user. Requires admin privilige.

### Example

```ts
import {
  Configuration,
  UserApi,
} from '@geoengine/api-client';
import type { RevokeRoleHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UserApi(config);

  const body = {
    // string | User id
    user: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | Role id
    role: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies RevokeRoleHandlerRequest;

  try {
    const data = await api.revokeRoleHandler(body);
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
| **user** | `string` | User id | [Defaults to `undefined`] |
| **role** | `string` | Role id | [Defaults to `undefined`] |

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
| **200** | Role was revoked |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## updateUserQuotaHandler

> updateUserQuotaHandler(user, updateQuota)

Update the available quota of a specific user.

### Example

```ts
import {
  Configuration,
  UserApi,
} from '@geoengine/api-client';
import type { UpdateUserQuotaHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UserApi(config);

  const body = {
    // string | User id
    user: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // UpdateQuota
    updateQuota: ...,
  } satisfies UpdateUserQuotaHandlerRequest;

  try {
    const data = await api.updateUserQuotaHandler(body);
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
| **user** | `string` | User id | [Defaults to `undefined`] |
| **updateQuota** | [UpdateQuota](UpdateQuota.md) |  | |

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
| **200** | Quota was updated |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)

