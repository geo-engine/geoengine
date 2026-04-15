# ProjectsApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createProjectHandler**](ProjectsApi.md#createprojecthandler) | **POST** /project | Create a new project for the user. |
| [**deleteProjectHandler**](ProjectsApi.md#deleteprojecthandler) | **DELETE** /project/{project} | Deletes a project. |
| [**listProjectsHandler**](ProjectsApi.md#listprojectshandler) | **GET** /projects | List all projects accessible to the user that match the selected criteria. |
| [**loadProjectLatestHandler**](ProjectsApi.md#loadprojectlatesthandler) | **GET** /project/{project} | Retrieves details about the latest version of a project. |
| [**loadProjectVersionHandler**](ProjectsApi.md#loadprojectversionhandler) | **GET** /project/{project}/{version} | Retrieves details about the given version of a project. |
| [**projectVersionsHandler**](ProjectsApi.md#projectversionshandler) | **GET** /project/{project}/versions | Lists all available versions of a project. |
| [**updateProjectHandler**](ProjectsApi.md#updateprojecthandler) | **PATCH** /project/{project} | Updates a project. This will create a new version. |



## createProjectHandler

> IdResponse createProjectHandler(createProject)

Create a new project for the user.

### Example

```ts
import {
  Configuration,
  ProjectsApi,
} from '@geoengine/api-client';
import type { CreateProjectHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new ProjectsApi(config);

  const body = {
    // CreateProject
    createProject: ...,
  } satisfies CreateProjectHandlerRequest;

  try {
    const data = await api.createProjectHandler(body);
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
| **createProject** | [CreateProject](CreateProject.md) |  | |

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


## deleteProjectHandler

> deleteProjectHandler(project)

Deletes a project.

### Example

```ts
import {
  Configuration,
  ProjectsApi,
} from '@geoengine/api-client';
import type { DeleteProjectHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new ProjectsApi(config);

  const body = {
    // string | Project id
    project: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies DeleteProjectHandlerRequest;

  try {
    const data = await api.deleteProjectHandler(body);
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
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## listProjectsHandler

> Array&lt;ProjectListing&gt; listProjectsHandler(order, offset, limit)

List all projects accessible to the user that match the selected criteria.

### Example

```ts
import {
  Configuration,
  ProjectsApi,
} from '@geoengine/api-client';
import type { ListProjectsHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new ProjectsApi(config);

  const body = {
    // OrderBy
    order: NameAsc,
    // number
    offset: 0,
    // number
    limit: 2,
  } satisfies ListProjectsHandlerRequest;

  try {
    const data = await api.listProjectsHandler(body);
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
| **order** | `OrderBy` |  | [Defaults to `undefined`] [Enum: NameAsc, NameDesc] |
| **offset** | `number` |  | [Defaults to `undefined`] |
| **limit** | `number` |  | [Defaults to `undefined`] |

### Return type

[**Array&lt;ProjectListing&gt;**](ProjectListing.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | List of projects the user can access |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## loadProjectLatestHandler

> Project loadProjectLatestHandler(project)

Retrieves details about the latest version of a project.

### Example

```ts
import {
  Configuration,
  ProjectsApi,
} from '@geoengine/api-client';
import type { LoadProjectLatestHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new ProjectsApi(config);

  const body = {
    // string | Project id
    project: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies LoadProjectLatestHandlerRequest;

  try {
    const data = await api.loadProjectLatestHandler(body);
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

[**Project**](Project.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Project loaded from database |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## loadProjectVersionHandler

> Project loadProjectVersionHandler(project, version)

Retrieves details about the given version of a project.

### Example

```ts
import {
  Configuration,
  ProjectsApi,
} from '@geoengine/api-client';
import type { LoadProjectVersionHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new ProjectsApi(config);

  const body = {
    // string | Project id
    project: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | Version id
    version: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies LoadProjectVersionHandlerRequest;

  try {
    const data = await api.loadProjectVersionHandler(body);
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
| **version** | `string` | Version id | [Defaults to `undefined`] |

### Return type

[**Project**](Project.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Project loaded from database |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## projectVersionsHandler

> Array&lt;ProjectVersion&gt; projectVersionsHandler(project)

Lists all available versions of a project.

### Example

```ts
import {
  Configuration,
  ProjectsApi,
} from '@geoengine/api-client';
import type { ProjectVersionsHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new ProjectsApi(config);

  const body = {
    // string | Project id
    project: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies ProjectVersionsHandlerRequest;

  try {
    const data = await api.projectVersionsHandler(body);
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

[**Array&lt;ProjectVersion&gt;**](ProjectVersion.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## updateProjectHandler

> updateProjectHandler(project, updateProject)

Updates a project. This will create a new version.

### Example

```ts
import {
  Configuration,
  ProjectsApi,
} from '@geoengine/api-client';
import type { UpdateProjectHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new ProjectsApi(config);

  const body = {
    // string | Project id
    project: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // UpdateProject
    updateProject: ...,
  } satisfies UpdateProjectHandlerRequest;

  try {
    const data = await api.updateProjectHandler(body);
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
| **updateProject** | [UpdateProject](UpdateProject.md) |  | |

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
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)

