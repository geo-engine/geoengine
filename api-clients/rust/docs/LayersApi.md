# \LayersApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_collection**](LayersApi.md#add_collection) | **POST** /layerDb/collections/{collection}/collections | Add a new collection to an existing collection
[**add_existing_collection_to_collection**](LayersApi.md#add_existing_collection_to_collection) | **POST** /layerDb/collections/{parent}/collections/{collection} | Add an existing collection to a collection
[**add_existing_layer_to_collection**](LayersApi.md#add_existing_layer_to_collection) | **POST** /layerDb/collections/{collection}/layers/{layer} | Add an existing layer to a collection
[**add_layer**](LayersApi.md#add_layer) | **POST** /layerDb/collections/{collection}/layers | Add a new layer to a collection
[**add_provider**](LayersApi.md#add_provider) | **POST** /layerDb/providers | Add a new provider
[**autocomplete_handler**](LayersApi.md#autocomplete_handler) | **GET** /layers/collections/search/autocomplete/{provider}/{collection} | Autocompletes the search on the contents of the collection of the given provider
[**delete_provider**](LayersApi.md#delete_provider) | **DELETE** /layerDb/providers/{provider} | Delete an existing provider
[**get_provider_definition**](LayersApi.md#get_provider_definition) | **GET** /layerDb/providers/{provider} | Get an existing provider's definition
[**layer_handler**](LayersApi.md#layer_handler) | **GET** /layers/{provider}/{layer} | Retrieves the layer of the given provider
[**layer_to_dataset**](LayersApi.md#layer_to_dataset) | **POST** /layers/{provider}/{layer}/dataset | Persist a raster layer from a provider as a dataset.
[**layer_to_workflow_id_handler**](LayersApi.md#layer_to_workflow_id_handler) | **POST** /layers/{provider}/{layer}/workflowId | Registers a layer from a provider as a workflow and returns the workflow id
[**list_collection_handler**](LayersApi.md#list_collection_handler) | **GET** /layers/collections/{provider}/{collection} | List the contents of the collection of the given provider
[**list_providers**](LayersApi.md#list_providers) | **GET** /layerDb/providers | List all providers
[**list_root_collections_handler**](LayersApi.md#list_root_collections_handler) | **GET** /layers/collections | List all layer collections
[**provider_capabilities_handler**](LayersApi.md#provider_capabilities_handler) | **GET** /layers/{provider}/capabilities | 
[**remove_collection**](LayersApi.md#remove_collection) | **DELETE** /layerDb/collections/{collection} | Remove a collection
[**remove_collection_from_collection**](LayersApi.md#remove_collection_from_collection) | **DELETE** /layerDb/collections/{parent}/collections/{collection} | Delete a collection from a collection
[**remove_layer**](LayersApi.md#remove_layer) | **DELETE** /layerDb/layers/{layer} | Remove a collection
[**remove_layer_from_collection**](LayersApi.md#remove_layer_from_collection) | **DELETE** /layerDb/collections/{collection}/layers/{layer} | Remove a layer from a collection
[**search_handler**](LayersApi.md#search_handler) | **GET** /layers/collections/search/{provider}/{collection} | Searches the contents of the collection of the given provider
[**update_collection**](LayersApi.md#update_collection) | **PUT** /layerDb/collections/{collection} | Update a collection
[**update_layer**](LayersApi.md#update_layer) | **PUT** /layerDb/layers/{layer} | Update a layer
[**update_provider_definition**](LayersApi.md#update_provider_definition) | **PUT** /layerDb/providers/{provider} | Update an existing provider's definition



## add_collection

> models::IdResponse add_collection(collection, add_layer_collection)
Add a new collection to an existing collection

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**collection** | **String** | Layer collection id | [required] |
**add_layer_collection** | [**AddLayerCollection**](AddLayerCollection.md) |  | [required] |

### Return type

[**models::IdResponse**](IdResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## add_existing_collection_to_collection

> add_existing_collection_to_collection(parent, collection)
Add an existing collection to a collection

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**parent** | **String** | Parent layer collection id | [required] |
**collection** | **String** | Layer collection id | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## add_existing_layer_to_collection

> add_existing_layer_to_collection(collection, layer)
Add an existing layer to a collection

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**collection** | **String** | Layer collection id | [required] |
**layer** | **String** | Layer id | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## add_layer

> models::IdResponse add_layer(collection, add_layer)
Add a new layer to a collection

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**collection** | **String** | Layer collection id | [required] |
**add_layer** | [**AddLayer**](AddLayer.md) |  | [required] |

### Return type

[**models::IdResponse**](IdResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## add_provider

> models::IdResponse add_provider(typed_data_provider_definition)
Add a new provider

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**typed_data_provider_definition** | [**TypedDataProviderDefinition**](TypedDataProviderDefinition.md) |  | [required] |

### Return type

[**models::IdResponse**](IdResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## autocomplete_handler

> Vec<String> autocomplete_handler(provider, collection, search_type, search_string, limit, offset)
Autocompletes the search on the contents of the collection of the given provider

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**provider** | **uuid::Uuid** | Data provider id | [required] |
**collection** | **String** | Layer collection id | [required] |
**search_type** | [**SearchType**](SearchType.md) |  | [required] |
**search_string** | **String** |  | [required] |
**limit** | **i32** |  | [required] |
**offset** | **i32** |  | [required] |

### Return type

**Vec<String>**

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## delete_provider

> delete_provider(provider)
Delete an existing provider

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**provider** | **uuid::Uuid** | Layer provider id | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_provider_definition

> models::TypedDataProviderDefinition get_provider_definition(provider)
Get an existing provider's definition

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**provider** | **uuid::Uuid** | Layer provider id | [required] |

### Return type

[**models::TypedDataProviderDefinition**](TypedDataProviderDefinition.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## layer_handler

> models::Layer layer_handler(provider, layer)
Retrieves the layer of the given provider

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**provider** | **uuid::Uuid** | Data provider id | [required] |
**layer** | **String** | Layer id | [required] |

### Return type

[**models::Layer**](Layer.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## layer_to_dataset

> models::TaskResponse layer_to_dataset(provider, layer)
Persist a raster layer from a provider as a dataset.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**provider** | **uuid::Uuid** | Data provider id | [required] |
**layer** | **String** | Layer id | [required] |

### Return type

[**models::TaskResponse**](TaskResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## layer_to_workflow_id_handler

> models::IdResponse layer_to_workflow_id_handler(provider, layer)
Registers a layer from a provider as a workflow and returns the workflow id

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**provider** | **uuid::Uuid** | Data provider id | [required] |
**layer** | **String** | Layer id | [required] |

### Return type

[**models::IdResponse**](IdResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_collection_handler

> models::LayerCollection list_collection_handler(provider, collection, offset, limit)
List the contents of the collection of the given provider

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**provider** | **uuid::Uuid** | Data provider id | [required] |
**collection** | **String** | Layer collection id | [required] |
**offset** | **i32** |  | [required] |
**limit** | **i32** |  | [required] |

### Return type

[**models::LayerCollection**](LayerCollection.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_providers

> Vec<models::LayerProviderListing> list_providers(offset, limit)
List all providers

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**offset** | **i32** |  | [required] |
**limit** | **i32** |  | [required] |

### Return type

[**Vec<models::LayerProviderListing>**](LayerProviderListing.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_root_collections_handler

> models::LayerCollection list_root_collections_handler(offset, limit)
List all layer collections

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**offset** | **i32** |  | [required] |
**limit** | **i32** |  | [required] |

### Return type

[**models::LayerCollection**](LayerCollection.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## provider_capabilities_handler

> models::ProviderCapabilities provider_capabilities_handler(provider)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**provider** | **uuid::Uuid** | Data provider id | [required] |

### Return type

[**models::ProviderCapabilities**](ProviderCapabilities.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## remove_collection

> remove_collection(collection)
Remove a collection

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**collection** | **String** | Layer collection id | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## remove_collection_from_collection

> remove_collection_from_collection(parent, collection)
Delete a collection from a collection

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**parent** | **String** | Parent layer collection id | [required] |
**collection** | **String** | Layer collection id | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## remove_layer

> remove_layer(layer)
Remove a collection

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**layer** | **String** | Layer id | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## remove_layer_from_collection

> remove_layer_from_collection(collection, layer)
Remove a layer from a collection

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**collection** | **String** | Layer collection id | [required] |
**layer** | **String** | Layer id | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## search_handler

> models::LayerCollection search_handler(provider, collection, search_type, search_string, limit, offset)
Searches the contents of the collection of the given provider

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**provider** | **uuid::Uuid** | Data provider id | [required] |
**collection** | **String** | Layer collection id | [required] |
**search_type** | [**SearchType**](SearchType.md) |  | [required] |
**search_string** | **String** |  | [required] |
**limit** | **i32** |  | [required] |
**offset** | **i32** |  | [required] |

### Return type

[**models::LayerCollection**](LayerCollection.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## update_collection

> update_collection(collection, update_layer_collection)
Update a collection

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**collection** | **String** | Layer collection id | [required] |
**update_layer_collection** | [**UpdateLayerCollection**](UpdateLayerCollection.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## update_layer

> update_layer(layer, update_layer)
Update a layer

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**layer** | **String** | Layer id | [required] |
**update_layer** | [**UpdateLayer**](UpdateLayer.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## update_provider_definition

> update_provider_definition(provider, typed_data_provider_definition)
Update an existing provider's definition

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**provider** | **uuid::Uuid** | Layer provider id | [required] |
**typed_data_provider_definition** | [**TypedDataProviderDefinition**](TypedDataProviderDefinition.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

