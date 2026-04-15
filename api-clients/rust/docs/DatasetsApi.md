# \DatasetsApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_dataset_tiles_handler**](DatasetsApi.md#add_dataset_tiles_handler) | **POST** /dataset/{dataset}/tiles | Add a tile to a gdal dataset.
[**auto_create_dataset_handler**](DatasetsApi.md#auto_create_dataset_handler) | **POST** /dataset/auto | Creates a new dataset using previously uploaded files. The format of the files will be automatically detected when possible.
[**create_dataset_handler**](DatasetsApi.md#create_dataset_handler) | **POST** /dataset | Creates a new dataset referencing files. Users can reference previously uploaded files. Admins can reference files from a volume.
[**delete_dataset_handler**](DatasetsApi.md#delete_dataset_handler) | **DELETE** /dataset/{dataset} | Delete a dataset
[**get_dataset_handler**](DatasetsApi.md#get_dataset_handler) | **GET** /dataset/{dataset} | Retrieves details about a dataset using the internal name.
[**get_loading_info_handler**](DatasetsApi.md#get_loading_info_handler) | **GET** /dataset/{dataset}/loadingInfo | Retrieves the loading information of a dataset
[**list_datasets_handler**](DatasetsApi.md#list_datasets_handler) | **GET** /datasets | Lists available datasets.
[**list_volume_file_layers_handler**](DatasetsApi.md#list_volume_file_layers_handler) | **GET** /dataset/volumes/{volume_name}/files/{file_name}/layers | List the layers of a file in a volume.
[**list_volumes_handler**](DatasetsApi.md#list_volumes_handler) | **GET** /dataset/volumes | Lists available volumes.
[**suggest_meta_data_handler**](DatasetsApi.md#suggest_meta_data_handler) | **POST** /dataset/suggest | Inspects an upload and suggests metadata that can be used when creating a new dataset based on it. Tries to automatically detect the main file and layer name if not specified.
[**update_dataset_handler**](DatasetsApi.md#update_dataset_handler) | **POST** /dataset/{dataset} | Update details about a dataset using the internal name.
[**update_dataset_provenance_handler**](DatasetsApi.md#update_dataset_provenance_handler) | **PUT** /dataset/{dataset}/provenance | 
[**update_dataset_symbology_handler**](DatasetsApi.md#update_dataset_symbology_handler) | **PUT** /dataset/{dataset}/symbology | Updates the dataset's symbology
[**update_loading_info_handler**](DatasetsApi.md#update_loading_info_handler) | **PUT** /dataset/{dataset}/loadingInfo | Updates the dataset's loading info



## add_dataset_tiles_handler

> add_dataset_tiles_handler(dataset, auto_create_dataset)
Add a tile to a gdal dataset.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**dataset** | **String** | Dataset Name | [required] |
**auto_create_dataset** | [**AutoCreateDataset**](AutoCreateDataset.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## auto_create_dataset_handler

> models::DatasetNameResponse auto_create_dataset_handler(auto_create_dataset)
Creates a new dataset using previously uploaded files. The format of the files will be automatically detected when possible.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**auto_create_dataset** | [**AutoCreateDataset**](AutoCreateDataset.md) |  | [required] |

### Return type

[**models::DatasetNameResponse**](DatasetNameResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## create_dataset_handler

> models::DatasetNameResponse create_dataset_handler(create_dataset)
Creates a new dataset referencing files. Users can reference previously uploaded files. Admins can reference files from a volume.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**create_dataset** | [**CreateDataset**](CreateDataset.md) |  | [required] |

### Return type

[**models::DatasetNameResponse**](DatasetNameResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## delete_dataset_handler

> delete_dataset_handler(dataset)
Delete a dataset

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**dataset** | **String** | Dataset id | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_dataset_handler

> models::Dataset get_dataset_handler(dataset)
Retrieves details about a dataset using the internal name.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**dataset** | **String** | Dataset Name | [required] |

### Return type

[**models::Dataset**](Dataset.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_loading_info_handler

> models::MetaDataDefinition get_loading_info_handler(dataset)
Retrieves the loading information of a dataset

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**dataset** | **String** | Dataset Name | [required] |

### Return type

[**models::MetaDataDefinition**](MetaDataDefinition.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_datasets_handler

> Vec<models::DatasetListing> list_datasets_handler(order, offset, limit, filter, tags)
Lists available datasets.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**order** | [**OrderBy**](OrderBy.md) |  | [required] |
**offset** | **i32** |  | [required] |
**limit** | **i32** |  | [required] |
**filter** | Option<**String**> |  |  |
**tags** | Option<[**Vec<String>**](String.md)> |  |  |

### Return type

[**Vec<models::DatasetListing>**](DatasetListing.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_volume_file_layers_handler

> models::VolumeFileLayersResponse list_volume_file_layers_handler(volume_name, file_name)
List the layers of a file in a volume.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**volume_name** | **String** | Volume name | [required] |
**file_name** | **String** | File name | [required] |

### Return type

[**models::VolumeFileLayersResponse**](VolumeFileLayersResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_volumes_handler

> Vec<models::Volume> list_volumes_handler()
Lists available volumes.

### Parameters

This endpoint does not need any parameter.

### Return type

[**Vec<models::Volume>**](Volume.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## suggest_meta_data_handler

> models::MetaDataSuggestion suggest_meta_data_handler(suggest_meta_data)
Inspects an upload and suggests metadata that can be used when creating a new dataset based on it. Tries to automatically detect the main file and layer name if not specified.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**suggest_meta_data** | [**SuggestMetaData**](SuggestMetaData.md) |  | [required] |

### Return type

[**models::MetaDataSuggestion**](MetaDataSuggestion.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## update_dataset_handler

> update_dataset_handler(dataset, update_dataset)
Update details about a dataset using the internal name.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**dataset** | **String** | Dataset Name | [required] |
**update_dataset** | [**UpdateDataset**](UpdateDataset.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## update_dataset_provenance_handler

> update_dataset_provenance_handler(dataset, provenances)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**dataset** | **String** | Dataset Name | [required] |
**provenances** | [**Provenances**](Provenances.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## update_dataset_symbology_handler

> update_dataset_symbology_handler(dataset, symbology)
Updates the dataset's symbology

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**dataset** | **String** | Dataset Name | [required] |
**symbology** | [**Symbology**](Symbology.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## update_loading_info_handler

> update_loading_info_handler(dataset, meta_data_definition)
Updates the dataset's loading info

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**dataset** | **String** | Dataset Name | [required] |
**meta_data_definition** | [**MetaDataDefinition**](MetaDataDefinition.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

