# @geoengine/api-client@0.9.2

A TypeScript SDK client for the geoengine.io API.

## Usage

First, install the SDK from npm.

```bash
npm install @geoengine/api-client --save
```

Next, try it out.


```ts
import {
  Configuration,
  DatasetsApi,
} from '@geoengine/api-client';
import type { AddDatasetTilesHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new DatasetsApi(config);

  const body = {
    // string | Dataset Name
    dataset: dataset_example,
    // AutoCreateDataset
    autoCreateDataset: ...,
  } satisfies AddDatasetTilesHandlerRequest;

  try {
    const data = await api.addDatasetTilesHandler(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```


## Documentation

### API Endpoints

All URIs are relative to *https://geoengine.io/api*

| Class | Method | HTTP request | Description
| ----- | ------ | ------------ | -------------
*DatasetsApi* | [**addDatasetTilesHandler**](docs/DatasetsApi.md#adddatasettileshandler) | **POST** /dataset/{dataset}/tiles | Add a tile to a gdal dataset.
*DatasetsApi* | [**autoCreateDatasetHandler**](docs/DatasetsApi.md#autocreatedatasethandler) | **POST** /dataset/auto | Creates a new dataset using previously uploaded files. The format of the files will be automatically detected when possible.
*DatasetsApi* | [**createDatasetHandler**](docs/DatasetsApi.md#createdatasethandler) | **POST** /dataset | Creates a new dataset referencing files. Users can reference previously uploaded files. Admins can reference files from a volume.
*DatasetsApi* | [**deleteDatasetHandler**](docs/DatasetsApi.md#deletedatasethandler) | **DELETE** /dataset/{dataset} | Delete a dataset
*DatasetsApi* | [**getDatasetHandler**](docs/DatasetsApi.md#getdatasethandler) | **GET** /dataset/{dataset} | Retrieves details about a dataset using the internal name.
*DatasetsApi* | [**getLoadingInfoHandler**](docs/DatasetsApi.md#getloadinginfohandler) | **GET** /dataset/{dataset}/loadingInfo | Retrieves the loading information of a dataset
*DatasetsApi* | [**listDatasetsHandler**](docs/DatasetsApi.md#listdatasetshandler) | **GET** /datasets | Lists available datasets.
*DatasetsApi* | [**listVolumeFileLayersHandler**](docs/DatasetsApi.md#listvolumefilelayershandler) | **GET** /dataset/volumes/{volume_name}/files/{file_name}/layers | List the layers of a file in a volume.
*DatasetsApi* | [**listVolumesHandler**](docs/DatasetsApi.md#listvolumeshandler) | **GET** /dataset/volumes | Lists available volumes.
*DatasetsApi* | [**suggestMetaDataHandler**](docs/DatasetsApi.md#suggestmetadatahandler) | **POST** /dataset/suggest | Inspects an upload and suggests metadata that can be used when creating a new dataset based on it. Tries to automatically detect the main file and layer name if not specified.
*DatasetsApi* | [**updateDatasetHandler**](docs/DatasetsApi.md#updatedatasethandler) | **POST** /dataset/{dataset} | Update details about a dataset using the internal name.
*DatasetsApi* | [**updateDatasetProvenanceHandler**](docs/DatasetsApi.md#updatedatasetprovenancehandler) | **PUT** /dataset/{dataset}/provenance | 
*DatasetsApi* | [**updateDatasetSymbologyHandler**](docs/DatasetsApi.md#updatedatasetsymbologyhandler) | **PUT** /dataset/{dataset}/symbology | Updates the dataset\&#39;s symbology
*DatasetsApi* | [**updateLoadingInfoHandler**](docs/DatasetsApi.md#updateloadinginfohandler) | **PUT** /dataset/{dataset}/loadingInfo | Updates the dataset\&#39;s loading info
*GeneralApi* | [**availableHandler**](docs/GeneralApi.md#availablehandler) | **GET** /available | Server availablity check.
*GeneralApi* | [**serverInfoHandler**](docs/GeneralApi.md#serverinfohandler) | **GET** /info | Shows information about the server software version.
*LayersApi* | [**addCollection**](docs/LayersApi.md#addcollection) | **POST** /layerDb/collections/{collection}/collections | Add a new collection to an existing collection
*LayersApi* | [**addExistingCollectionToCollection**](docs/LayersApi.md#addexistingcollectiontocollection) | **POST** /layerDb/collections/{parent}/collections/{collection} | Add an existing collection to a collection
*LayersApi* | [**addExistingLayerToCollection**](docs/LayersApi.md#addexistinglayertocollection) | **POST** /layerDb/collections/{collection}/layers/{layer} | Add an existing layer to a collection
*LayersApi* | [**addLayer**](docs/LayersApi.md#addlayer) | **POST** /layerDb/collections/{collection}/layers | Add a new layer to a collection
*LayersApi* | [**addProvider**](docs/LayersApi.md#addprovider) | **POST** /layerDb/providers | Add a new provider
*LayersApi* | [**autocompleteHandler**](docs/LayersApi.md#autocompletehandler) | **GET** /layers/collections/search/autocomplete/{provider}/{collection} | Autocompletes the search on the contents of the collection of the given provider
*LayersApi* | [**deleteProvider**](docs/LayersApi.md#deleteprovider) | **DELETE** /layerDb/providers/{provider} | Delete an existing provider
*LayersApi* | [**getProviderDefinition**](docs/LayersApi.md#getproviderdefinition) | **GET** /layerDb/providers/{provider} | Get an existing provider\&#39;s definition
*LayersApi* | [**layerHandler**](docs/LayersApi.md#layerhandler) | **GET** /layers/{provider}/{layer} | Retrieves the layer of the given provider
*LayersApi* | [**layerToDataset**](docs/LayersApi.md#layertodataset) | **POST** /layers/{provider}/{layer}/dataset | Persist a raster layer from a provider as a dataset.
*LayersApi* | [**layerToWorkflowIdHandler**](docs/LayersApi.md#layertoworkflowidhandler) | **POST** /layers/{provider}/{layer}/workflowId | Registers a layer from a provider as a workflow and returns the workflow id
*LayersApi* | [**listCollectionHandler**](docs/LayersApi.md#listcollectionhandler) | **GET** /layers/collections/{provider}/{collection} | List the contents of the collection of the given provider
*LayersApi* | [**listProviders**](docs/LayersApi.md#listproviders) | **GET** /layerDb/providers | List all providers
*LayersApi* | [**listRootCollectionsHandler**](docs/LayersApi.md#listrootcollectionshandler) | **GET** /layers/collections | List all layer collections
*LayersApi* | [**providerCapabilitiesHandler**](docs/LayersApi.md#providercapabilitieshandler) | **GET** /layers/{provider}/capabilities | 
*LayersApi* | [**removeCollection**](docs/LayersApi.md#removecollection) | **DELETE** /layerDb/collections/{collection} | Remove a collection
*LayersApi* | [**removeCollectionFromCollection**](docs/LayersApi.md#removecollectionfromcollection) | **DELETE** /layerDb/collections/{parent}/collections/{collection} | Delete a collection from a collection
*LayersApi* | [**removeLayer**](docs/LayersApi.md#removelayer) | **DELETE** /layerDb/layers/{layer} | Remove a collection
*LayersApi* | [**removeLayerFromCollection**](docs/LayersApi.md#removelayerfromcollection) | **DELETE** /layerDb/collections/{collection}/layers/{layer} | Remove a layer from a collection
*LayersApi* | [**searchHandler**](docs/LayersApi.md#searchhandler) | **GET** /layers/collections/search/{provider}/{collection} | Searches the contents of the collection of the given provider
*LayersApi* | [**updateCollection**](docs/LayersApi.md#updatecollection) | **PUT** /layerDb/collections/{collection} | Update a collection
*LayersApi* | [**updateLayer**](docs/LayersApi.md#updatelayer) | **PUT** /layerDb/layers/{layer} | Update a layer
*LayersApi* | [**updateProviderDefinition**](docs/LayersApi.md#updateproviderdefinition) | **PUT** /layerDb/providers/{provider} | Update an existing provider\&#39;s definition
*MLApi* | [**addMlModel**](docs/MLApi.md#addmlmodel) | **POST** /ml/models | Create a new ml model.
*MLApi* | [**getMlModel**](docs/MLApi.md#getmlmodel) | **GET** /ml/models/{model_name} | Get ml model by name.
*MLApi* | [**listMlModels**](docs/MLApi.md#listmlmodels) | **GET** /ml/models | List ml models.
*OGCAPIApi* | [**collection**](docs/OGCAPIApi.md#collection) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId} | OGC API Collection Metadata
*OGCAPIApi* | [**collectionTileset**](docs/OGCAPIApi.md#collectiontileset) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles/{tileMatrixSetId} | OGC API Collection Tileset Metadata
*OGCAPIApi* | [**collectionTilesets**](docs/OGCAPIApi.md#collectiontilesets) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles | OGC API Collection Tilesets List
*OGCAPIApi* | [**collections**](docs/OGCAPIApi.md#collections) | **GET** /ogc/{dataConnectorId}/{layerId}/collections | OGC API Collections List
*OGCAPIApi* | [**conformance**](docs/OGCAPIApi.md#conformance) | **GET** /ogc/{dataConnectorId}/{layerId}/conformance | OGC API Conformance Classes
*OGCAPIApi* | [**landingPage**](docs/OGCAPIApi.md#landingpage) | **GET** /ogc/{dataConnectorId}/{layerId}/ | OGC API Landing Page
*OGCAPIApi* | [**tile**](docs/OGCAPIApi.md#tile) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles/{tileMatrixSetId}/{tileMatrix}/{tileRow}/{tileCol} | OGC API Tile
*OGCAPIApi* | [**tileMatrixSet**](docs/OGCAPIApi.md#tilematrixset) | **GET** /ogc/{dataConnectorId}/{layerId}/tileMatrixSets/{tileMatrixSetId} | OGC API Tile Matrix Set Definition
*OGCAPIApi* | [**tileMatrixSets**](docs/OGCAPIApi.md#tilematrixsets) | **GET** /ogc/{dataConnectorId}/{layerId}/tileMatrixSets | OGC API Tile Matrix Set List
*OGCWCSApi* | [**wcsHandler**](docs/OGCWCSApi.md#wcshandler) | **GET** /wcs/{workflow} | OGC WCS endpoint
*OGCWFSApi* | [**wfsHandler**](docs/OGCWFSApi.md#wfshandler) | **GET** /wfs/{workflow} | OGC WFS endpoint
*OGCWMSApi* | [**wmsHandler**](docs/OGCWMSApi.md#wmshandler) | **GET** /wms/{workflow} | OGC WMS endpoint
*PermissionsApi* | [**addPermissionHandler**](docs/PermissionsApi.md#addpermissionhandler) | **PUT** /permissions | Adds a new permission.
*PermissionsApi* | [**getResourcePermissionsHandler**](docs/PermissionsApi.md#getresourcepermissionshandler) | **GET** /permissions/resources/{resource_type}/{resource_id} | Lists permission for a given resource.
*PermissionsApi* | [**removePermissionHandler**](docs/PermissionsApi.md#removepermissionhandler) | **DELETE** /permissions | Removes an existing permission.
*PlotsApi* | [**getPlotHandler**](docs/PlotsApi.md#getplothandler) | **GET** /plot/{id} | Generates a plot.
*ProjectsApi* | [**createProjectHandler**](docs/ProjectsApi.md#createprojecthandler) | **POST** /project | Create a new project for the user.
*ProjectsApi* | [**deleteProjectHandler**](docs/ProjectsApi.md#deleteprojecthandler) | **DELETE** /project/{project} | Deletes a project.
*ProjectsApi* | [**listProjectsHandler**](docs/ProjectsApi.md#listprojectshandler) | **GET** /projects | List all projects accessible to the user that match the selected criteria.
*ProjectsApi* | [**loadProjectLatestHandler**](docs/ProjectsApi.md#loadprojectlatesthandler) | **GET** /project/{project} | Retrieves details about the latest version of a project.
*ProjectsApi* | [**loadProjectVersionHandler**](docs/ProjectsApi.md#loadprojectversionhandler) | **GET** /project/{project}/{version} | Retrieves details about the given version of a project.
*ProjectsApi* | [**projectVersionsHandler**](docs/ProjectsApi.md#projectversionshandler) | **GET** /project/{project}/versions | Lists all available versions of a project.
*ProjectsApi* | [**updateProjectHandler**](docs/ProjectsApi.md#updateprojecthandler) | **PATCH** /project/{project} | Updates a project. This will create a new version.
*SessionApi* | [**anonymousHandler**](docs/SessionApi.md#anonymoushandler) | **POST** /anonymous | Creates session for anonymous user. The session\&#39;s id serves as a Bearer token for requests.
*SessionApi* | [**loginHandler**](docs/SessionApi.md#loginhandler) | **POST** /login | Creates a session by providing user credentials. The session\&#39;s id serves as a Bearer token for requests.
*SessionApi* | [**logoutHandler**](docs/SessionApi.md#logouthandler) | **POST** /logout | Ends a session.
*SessionApi* | [**oidcInit**](docs/SessionApi.md#oidcinit) | **POST** /oidcInit | Initializes the Open Id Connect login procedure by requesting a parametrized url to the configured Id Provider.
*SessionApi* | [**oidcLogin**](docs/SessionApi.md#oidclogin) | **POST** /oidcLogin | Creates a session for a user via a login with Open Id Connect. This call must be preceded by a call to oidcInit and match the parameters of that call.
*SessionApi* | [**registerUserHandler**](docs/SessionApi.md#registeruserhandler) | **POST** /user | Registers a user.
*SessionApi* | [**sessionHandler**](docs/SessionApi.md#sessionhandler) | **GET** /session | Retrieves details about the current session.
*SessionApi* | [**sessionProjectHandler**](docs/SessionApi.md#sessionprojecthandler) | **POST** /session/project/{project} | Sets the active project of the session.
*SessionApi* | [**sessionViewHandler**](docs/SessionApi.md#sessionviewhandler) | **POST** /session/view | 
*SpatialReferencesApi* | [**getSpatialReferenceSpecificationHandler**](docs/SpatialReferencesApi.md#getspatialreferencespecificationhandler) | **GET** /spatialReferenceSpecification/{srsString} | 
*TasksApi* | [**abortHandler**](docs/TasksApi.md#aborthandler) | **DELETE** /tasks/{id} | Abort a running task.
*TasksApi* | [**listHandler**](docs/TasksApi.md#listhandler) | **GET** /tasks/list | Retrieve the status of all tasks.
*TasksApi* | [**statusHandler**](docs/TasksApi.md#statushandler) | **GET** /tasks/{id}/status | Retrieve the status of a task.
*UploadsApi* | [**listUploadFileLayersHandler**](docs/UploadsApi.md#listuploadfilelayershandler) | **GET** /uploads/{upload_id}/files/{file_name}/layers | List the layers of on uploaded file.
*UploadsApi* | [**listUploadFilesHandler**](docs/UploadsApi.md#listuploadfileshandler) | **GET** /uploads/{upload_id}/files | List the files of on upload.
*UploadsApi* | [**uploadHandler**](docs/UploadsApi.md#uploadhandler) | **POST** /upload | Uploads files.
*UserApi* | [**addRoleHandler**](docs/UserApi.md#addrolehandler) | **PUT** /roles | Add a new role. Requires admin privilige.
*UserApi* | [**assignRoleHandler**](docs/UserApi.md#assignrolehandler) | **POST** /users/{user}/roles/{role} | Assign a role to a user. Requires admin privilige.
*UserApi* | [**computationQuotaHandler**](docs/UserApi.md#computationquotahandler) | **GET** /quota/computations/{computation} | Retrieves the quota used by computation with the given computation id
*UserApi* | [**computationsQuotaHandler**](docs/UserApi.md#computationsquotahandler) | **GET** /quota/computations | Retrieves the quota used by computations
*UserApi* | [**dataUsageHandler**](docs/UserApi.md#datausagehandler) | **GET** /quota/dataUsage | Retrieves the data usage
*UserApi* | [**dataUsageSummaryHandler**](docs/UserApi.md#datausagesummaryhandler) | **GET** /quota/dataUsage/summary | Retrieves the data usage summary
*UserApi* | [**getRoleByNameHandler**](docs/UserApi.md#getrolebynamehandler) | **GET** /roles/byName/{name} | Get role by name
*UserApi* | [**getRoleDescriptions**](docs/UserApi.md#getroledescriptions) | **GET** /user/roles/descriptions | Query roles for the current user.
*UserApi* | [**getUserQuotaHandler**](docs/UserApi.md#getuserquotahandler) | **GET** /quotas/{user} | Retrieves the available and used quota of a specific user.
*UserApi* | [**quotaHandler**](docs/UserApi.md#quotahandler) | **GET** /quota | Retrieves the available and used quota of the current user.
*UserApi* | [**removeRoleHandler**](docs/UserApi.md#removerolehandler) | **DELETE** /roles/{role} | Remove a role. Requires admin privilige.
*UserApi* | [**revokeRoleHandler**](docs/UserApi.md#revokerolehandler) | **DELETE** /users/{user}/roles/{role} | Revoke a role from a user. Requires admin privilige.
*UserApi* | [**updateUserQuotaHandler**](docs/UserApi.md#updateuserquotahandler) | **POST** /quotas/{user} | Update the available quota of a specific user.
*WorkflowsApi* | [**datasetFromWorkflowHandler**](docs/WorkflowsApi.md#datasetfromworkflowhandler) | **POST** /datasetFromWorkflow/{id} | Create a task for creating a new dataset from the result of the workflow given by its &#x60;id&#x60; and the dataset parameters in the request body. Returns the id of the created task
*WorkflowsApi* | [**getWorkflowAllMetadataZipHandler**](docs/WorkflowsApi.md#getworkflowallmetadataziphandler) | **GET** /workflow/{id}/allMetadata/zip | Gets a ZIP archive of the worklow, its provenance and the output metadata.
*WorkflowsApi* | [**getWorkflowMetadataHandler**](docs/WorkflowsApi.md#getworkflowmetadatahandler) | **GET** /workflow/{id}/metadata | Gets the metadata of a workflow
*WorkflowsApi* | [**getWorkflowProvenanceHandler**](docs/WorkflowsApi.md#getworkflowprovenancehandler) | **GET** /workflow/{id}/provenance | Gets the provenance of all datasets used in a workflow.
*WorkflowsApi* | [**loadWorkflowHandler**](docs/WorkflowsApi.md#loadworkflowhandler) | **GET** /workflow/{id} | Retrieves an existing Workflow.
*WorkflowsApi* | [**rasterStreamWebsocket**](docs/WorkflowsApi.md#rasterstreamwebsocket) | **GET** /workflow/{id}/rasterStream | Query a workflow raster result as a stream of tiles via a websocket connection.
*WorkflowsApi* | [**registerWorkflowHandler**](docs/WorkflowsApi.md#registerworkflowhandler) | **POST** /workflow | Registers a new Workflow.


### Models

- [AccessConstraints](docs/AccessConstraints.md)
- [AddDataset](docs/AddDataset.md)
- [AddLayer](docs/AddLayer.md)
- [AddLayerCollection](docs/AddLayerCollection.md)
- [AddRole](docs/AddRole.md)
- [Aggregation](docs/Aggregation.md)
- [ArunaDataProviderDefinition](docs/ArunaDataProviderDefinition.md)
- [AuthCodeRequestURL](docs/AuthCodeRequestURL.md)
- [AuthCodeResponse](docs/AuthCodeResponse.md)
- [AutoCreateDataset](docs/AutoCreateDataset.md)
- [AxisOrder](docs/AxisOrder.md)
- [BandFilter](docs/BandFilter.md)
- [BandFilterParameters](docs/BandFilterParameters.md)
- [BandsByNameOrIndex](docs/BandsByNameOrIndex.md)
- [BoundingBox2D](docs/BoundingBox2D.md)
- [Breakpoint](docs/Breakpoint.md)
- [ClassificationMeasurement](docs/ClassificationMeasurement.md)
- [Collection](docs/Collection.md)
- [CollectionItem](docs/CollectionItem.md)
- [CollectionType](docs/CollectionType.md)
- [Collections](docs/Collections.md)
- [CollectionsResponseFormat](docs/CollectionsResponseFormat.md)
- [ColorParam](docs/ColorParam.md)
- [Colorizer](docs/Colorizer.md)
- [ColumnNames](docs/ColumnNames.md)
- [ComputationQuota](docs/ComputationQuota.md)
- [Conformance](docs/Conformance.md)
- [ContinuousMeasurement](docs/ContinuousMeasurement.md)
- [Coordinate2D](docs/Coordinate2D.md)
- [CopernicusDataspaceDataProviderDefinition](docs/CopernicusDataspaceDataProviderDefinition.md)
- [CornerOfOrigin](docs/CornerOfOrigin.md)
- [CountAggregation](docs/CountAggregation.md)
- [CreateDataset](docs/CreateDataset.md)
- [CreateProject](docs/CreateProject.md)
- [CsvHeader](docs/CsvHeader.md)
- [Data](docs/Data.md)
- [DataId](docs/DataId.md)
- [DataPath](docs/DataPath.md)
- [DataPathUpload](docs/DataPathUpload.md)
- [DataPathVolume](docs/DataPathVolume.md)
- [DataProviderResource](docs/DataProviderResource.md)
- [DataUsage](docs/DataUsage.md)
- [DataUsageSummary](docs/DataUsageSummary.md)
- [DatabaseConnectionConfig](docs/DatabaseConnectionConfig.md)
- [Dataset](docs/Dataset.md)
- [DatasetDefinition](docs/DatasetDefinition.md)
- [DatasetLayerListingCollection](docs/DatasetLayerListingCollection.md)
- [DatasetLayerListingProviderDefinition](docs/DatasetLayerListingProviderDefinition.md)
- [DatasetListing](docs/DatasetListing.md)
- [DatasetNameResponse](docs/DatasetNameResponse.md)
- [DatasetResource](docs/DatasetResource.md)
- [Default](docs/Default.md)
- [DeriveOutRasterSpecsSource](docs/DeriveOutRasterSpecsSource.md)
- [DerivedColor](docs/DerivedColor.md)
- [DerivedNumber](docs/DerivedNumber.md)
- [EbvPortalDataProviderDefinition](docs/EbvPortalDataProviderDefinition.md)
- [EdrDataProviderDefinition](docs/EdrDataProviderDefinition.md)
- [EdrVectorSpec](docs/EdrVectorSpec.md)
- [ErrorResponse](docs/ErrorResponse.md)
- [Expression](docs/Expression.md)
- [ExpressionParameters](docs/ExpressionParameters.md)
- [Extent](docs/Extent.md)
- [ExternalDataId](docs/ExternalDataId.md)
- [FeatureAggregationMethod](docs/FeatureAggregationMethod.md)
- [FeatureDataType](docs/FeatureDataType.md)
- [FileNotFoundHandling](docs/FileNotFoundHandling.md)
- [FirstAggregation](docs/FirstAggregation.md)
- [FormatSpecifics](docs/FormatSpecifics.md)
- [FormatSpecificsCsv](docs/FormatSpecificsCsv.md)
- [Fraction](docs/Fraction.md)
- [GbifDataProviderDefinition](docs/GbifDataProviderDefinition.md)
- [GdalDatasetParameters](docs/GdalDatasetParameters.md)
- [GdalLoadingInfoTemporalSlice](docs/GdalLoadingInfoTemporalSlice.md)
- [GdalMetaDataList](docs/GdalMetaDataList.md)
- [GdalMetaDataRegular](docs/GdalMetaDataRegular.md)
- [GdalMetaDataStatic](docs/GdalMetaDataStatic.md)
- [GdalMetadataMapping](docs/GdalMetadataMapping.md)
- [GdalMetadataNetCdfCf](docs/GdalMetadataNetCdfCf.md)
- [GdalMultiBand](docs/GdalMultiBand.md)
- [GdalSource](docs/GdalSource.md)
- [GdalSourceParameters](docs/GdalSourceParameters.md)
- [GdalSourceTimePlaceholder](docs/GdalSourceTimePlaceholder.md)
- [GeoJson](docs/GeoJson.md)
- [GeoTransform](docs/GeoTransform.md)
- [GeometryDimension](docs/GeometryDimension.md)
- [GeospatialData](docs/GeospatialData.md)
- [GeospatialDataDataType](docs/GeospatialDataDataType.md)
- [GeospatialDataDataTypeOneOf](docs/GeospatialDataDataTypeOneOf.md)
- [GetCoverageFormat](docs/GetCoverageFormat.md)
- [GetMapExceptionFormat](docs/GetMapExceptionFormat.md)
- [GfbioAbcdDataProviderDefinition](docs/GfbioAbcdDataProviderDefinition.md)
- [GfbioCollectionsDataProviderDefinition](docs/GfbioCollectionsDataProviderDefinition.md)
- [GridBoundingBox2D](docs/GridBoundingBox2D.md)
- [GridIdx2D](docs/GridIdx2D.md)
- [Histogram](docs/Histogram.md)
- [HistogramBounds](docs/HistogramBounds.md)
- [HistogramBoundsValues](docs/HistogramBoundsValues.md)
- [HistogramBuckets](docs/HistogramBuckets.md)
- [HistogramBucketsNumber](docs/HistogramBucketsNumber.md)
- [HistogramBucketsSquareRootChoiceRule](docs/HistogramBucketsSquareRootChoiceRule.md)
- [HistogramParameters](docs/HistogramParameters.md)
- [IdResponse](docs/IdResponse.md)
- [InternalDataId](docs/InternalDataId.md)
- [Interpolation](docs/Interpolation.md)
- [InterpolationMethod](docs/InterpolationMethod.md)
- [InterpolationParameters](docs/InterpolationParameters.md)
- [InterpolationResolution](docs/InterpolationResolution.md)
- [Irregular](docs/Irregular.md)
- [LandingPage](docs/LandingPage.md)
- [LastAggregation](docs/LastAggregation.md)
- [Layer](docs/Layer.md)
- [LayerCollection](docs/LayerCollection.md)
- [LayerCollectionListing](docs/LayerCollectionListing.md)
- [LayerCollectionResource](docs/LayerCollectionResource.md)
- [LayerListing](docs/LayerListing.md)
- [LayerProviderListing](docs/LayerProviderListing.md)
- [LayerResource](docs/LayerResource.md)
- [LayerVisibility](docs/LayerVisibility.md)
- [LegacyTypedOperator](docs/LegacyTypedOperator.md)
- [LegacyTypedOperatorOperator](docs/LegacyTypedOperatorOperator.md)
- [LineSymbology](docs/LineSymbology.md)
- [LinearGradient](docs/LinearGradient.md)
- [Link](docs/Link.md)
- [LogarithmicGradient](docs/LogarithmicGradient.md)
- [MaxAggregation](docs/MaxAggregation.md)
- [MeanAggregation](docs/MeanAggregation.md)
- [Measurement](docs/Measurement.md)
- [MetaDataDefinition](docs/MetaDataDefinition.md)
- [MetaDataSuggestion](docs/MetaDataSuggestion.md)
- [MinAggregation](docs/MinAggregation.md)
- [MlModel](docs/MlModel.md)
- [MlModelInputNoDataHandling](docs/MlModelInputNoDataHandling.md)
- [MlModelInputNoDataHandlingVariant](docs/MlModelInputNoDataHandlingVariant.md)
- [MlModelMetadata](docs/MlModelMetadata.md)
- [MlModelNameResponse](docs/MlModelNameResponse.md)
- [MlModelOutputNoDataHandling](docs/MlModelOutputNoDataHandling.md)
- [MlModelOutputNoDataHandlingVariant](docs/MlModelOutputNoDataHandlingVariant.md)
- [MlModelResource](docs/MlModelResource.md)
- [MlTensorShape3D](docs/MlTensorShape3D.md)
- [MockDatasetDataSourceLoadingInfo](docs/MockDatasetDataSourceLoadingInfo.md)
- [MockMetaData](docs/MockMetaData.md)
- [MockPointSource](docs/MockPointSource.md)
- [MockPointSourceParameters](docs/MockPointSourceParameters.md)
- [MultiBandGdalSource](docs/MultiBandGdalSource.md)
- [MultiBandRasterColorizer](docs/MultiBandRasterColorizer.md)
- [MultiLineString](docs/MultiLineString.md)
- [MultiPoint](docs/MultiPoint.md)
- [MultiPolygon](docs/MultiPolygon.md)
- [MultipleRasterOrSingleVectorOperator](docs/MultipleRasterOrSingleVectorOperator.md)
- [MultipleRasterOrSingleVectorSource](docs/MultipleRasterOrSingleVectorSource.md)
- [MultipleRasterSources](docs/MultipleRasterSources.md)
- [Names](docs/Names.md)
- [NetCdfCfDataProviderDefinition](docs/NetCdfCfDataProviderDefinition.md)
- [NumberParam](docs/NumberParam.md)
- [OgrMetaData](docs/OgrMetaData.md)
- [OgrSource](docs/OgrSource.md)
- [OgrSourceColumnSpec](docs/OgrSourceColumnSpec.md)
- [OgrSourceDataset](docs/OgrSourceDataset.md)
- [OgrSourceDatasetTimeType](docs/OgrSourceDatasetTimeType.md)
- [OgrSourceDatasetTimeTypeNone](docs/OgrSourceDatasetTimeTypeNone.md)
- [OgrSourceDatasetTimeTypeStart](docs/OgrSourceDatasetTimeTypeStart.md)
- [OgrSourceDatasetTimeTypeStartDuration](docs/OgrSourceDatasetTimeTypeStartDuration.md)
- [OgrSourceDatasetTimeTypeStartEnd](docs/OgrSourceDatasetTimeTypeStartEnd.md)
- [OgrSourceDurationSpec](docs/OgrSourceDurationSpec.md)
- [OgrSourceDurationSpecInfinite](docs/OgrSourceDurationSpecInfinite.md)
- [OgrSourceDurationSpecValue](docs/OgrSourceDurationSpecValue.md)
- [OgrSourceDurationSpecZero](docs/OgrSourceDurationSpecZero.md)
- [OgrSourceErrorSpec](docs/OgrSourceErrorSpec.md)
- [OgrSourceParameters](docs/OgrSourceParameters.md)
- [OgrSourceTimeFormat](docs/OgrSourceTimeFormat.md)
- [OgrSourceTimeFormatAuto](docs/OgrSourceTimeFormatAuto.md)
- [OgrSourceTimeFormatCustom](docs/OgrSourceTimeFormatCustom.md)
- [OgrSourceTimeFormatUnixTimeStamp](docs/OgrSourceTimeFormatUnixTimeStamp.md)
- [OperatorQuota](docs/OperatorQuota.md)
- [OrderBy](docs/OrderBy.md)
- [PaletteColorizer](docs/PaletteColorizer.md)
- [PangaeaDataProviderDefinition](docs/PangaeaDataProviderDefinition.md)
- [PercentileEstimateAggregation](docs/PercentileEstimateAggregation.md)
- [Permission](docs/Permission.md)
- [PermissionListing](docs/PermissionListing.md)
- [PermissionRequest](docs/PermissionRequest.md)
- [Plot](docs/Plot.md)
- [PlotOperator](docs/PlotOperator.md)
- [PlotOutputFormat](docs/PlotOutputFormat.md)
- [PlotResultDescriptor](docs/PlotResultDescriptor.md)
- [PointSymbology](docs/PointSymbology.md)
- [PolygonSymbology](docs/PolygonSymbology.md)
- [Project](docs/Project.md)
- [ProjectLayer](docs/ProjectLayer.md)
- [ProjectListing](docs/ProjectListing.md)
- [ProjectResource](docs/ProjectResource.md)
- [ProjectUpdateToken](docs/ProjectUpdateToken.md)
- [ProjectVersion](docs/ProjectVersion.md)
- [Provenance](docs/Provenance.md)
- [ProvenanceEntry](docs/ProvenanceEntry.md)
- [Provenances](docs/Provenances.md)
- [ProviderCapabilities](docs/ProviderCapabilities.md)
- [ProviderLayerCollectionId](docs/ProviderLayerCollectionId.md)
- [ProviderLayerId](docs/ProviderLayerId.md)
- [Quota](docs/Quota.md)
- [RasterBandDescriptor](docs/RasterBandDescriptor.md)
- [RasterColorizer](docs/RasterColorizer.md)
- [RasterDataType](docs/RasterDataType.md)
- [RasterDatasetFromWorkflow](docs/RasterDatasetFromWorkflow.md)
- [RasterOperator](docs/RasterOperator.md)
- [RasterPropertiesEntryType](docs/RasterPropertiesEntryType.md)
- [RasterPropertiesKey](docs/RasterPropertiesKey.md)
- [RasterResultDescriptor](docs/RasterResultDescriptor.md)
- [RasterStacker](docs/RasterStacker.md)
- [RasterStackerParameters](docs/RasterStackerParameters.md)
- [RasterStreamWebsocketResultType](docs/RasterStreamWebsocketResultType.md)
- [RasterSymbology](docs/RasterSymbology.md)
- [RasterToDatasetQueryRectangle](docs/RasterToDatasetQueryRectangle.md)
- [RasterTypeConversion](docs/RasterTypeConversion.md)
- [RasterTypeConversionParameters](docs/RasterTypeConversionParameters.md)
- [RasterVectorJoin](docs/RasterVectorJoin.md)
- [RasterVectorJoinParameters](docs/RasterVectorJoinParameters.md)
- [Regular](docs/Regular.md)
- [RegularTimeDimension](docs/RegularTimeDimension.md)
- [Rename](docs/Rename.md)
- [RenameBands](docs/RenameBands.md)
- [Reprojection](docs/Reprojection.md)
- [ReprojectionParameters](docs/ReprojectionParameters.md)
- [Resolution](docs/Resolution.md)
- [Resource](docs/Resource.md)
- [Role](docs/Role.md)
- [RoleDescription](docs/RoleDescription.md)
- [STRectangle](docs/STRectangle.md)
- [SearchCapabilities](docs/SearchCapabilities.md)
- [SearchType](docs/SearchType.md)
- [SearchTypes](docs/SearchTypes.md)
- [SentinelS2L2ACogsProviderDefinition](docs/SentinelS2L2ACogsProviderDefinition.md)
- [ServerInfo](docs/ServerInfo.md)
- [SingleBandRasterColorizer](docs/SingleBandRasterColorizer.md)
- [SingleRasterOrVectorOperator](docs/SingleRasterOrVectorOperator.md)
- [SingleRasterOrVectorSource](docs/SingleRasterOrVectorSource.md)
- [SingleRasterSource](docs/SingleRasterSource.md)
- [SingleVectorMultipleRasterSources](docs/SingleVectorMultipleRasterSources.md)
- [SingleVectorOrRasterSource](docs/SingleVectorOrRasterSource.md)
- [SpatialBoundsDerive](docs/SpatialBoundsDerive.md)
- [SpatialBoundsDeriveBounds](docs/SpatialBoundsDeriveBounds.md)
- [SpatialBoundsDeriveDerive](docs/SpatialBoundsDeriveDerive.md)
- [SpatialBoundsDeriveNone](docs/SpatialBoundsDeriveNone.md)
- [SpatialExtent](docs/SpatialExtent.md)
- [SpatialGridDefinition](docs/SpatialGridDefinition.md)
- [SpatialGridDescriptor](docs/SpatialGridDescriptor.md)
- [SpatialGridDescriptorState](docs/SpatialGridDescriptorState.md)
- [SpatialPartition2D](docs/SpatialPartition2D.md)
- [SpatialReferenceSpecification](docs/SpatialReferenceSpecification.md)
- [SpatialResolution](docs/SpatialResolution.md)
- [StacApiRetries](docs/StacApiRetries.md)
- [StacDataProviderDefinition](docs/StacDataProviderDefinition.md)
- [StacProviderDataset](docs/StacProviderDataset.md)
- [StacProviderDatasetBand](docs/StacProviderDatasetBand.md)
- [StacProviderS3Config](docs/StacProviderS3Config.md)
- [StacQueryBuffer](docs/StacQueryBuffer.md)
- [StaticColor](docs/StaticColor.md)
- [StaticNumber](docs/StaticNumber.md)
- [Statistics](docs/Statistics.md)
- [StatisticsParameters](docs/StatisticsParameters.md)
- [StrokeParam](docs/StrokeParam.md)
- [Style](docs/Style.md)
- [Suffix](docs/Suffix.md)
- [SuggestMetaData](docs/SuggestMetaData.md)
- [SumAggregation](docs/SumAggregation.md)
- [Symbology](docs/Symbology.md)
- [TaskFilter](docs/TaskFilter.md)
- [TaskResponse](docs/TaskResponse.md)
- [TaskStatus](docs/TaskStatus.md)
- [TaskStatusAborted](docs/TaskStatusAborted.md)
- [TaskStatusCompleted](docs/TaskStatusCompleted.md)
- [TaskStatusFailed](docs/TaskStatusFailed.md)
- [TaskStatusRunning](docs/TaskStatusRunning.md)
- [TaskStatusWithId](docs/TaskStatusWithId.md)
- [TemporalAggregationMethod](docs/TemporalAggregationMethod.md)
- [TemporalExtent](docs/TemporalExtent.md)
- [TemporalRasterAggregation](docs/TemporalRasterAggregation.md)
- [TemporalRasterAggregationParameters](docs/TemporalRasterAggregationParameters.md)
- [TextSymbology](docs/TextSymbology.md)
- [TileMatrix](docs/TileMatrix.md)
- [TileMatrixLimits](docs/TileMatrixLimits.md)
- [TileMatrixSet](docs/TileMatrixSet.md)
- [TileMatrixSetId](docs/TileMatrixSetId.md)
- [TileMatrixSetIdOneOf](docs/TileMatrixSetIdOneOf.md)
- [TileMatrixSetItem](docs/TileMatrixSetItem.md)
- [TileMatrixSets](docs/TileMatrixSets.md)
- [TilePoint](docs/TilePoint.md)
- [TileSet](docs/TileSet.md)
- [TileSetItem](docs/TileSetItem.md)
- [TileSets](docs/TileSets.md)
- [TilesCrs](docs/TilesCrs.md)
- [TilesCrsOneOf](docs/TilesCrsOneOf.md)
- [TilesCrsOneOf1](docs/TilesCrsOneOf1.md)
- [TilesCrsOneOf2](docs/TilesCrsOneOf2.md)
- [TimeDescriptor](docs/TimeDescriptor.md)
- [TimeDimension](docs/TimeDimension.md)
- [TimeGranularity](docs/TimeGranularity.md)
- [TimeInterval](docs/TimeInterval.md)
- [TimeReference](docs/TimeReference.md)
- [TimeStep](docs/TimeStep.md)
- [TypedDataProviderDefinition](docs/TypedDataProviderDefinition.md)
- [TypedGeometry](docs/TypedGeometry.md)
- [TypedGeometryMultiLineString](docs/TypedGeometryMultiLineString.md)
- [TypedGeometryMultiPoint](docs/TypedGeometryMultiPoint.md)
- [TypedGeometryMultiPolygon](docs/TypedGeometryMultiPolygon.md)
- [TypedGeometryNoGeometry](docs/TypedGeometryNoGeometry.md)
- [TypedOperator](docs/TypedOperator.md)
- [TypedPlotOperator](docs/TypedPlotOperator.md)
- [TypedPlotResultDescriptor](docs/TypedPlotResultDescriptor.md)
- [TypedRasterOperator](docs/TypedRasterOperator.md)
- [TypedRasterResultDescriptor](docs/TypedRasterResultDescriptor.md)
- [TypedResultDescriptor](docs/TypedResultDescriptor.md)
- [TypedVectorOperator](docs/TypedVectorOperator.md)
- [TypedVectorResultDescriptor](docs/TypedVectorResultDescriptor.md)
- [UnitlessMeasurement](docs/UnitlessMeasurement.md)
- [UnixTimeStampType](docs/UnixTimeStampType.md)
- [UpdateDataset](docs/UpdateDataset.md)
- [UpdateLayer](docs/UpdateLayer.md)
- [UpdateLayerCollection](docs/UpdateLayerCollection.md)
- [UpdateProject](docs/UpdateProject.md)
- [UpdateQuota](docs/UpdateQuota.md)
- [UploadFileLayersResponse](docs/UploadFileLayersResponse.md)
- [UploadFilesResponse](docs/UploadFilesResponse.md)
- [UsageSummaryGranularity](docs/UsageSummaryGranularity.md)
- [UserCredentials](docs/UserCredentials.md)
- [UserInfo](docs/UserInfo.md)
- [UserRegistration](docs/UserRegistration.md)
- [UserSession](docs/UserSession.md)
- [VariableMatrixWidth](docs/VariableMatrixWidth.md)
- [VecUpdate](docs/VecUpdate.md)
- [VectorColumnInfo](docs/VectorColumnInfo.md)
- [VectorDataType](docs/VectorDataType.md)
- [VectorOperator](docs/VectorOperator.md)
- [VectorResultDescriptor](docs/VectorResultDescriptor.md)
- [Volume](docs/Volume.md)
- [VolumeFileLayersResponse](docs/VolumeFileLayersResponse.md)
- [WcsRequest](docs/WcsRequest.md)
- [WcsService](docs/WcsService.md)
- [WcsVersion](docs/WcsVersion.md)
- [WfsRequest](docs/WfsRequest.md)
- [WfsService](docs/WfsService.md)
- [WfsVersion](docs/WfsVersion.md)
- [WildliveDataConnectorDefinition](docs/WildliveDataConnectorDefinition.md)
- [WmsRequest](docs/WmsRequest.md)
- [WmsResponseFormat](docs/WmsResponseFormat.md)
- [WmsService](docs/WmsService.md)
- [WmsVersion](docs/WmsVersion.md)
- [Workflow](docs/Workflow.md)
- [WrappedPlotOutput](docs/WrappedPlotOutput.md)

### Authorization


Authentication schemes defined for the API:
<a id="session_token"></a>
#### session_token


- **Type**: HTTP Bearer Token authentication (UUID)

## About

This TypeScript SDK client supports the [Fetch API](https://fetch.spec.whatwg.org/)
and is automatically generated by the
[OpenAPI Generator](https://openapi-generator.tech) project:

- API version: `0.9.2`
- Package version: `0.9.2`
- Generator version: `7.20.0`
- Build package: `org.openapitools.codegen.languages.TypeScriptFetchClientCodegen`

The generated npm module supports the following:

- Environments
  * Node.js
  * Webpack
  * Browserify
- Language levels
  * ES5 - you must have a Promises/A+ library installed
  * ES6
- Module systems
  * CommonJS
  * ES6 module system


## Development

### Building

To build the TypeScript source code, you need to have Node.js and npm installed.
After cloning the repository, navigate to the project directory and run:

```bash
npm install
npm run build
```

### Publishing

Once you've built the package, you can publish it to npm:

```bash
npm publish
```

## License

[Apache-2.0](https://github.com/geo-engine/geoengine/blob/main/LICENSE)
