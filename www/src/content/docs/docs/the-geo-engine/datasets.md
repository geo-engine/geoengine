---
title: Datasets
sidebar:
    order: 7
---

A dataset is a loadable unit in Geo Engine.
It is a parameter of a source operator (e.g., a [`GdalSource`](/docs/operators/gdalsource)) and identifies the data that is loaded.
Geo Engine supports different types of data, reflected by a `DataId`, which refers to internal datasets and external data.

## Internal dataset

An internal dataset is a dataset that is stored in the Geo Engine.
Thus, it is efficiently accessible and can be used in processing graphs.
The dataset is identified by a `DatasetName` and contains a `DatasetDefinition` that describes the data.

The `DatasetName` is a string that consists of a _namespace_ (optional) and a _name_, separated by a colon.
For instance, `namespace:name` or `name` refer to datasets.
The `name` can consist of characters (`a-Z` & `A-Z`), numbers (`0-9`), dashes (`-`) and underscores (`_`).

### Dataset definition

A dataset definition has two parts:

- **Properties**: the name, display name, description, tags, the source operator that loads it (`GdalSource`, `MultiBandGdalSource` or `OgrSource`), a default symbology and the provenance, i.e., citation, license and URI.
- **Metadata**: how to load the files, e.g., the file paths, the layer of a vector file, how to read time from file names or attributes, and the [result descriptor](/docs/the-geo-engine/processing-graphs#result-descriptor).

Raster datasets can describe a single file, a regular time series of files with a file name pattern, a list of files with their time intervals, or a NetCDF-CF cube.
Multi-band datasets (`MultiBandGdalSource`) consist of tiles that can be added to the dataset over time with `POST /dataset/{dataset}/tiles`.

### Where the files are

The files of a dataset come from one of these places:

- **Uploads.**
  Every user can upload files with `POST /upload` and create a dataset from them.
  `POST /dataset/suggest` inspects an upload and suggests a dataset definition, and `POST /dataset/auto` creates a dataset with the suggested definition in one step.
  In Python, `ge.upload_dataframe` uploads a GeoPandas data frame as a vector dataset.
- **Volumes.**
  Volumes are directories on the server that the administrator configures under `[data] volumes`.
  Only administrators can create datasets from volumes, e.g., with `ge.add_dataset` in Python or in the `manager` app.
- **Dataset definition files.**
  When the server creates its database schema, i.e., on the first start with an empty schema, it loads the dataset definitions in `[dataprovider] dataset_defs_path`.
  Files added there later are not loaded on a restart.
  This is how the example datasets of a development instance are created.
- **Processing graphs.**
  `POST /datasetFromProcessingGraph/{id}` computes a raster processing graph and stores the result as a new dataset.
  It runs as a background task; see `GET /tasks/{id}/status`.

The user who creates a dataset is its owner and can share it with other users and roles.
See [Users and Permissions](/docs/the-geo-engine/users-and-permissions).

## External data

An external dataset is a dataset that is not stored in the Geo Engine.
Geo Engine accesses it from a foreign location through a [data connector](/docs/the-geo-engine/data-connectors).
The dataset is identified by an `ExternalDataId` that consists of a `DataProviderId` and a `LayerId`.
While the `DataProviderId` is usually a UUID that identifies the data connector for Geo Engine itself, the `LayerId` is a string that identifies the layer in the data connector.

The `ExternalDataId` is a string that consists of a _namespace_, the `DataProviderId` and a _name_, separated by a colon.
The namespace cannot be omitted and is `_` for the global namespace.
For instance, `_:{uuid}:name` or `namespace:{uuid}:name` refer to datasets.
If the _name_ is a complex string, it can be enclosed by backticks, e.g., <code>namespace:{uuid}:\`name with spaces\`</code>.
