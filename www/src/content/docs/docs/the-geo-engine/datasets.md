---
title: Datasets
---

A dataset is a loadable unit in Geo Engine.
It is a parameter of a source operator (e.g., a [`GdalSource`](/docs/operators/gdalsource)) and identifies the data that is loaded.
Geo Engine supports different types of data, reflected by a `DataId`, which refers to internal datasets and external data.

## Internal dataset

An internal dataset is a dataset that is stored in the Geo Engine.
Thus, it is efficiently accessible and can be used in workflows.
The dataset is identified by a `DatasetName` and contains a `DatasetDefinition` that describes the data.

The `DatasetName` is a string that consists of a _namespace_ (optional) and a _name_, separated by a colon.
For instance, `namespace:name` or `name` refer to datasets.
The `name` can consist of characters (`a-Z` & `A-Z`), numbers (`0-9`), dashes (`-`) and underscores (`_`).

## External data

An external dataset is a dataset that is not stored in the Geo Engine.
Geo Engine accesses it from a foreign location.
The dataset is identified by an `ExternalDataId` that consists of a `DataProviderId` and a `LayerId`.
While the `DatasetProviderId` is usually a UUID that identifies the data provider for Geo Engine itself, the `LayerId` is a string that identifies the layer in the data provider.

The `ExternalDataId` is a string that consists of a _namespace_, the `DataProviderId` and a _name_, separated by a colon.
The namespace cannot be omitted and is `_` for the global namespace.
For instance, `_:{uuid}:name` or `namespace:{uuid}:name` refer to datasets.
If the _name_ is a complex string, it can be enclosed by backticks, e.g., <code>namespace:{uuid}:\`name with spaces\`</code>.
