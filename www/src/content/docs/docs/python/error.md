---
sidebar_label: error
title: error
---

Package errors and backend mapped error types

## GeoEngineException Objects

```python
class GeoEngineException(Exception)
```

Base class for exceptions from the backend

## InputException Objects

```python
class InputException(Exception)
```

Exception that is thrown on wrong inputs

## UninitializedException Objects

```python
class UninitializedException(Exception)
```

Exception that is thrown when there is no connection to the backend but methods on the backend are called

## TypeException Objects

```python
class TypeException(Exception)
```

Exception on wrong types of input

## ModificationNotOnLayerDbException Objects

```python
class ModificationNotOnLayerDbException(Exception)
```

Exception that is when trying to modify layers that are not part of the layerdb

## MethodNotCalledOnRasterException Objects

```python
class MethodNotCalledOnRasterException(Exception)
```

Exception for calling a raster method on a, e.g., vector layer

## MethodNotCalledOnVectorException Objects

```python
class MethodNotCalledOnVectorException(Exception)
```

Exception for calling a vector method on a, e.g., raster layer

## MethodNotCalledOnPlotException Objects

```python
class MethodNotCalledOnPlotException(Exception)
```

Exception for calling a plot method on a, e.g., vector layer

## SpatialReferenceMismatchException Objects

```python
class SpatialReferenceMismatchException(Exception)
```

Exception for calling a method on a workflow with a query rectangle that has a different spatial reference

## InvalidUrlException Objects

```python
class InvalidUrlException(Exception)
```

Exception for when no valid url is provided

## MissingFieldInResponseException Objects

```python
class MissingFieldInResponseException(Exception)
```

Exception for when a field is missing in a response

#### check_response_for_error

```python
def check_response_for_error(response: Response) -> None
```

Checks a `Response` for an error and raises it if there is one.

## MethodOnlyAvailableInGeoEnginePro Objects

```python
class MethodOnlyAvailableInGeoEnginePro(Exception)
```

Exception when trying to use a method that is only available in Geo Engine Pro

## OGCXMLError Objects

```python
class OGCXMLError(Exception)
```

Exception when an OGC XML error is returned
