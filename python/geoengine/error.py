"""
Package errors and backend mapped error types
"""

import json
import xml.etree.ElementTree as ET
from typing import Any

import geoengine_openapi_client
from requests import HTTPError, Response


class GeoEngineException(Exception):
    """
    Base class for exceptions from the backend
    """

    error: str
    message: str

    def __init__(self, response: geoengine_openapi_client.ApiException | dict[str, str]) -> None:
        super().__init__()

        if isinstance(response, geoengine_openapi_client.ApiException):
            obj = json.loads(response.body) if response.body else {"error": "unknown", "message": "unknown"}
        else:
            obj = response

        self.error = obj.get("error", "?")
        self.message = obj.get("message", "?")

    def __str__(self) -> str:
        return f"{self.error}: {self.message}"


class InputException(Exception):
    """
    Exception that is thrown on wrong inputs
    """

    __message: str

    def __init__(self, message: str) -> None:
        super().__init__()

        self.__message = message

    def __str__(self) -> str:
        return f"{self.__message}"


class UninitializedException(Exception):
    """
    Exception that is thrown when there is no connection to the backend but methods on the backend are called
    """

    def __str__(self) -> str:
        return "You have to call `initialize` before using other functionality"


class TypeException(Exception):
    """
    Exception on wrong types of input
    """

    __message: str

    def __init__(self, message: str) -> None:
        super().__init__()

        self.__message = message

    def __str__(self) -> str:
        return f"{self.__message}"


class ModificationNotOnLayerDbException(Exception):
    """
    Exception that is when trying to modify layers that are not part of the layerdb
    """

    __message: str

    def __init__(self, message: str) -> None:
        super().__init__()

        self.__message = message

    def __str__(self) -> str:
        return f"{self.__message}"


# TODO: remove methods and forbid calling methods in the first place


class MethodNotCalledOnRasterException(Exception):
    """
    Exception for calling a raster method on a, e.g., vector layer
    """

    def __str__(self) -> str:
        return "Only allowed to call method on raster result"


# TODO: remove methods and forbid calling methods in the first place
class MethodNotCalledOnVectorException(Exception):
    """
    Exception for calling a vector method on a, e.g., raster layer
    """

    def __str__(self) -> str:
        return "Only allowed to call method on vector result"


# TODO: remove methods and forbid calling methods in the first place
class MethodNotCalledOnPlotException(Exception):
    """
    Exception for calling a plot method on a, e.g., vector layer
    """

    def __str__(self) -> str:
        return "Only allowed to call method on plot result"


class SpatialReferenceMismatchException(Exception):
    """
    Exception for calling a method on a workflow with a query rectangle that has a different spatial reference
    """

    def __init__(self, spatial_reference_a: str, spatial_reference_b: str) -> None:
        super().__init__()

        self.__spatial_reference_a = spatial_reference_a
        self.__spatial_reference_b = spatial_reference_b

    def __str__(self) -> str:
        return f"Spatial reference mismatch {self.__spatial_reference_a} != {self.__spatial_reference_b}"


class InvalidUrlException(Exception):
    """
    Exception for when no valid url is provided
    """

    def __init__(self, msg: str) -> None:
        super().__init__()

        self.__msg = msg

    def __str__(self) -> str:
        return f"{self.__msg}"


class MissingFieldInResponseException(Exception):
    """
    Exception for when a field is missing in a response
    """

    missing_field: str
    response: Any

    def __init__(self, missing_field: str, response: Any) -> None:
        super().__init__()

        self.missing_field = missing_field
        self.response = response

    def __str__(self) -> str:
        return f"Missing field '{self.missing_field}' in response: {self.response}"


def check_response_for_error(response: Response) -> None:
    """
    Checks a `Response` for an error and raises it if there is one.
    """

    try:
        response.raise_for_status()

        return  # no error
    except HTTPError as http_error:
        exception = http_error

    # try to parse it as a Geo Engine error
    try:
        response_json = response.json()
    except Exception:  # pylint: disable=broad-except
        pass  # ignore errors, it seemed not to be JSON
    else:
        # if parsing was successful, raise the appropriate exception
        if "error" in response_json:
            raise GeoEngineException(response_json)

    # raise `HTTPError` if `GeoEngineException` or any other was not thrown
    raise exception


class MethodOnlyAvailableInGeoEnginePro(Exception):
    """
    Exception when trying to use a method that is only available in Geo Engine Pro
    """

    __message: str

    def __init__(self, message: str) -> None:
        super().__init__()

        self.__message = message

    def __str__(self) -> str:
        return f"Method is only available in Geo Engine Pro: {self.__message}"


class OGCXMLError(Exception):
    """
    Exception when an OGC XML error is returned
    """

    __xml: ET.Element

    def __init__(self, xml: bytearray) -> None:
        super().__init__()

        self.__xml = ET.fromstring(xml[1:])

    def __str__(self) -> str:
        service_exception = ""
        for e in self.__xml:
            if "ServiceException" in e.tag and e.text is not None:
                service_exception = e.text
                break

        return f"OGC API error: {service_exception}"

    @classmethod
    def is_ogc_error(cls, xml: bytearray) -> bool:
        return xml.startswith(b"\n<?xml")
