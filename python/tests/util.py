"""Utility methods for testing"""

import sys
import unittest
from json import dumps, loads
from unittest.mock import _patch, patch
from urllib.parse import parse_qs
from uuid import UUID

import urllib3


def eprint(*args, **kwargs):
    """Print to stderr"""
    print(*args, file=sys.stderr, **kwargs)


def is_url_match(url1: str, url2: str) -> bool:
    """Checks if two urls point to the same resource"""
    parsed1 = urllib3.util.parse_url(url1)
    parsed2 = urllib3.util.parse_url(url2)
    return (
        parsed1.host == parsed2.host
        and parsed1.port == parsed2.port
        and parsed1.path == parsed2.path
        and parse_qs(parsed1.query) == parse_qs(parsed2.query)
    )


NOT_FOUND_UUID: UUID = UUID("9d70d443-0f9f-4455-87a7-9ce2d406af07")


class UrllibMocker:
    """Mock urllib3 requests"""

    _mock_context: _patch
    _matchers: list
    request_history: list

    STATUS_CODE_REASON_MAP = {200: "OK", 400: "Bad Request", 404: "Not Found"}

    def __enter__(self):
        """
        Enter method for the context manager.
        Initializes the necessary attributes and sets up the mock request.
        """

        self._matchers = []
        self.request_history = []
        self._mock_context = patch("geoengine_openapi_client.rest.urllib3.PoolManager.request")
        mock_request = self._mock_context.__enter__()
        mock_request.side_effect = self._handle_request
        return self

    def _handle_request(self, method, url, *_args, **kwargs):
        """
        Handles the HTTP request by matching the method, URL, headers, and body with the registered matchers.
        If a match is found, it returns an HTTPResponse object with the corresponding status code, reason, and body.
        If no match is found, it raises a KeyError.

        Args:
            method (str): The HTTP method of the request.
            url (str): The URL of the request.
            *_args: Additional positional arguments (not used in this method).
            **kwargs: Additional keyword arguments, including headers and body.

        Returns:
            urllib3.response.HTTPResponse: The HTTP response object.

        Raises:
            KeyError: If no handler is found for the given method and URL.
        """

        self.request_history.append({"method": method, "url": url, **kwargs})
        if "json" in kwargs:
            sent_body = kwargs["json"]
        elif kwargs.get("body") is not None:
            sent_body = loads(kwargs["body"])
        else:
            sent_body = None

        for matcher in self._matchers:
            if matcher["method"] != method or not is_url_match(matcher["url"], url):
                continue

            if matcher["requestHeaders"] is not None and (
                "headers" in kwargs and matcher["requestHeaders"].items() > kwargs["headers"].items()
            ):
                continue

            if matcher["expectedRequestBody"] is not None and matcher["expectedRequestBody"] != sent_body:
                continue

            return urllib3.response.HTTPResponse(
                status=matcher["statusCode"],
                reason=UrllibMocker.STATUS_CODE_REASON_MAP[matcher["statusCode"]],
                body=matcher["body"],
            )

        # Note: Use for debgging
        # eprint([matcher["url"] for matcher in self._matchers])

        eprint(f"No handler found for {method} {url} with body {dumps(sent_body, indent=4)}")

        raise KeyError(f"No handler found for {method} {url}")

    # follows `requests-mock` API
    # pylint: disable-next=too-many-arguments,too-many-positional-arguments
    def register_uri(
        self,
        method,
        url,
        request_headers=None,
        expected_request_body=None,
        status_code=200,
        json=None,
        text=None,
        body=None,
    ):
        """
        Register a URI matcher for HTTP requests.

        Args:
            method (str): The HTTP method of the request.
            url (str): The URL of the request.
            request_headers (dict, optional): The headers of the request. Defaults to None.
            expected_request_body (str, optional): The expected request body. Defaults to None.
            status_code (int, optional): The status code to return. Defaults to 200.
            json (dict, optional): The JSON response body. Defaults to None.
            text (str, optional): The text response body. Defaults to None.
            body (bytes, optional): The response body as bytes. Defaults to None.
        """

        matcher = {
            "method": method,
            "url": url,
            "requestHeaders": request_headers,
            "expectedRequestBody": expected_request_body,
            "statusCode": status_code,
            "body": b"",
        }
        if json is not None:
            matcher["body"] = dumps(json).encode("utf-8")
        elif text is not None:
            matcher["body"] = text.encode("utf-8")
        elif body is not None:
            matcher["body"] = body

        self._matchers.append(matcher)

    def get(self, url, **kwargs):
        self.register_uri("GET", url, **kwargs)

    def post(self, url, **kwargs):
        self.register_uri("POST", url, **kwargs)

    def delete(self, url, **kwargs):
        self.register_uri("DELETE", url, **kwargs)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._mock_context.__exit__(exc_type, exc_val, exc_tb)


class UtilTests(unittest.TestCase):
    """Utilities test runner."""

    def test_is_url_match(self):
        self.assertTrue(is_url_match("http://example.com?a=1234&b=hello", "http://example.com?b=hello&a=1234"))
        self.assertFalse(is_url_match("http://example.com?a=1234&b=hello", "http://example.de?b=hello&a=1234"))
        self.assertFalse(is_url_match("http://example.com:80?a=1234&b=hello", "http://example.com:443b=hello&a=1234"))
        self.assertFalse(is_url_match("http://example.com/x", "http://example.com/y"))


if __name__ == "__main__":
    unittest.main()
