#!/bin/python3

"""
Post-processing of generated code.
"""

from pathlib import Path
from typing import Literal
from collections.abc import Generator
from textwrap import dedent, indent
from util import FileModifier, modify_files, version
from enum import Flag, auto

INDENT = "    "
HALF_INDENT = "  "


def file_modifications() -> Generator[tuple[Path, FileModifier], None, None]:
    """Return a generator of file paths and their corresponding modification functions."""

    yield Path("api_client.py"), api_client_py
    yield Path("api/layers_api.py"), layers_api_py
    yield Path("api/tasks_api.py"), tasks_api_py
    yield Path("exceptions.py"), exceptions_py
    yield Path("models/plot_result_descriptor.py"), plot_result_descriptor_py
    yield Path("models/raster_result_descriptor.py"), raster_result_descriptor_py
    yield Path("models/task_status_with_id.py"), task_status_with_id_py
    yield Path("models/time_step.py"), time_step_py
    yield Path("models/vector_result_descriptor.py"), vector_result_descriptor_py
    yield Path("models/workflow.py"), workflow_py


def main():
    """Main function to perform file modifications."""
    modify_files(
        file_modifications(),
        Path("python/geoengine_openapi_client"),
        Path("python/diffs"),
    )


def workflow_py(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the workflow.py file."""

    class Method(Flag):
        ACTUAL_INSTANCE_MUST_VALIDATE_ONEOF = auto()
        FROM_JSON = auto()
        OTHER = auto()

    method = Method.OTHER

    for line in file_contents:
        dedented_line = dedent(line)

        if dedented_line.startswith("def actual_instance_must_validate_oneof"):
            method = Method.ACTUAL_INSTANCE_MUST_VALIDATE_ONEOF
        elif dedented_line.startswith("def from_json("):
            method = Method.FROM_JSON
        elif dedented_line.startswith("def "):
            method = Method.OTHER
        elif (
            method == Method.ACTUAL_INSTANCE_MUST_VALIDATE_ONEOF
            and dedented_line.startswith("match += 1")
        ):
            line = indent("return v", 3 * INDENT) + "\n"
        elif method == Method.FROM_JSON and dedented_line.startswith("match += 1"):
            line = indent("return instance", 3 * INDENT) + "\n"

        yield line


def api_client_py(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the api_client.py file."""
    for line in file_contents:
        dedented_line = dedent(line)

        if dedented_line.startswith("self.user_agent = "):
            line = indent(
                dedent(f"""\
            self.user_agent = 'geoengine/openapi-client/python/{version()}'
            """),
                2 * INDENT,
            )

        yield line


def exceptions_py(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the exceptions.py file."""
    for line in file_contents:
        if dedent(line).startswith('"""Custom error messages for exception"""'):
            line = (
                line
                + "\n"
                + indent(
                    dedent("""\
            # Note: changed message formatting
            import json
            parsed_body = json.loads(self.body)
            return f'{parsed_body["error"]}: {parsed_body["message"]}'
            
            """),
                    2 * INDENT,
                )
            )

        yield line


def task_status_with_id_py(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the task_status_with_id.py file."""
    for line in file_contents:
        dedented_line = dedent(line)
        if dedented_line.startswith("if self.info is None and"):
            line = indent(
                dedent("""\
            # Note: fixed handling of actual_instance
            if getattr(self.actual_instance, "info", None) is None and "info" in self.actual_instance.__fields_set__:
            """),
                2 * INDENT,
            )

        elif dedented_line.startswith("if self.clean_up is None and"):
            line = indent(
                dedent("""\
            # Note: fixed handling of actual_instance
            if getattr(self.actual_instance, "clean_up", None) is None and "clean_up" in self.actual_instance.__fields_set__:
            """),
                2 * INDENT,
            )

        elif dedented_line.startswith("if self.error is None and"):
            line = indent(
                dedent("""\
            # Note: fixed handling of actual_instance
            if getattr(self.actual_instance, "error", None) is None and "error" in self.actual_instance.__fields_set__:
            """),
                2 * INDENT,
            )

        elif dedented_line.startswith("_obj = cls.model_validate({"):
            line = (
                indent(
                    dedent("""\
            # Note: fixed handling of actual_instance
            _obj = cls.model_validate({
                "actual_instance": TaskStatus.from_dict(obj).actual_instance,
                "task_id": obj.get("taskId")
            })
            return _obj
            """),
                    2 * INDENT,
                )
                + "\n"
                + line
            )

        yield line


EARLY_RETURN_EMPTY_HTTP_RESPONSE = indent(
    dedent("""\
    # Note: fixed handling of empty responses
    if response_data.data is None:
        return None
    """),
    2 * INDENT,
)


def tasks_api_py(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the tasks_api.py file."""
    state: Literal[None, "abort_handler"] = None
    for line in file_contents:
        dedented_line = dedent(line)
        if state is None and dedented_line.startswith("def abort_handler("):
            state = "abort_handler"

        elif state == "abort_handler" and dedented_line.startswith(
            "return self.api_client.response_deserialize("
        ):
            line = EARLY_RETURN_EMPTY_HTTP_RESPONSE + "\n" + line
            state = None

        yield line


def layers_api_py(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the tasks_api.py file."""
    state: Literal[
        None,
        "add_early_return_empty_http_response",
    ] = None
    for line in file_contents:
        dedented_line = dedent(line)
        if state is None and (
            dedented_line.startswith("def add_existing_layer_to_collection(")
            or dedented_line.startswith("def add_existing_collection_to_collection(")
            or dedented_line.startswith("def remove_collection_from_collection(")
            or dedented_line.startswith("def remove_layer_from_collection(")
            or dedented_line.startswith("def remove_collection(")
        ):
            state = "add_early_return_empty_http_response"

        elif (
            state == "add_early_return_empty_http_response"
            and dedented_line.startswith("return self.api_client.response_deserialize(")
        ):
            line = EARLY_RETURN_EMPTY_HTTP_RESPONSE + "\n" + line
            state = None

        yield line


def raster_result_descriptor_py(file_contents: list[str]) -> Generator[str, None, None]:
    """
    Modify the raster_result_descriptor.py file.

    TODO: remove when bug is fixed:
    https://github.com/OpenAPITools/openapi-generator/issues/19926
    """

    for line in file_contents:
        dedented_line = dedent(line)
        if dedented_line.startswith(
            '"""Create an instance of RasterResultDescriptor from a dict"""'
        ):
            line = (
                line
                + "\n"
                + indent(
                    dedent("""\
            # Note: fixed <https://github.com/OpenAPITools/openapi-generator/issues/19926>
            if obj is None:
                return None

            if not isinstance(obj, dict):
                return cls.model_validate(obj)

            _obj = cls.model_validate({
                "bands": [RasterBandDescriptor.from_dict(_item) for _item in obj["bands"]] if obj.get("bands") is not None else None,
                "spatialGrid": SpatialGridDescriptor.from_dict(obj["spatialGrid"]) if obj.get("spatialGrid") is not None else None,
                "dataType": obj.get("dataType"),
                "spatialReference": obj.get("spatialReference"),
                "time": TimeDescriptor.from_dict(obj["time"]) if obj.get("time") is not None else None,
            })
            return _obj
            """),
                    2 * INDENT,
                )
            )

        yield line


def vector_result_descriptor_py(file_contents: list[str]) -> Generator[str, None, None]:
    """
    Modify the vector_result_descriptor.py file.

    TODO: remove when bug is fixed:
    https://github.com/OpenAPITools/openapi-generator/issues/19926
    """

    for line in file_contents:
        dedented_line = dedent(line)
        if dedented_line.startswith(
            '"""Create an instance of VectorResultDescriptor from a dict"""'
        ):
            line = (
                line
                + "\n"
                + indent(
                    dedent("""\
            # Note: fixed <https://github.com/OpenAPITools/openapi-generator/issues/19926>
            if obj is None:
                return None

            if not isinstance(obj, dict):
                return cls.model_validate(obj)

            _obj = cls.model_validate({
                "bbox": BoundingBox2D.from_dict(obj["bbox"]) if obj.get("bbox") is not None else None,
                "columns": dict(
                    (_k, VectorColumnInfo.from_dict(_v))
                    for _k, _v in obj["columns"].items()
                )
                if obj.get("columns") is not None
                else None,
                "dataType": obj.get("dataType"),
                "spatialReference": obj.get("spatialReference"),
                "time": TimeInterval.from_dict(obj["time"]) if obj.get("time") is not None else None,
            })
            return _obj
            """),
                    2 * INDENT,
                )
            )

        yield line


def plot_result_descriptor_py(file_contents: list[str]) -> Generator[str, None, None]:
    """
    Modify the plot_result_descriptor.py file.

    TODO: remove when bug is fixed:
    https://github.com/OpenAPITools/openapi-generator/issues/19926
    """

    for line in file_contents:
        dedented_line = dedent(line)
        if dedented_line.startswith(
            '"""Create an instance of PlotResultDescriptor from a dict"""'
        ):
            line = (
                line
                + "\n"
                + indent(
                    dedent("""\
            # Note: fixed <https://github.com/OpenAPITools/openapi-generator/issues/19926>
            if obj is None:
                return None

            if not isinstance(obj, dict):
                return cls.model_validate(obj)

            _obj = cls.model_validate({
                "bbox": BoundingBox2D.from_dict(obj["bbox"]) if obj.get("bbox") is not None else None,
                "spatialReference": obj.get("spatialReference"),
                "time": TimeInterval.from_dict(obj["time"]) if obj.get("time") is not None else None,
            })
            return _obj
            """),
                    2 * INDENT,
                )
            )

        yield line


def time_step_py(file_contents: list[str]) -> Generator[str, None, None]:
    """
    Modify the time_step.py file.
    """

    for line in file_contents:
        dedented_line = dedent(line)
        if dedented_line.startswith('"""Create an instance of TimeStep from a dict"""'):
            line = (
                line
                + "\n"
                + indent(
                    dedent("""\
            # Note: fixed handling of TimeGranularity enum
            if obj is None:
                return None

            if not isinstance(obj, dict):
                return cls.model_validate(obj)

            _obj = cls.model_validate({
                "granularity": TimeGranularity(obj["granularity"]) if obj.get("granularity") is not None else None,
                "step": obj.get("step"),
            })
            return _obj
            """),
                    2 * INDENT,
                )
            )

        yield line


if __name__ == "__main__":
    main()
