#!/bin/python3

"""
Post-processing of generated code.
"""

from pathlib import Path
from collections.abc import Generator
from textwrap import dedent, indent
from util import FileModifier, modify_files, version

INDENT = "    "


def file_modifications() -> Generator[tuple[Path, FileModifier], None, None]:
    """Return a generator of file paths and their corresponding modification functions."""

    yield Path("models/RasterOperator.ts"), raster_operator_ts
    yield Path("models/VectorOperator.ts"), vector_operator_ts
    yield Path("models/TaskStatusWithId.ts"), task_status_with_id_ts
    yield Path("models/VecUpdate.ts"), vec_update_ts
    yield Path("models/TypedOperator.ts"), typed_operator_ts
    yield Path("models/HistogramBounds.ts"), histogram_bounds_ts
    yield Path("runtime.ts"), runtime_ts


def main():
    """Main function to perform file modifications."""
    modify_files(file_modifications(), Path("typescript/src"), Path("typescript/diffs"))


def raster_operator_ts(
    file_contents: list[str],
) -> Generator[str, None, None]:
    """Modify the RasterOperator.ts file."""
    for line in file_contents:
        yield line

    new_lines = dedent("""\
                       
        /**
        * Check if a given object implements the RasterOperator interface.
        */
        export function instanceOfRasterOperator(value: object): value is RasterOperator {
            return instanceOfExpression(value)
                || instanceOfGdalSource(value);
        }
    """).splitlines(keepends=True)

    for line in new_lines:
        yield line


def vector_operator_ts(
    file_contents: list[str],
) -> Generator[str, None, None]:
    """Modify the VectorOperator.ts file."""
    for line in file_contents:
        yield line

    new_lines = dedent("""\
                       
        /**
        * Check if a given object implements the VectorOperator interface.
        */
        export function instanceOfVectorOperator(value: object): value is VectorOperator {
            return instanceOfMockPointSource(value)
                || instanceOfRasterVectorJoin(value);
        }
    """).splitlines(keepends=True)

    for line in new_lines:
        yield line


def typed_operator_ts(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the TypedOperator.ts file."""
    for line in file_contents:
        yield line

    new_lines = dedent("""\
                       
        /**
        * Check if a given object implements the TypedOperator interface.
        */
        export function instanceOfTypedOperator(value: object): value is TypedOperator {
            return instanceOfTypedPlotOperator(value)
                || instanceOfTypedRasterOperator(value)
                || instanceOfTypedVectorOperator(value);
        }
    """).splitlines(keepends=True)

    for line in new_lines:
        yield line


def runtime_ts(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the runtime.ts file."""
    for line in file_contents:
        if line.startswith("export const DefaultConfig ="):
            line = dedent(f"""\
            export const DefaultConfig = new Configuration({{
                headers: {{
                    'User-Agent': 'geoengine/openapi-client/typescript/{version()}'
                }}
            }});
            """)

        yield line


# fixes due to https://github.com/OpenAPITools/openapi-generator/issues/14831
def plot_update_ts(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the PlotUpdate.ts file."""
    for line in file_contents:
        dedented_line = dedent(line)

        if dedented_line.startswith("if (instanceOfPlot(value))"):
            line = indent(
                dedent("""\
            if (typeof value === 'object' && instanceOfPlot(value)) {
            """),
                INDENT,
            )

        yield line


# fixes due to https://github.com/OpenAPITools/openapi-generator/issues/14831
def layer_update_ts(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the LayerUpdate.ts file."""
    for line in file_contents:
        dedented_line = dedent(line)

        if dedented_line.startswith("if (instanceOfProjectLayer(value))"):
            line = indent(
                dedent("""\
            if (typeof value === 'object' && instanceOfProjectLayer(value)) {
            """),
                INDENT,
            )

        yield line


# Fix: interface cannot inherit union type
def task_status_with_id_ts(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the TaskStatusWithId.ts file."""
    for line in file_contents:
        dedented_line = dedent(line)

        if dedented_line.startswith(
            "export interface TaskStatusWithId extends TaskStatus"
        ):
            line = dedent("""\
            export type TaskStatusWithId = { taskId: string } & TaskStatus;
                          
            export interface _TaskStatusWithId /* extends TaskStatus */ {
            """)

        yield line


def vec_update_ts(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the VecUpdate.ts file."""
    for line in file_contents:
        dedented_line = dedent(line)

        if dedented_line.startswith("if (instanceOfPlot(value)) {"):
            line = indent(
                "if (typeof value === 'object' && instanceOfPlot(value)) {\n", INDENT
            )

        yield line


def histogram_bounds_ts(file_contents: list[str]) -> Generator[str, None, None]:
    """Fix TS2352 in HistogramBoundsToJSONTyped for Data | object oneOf."""
    for line in file_contents:
        yield line.replace(
            "DataToJSON(value as Data)", "DataToJSON(value as unknown as Data)"
        )


if __name__ == "__main__":
    main()
