#!/bin/python3

"""
Post-processing of generated code.
"""

from pathlib import Path
import re
from collections.abc import Generator
from textwrap import dedent, indent
from util import FileModifier, modify_files, version

INDENT = "    "


def file_modifications() -> Generator[tuple[Path, FileModifier], None, None]:
    """Return a generator of file paths and their corresponding modification functions."""

    yield Path("models/RasterOperator.ts"), raster_operator_ts
    yield Path("models/VectorOperator.ts"), vector_operator_ts
    yield Path("models/LayerUpdate.ts"), layer_update_ts
    yield Path("models/TypedOperator.ts"), typed_operator_ts
    yield Path("models/HistogramBounds.ts"), histogram_bounds_ts
    yield Path("models/AttributeSelectionNames.ts"), attribute_selection_names_ts
    yield Path("runtime.ts"), runtime_ts


def main():
    """Main function to perform file modifications."""
    modify_files(file_modifications(), Path("typescript/src"), Path("typescript/diffs"))


def raster_operator_ts(
    file_contents: list[str],
) -> Generator[str, None, None]:
    """Modify the RasterOperator.ts file."""
    yield from operator_union_ts(file_contents, "RasterOperator")


def vector_operator_ts(
    file_contents: list[str],
) -> Generator[str, None, None]:
    """Modify the VectorOperator.ts file."""
    yield from operator_union_ts(file_contents, "VectorOperator")


def operator_union_ts(
    file_contents: list[str], union_name: str
) -> Generator[str, None, None]:
    """
    Add the missing `instanceOf` check for a union of operators.

    An operator belongs to the union if its `type` is one of the variants of the union,
    which are read from the generated type definition.
    """
    variants: list[str] = []
    for line in file_contents:
        if line.startswith(f"export type {union_name} ="):
            variants = re.findall(r"\{ type: '(\w+)' \}", line)
        yield line

    if not variants:
        raise ValueError(f"Could not find the variants of `{union_name}`")

    variant_list = ", ".join(f"'{variant}'" for variant in variants)

    new_lines = dedent(f"""\

        const {union_name}Types: ReadonlySet<string> = new Set([{variant_list}]);

        /**
        * Check if a given object implements the {union_name} interface.
        */
        export function instanceOf{union_name}(value: object): value is {union_name} {{
            return 'type' in value && typeof value.type === 'string' && {union_name}Types.has(value.type);
        }}
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
                    'User-Agent': 'geoengine/api-client/typescript/{version()}'
                }}
            }});
            """)

        yield line


# fixes due to https://github.com/OpenAPITools/openapi-generator/issues/14831
def layer_update_ts(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the LayerUpdate.ts file."""
    yield from vec_update_ts(file_contents, "instanceOfProjectLayerUpdate")


def vec_update_ts(
    file_contents: list[str], instance_of_content: str
) -> Generator[str, None, None]:
    """Guard the content check of a `VecUpdate` against the string tokens."""
    for line in file_contents:
        dedented_line = dedent(line)

        if dedented_line.startswith(f"if ({instance_of_content}(value)) {{"):
            line = indent(
                f"if (typeof value === 'object' && {instance_of_content}(value)) {{\n",
                INDENT,
            )

        yield line


def histogram_bounds_ts(file_contents: list[str]) -> Generator[str, None, None]:
    """Fix TS2352 in HistogramBoundsToJSONTyped for Data | object oneOf."""
    for line in file_contents:
        yield line.replace(
            "DataToJSON(value as Data)", "DataToJSON(value as unknown as Data)"
        )


def attribute_selection_names_ts(
    file_contents: list[str],
) -> Generator[str, None, None]:
    """Use an array instead of a `Set` for `uniqueItems`, since the order of the values matters."""
    for line in file_contents:
        yield (
            line.replace("values: Set<string>;", "values: Array<string>;")
            .replace("new Set(json['values'])", "json['values']")
            .replace("Array.from(value['values'] as Set<any>)", "value['values']")
        )


if __name__ == "__main__":
    main()
