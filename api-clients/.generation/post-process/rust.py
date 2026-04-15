#!/bin/python3

"""
Post-processing of generated code.
"""

from pathlib import Path
from collections.abc import Generator
from textwrap import dedent, indent
from util import FileModifier, modify_files

INDENT = "    "
HALF_INDENT = "  "


def file_modifications() -> Generator[tuple[Path, FileModifier], None, None]:
    """Return a generator of file paths and their corresponding modification functions."""

    yield Path("src/apis/projects_api.rs"), projects_api_rs
    yield Path("src/apis/tasks_api.rs"), tasks_api_rs
    yield Path("src/models/default.rs"), default_rs
    yield Path("src/models/spatial_partition2_d.rs"), spatial_partition2_d_rs
    yield (
        Path("src/models/multiple_raster_or_single_vector_operator.rs"),
        multiple_raster_or_single_vector_operator_rs,
    )

    for file_path in [
        Path("src/models/raster_operator.rs"),
        Path("src/models/vector_operator.rs"),
        Path("src/models/spatial_bounds_derive.rs"),
        Path("src/models/measurement.rs"),
    ]:
        yield file_path, make_untagged


def main():
    """Main function to perform file modifications."""
    modify_files(
        file_modifications(),
        Path("rust"),
        Path("rust/diffs"),
    )


# cf. <https://github.com/OpenAPITools/openapi-generator/issues/22344>
def make_untagged(file_contents: list[str]) -> Generator[str, None, None]:
    """Make the enum untagged by replacing the serde attribute."""
    for line in file_contents:
        dedented_line = dedent(line)
        if dedented_line.startswith('#[serde(tag = "type")]'):
            line = "#[serde(untagged)]" + "\n"

        yield line


def multiple_raster_or_single_vector_operator_rs(
    file_contents: list[str],
) -> Generator[str, None, None]:
    """Modify the multiple_raster_or_single_vector_operator.rs file."""
    for line in file_contents:
        dedented_line = dedent(line)
        if dedented_line.startswith("ArrayVecmodels::RasterOperator"):
            line = indent("RasterOperator(Vec<models::RasterOperator>),", INDENT) + "\n"
        elif dedented_line.startswith("Self::ArrayVecmodels::RasterOperator"):
            line = indent("Self::RasterOperator(Default::default())", 2 * INDENT) + "\n"

        yield line


def spatial_resolution_rs(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the spatial_resolution.rs file."""
    for line in file_contents:
        yield line

    yield dedent("""\
        impl std::fmt::Display for SpatialResolution {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{},{}", self.x, self.y)
            }
        }
        """)


def spatial_partition2_d_rs(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the SpatialPartition2D.rs file."""
    for line in file_contents:
        yield line

    yield dedent("""\
        impl std::fmt::Display for SpatialPartition2D {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "{xmin},{ymin},{xmax},{ymax}",
                    xmin = self.upper_left_coordinate.x,
                    ymin = self.lower_right_coordinate.y,
                    xmax = self.lower_right_coordinate.x,
                    ymax = self.upper_left_coordinate.y
                )
            }
        }
        """)


def uploads_api_rs(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the uploads_api.rs file."""
    for line in file_contents:
        dedented_line = dedent(line)

        if dedented_line.startswith(
            'multipart_form = multipart_form.file("files[]", '
            "p_form_files_left_square_bracket_right_square_bracket.as_os_str()).await?;"
        ):
            line = indent(
                "for form_file in p_form_files_left_square_bracket_right_square_bracket {\n",
                INDENT,
            )
            line += indent(
                'multipart_form = multipart_form.file("files[]", form_file).await?;\n',
                2 * INDENT,
            )
            line += indent("}\n", INDENT)

        yield line


def tasks_api_rs(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the tasks_api.rs file."""
    for line in file_contents:
        dedented_line = dedent(line)

        if dedented_line.startswith('let uri_str = format!("{}/tasks/list"'):
            line = indent(
                """\
                let uri_str = format!(
                    "{}/tasks/list?filter={}&offset={}&limit={}",
                    configuration.base_path,
                    p_path_filter.unwrap().to_string(),
                    p_path_offset,
                    p_path_limit
                );
            """,
                INDENT,
            )

        yield line


def projects_api_rs(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the projects_api.rs file."""
    for line in file_contents:
        dedented_line = dedent(line)

        if dedented_line.startswith('let uri_str = format!("{}/projects"'):
            line = indent(
                """\
                let uri_str = format!(
                    "{}/projects?order={}&offset={}&limit={}",
                    configuration.base_path,
                    p_path_order.to_string(),
                    p_path_offset,
                    p_path_limit
                );
            """,
                INDENT,
            )

        yield line


def ogcwms_api_rs(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the ogcwms_api.rs file."""
    for line in file_contents:
        dedented_line = dedent(line)

        if dedented_line.startswith(
            'let uri_str = format!("{}/wms/{workflow}?request=GetLegendGraphic"'
        ):
            line = indent(
                """\
                let uri_str = format!(
                    "{}/wms/{workflow}?request={request}&version={version}&service={service}&layer={layer}",
                    configuration.base_path,
                    workflow = crate::apis::urlencode(p_path_workflow),
                    version = p_path_version.to_string(),
                    service = p_path_service.to_string(),
                    request = p_path_request.to_string(),
                    layer = crate::apis::urlencode(p_path_layer)
                );
            """,
                INDENT,
            )
        elif dedented_line.startswith(
            'let uri_str = format!("{}/wms/{workflow}?request=GetCapabilities"'
        ):
            line = indent(
                """\
                let uri_str = format!(
                    "{}/wms/{workflow}?request={request}&service={service}&version={version}&format={format}",
                    configuration.base_path,
                    workflow = crate::apis::urlencode(p_path_workflow),
                    version = p_path_version.unwrap().to_string(),
                    service = p_path_service.to_string(),
                    request = p_path_request.to_string(),
                    format = p_path_format.unwrap().to_string()
                );
            """,
                INDENT,
            )

        yield line


def ogcwfs_api_rs(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the ogcwfs_api.rs file."""
    for line in file_contents:
        dedented_line = dedent(line)

        if dedented_line.startswith(
            'req_builder = req_builder.query(&[("service", &serde_json::to_string(param_value)?)]);'
        ):
            line = indent(
                'req_builder = req_builder.query(&[("service", &param_value.to_string())]);'
                "\n",
                2 * INDENT,
            )

        yield line


def default_rs(file_contents: list[str]) -> Generator[str, None, None]:
    """Modify the default.rs file."""
    for line in file_contents:
        if line.startswith("impl Default for Type {"):
            line = "impl std::default::Default for Type {\n"

        yield line


if __name__ == "__main__":
    main()
