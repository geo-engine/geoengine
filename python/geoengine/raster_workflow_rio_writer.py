"""Deprecated: use `geoengine.raster_processing_graph_rio_writer` instead."""

from geoengine._deprecation import deprecated_module_getattr, warn_deprecated_module

warn_deprecated_module("geoengine.raster_workflow_rio_writer", "geoengine.raster_processing_graph_rio_writer")

__getattr__ = deprecated_module_getattr(
    __name__,
    {
        "RasterWorkflowRioWriter": "geoengine.raster_processing_graph_rio_writer.RasterProcessingGraphRioWriter",
    },
    fallback_module="geoengine.raster_processing_graph_rio_writer",
)
