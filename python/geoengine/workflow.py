"""Deprecated: use `geoengine.processing_graph` instead."""

from geoengine._deprecation import deprecated_module_getattr, warn_deprecated_module

warn_deprecated_module("geoengine.workflow", "geoengine.processing_graph")

__getattr__ = deprecated_module_getattr(
    __name__,
    {
        "Workflow": "geoengine.processing_graph.ProcessingGraph",
        "WorkflowId": "geoengine.processing_graph.ProcessingGraphId",
    },
    fallback_module="geoengine.processing_graph",
)
