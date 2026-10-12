"""Tests that the deprecated `workflow` names still work and emit a `DeprecationWarning`"""

import importlib
import sys
import unittest
from uuid import UUID

import geoengine as ge
from geoengine.resource_identifier import LayerId, LayerProviderId

from . import UrllibMocker

SESSION_ID = "c4983c3e-9b53-47ae-bda9-382223bd5081"
PROCESSING_GRAPH_ID = "5b9508a8-bd34-5a1c-acd6-75bb832d2d38"
PROCESSING_GRAPH_DEFINITION = {
    "type": "Raster",
    "operator": {"type": "GdalSource", "params": {"data": "ndvi"}},
}
RESULT_DESCRIPTOR = {
    "type": "raster",
    "dataType": "U8",
    "spatialReference": "EPSG:4326",
    "spatialGrid": {
        "descriptor": "source",
        "spatialGrid": {
            "geoTransform": {
                "originCoordinate": {"x": 0.0, "y": 0.0},
                "xPixelSize": 1.0,
                "yPixelSize": -1.0,
            },
            "gridBounds": {
                "topLeftIdx": {"xIdx": 0, "yIdx": 0},
                "bottomRightIdx": {"xIdx": 10, "yIdx": 20},
            },
        },
    },
    "bands": [{"name": "band", "measurement": {"type": "unitless"}}],
    "time": {
        "bounds": {"start": 0, "end": 100000},
        "dimension": {"type": "irregular"},
    },
}


DEPRECATED_MODULES = [
    "geoengine.workflow",
    "geoengine.raster_workflow_rio_writer",
    "geoengine.workflow_builder",
    "geoengine.workflow_builder.operators",
    "geoengine.workflow_builder.blueprints",
]


class DeprecationTests(unittest.TestCase):
    """Test runner for deprecated names"""

    def setUp(self) -> None:
        ge.reset(False)

        # forget imported deprecated modules, so that importing them warns again
        for module_name in DEPRECATED_MODULES:
            sys.modules.pop(module_name, None)
            parent_name, _, attribute = module_name.rpartition(".")
            if parent_name in sys.modules and attribute in vars(sys.modules[parent_name]):
                delattr(sys.modules[parent_name], attribute)

    def test_deprecated_package_attributes(self):
        with self.assertWarns(DeprecationWarning):
            self.assertIs(ge.Workflow, ge.ProcessingGraph)
        with self.assertWarns(DeprecationWarning):
            self.assertIs(ge.WorkflowId, ge.ProcessingGraphId)
        with self.assertWarns(DeprecationWarning):
            self.assertIs(ge.RasterWorkflowRioWriter, ge.RasterProcessingGraphRioWriter)
        with self.assertWarns(DeprecationWarning):
            self.assertIs(ge.workflow_builder.operators.GdalSource, ge.processing_graph_builder.operators.GdalSource)

        with self.assertRaises(AttributeError):
            _ = ge.DoesNotExist  # pylint: disable=no-member

    def test_deprecated_modules(self):
        for module_name, old_name, new_object in [
            ("geoengine.workflow", "Workflow", ge.ProcessingGraph),
            ("geoengine.workflow", "WorkflowId", ge.ProcessingGraphId),
            ("geoengine.raster_workflow_rio_writer", "RasterWorkflowRioWriter", ge.RasterProcessingGraphRioWriter),
        ]:
            with self.assertWarns(DeprecationWarning):
                module = importlib.import_module(module_name)
            with self.assertWarns(DeprecationWarning):
                self.assertIs(getattr(module, old_name), new_object)
            self.setUp()

        # unchanged names are still reachable via the old module
        self.assertIs(importlib.import_module("geoengine.workflow").get_quota, ge.get_quota)

        for module_name in ["geoengine.workflow_builder.operators", "geoengine.workflow_builder.blueprints"]:
            self.setUp()
            with self.assertWarns(DeprecationWarning):
                module = importlib.import_module(module_name)
            self.assertIs(
                module, importlib.import_module(module_name.replace("workflow_builder", "processing_graph_builder"))
            )

    def test_deprecated_operator_methods(self):
        operator = ge.processing_graph_builder.operators.GdalSource("ndvi")

        with self.assertWarns(DeprecationWarning):
            processing_graph_dict = operator.to_workflow_dict()
        self.assertEqual(processing_graph_dict, operator.to_processing_graph_dict())

        with self.assertWarns(DeprecationWarning):
            other_operator = ge.processing_graph_builder.operators.Operator.from_workflow_dict(processing_graph_dict)
        self.assertEqual(other_operator.to_processing_graph_dict(), processing_graph_dict)

    def test_deprecated_layer_names(self):
        with self.assertWarns(DeprecationWarning):
            layer = ge.Layer(
                name="foo",
                description="bar",
                layer_id=LayerId("layer"),
                provider_id=LayerProviderId(UUID("ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74")),
                workflow=PROCESSING_GRAPH_DEFINITION,
                symbology=None,
                properties=[],
                metadata={},
            )
        self.assertEqual(layer.processing_graph, PROCESSING_GRAPH_DEFINITION)

        with self.assertWarns(DeprecationWarning):
            self.assertEqual(layer.workflow, PROCESSING_GRAPH_DEFINITION)

        with self.assertRaises(TypeError):
            ge.Layer(
                name="foo",
                description="bar",
                layer_id=LayerId("layer"),
                provider_id=LayerProviderId(UUID("ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74")),
                workflow=PROCESSING_GRAPH_DEFINITION,
                processing_graph=PROCESSING_GRAPH_DEFINITION,
                symbology=None,
                properties=[],
                metadata={},
            )

    def test_deprecated_functions(self):
        with UrllibMocker() as m:
            m.post(
                "http://mock-instance/anonymous",
                json={"id": SESSION_ID, "project": None, "view": None},
            )
            m.post(
                "http://mock-instance/processingGraphs",
                json={"id": PROCESSING_GRAPH_ID},
                request_headers={"Authorization": f"Bearer {SESSION_ID}"},
            )
            m.get(
                f"http://mock-instance/processingGraphs/{PROCESSING_GRAPH_ID}/metadata",
                json=RESULT_DESCRIPTOR,
                request_headers={"Authorization": f"Bearer {SESSION_ID}"},
            )
            m.get(
                f"http://mock-instance/processingGraphs/{PROCESSING_GRAPH_ID}",
                json=PROCESSING_GRAPH_DEFINITION,
                request_headers={"Authorization": f"Bearer {SESSION_ID}"},
            )

            ge.initialize("http://mock-instance")

            with self.assertWarns(DeprecationWarning):
                processing_graph = ge.register_workflow(PROCESSING_GRAPH_DEFINITION)
            self.assertIsInstance(processing_graph, ge.ProcessingGraph)
            self.assertEqual(str(processing_graph), PROCESSING_GRAPH_ID)

            with self.assertWarns(DeprecationWarning):
                processing_graph = ge.workflow_by_id(PROCESSING_GRAPH_ID)
            self.assertIsInstance(processing_graph, ge.ProcessingGraph)

            with self.assertWarns(DeprecationWarning):
                definition = processing_graph.workflow_definition()
            self.assertEqual(definition, processing_graph.definition())


if __name__ == "__main__":
    unittest.main()
