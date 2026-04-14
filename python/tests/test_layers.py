"""Tests for the layers module."""

import unittest
from uuid import UUID

import geoengine as ge
from geoengine import BadRequestException, StoredDataset, api
from geoengine.layers import Layer, LayerId, LayerProviderId
from geoengine.permissions import REGISTERED_USER_ROLE_ID, Permission, PermissionListing, Role
from geoengine.resource_identifier import LAYER_DB_PROVIDER_ID, Resource
from geoengine.tasks import TaskStatus
from geoengine.types import RasterSymbology
from tests.ge_test import GeoEngineTestInstance


class LayerTests(unittest.TestCase):
    """Layer test runner."""

    def setUp(self) -> None:
        """Set up the geo engine session."""
        ge.reset(False)

    def test_layer(self):
        """Test `add_layer`."""

        expected = ge.Layer(
            name="Natural Earth II – RGB",
            description="A raster with three bands for RGB visualization",
            layer_id=LayerId("83866f7b-dcee-47b8-9242-e5636ceaf402"),
            provider_id=LAYER_DB_PROVIDER_ID,
            metadata={},
            properties=[],
            workflow={
                "operator": {
                    "params": {"renameBands": {"type": "rename", "values": ["blue", "green", "red"]}},
                    "sources": {
                        "rasters": [
                            {"type": "GdalSource", "params": {"data": "ne2_raster_blue", "overviewLevel": None}},
                            {"type": "GdalSource", "params": {"data": "ne2_raster_green", "overviewLevel": None}},
                            {"type": "GdalSource", "params": {"data": "ne2_raster_red", "overviewLevel": None}},
                        ]
                    },
                    "type": "RasterStacker",
                },
                "type": "Raster",
            },
            symbology=ge.RasterSymbology(
                raster_colorizer=ge.MultiBandRasterColorizer(
                    red_band=2,
                    green_band=1,
                    blue_band=0,
                    red_min=0.0,
                    red_max=255.0,
                    red_scale=1.0,
                    green_min=0.0,
                    green_max=255.0,
                    green_scale=1.0,
                    blue_min=0.0,
                    blue_max=255.0,
                    blue_scale=1.0,
                ),
                opacity=1.0,
            ),
        )

        # TODO: use `enterContext(cm)` instead of `with cm:` in Python 3.11
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address())

            layer = ge.layer(LayerId("83866f7b-dcee-47b8-9242-e5636ceaf402"), LAYER_DB_PROVIDER_ID)

            self.assertEqual(layer, expected)

    def test_layer_collection(self):
        """Test `add_layer`."""

        # TODO: use `enterContext(cm)` instead of `with cm:` in Python 3.11
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address())

            layer_collection = ge.layer_collection(
                LayerId("272bf675-2e27-4412-824c-287c1e6841ac"), LAYER_DB_PROVIDER_ID
            )

            print(layer_collection)

            self.assertEqual(
                layer_collection,
                ge.LayerCollection(
                    name="A test collection",
                    description="Some layers for testing and an empty subcollection",
                    collection_id=ge.LayerCollectionId("272bf675-2e27-4412-824c-287c1e6841ac"),
                    provider_id=LAYER_DB_PROVIDER_ID,
                    items=[
                        ge.LayerCollectionListing(
                            listing_id=ge.LayerCollectionId("a29f77cc-51ce-466b-86ef-d0ab2170bc0a"),
                            provider_id=LAYER_DB_PROVIDER_ID,
                            name="An empty collection",
                            description="There is nothing here",
                        ),
                        ge.LayerListing(
                            listing_id=ge.LayerCollectionId("52ef9e16-acd1-4c61-9a80-7d5b335d0d5a"),
                            provider_id=LAYER_DB_PROVIDER_ID,
                            name="Natural Earth II – R",
                            description="A raster with one band (R from RGB)",
                        ),
                        ge.LayerListing(
                            listing_id=ge.LayerCollectionId("83866f7b-dcee-47b8-9242-e5636ceaf402"),
                            provider_id=LAYER_DB_PROVIDER_ID,
                            name="Natural Earth II – RGB",
                            description="A raster with three bands for RGB visualization",
                        ),
                        ge.LayerListing(
                            listing_id=ge.LayerId("b75db46e-2b9a-4a86-b33f-bc06a73cd711"),
                            provider_id=LAYER_DB_PROVIDER_ID,
                            name="Ports in Germany",
                            description="Natural Earth Ports point filtered with Germany polygon",
                        ),
                        ge.LayerListing(
                            listing_id=ge.LayerId("c078db52-2dc6-4838-ad75-340cefeab476"),
                            provider_id=LAYER_DB_PROVIDER_ID,
                            name="Stacked Raster",
                            description="A raster with two bands for testing",
                        ),
                    ],
                ),
            )

    def test_layer_collection_modification(self):
        """Test addition and removal to a data collection."""

        # TODO: use `enterContext(cm)` instead of `with cm:` in Python 3.11
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address(), credentials=("admin@localhost", "adminadmin"))

            root_of_layerdb = ge.layer_collection("05102bb3-a855-4a37-8a8a-30026a91fef1")

            root_of_layerdb.add_collection("my test collection", "test description")

            test_collection = next(filter(lambda item: item.name == "my test collection", root_of_layerdb.items)).load()

            test_collection.add_collection("sub collection", "another description")

            test_collection.add_layer(
                name="ports clone",
                description="test description",
                workflow={
                    "type": "Vector",
                    "operator": {
                        "type": "PointInPolygonFilter",
                        "params": {},
                        "sources": {
                            "points": {
                                "type": "OgrSource",
                                "params": {
                                    "data": "ne_10m_ports",
                                    "attributeProjection": None,
                                    "attributeFilters": None,
                                },
                            },
                            "polygons": {
                                "type": "OgrSource",
                                "params": {
                                    "data": "germany_outline",
                                    "attributeProjection": None,
                                    "attributeFilters": None,
                                },
                            },
                        },
                    },
                },
                symbology=None,
            )

            sub_collection = test_collection.items[0].load()

            sub_collection.add_existing_layer(test_collection.items[1])

            layer_collection_id = sub_collection.add_collection("sub sub collection", "yet another description")

            test_collection.add_existing_collection(layer_collection_id)

            test_collection.remove_item(0)

            test_collection.remove_item(0)

            test_collection.remove_item(0)

            test_collection.remove()

    def test_save_as_dataset(self):
        """Test `layer.save_as_dataset`."""

        # TODO: use `enterContext(cm)` instead of `with cm:` in Python 3.11
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address())

            # Success case
            layer = ge.layer(LayerId("52ef9e16-acd1-4c61-9a80-7d5b335d0d5a"), LAYER_DB_PROVIDER_ID)

            task = layer.save_as_dataset()
            task_status = task.wait_for_finish()
            self.assertEqual(task_status.status, TaskStatus.COMPLETED)
            stored_dataset = StoredDataset.from_response(task_status.info)
            self.assertNotEqual(stored_dataset.dataset_name, "")
            self.assertNotEqual(stored_dataset.upload_id, "")

            # Some processing error occurred (e.g., layer does not exist)
            layer = ge.Layer(
                name="Test Error Raster Layer",
                description="Test Error Raster Layer Description",
                layer_id=ge.LayerId(UUID("86c81654-e572-42ed-96ee-8b38ebcd84ab")),
                provider_id=ge.LayerProviderId(UUID("ac50ed0d-c9a0-41f8-9ce8-35fc9e38299b")),
                workflow={
                    "operator": {"params": {"data": "ndvi", "overviewLevel": None}, "type": "GdalSource"},
                    "type": "Raster",
                },
                symbology=None,
                properties=[],
                metadata={},
            )

            with self.assertRaises(BadRequestException):
                layer.save_as_dataset()

    def test_layer_repr_html_does_not_crash(self):
        """Test `layer._repr_html_`."""

        layer = Layer(
            name="Test Raster Layer",
            description="Test Raster Layer Description",
            layer_id=LayerId(UUID("9ee3619e-d0f9-4ced-9c44-3d407c3aed69")),
            provider_id=LayerProviderId(UUID("ac50ed0d-c9a0-41f8-9ce8-35fc9e38299b")),
            workflow={
                "operator": {"params": {"data": "ndvi", "overviewLevel": None}, "type": "GdalSource"},
                "type": "Raster",
            },
            symbology=RasterSymbology.from_response(
                api.Symbology(
                    api.RasterSymbology(
                        type="raster",
                        opacity=1,
                        rasterColorizer=api.RasterColorizer(
                            api.SingleBandRasterColorizer(
                                type="singleBand",
                                band=0,
                                bandColorizer=api.Colorizer(
                                    api.LinearGradient(
                                        type="linearGradient",
                                        no_data_color=[0, 0, 0, 0],
                                        over_color=[0, 0, 0, 0],
                                        under_color=[0, 0, 0, 0],
                                        breakpoints=[
                                            api.Breakpoint(value=0.0, color=[0, 0, 0, 0]),
                                            api.Breakpoint(value=1.0, color=[0, 0, 0, 0]),
                                        ],
                                    )
                                ),
                            )
                        ),
                    )
                )
            ),
            properties=[],
            metadata={},
        )

        _html = layer._repr_html_()  # pylint: disable=protected-access

        layer.symbology = RasterSymbology.from_response(
            api.Symbology(
                api.RasterSymbology(
                    type="raster",
                    opacity=1,
                    raster_colorizer=api.RasterColorizer(
                        api.SingleBandRasterColorizer(
                            type="singleBand",
                            band=0,
                            band_colorizer=api.Colorizer(
                                api.PaletteColorizer(
                                    type="palette",
                                    no_data_color=[0, 0, 0, 0],
                                    colors={
                                        "0": [0, 0, 0, 0],
                                        "1": [0, 0, 0, 0],
                                    },
                                    default_color=[0, 0, 0, 0],
                                )
                            ),
                        )
                    ),
                )
            )
        )

        _html = layer._repr_html_()  # pylint: disable=protected-access

    def test_layer_collection_advanced_modification(self):
        """Test addition and overwrite to a data collection."""

        permisions = [(REGISTERED_USER_ROLE_ID, Permission.READ)]

        # TODO: use `enterContext(cm)` instead of `with cm:` in Python 3.11
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address(), credentials=("admin@localhost", "adminadmin"))

            root_of_layerdb = ge.layer_collection("05102bb3-a855-4a37-8a8a-30026a91fef1")

            # create a new collection with permissions
            created_collection = root_of_layerdb.get_or_create_unique_collection(
                "my test collection",
                create_collection_description="the first collection description",
                create_permissions_tuples=permisions,
            )

            root_of_layerdb = root_of_layerdb.reload()

            collection_in = root_of_layerdb.get_items_by_name("my test collection")[0].load()

            self.assertEqual(created_collection.description, "the first collection description")
            self.assertEqual(created_collection, collection_in)

            expected_permission = PermissionListing(
                role=Role(role_name="user", role_id=REGISTERED_USER_ROLE_ID),
                resource=Resource.from_layer_collection_id(created_collection.collection_id),
                permission=Permission.READ,
            )

            self.assertIn(
                expected_permission,
                ge.permissions.list_permissions(Resource.from_layer_collection_id(created_collection.collection_id)),
            )

            # get the existing collection (no overwrite)
            existing_collection = root_of_layerdb.get_or_create_unique_collection(
                "my test collection",
                create_collection_description="the second collection description",
                create_permissions_tuples=permisions,
            )

            root_of_layerdb = root_of_layerdb.reload()

            collection_in = root_of_layerdb.get_items_by_name("my test collection")[0].load()
            self.assertEqual(existing_collection.description, "the first collection description")
            self.assertEqual(existing_collection, collection_in)

            # now overwrite existing collection
            overwrite_collection = root_of_layerdb.get_or_create_unique_collection(
                "my test collection",
                create_collection_description="the third collection description",
                create_permissions_tuples=permisions,
                delete_existing_with_same_name=True,
            )

            root_of_layerdb = root_of_layerdb.reload()

            collection_in = root_of_layerdb.get_items_by_name("my test collection")[0].load()
            self.assertEqual(overwrite_collection.description, "the third collection description")
            self.assertEqual(overwrite_collection, collection_in)

            new_layer = overwrite_collection.add_layer_with_permissions(
                name="ports clone",
                description="test description",
                workflow={
                    "type": "Vector",
                    "operator": {
                        "type": "PointInPolygonFilter",
                        "params": {},
                        "sources": {
                            "points": {
                                "type": "OgrSource",
                                "params": {
                                    "data": "ne_10m_ports",
                                    "attributeProjection": None,
                                    "attributeFilters": None,
                                },
                            },
                            "polygons": {
                                "type": "OgrSource",
                                "params": {
                                    "data": "germany_outline",
                                    "attributeProjection": None,
                                    "attributeFilters": None,
                                },
                            },
                        },
                    },
                },
                symbology=None,
                permission_tuples=permisions,
            )

            expected_permission = PermissionListing(
                role=Role(role_name="user", role_id=REGISTERED_USER_ROLE_ID),
                resource=Resource.from_layer_id(new_layer),
                permission=Permission.READ,
            )
            self.assertIn(expected_permission, ge.permissions.list_permissions(Resource.from_layer_id(new_layer)))


if __name__ == "__main__":
    unittest.main()
