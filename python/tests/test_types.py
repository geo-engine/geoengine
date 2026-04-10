"""Tests for the types module."""

import unittest
from datetime import datetime, timezone

import numpy as np

import geoengine as ge


class TypesTests(unittest.TestCase):
    """Types test runner."""

    def test_time_interval(self):
        """Test the construction of time intervals."""

        time_with_tz = datetime.strptime("2014-04-01T12:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%f%z")

        time_without_tz = datetime.strptime("2014-05-01T12:00:00.000", "%Y-%m-%dT%H:%M:%S.%f")

        with self.assertRaises(ge.InputException):
            ge.TimeInterval(time_without_tz, time_with_tz)

        time_interval_with_tz = ge.TimeInterval(time_with_tz)

        time_interval_without_tz = ge.TimeInterval(time_without_tz)

        self.assertEqual(time_interval_with_tz.time_str, "2014-04-01T12:00:00.000+00:00")

        self.assertEqual(time_interval_without_tz.time_str, "2014-05-01T12:00:00.000+00:00")

        self.assertEqual(
            ge.TimeInterval(np.datetime64("-10000-01-01T00:00:00.000")).time_str, "-10000-01-01T00:00:00.000+00:00"
        )

    def test_time_interval_with_end(self):
        """Test time intervals with start and end times."""
        start = datetime(2014, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
        end = datetime(2014, 5, 1, 12, 0, 0, tzinfo=timezone.utc)

        ti = ge.TimeInterval(start, end)
        self.assertEqual(ti.start, np.datetime64("2014-04-01T12:00:00"))
        self.assertFalse(ti.is_instant())

    def test_time_interval_invalid_range(self):
        """Test that time intervals reject invalid ranges."""
        start = datetime(2014, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        end = datetime(2014, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

        with self.assertRaises(ge.InputException):
            ge.TimeInterval(start, end)

    def test_time_interval_equality(self):
        """Test time interval equality."""
        ti1 = ge.TimeInterval(np.datetime64("2014-04-01"))
        ti2 = ge.TimeInterval(np.datetime64("2014-04-01"))
        ti3 = ge.TimeInterval(np.datetime64("2014-04-02"))

        self.assertEqual(ti1, ti2)
        self.assertNotEqual(ti1, ti3)

    def test_spatial_resolution(self):
        """Test the construction of spatial resolutions."""

        res1 = ge.SpatialResolution(10.0, 20.0)

        self.assertEqual(res1.x_resolution, 10.0)
        self.assertEqual(res1.y_resolution, 20.0)

    def test_geo_transform(self):
        """Test the construction of geo transforms."""

        gt = ge.GeoTransform(100.0, 200.0, 10.0, -20.0)

        self.assertEqual(gt.x_min, 100.0)
        self.assertEqual(gt.y_max, 200.0)
        self.assertEqual(gt.x_pixel_size, 10.0)
        self.assertEqual(gt.y_pixel_size, -20.0)

    def test_bounding_box_2d(self):
        """Test bounding box 2D."""
        bbox = ge.BoundingBox2D(0.0, 10.0, 100.0, 110.0)

        self.assertEqual(bbox.xmin, 0.0)
        self.assertEqual(bbox.ymin, 10.0)
        self.assertEqual(bbox.xmax, 100.0)
        self.assertEqual(bbox.ymax, 110.0)

    def test_spatial_bounds(self):
        """Test spatial bounds construction."""
        bounds = ge.BoundingBox2D(0.0, 10.0, 100.0, 110.0)

        self.assertEqual(bounds.x_axis_size(), 100.0)
        self.assertEqual(bounds.y_axis_size(), 100.0)

    def test_bounding_box_string_representation(self):
        """Test bounding box string representations."""
        bbox = ge.BoundingBox2D(0.0, 10.0, 100.0, 110.0)

        bbox_str = bbox.as_bbox_str()
        self.assertEqual(bbox_str, "0.0,10.0,100.0,110.0")

    def test_spatial_partition_2d(self):
        """Test spatial partition 2D."""
        partition = ge.SpatialPartition2D(0.0, 10.0, 100.0, 110.0)

        self.assertEqual(partition.xmin, 0.0)
        self.assertEqual(partition.ymin, 10.0)

    def test_spatial_partition_to_bounding_box(self):
        """Test conversion from spatial partition to bounding box."""
        partition = ge.SpatialPartition2D(0.0, 10.0, 100.0, 110.0)
        bbox = partition.to_bounding_box()

        self.assertIsInstance(bbox, ge.BoundingBox2D)
        self.assertEqual(bbox.xmin, partition.xmin)
        self.assertEqual(bbox.ymin, partition.ymin)

    def test_query_rectangle_with_bbox(self):
        """Test query rectangle construction with bounding box."""
        bbox = ge.BoundingBox2D(0.0, 10.0, 100.0, 110.0)
        time_interval = ge.TimeInterval(datetime(2014, 4, 1, tzinfo=timezone.utc))

        qr = ge.QueryRectangle(bbox, time_interval)

        self.assertEqual(qr.spatial_bounds, bbox)
        self.assertEqual(qr.time, time_interval)
        self.assertEqual(qr.srs, "EPSG:4326")

    def test_query_rectangle_to_raster(self):
        """Test conversion of query rectangle to raster query rectangle."""
        bbox = ge.BoundingBox2D(0.0, 10.0, 100.0, 110.0)
        time_interval = ge.TimeInterval(datetime(2014, 4, 1, tzinfo=timezone.utc))

        qr = ge.QueryRectangle(bbox, time_interval)
        rqr = qr.with_raster_bands([1, 2, 3])

        self.assertIsInstance(rqr, ge.RasterQueryRectangle)
        self.assertEqual(rqr.raster_bands, [1, 2, 3])

    def test_raster_query_rectangle_with_single_band(self):
        """Test raster query rectangle with single band."""
        bbox = ge.BoundingBox2D(0.0, 10.0, 100.0, 110.0)
        time_interval = ge.TimeInterval(datetime(2014, 4, 1, tzinfo=timezone.utc))

        rqr = ge.RasterQueryRectangle(bbox, time_interval, 1)
        self.assertEqual(rqr.raster_bands, [1])

    def test_raster_query_rectangle_with_no_bands(self):
        """Test raster query rectangle with no bands."""
        bbox = ge.BoundingBox2D(0.0, 10.0, 100.0, 110.0)
        time_interval = ge.TimeInterval(datetime(2014, 4, 1, tzinfo=timezone.utc))

        rqr = ge.RasterQueryRectangle(bbox, time_interval, None)
        self.assertEqual(rqr.raster_bands, [0])

    def test_grid_index(self):
        """Test the construction of grid indices."""

        gi = ge.GridIdx2D(y_idx=5, x_idx=10)

        self.assertEqual(gi.y_idx, 5)
        self.assertEqual(gi.x_idx, 10)

    def test_geo_transform_coord_to_pixel(self):
        """Test the coordinate to pixel conversion of geo transforms."""

        gt = ge.GeoTransform(100.0, 200.0, 10.0, -10.0)

        grid_idx = gt.coord_to_pixel_ul(150.0, 100.0)

        self.assertEqual(grid_idx.x_idx, 5)
        self.assertEqual(grid_idx.y_idx, 10)

    def test_geo_transform_pixel_to_coord(self):
        """Test the pixel to coordinate conversion of geo transforms."""
        gt = ge.GeoTransform(100.0, 200.0, 10.0, -10.0)

        x, y = gt.pixel_ul_to_coord(5, 10)

        self.assertEqual(x, 150.0)
        self.assertEqual(y, 100.0)


if __name__ == "__main__":
    unittest.main()
