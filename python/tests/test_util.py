"""Test for utility functions"""

import unittest

import numpy as np

import geoengine as ge


class TypesTests(unittest.TestCase):
    """Types test runner."""

    def test_clamp(self):
        self.assertEqual(
            ge.clamp_datetime_ms_ns(np.datetime64("1500-09-21", "ms")),
            np.datetime64("1678-09-21 00:12:43.145224192", "ns"),
        )
        self.assertEqual(
            ge.clamp_datetime_ms_ns(np.datetime64("-11500-09-21", "ms")),
            np.datetime64("1678-09-21 00:12:43.145224192", "ns"),
        )
        self.assertEqual(
            ge.clamp_datetime_ms_ns(np.datetime64("3000-09-21", "ms")),
            np.datetime64("2262-04-11 23:47:16.854775807", "ns"),
        )

        self.assertEqual(
            ge.clamp_datetime_ms_ns(np.datetime64("2000-01-02 11:22:33.44", "ms")),
            np.datetime64("2000-01-02 11:22:33.44", "ns"),
        )


if __name__ == "__main__":
    unittest.main()
