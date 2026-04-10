"""
Module for utility functions
"""

import numpy as np


def clamp_datetime_ms_ns(value: np.datetime64) -> np.datetime64:
    """Clamp a datetime64[ms] to the range of datetime64[ns] used by xarray"""

    min_date = np.datetime64("1678-09-21 00:12:43.145224192", "ns")
    max_date = np.datetime64("2262-04-11 23:47:16.854775807", "ns")

    min_date_ms = min_date.astype("datetime64[ms]")
    max_date_ms = max_date.astype("datetime64[ms]")

    if value < min_date_ms:
        return min_date

    if value > max_date_ms:
        return max_date

    return value.astype("datetime64[ns]")
