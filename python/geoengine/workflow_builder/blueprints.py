"""This module contains blueprints for workflows."""

from . import operators


def sentinel2_band(band_name, provider="5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5", utm_zone="UTM32N"):
    """Creates a workflow for a band from Sentinel 2 data."""
    band_source = operators.GdalSource(f"_:{provider}:`{utm_zone}:{band_name}`")
    return band_source


def sentinel2_cloud_free_band(
    band_name: str, provider: str = "5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5", utm_zone: str = "UTM32N"
):
    """Creates a workflow for a cloud free band from Sentinel 2 data."""

    valid_sentinel_bands = ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"]
    if band_name.upper() not in valid_sentinel_bands:
        raise ValueError(f"Invalid band name {band_name}. Valid band names are: {valid_sentinel_bands}")

    band_id = f"_:{provider}:`{utm_zone}:{band_name.upper()}`"
    scl_id = f"_:{provider}:`{utm_zone}:SCL`"
    return sentinel2_cloud_free_band_custom_input(band_id, scl_id)


def sentinel2_cloud_free_ndvi(provider="5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5", utm_zone="UTM32N"):
    """Creates a workflow for a cloud free NDVI from Sentinel 2 data."""

    nir_id = f"_:{provider}:`{utm_zone}:B08`"
    red_id = f"_:{provider}:`{utm_zone}:B04`"
    scl_id = f"_:{provider}:`{utm_zone}:SCL`"

    return sentinel2_cloud_free_ndvi_custom_input(nir_id, red_id, scl_id)


def sentinel2_cloud_free_band_custom_input(band_dataset: str, scl_dataset: str):
    """Creates a workflow for a cloud free band from Sentinel 2 data provided by the inputs."""
    band_source = operators.GdalSource(dataset=band_dataset)
    scl_source = operators.GdalSource(dataset=scl_dataset)

    scl_source_u16 = operators.RasterTypeConversion(source=scl_source, output_data_type="U16")

    # [sen2_mask == 3 |sen2_mask == 7 |sen2_mask == 8 | sen2_mask == 9 |sen2_mask == 10 |sen2_mask == 11 ]
    cloud_free = operators.Expression(
        expression="if (B == 3 || (B >= 7 && B <= 11)) { NODATA } else { A }",
        output_type="U16",
        source=operators.RasterStacker([band_source, scl_source_u16]),
    )

    return cloud_free


def sentinel2_cloud_free_ndvi_custom_input(nir_dataset: str, red_dataset: str, scl_dataset: str):
    """Creates a workflow for a cloud free NDVI from Sentinel 2 data provided by the inputs."""
    nir_source = operators.GdalSource(dataset=nir_dataset)
    red_source = operators.GdalSource(dataset=red_dataset)
    scl_source = operators.GdalSource(dataset=scl_dataset)
    scl_source_u16 = operators.RasterTypeConversion(source=scl_source, output_data_type="U16")

    # [sen2_mask == 3 |sen2_mask == 7 |sen2_mask == 8 | sen2_mask == 9 |sen2_mask == 10 |sen2_mask == 11 ]
    cloud_free = operators.Expression(
        expression="if (C == 3 || (C >= 7 && C <= 11)) { NODATA } else { (A - B) / (A + B) }",
        output_type="F32",
        source=operators.RasterStacker([nir_source, red_source, scl_source_u16]),
    )

    return cloud_free


def s2_cloud_free_aggregated_band(
    band,
    provider="5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5",
    utm_zone="UTM32N",
    granularity="days",
    window_size=1,
    aggregation_type="mean",
):
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    """Creates a workflow for a cloud free monthly band (or NDVI) from Sentinel 2 data."""
    valid_sentinel_bands = ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"]

    band_upper = band.upper()

    if band_upper not in valid_sentinel_bands and band_upper != "NDVI":
        raise ValueError(f"Invalid band: {band_upper}")

    if band_upper == "NDVI":
        s2_cloud_free_operator = sentinel2_cloud_free_ndvi(provider, utm_zone)
    else:
        s2_cloud_free_operator = sentinel2_cloud_free_band(band_upper, provider, utm_zone)

    return operators.TemporalRasterAggregation(
        source=s2_cloud_free_operator,
        aggregation_type=aggregation_type,
        granularity=granularity,
        window_size=window_size,
        ignore_no_data=True,
        output_type="F32",
    )


def s2_cloud_free_aggregated_band_custom_input(
    band_id: str, scl_id: str, granularity="days", window_size=1, aggregation_type="mean"
):
    """Creates a workflow for a cloud free monthly band from Sentinel 2 data provided by the inputs."""

    s2_cloud_free_operator = sentinel2_cloud_free_band_custom_input(band_id, scl_id)

    # We could also leave out the scaling and use the I64 data directly
    # s2_cloud_free_operator = geoengine.unstable.workflow_operators.RasterTypeConversion(
    #    source=s2_cloud_free_operator,
    #    output_data_type="F32"
    # )
    # s2_cloud_free_operator = geoengine.unstable.workflow_operators.RasterScaling(
    #    source=s2_cloud_free_operator,
    #    slope = 0.0001,
    #    offset = -0.1, # this should be -0.1 but the values are too small?
    #    scaling_mode="mulSlopeAddOffset"
    # )

    monthly_s2_cloud_free_operator = operators.TemporalRasterAggregation(
        source=s2_cloud_free_operator,
        aggregation_type=aggregation_type,
        granularity=granularity,
        window_size=window_size,
        ignore_no_data=True,
        output_type="F32",
    )

    return monthly_s2_cloud_free_operator


def s2_cloud_free_aggregated_ndvi_custom_input(
    nir_dataset: str, red_dataset: str, scl_dataset: str, granularity="days", window_size=1, aggregation_type="mean"
):
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    """Creates a workflow for a cloud free monthly NDVI from Sentinel 2 data provided by the inputs."""

    s2_cloud_free_operator = sentinel2_cloud_free_ndvi_custom_input(nir_dataset, red_dataset, scl_dataset)

    monthly_s2_cloud_free_operator = operators.TemporalRasterAggregation(
        source=s2_cloud_free_operator,
        aggregation_type=aggregation_type,
        granularity=granularity,
        window_size=window_size,
        ignore_no_data=True,
        output_type="F32",
    )

    return monthly_s2_cloud_free_operator
