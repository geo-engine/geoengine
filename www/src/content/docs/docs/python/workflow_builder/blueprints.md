---
sidebar_label: blueprints
title: workflow_builder.blueprints
---

This module contains blueprints for workflows.

#### sentinel2\_band

```python
def sentinel2_band(band_name,
                   provider="5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5",
                   utm_zone="UTM32N")
```

Creates a workflow for a band from Sentinel 2 data.

#### sentinel2\_cloud\_free\_band

```python
def sentinel2_cloud_free_band(
        band_name: str,
        provider: str = "5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5",
        utm_zone: str = "UTM32N")
```

Creates a workflow for a cloud free band from Sentinel 2 data.

#### sentinel2\_cloud\_free\_ndvi

```python
def sentinel2_cloud_free_ndvi(provider="5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5",
                              utm_zone="UTM32N")
```

Creates a workflow for a cloud free NDVI from Sentinel 2 data.

#### sentinel2\_cloud\_free\_band\_custom\_input

```python
def sentinel2_cloud_free_band_custom_input(band_dataset: str,
                                           scl_dataset: str)
```

Creates a workflow for a cloud free band from Sentinel 2 data provided by the inputs.

#### sentinel2\_cloud\_free\_ndvi\_custom\_input

```python
def sentinel2_cloud_free_ndvi_custom_input(nir_dataset: str, red_dataset: str,
                                           scl_dataset: str)
```

Creates a workflow for a cloud free NDVI from Sentinel 2 data provided by the inputs.

#### s2\_cloud\_free\_aggregated\_band

```python
def s2_cloud_free_aggregated_band(
        band,
        provider="5779494c-f3a2-48b3-8a2d-5fbba8c5b6c5",
        utm_zone="UTM32N",
        granularity="days",
        window_size=1,
        aggregation_type="mean")
```

Creates a workflow for a cloud free monthly band (or NDVI) from Sentinel 2 data.

#### s2\_cloud\_free\_aggregated\_band\_custom\_input

```python
def s2_cloud_free_aggregated_band_custom_input(band_id: str,
                                               scl_id: str,
                                               granularity="days",
                                               window_size=1,
                                               aggregation_type="mean")
```

Creates a workflow for a cloud free monthly band from Sentinel 2 data provided by the inputs.

#### s2\_cloud\_free\_aggregated\_ndvi\_custom\_input

```python
def s2_cloud_free_aggregated_ndvi_custom_input(nir_dataset: str,
                                               red_dataset: str,
                                               scl_dataset: str,
                                               granularity="days",
                                               window_size=1,
                                               aggregation_type="mean")
```

Creates a workflow for a cloud free monthly NDVI from Sentinel 2 data provided by the inputs.
