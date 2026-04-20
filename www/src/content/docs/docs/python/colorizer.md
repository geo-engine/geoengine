---
sidebar_label: colorizer
title: colorizer
---

This module is used to generate geoengine compatible color map definitions as a json string.

## ColorBreakpoint Objects

```python
@dataclass
class ColorBreakpoint()
```

This class is used to generate geoengine compatible color breakpoint definitions.

#### to_api_dict

```python
def to_api_dict() -> geoengine_api_client.Breakpoint
```

Return the color breakpoint as a dictionary.

#### from_response

```python
@staticmethod
def from_response(
        response: geoengine_api_client.Breakpoint) -> ColorBreakpoint
```

Parse a http response to a `ColorBreakpoint`.

## Colorizer Objects

```python
@dataclass
class Colorizer()
```

This class is used to generate geoengine compatible color map definitions as a json string.

#### linear_with_mpl_cmap

```python
@staticmethod
def linear_with_mpl_cmap(
    color_map: str | Colormap,
    min_max: tuple[float, float],
    n_steps: int = 10,
    over_color: Rgba = (0, 0, 0, 0),
    under_color: Rgba = (0, 0, 0, 0),
    no_data_color: Rgba = (0, 0, 0, 0)
) -> LinearGradientColorizer
```

Initialize the colorizer.

#### logarithmic_with_mpl_cmap

```python
@staticmethod
def logarithmic_with_mpl_cmap(
    color_map: str | Colormap,
    min_max: tuple[float, float],
    n_steps: int = 10,
    over_color: Rgba = (0, 0, 0, 0),
    under_color: Rgba = (0, 0, 0, 0),
    no_data_color: Rgba = (0, 0, 0, 0)
) -> LogarithmicGradientColorizer
```

Initialize the colorizer.

#### palette

```python
@staticmethod
def palette(
    color_mapping: dict[float, Rgba],
    default_color: Rgba = (0, 0, 0, 0),
    no_data_color: Rgba = (0, 0, 0, 0)
) -> PaletteColorizer
```

Initialize the colorizer.

#### palette_with_colormap

```python
@staticmethod
def palette_with_colormap(
    values: list[float],
    color_map: str | Colormap,
    default_color: Rgba = (0, 0, 0, 0),
    no_data_color: Rgba = (0, 0, 0, 0)
) -> PaletteColorizer
```

This method generates a palette colorizer from a given list of values.
A colormap can be given as an object or by name only.

#### to_json

```python
def to_json() -> str
```

Return the colorizer as a JSON string.

#### from_response

```python
@staticmethod
def from_response(response: geoengine_api_client.Colorizer) -> Colorizer
```

Create a colorizer from a response.

#### rgba_from_list

```python
def rgba_from_list(values: list[int]) -> Rgba
```

Convert a list of integers to an RGBA tuple.

## LinearGradientColorizer Objects

```python
@dataclass
class LinearGradientColorizer(Colorizer)
```

A linear gradient colorizer.

#### from_response_linear

```python
@staticmethod
def from_response_linear(
        response: geoengine_api_client.LinearGradient
) -> LinearGradientColorizer
```

Create a colorizer from a response.

#### to_api_dict

```python
def to_api_dict() -> geoengine_api_client.Colorizer
```

Return the colorizer as a dictionary.

## LogarithmicGradientColorizer Objects

```python
@dataclass
class LogarithmicGradientColorizer(Colorizer)
```

A logarithmic gradient colorizer.

#### from_response_logarithmic

```python
@staticmethod
def from_response_logarithmic(
    response: geoengine_api_client.LogarithmicGradient
) -> LogarithmicGradientColorizer
```

Create a colorizer from a response.

#### to_api_dict

```python
def to_api_dict() -> geoengine_api_client.Colorizer
```

Return the colorizer as a dictionary.

## PaletteColorizer Objects

```python
@dataclass
class PaletteColorizer(Colorizer)
```

A palette colorizer.

#### from_response_palette

```python
@staticmethod
def from_response_palette(
        response: geoengine_api_client.PaletteColorizer) -> PaletteColorizer
```

Create a colorizer from a response.

#### to_api_dict

```python
def to_api_dict() -> geoengine_api_client.Colorizer
```

Return the colorizer as a dictionary.
