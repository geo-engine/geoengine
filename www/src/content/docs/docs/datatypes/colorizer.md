---
title: Colorizer
---

A colorizer specifies a mapping between values and pixels/objects of an output image.
Different variants of colorizers perform different kinds of mapping.
In general, there are two families of colorizers: **_gradient_** and **_palette_**.
_Gradients_ are used to interpolate a continuous spectrum of colors between explicitly stated tuples (`breakpoints`) of a value and a color.
A `palette` colorizer on the other hand, is used to generate a discrete set of colors, each mapped to a specific value.

There are three miscellaneous fields in both of the gradient colorizers, namely `noDataColor`, `overColor` and `underColor`.
The field `noDataColor` is used for all missing, `NaN` or no data values.
The fields `overColor` and `underColor` are used for all overflowing values.
For instance, if there are breakpoints defined from `0` to `10`, but a value of `-5` or `11` is mapped to a color, the respective field will be chosen instead.
This way, you can specifically highlight values that lie outside of a given range.

For a `palette` colorizer, there are no `overColor` and `underColor` fields.
If a given value does not match any entry in the palette's definition, it is mapped to the `defaultColor`.
The `noDataColor` works in the same manner as in the _gradiant_ variants.

Colors are defined as RGBA arrays, where the first three values refer to red, green and blue and the fourth one to alpha, which means transparency.
The values range from `0` to `255`.
For instance, `[255, 255, 255, 255]` is opaque white and `[0, 0, 0, 127]` is semi-transparent black.

## Linear Gradient

A linear gradient linearly interpolates values within breakpoints of a color table.
For instance, the example below is showing a gradient representing the physical conditions of water at different temperatures.
The gradient is defined between `0.0` and `99.99`, where `0.0` is shown as a light blue and `99.99` as blue.
Any value less than `0.0`, hence being ice, is shown as white.
Values above `99.99` are shown as a light gray.

### Example JSON

```json
{
    "type": "linearGradient",
    "breakpoints": [
        {
            "value": 0.0,
            "color": [204, 229, 255, 255]
        },
        {
            "value": 99.99,
            "color": [0, 0, 255, 255]
        }
    ],
    "noDataColor": [0, 0, 0, 0],
    "overColor": [224, 224, 224, 255],
    "underColor": [255, 255, 255, 255]
}
```

## Logarithmic Gradient

A logarithmic gradient logarithmically interpolates values within breakpoints of a color table and allows only positive values.
This colorizer is particularly useful in situations,
where the data values increase exponentially and minor changes in the lower numbers would not be recognizable anymore.

### Errors

Services report errors that try to use a logarithmic gradient specification with values where `value <= 0`.

### Example JSON

```json
{
    "type": "logarithmicGradient",
    "breakpoints": [
        {
            "value": 1.0,
            "color": [255, 255, 255, 255]
        },
        {
            "value": 100.0,
            "color": [0, 0, 0, 255]
        }
    ],
    "noDataColor": [0, 0, 0, 0],
    "overColor": [0, 0, 0, 255],
    "underColor": [255, 255, 255, 255]
}
```

## Palette

A palette maps values as classes to a certain color.
Unmapped values result in the `defaultColor`.

### Example JSON

```json
{
    "type": "palette",
    "colors": {
        "1": [255, 255, 255, 255],
        "2": [0, 0, 0, 255]
    },
    "noDataColor": [0, 0, 0, 0],
    "defaultColor": [0, 0, 0, 0]
}
```

## RGBA

The RGBA colorizer maps `U32` values "as is" to RGBA colors.
8 and 16 bit values are interpreted as grayscale colors.
64 bit values are interpreted as RGBA colors (but loose precision).

### Example JSON

```json
{
    "type": "rgba"
}
```
