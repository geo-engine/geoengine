#!/usr/bin/env python3
"""Regenerate `src/epsg/epsg_registry.rs` from PROJ's EPSG database.

The registry stores static metadata per EPSG CRS code (type, area of use, and
semi-major axis) so that the `crs-constants` crate has no runtime dependency on
a PROJ installation.

Data sources:
- `proj.db` (from a PROJ installation): codes, names, CRS type, ellipsoid
  semi-major axis, and the geographic area of use.
- The projected area of use (native bounds) is computed by reprojecting a
  densified box along the area of use boundary via `pyproj`.

Usage:
    python3 scripts/generate_epsg_registry.py [path-to-proj.db] [output.rs]
"""

from __future__ import annotations

import math
import sqlite3
import sys

import numpy as np
import pyproj

PROJ_DB = sys.argv[1] if len(sys.argv) > 1 else "/usr/share/proj/proj.db"
OUTPUT = sys.argv[2] if len(sys.argv) > 2 else "src/epsg/epsg_registry.rs"

DENSIFICATION_POINTS_PER_EDGE = 31


def load_epsg_crs(db: sqlite3.Connection) -> list[dict]:
    """Fetch all active EPSG projected and geographic 2D CRS with their ellipsoids."""
    rows: list[dict] = []

    # active projected CRS (EPSG)
    projected = db.execute(
        """
        SELECT p.code, p.name, u.conv_factor, u.proj_short_name
        FROM projected_crs p
        JOIN coordinate_system cs
          ON cs.auth_name = p.coordinate_system_auth_name AND cs.code = p.coordinate_system_code
        JOIN axis a
          ON a.coordinate_system_auth_name = cs.auth_name AND a.coordinate_system_code = cs.code
          AND a.coordinate_system_order = 1
        JOIN unit_of_measure u
          ON u.auth_name = a.uom_auth_name AND u.code = a.uom_code
        WHERE p.auth_name = 'EPSG' AND p.deprecated = 0
        """
    ).fetchall()
    for code, name, conv_factor, unit in projected:
        rows.append(
            {
                "code": int(code),
                "name": name,
                "crs_type": "Projected",
                "meters_per_unit": float(conv_factor) if conv_factor is not None else 1.0,
                "unit": unit or "",
            }
        )

    # active geographic 2D CRS (EPSG); meters per unit = equatorial arc of one degree
    geodetic = db.execute(
        """
        SELECT g.code, g.name, e.semi_major_axis
        FROM geodetic_crs g
        JOIN geodetic_datum d
          ON d.auth_name = g.datum_auth_name AND d.code = g.datum_code
        JOIN ellipsoid e
          ON e.auth_name = d.ellipsoid_auth_name AND e.code = d.ellipsoid_code
        WHERE g.auth_name = 'EPSG' AND g.type = 'geographic 2D' AND g.deprecated = 0
        """
    ).fetchall()
    for code, name, semi_major in geodetic:
        rows.append(
            {
                "code": int(code),
                "name": name,
                "crs_type": "Geographic2d",
                "meters_per_unit": float(semi_major) * 2.0 * math.pi / 360.0,
                "unit": "deg",
            }
        )

    rows.sort(key=lambda r: r["code"])
    return rows


def area_of_use(proj: pyproj.CRS) -> tuple[float, float, float, float] | None:
    """Return (west, south, east, north) of the CRS area of use in degrees, if defined."""
    aou = proj.area_of_use
    if aou is None:
        return None
    return (aou.west, aou.south, aou.east, aou.north)


def projected_bounds(
    code: int, wgs84: tuple[float, float, float, float]
) -> tuple[float, float, float, float] | None:
    """Reproject a densified box along the area of use boundary into the CRS.

    Returns (min_x, min_y, max_x, max_y) in native CRS units.
    """
    west, south, east, north = wgs84
    edge = np.linspace(0.0, 1.0, DENSIFICATION_POINTS_PER_EDGE)

    lons = np.concatenate(
        [
            west + t * (east - west)
            for t in (edge, np.flip(edge))
        ]
        + [
            np.full(DENSIFICATION_POINTS_PER_EDGE, east),
            np.full(DENSIFICATION_POINTS_PER_EDGE, west),
        ]
    )
    lats = np.concatenate(
        [
            np.full(DENSIFICATION_POINTS_PER_EDGE, south),
            np.full(DENSIFICATION_POINTS_PER_EDGE, north),
        ]
        + [
            south + t * (north - south)
            for t in (edge, np.flip(edge))
        ]
    )

    try:
        transformer = pyproj.Transformer.from_crs(
            "EPSG:4326", f"EPSG:{code}", always_xy=True
        )
        xs, ys = transformer.transform(lons, lats, errcheck=True)
    except Exception:
        return None

    if not np.all(np.isfinite(xs)) or not np.all(np.isfinite(ys)):
        return None

    return (float(xs.min()), float(ys.min()), float(xs.max()), float(ys.max()))


def fmt_bounds(
    bounds: tuple[float, float, float, float] | None, decimals: int
) -> str:
    """Format as `[...]` (or `None`) with a fixed number of decimals."""
    if bounds is None:
        return "None"
    return "[" + ", ".join(f"{b:.{decimals}f}" for b in bounds) + "]"


def render(rows: list[dict]) -> str:
    lines = [
        "macro_rules! build_epsg_registry {",
        "    ($(",
        "        $name:ident | $code:literal | $string_name:literal | $crs_type:ident | $unit:literal | $meters_per_unit:literal | $wgs84:tt | $native_id:ident $( ( $($native_tt:tt)* ) )? |",
        "    )*) => {",
        "        $(",
        "            pub const $name: super::EpsgBounds = super::EpsgBounds {",
        '                #[cfg(feature = "metadata")]',
        "                code: $code,",
        '                #[cfg(feature = "metadata")]',
        "                name: $string_name,",
        "                crs_type: super::CrsType::$crs_type,",
        "                unit: $unit,",
        "                meters_per_unit: $meters_per_unit,",
        "                wgs84_bounds: $wgs84,",
        "                native_bounds: $native_id $( ( $($native_tt)* ) )?,",
        "            };",
        "        )*",
        "",
        "        pub const fn get_epsg_bounds(code: u16) -> Option<&'static super::EpsgBounds> {",
        "            match code {",
        "                $( $code => Some(&$name), )*",
        "                _ => None,",
        "            }",
        "        }",
        "    }",
        "}",
        "",
        "build_epsg_registry! {",
    ]
    for r in rows:
        sep = "|"
        lines.append(
            f'    EPSG_{r["code"]}{sep}{r["code"]}{sep}"{r["name"]}"{sep}'
            f'{r["crs_type"]}{sep}"{r["unit"]}"{sep}{r["meters_per_unit"]!r}{sep}'
            f'{fmt_bounds(r["wgs84"], 2)}{sep}'
            f'{"Some(" + fmt_bounds(r["native_bounds"], 3) + ")" if r["native_bounds"] is not None else "None"}{sep}'
        )
    lines.append("}")
    return "\n".join(lines)


def main() -> None:
    db = sqlite3.connect(PROJ_DB)
    rows = load_epsg_crs(db)

    n_with_aou = 0
    n_with_projected = 0
    for r in rows:
        try:
            proj = pyproj.CRS.from_epsg(r["code"])
        except Exception:
            r["wgs84"] = None
            r["native_bounds"] = None
            continue

        r["wgs84"] = area_of_use(proj)
        if r["wgs84"] is not None:
            n_with_aou += 1
            if r["crs_type"] == "Projected":
                r["native_bounds"] = projected_bounds(r["code"], r["wgs84"])
                if r["native_bounds"] is not None:
                    n_with_projected += 1
            else:
                r["native_bounds"] = list(r["wgs84"])
        else:
            r["native_bounds"] = None

    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write(render(rows))

    no_aou = sum(1 for r in rows if r["wgs84"] is None)
    print(
        f"wrote {len(rows)} CRS to {OUTPUT} "
        f"({n_with_aou} with area of use, {n_with_projected} projected with native bounds, "
        f"{no_aou} without area of use)"
    )


if __name__ == "__main__":
    main()