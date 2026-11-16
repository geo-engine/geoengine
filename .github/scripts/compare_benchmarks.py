#!/usr/bin/env python3
"""Compare the base and the pull request over two Criterion rounds and write a Markdown report.

The rounds run in the order base, PR, PR, base on one runner, so that neither side always runs
first (cold caches) or last:

- round 1: the base saves the baseline `r1`, the PR compares against it (`--baseline-lenient r1`),
  and the workflow moves the resulting `<id>/change` to `<id>/change-r1` (PR relative to base);
- round 2: the PR saves `r2`, the base compares against it, and `<id>/change` moves to
  `<id>/change-r2` (base relative to PR, which this script inverts).

Criterion's `change/estimates.json` holds the bootstrapped relative change of the median with its
confidence interval. That interval only covers the variation within one run, so a benchmark counts
as a regression (improvement) only if, in both rounds, the whole interval lies above (below) zero
and the change exceeds the threshold.
"""

import argparse
import json
import math
import sys
from pathlib import Path

MARKER = "<!-- geoengine-continuous-benchmarking -->"


def load(path: Path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def duration(ns: float) -> str:
    for unit, factor in (("s", 1e9), ("ms", 1e6), ("µs", 1e3)):
        if ns >= factor:
            return f"{ns / factor:.2f} {unit}"
    return f"{ns:.0f} ns"


def cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return "unknown"


def median_change(path: Path, invert: bool):
    """The relative change of the median of the PR against the base: (point, lower, upper)."""
    change = load(path)
    if change is None:
        return None
    median = change["median"]
    point = median["point_estimate"]
    lower = median["confidence_interval"]["lower_bound"]
    upper = median["confidence_interval"]["upper_bound"]
    if invert:
        # base/PR - 1 → PR/base - 1; the bounds swap, as the inversion is decreasing
        point, lower, upper = 1 / (1 + point) - 1, 1 / (1 + upper) - 1, 1 / (1 + lower) - 1
    return point, lower, upper


def fmt(change) -> str:
    point, lower, upper = change
    return f"{point:+.1%} [{lower:+.1%}, {upper:+.1%}]"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("criterion_dir", type=Path, help="Criterion output directory of both rounds")
    parser.add_argument("--threshold", type=float, default=0.10, help="relative change, e.g. 0.10")
    parser.add_argument("--base-ref", default="base", help="label of the base commit")
    parser.add_argument("--output", type=Path, required=True, help="file for the Markdown report")
    args = parser.parse_args()

    rows, unpaired = [], []
    regressions = improvements = 0
    # every benchmark that ran has a `new/benchmark.json`
    for benchmark_file in sorted(args.criterion_dir.glob("**/new/benchmark.json")):
        bench_dir = benchmark_file.parent.parent
        name = (load(benchmark_file) or {}).get("full_id", str(bench_dir.relative_to(args.criterion_dir)))
        base = load(bench_dir / "r1" / "estimates.json")  # the base saved `r1`
        pr = load(bench_dir / "r2" / "estimates.json")  # the PR saved `r2`
        round1 = median_change(bench_dir / "change-r1" / "estimates.json", invert=False)
        round2 = median_change(bench_dir / "change-r2" / "estimates.json", invert=True)
        if base is None or pr is None or round1 is None or round2 is None:
            unpaired.append((name, "new" if base is None else "only in base" if pr is None else "incomplete"))
            continue

        # the geometric mean of both rounds' ratios
        change = math.sqrt((1 + round1[0]) * (1 + round2[0])) - 1
        slower = all(lower > 0 and point > args.threshold for point, lower, _ in (round1, round2))
        faster = all(upper < 0 and point < -args.threshold for point, _, upper in (round1, round2))
        if slower:
            verdict, regressions = "🔴 slower", regressions + 1
        elif faster:
            verdict, improvements = "🟢 faster", improvements + 1
        elif abs(change) > args.threshold:
            verdict = "noisy"  # above the threshold, but not consistently in both rounds
        else:
            verdict = "≈"
        rows.append(
            f"| `{name}` | {duration(base['median']['point_estimate'])} "
            f"| {duration(pr['median']['point_estimate'])} "
            f"| {change:+.1%} | {fmt(round1)} | {fmt(round2)} | {verdict} |"
        )

    out = [
        MARKER,
        "## Continuous Benchmarking",
        "",
        f"Base `{args.base_ref}` and this pull request on the same runner (`{cpu_model()}`), "
        "in two rounds in the order base, PR, PR, base. "
        "Change of the median of the PR against the base, per round with its 95 % confidence interval; "
        f"🔴/🟢 only if both rounds' intervals exclude zero and both changes exceed {args.threshold:.0%}.",
        "",
        f"**{regressions} slower, {improvements} faster, {len(rows) - regressions - improvements} unchanged.**",
        "",
        "<details><summary>All benchmarks</summary>",
        "",
        "| Benchmark | Base | PR | Change | Round 1 | Round 2 | |",
        "| --- | ---: | ---: | ---: | --- | --- | --- |",
        *rows,
        "",
        "</details>",
    ]
    if unpaired:
        out += ["", "Not compared: " + ", ".join(f"`{name}` ({why})" for name, why in unpaired)]
    args.output.write_text("\n".join(out) + "\n")

    # workflow commands are read from stdout
    if regressions:
        print(f"::warning::{regressions} benchmark(s) slower than the base")
    return 0


if __name__ == "__main__":
    sys.exit(main())
