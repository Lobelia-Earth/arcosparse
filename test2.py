"""
Benchmark: eager per-chunk download vs. lazy Dask DataFrame, on a *big*
request, with a post-processing reduction (mean).

Two strategies, same request:

* ``subset_and_return_dataframe``      -> eager, ThreadPoolExecutor
  (the current strategy, ``max_concurrent_requests`` worker threads)
* ``subset_and_return_dask_dataframe`` -> lazy Dask DataFrame, one
  partition per SQLite chunk, materialised with ``.compute()``

Both ultimately call the same boto3-based ``download_and_convert_to_pandas``
for each chunk, so this measures orchestration overhead, not the per-chunk
download logic.

Two things are benchmarked:

1. Raw subset  -> download everything into a DataFrame.
2. Post-proc   -> mean ``value`` per ``variable``. For the eager path we
   download the full frame and then reduce it in pandas; for the Dask path
   the groupby-mean is folded into the lazy graph, so Dask reduces each
   partition and combines the results (it never has to hold the whole
   dataset in memory at once). This is where Dask is expected to help as
   the request grows.

Run with:

    poetry run python test2.py

Notes / caveats
---------------
* Wall-clock times depend heavily on the network, so we repeat and report
  best (min) and mean.
* The Dask path pays a one-off "meta inference" cost (it eagerly downloads
  the first non-empty chunk to learn the schema). That is included in the
  timings and also reported separately once.
* The "processes" scheduler may be unavailable in restricted environments;
  it is skipped gracefully if it fails.
* Tune ``BIG_REQUEST`` (time window) below to make the job larger/smaller.
"""

import statistics
import time
from typing import Any, Callable, Optional

import pandas as pd

from arcosparse import (
    subset_and_return_dask_dataframe,
    subset_and_return_dataframe,
)
from arcosparse.models import UserConfiguration

URL_METADATA = (
    "https://stac.marine.copernicus.eu/metadata/"
    "INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/"
    "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311--ext--history/"
    "dataset.stac.json"
)

USER_CONFIGURATION = UserConfiguration(
    extra_params={
        "x-cop-client": "copernicus-marine-toolbox",
        "x-cop-client-version": "1.3.4",
        "x-cop-user": "rjester",
    }
)

# A deliberately bigger request than test.py:
#   * no ``entities`` filter -> ALL platforms in the bbox (not just 2)
#   * a wider time window     -> ~2 months instead of ~1 week
# This forces many more chunks, which is where the orchestration strategy
# and the lazy reduction start to matter.
BIG_REQUEST = {
    "url_metadata": URL_METADATA,
    "minimum_latitude": -63.900001525878906,
    "maximum_latitude": 90.0,
    "minimum_longitude": -146.99937438964844,
    "maximum_longitude": 179.99998474121094,
    "minimum_time": 1696118400,  # 2023-10-01
    "maximum_time": 1701388800,  # 2023-12-01 (~2 months)
    "minimum_elevation": -10,
    "maximum_elevation": 120,
    "variables": ["TEMP", "PSAL"],
    "user_configuration": USER_CONFIGURATION,
}

REPEATS = 1

# Schedulers to benchmark. "synchronous" (serial) is left out by default
# because it roughly doubles the runtime while only confirming that serial
# downloading is slow; add it back here if you want the serial baseline.
# "processes" is skipped automatically in environments that forbid
# multiprocessing semaphores.
SCHEDULERS = ("threads", "processes")


def describe_rows(result: object) -> str:
    return f"{len(result.index)} rows"  # type: ignore[attr-defined]


def describe_mean(result: object) -> str:
    series = result  # type: ignore[assignment]
    parts = [f"{key}={value:.3f}" for key, value in series.items()]
    return "mean(value): " + ", ".join(parts)


def timeit(
    label: str,
    func: Callable[[], object],
    describe: Callable[[object], str],
    repeats: int = REPEATS,
) -> tuple[list[float], object]:
    """Run ``func`` ``repeats`` times, printing and returning the timings."""
    times: list[float] = []
    result: object = None
    for i in range(repeats):
        start = time.perf_counter()
        result = func()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        print(
            f"   [{label}] run {i + 1}/{repeats}: {elapsed:6.2f}s ({describe(result)})"
        )
    print(f"   [{label}] best={min(times):.2f}s mean={statistics.mean(times):.2f}s")
    return times, result


# --------------------------------------------------------------------------
# 1. Raw subset (download everything into a DataFrame)
# --------------------------------------------------------------------------
def eager_raw() -> object:
    return subset_and_return_dataframe(
        **BIG_REQUEST,
        progress_bar_configuration={"disable": True},
    )


def dask_raw(scheduler: str) -> Callable[[], object]:
    def run() -> object:
        ddf = subset_and_return_dask_dataframe(**BIG_REQUEST)
        return ddf.compute(scheduler=scheduler)

    return run


# --------------------------------------------------------------------------
# 2. Post-processing: mean ``value`` per ``variable``
# --------------------------------------------------------------------------
def postprocess_pandas(df: "pd.DataFrame") -> "pd.Series":
    return df.groupby("variable")["value"].mean().sort_index()


def eager_mean() -> object:
    # Download the full frame, THEN reduce it in pandas.
    df = subset_and_return_dataframe(
        **BIG_REQUEST,
        progress_bar_configuration={"disable": True},
    )
    return postprocess_pandas(df)


def dask_mean(scheduler: str) -> Callable[[], object]:
    def run() -> object:
        # The groupby-mean is folded into the lazy graph: Dask reduces each
        # partition and combines partial results, so the full dataset is
        # never held in memory at once.
        ddf: Any = subset_and_return_dask_dataframe(**BIG_REQUEST)
        lazy_mean = ddf.groupby("variable")["value"].mean()
        return lazy_mean.compute(scheduler=scheduler).sort_index()

    return run


def report_dask_build_vs_compute() -> None:
    """Break the Dask raw path into build (incl. meta) vs compute."""
    print("\n[dask breakdown] build graph vs compute (threads)")
    start = time.perf_counter()
    ddf = subset_and_return_dask_dataframe(**BIG_REQUEST)
    build = time.perf_counter() - start

    start = time.perf_counter()
    computed = ddf.compute(scheduler="threads")
    compute = time.perf_counter() - start

    print(
        f"   build (incl. meta inference): {build:6.2f}s | "
        f"compute: {compute:6.2f}s | rows: {len(computed.index)}"
    )


def print_summary(
    title: str,
    results: dict[str, list[float]],
    baseline_key: str = "eager",
) -> None:
    print(f"\nSummary - {title} (best wall-clock time)")
    print("-" * 55)
    baseline = min(results[baseline_key])
    for label, times in results.items():
        best = min(times)
        speedup = baseline / best
        print(f"  {label:<22} {best:6.2f}s  ({speedup:4.2f}x vs eager)")


def run_section(
    title: str,
    eager_func: Callable[[], object],
    dask_factory: Callable[[str], Callable[[], object]],
    describe: Callable[[object], str],
    schedulers: tuple[str, ...],
) -> dict[str, list[float]]:
    print(f"\n=== {title} ===")
    results: dict[str, list[float]] = {}

    print("\n[eager] current ThreadPoolExecutor strategy")
    results["eager"], eager_result = timeit("eager", eager_func, describe)

    for scheduler in schedulers:
        print(f"\n[dask/{scheduler}] lazy Dask DataFrame")
        try:
            times, dask_result = timeit(
                f"dask-{scheduler}", dask_factory(scheduler), describe
            )
        except Exception as error:  # noqa: BLE001
            print(f"   skipped ({type(error).__name__}: {error})")
            continue
        results[f"dask-{scheduler}"] = times

    print_summary(title, results)
    return results


def main() -> None:
    print("Benchmarking eager vs. Dask subsetting strategies (BIG request)")
    print("=" * 62)

    schedulers = SCHEDULERS

    # 1. Raw subset.
    run_section(
        "Raw subset (full download)",
        eager_raw,
        dask_raw,
        describe_rows,
        schedulers,
    )
    report_dask_build_vs_compute()

    # 2. Post-processing reduction (mean value per variable).
    run_section(
        "Post-processing: mean(value) per variable",
        eager_mean,
        dask_mean,
        describe_mean,
        schedulers,
    )


if __name__ == "__main__":
    df = subset_and_return_dask_dataframe(**BIG_REQUEST)
    print(df)
    # this head will randomly take one of the file and open it, so it randomly says that there is no data.
    print(df.head())
    # print(df.map_partitions(type).compute())
    # main()
