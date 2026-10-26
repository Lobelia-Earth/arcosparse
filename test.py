"""
Runnable demo / sanity test for the lazy, distributed Dask subsetting path.

This mirrors the eager `subset_and_return_dataframe` request used in the test
suite, but goes through `subset_and_return_dask_dataframe`, which returns a
lazy `dask.dataframe.DataFrame` where **each SQLite chunk is one partition**.

Nothing is downloaded until an action is triggered (`.compute()`,
`.to_parquet()`, ...). Under the hood each partition still uses the exact same
boto3-based `download_and_convert_to_pandas` function as the eager path, so the
results are identical; only the orchestration is distributed.

Run it with:

    poetry run python test.py

Requires the optional Dask dependency:

    pip install "dask[dataframe]"      # or: poetry install --with dev
"""

import time
from typing import Any

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

BASE_REQUEST = {
    "url_metadata": URL_METADATA,
    "minimum_latitude": -63.900001525878906,
    "maximum_latitude": 90.0,
    "minimum_longitude": -146.99937438964844,
    "maximum_longitude": 179.99998474121094,
    "minimum_time": 1700888000,
    "maximum_time": 1701516000,
    "minimum_elevation": -10,
    "maximum_elevation": 120,
    "variables": ["TEMP", "PSAL"],
    "entities": [
        "F-Vartdalsfjorden___MO",
        "B-Sulafjorden___MO",
    ],
    "user_configuration": USER_CONFIGURATION,
}


def main() -> None:
    print("1. Building the lazy Dask DataFrame (no download yet)...")
    ddf: Any = subset_and_return_dask_dataframe(**BASE_REQUEST)

    print(f"   -> type:        {type(ddf).__module__}.{type(ddf).__name__}")
    print(f"   -> npartitions: {ddf.npartitions} (one per chunk)")
    print(f"   -> columns:     {list(ddf.columns)}")
    print(f"   -> dtypes:\n{ddf.dtypes}")

    print("\n2. Triggering the distributed download with .compute()...")
    start = time.perf_counter()
    computed = ddf.compute()
    elapsed = time.perf_counter() - start
    print(f"   -> computed {len(computed.index)} rows in {elapsed:.1f}s")
    print("   -> head():")
    print(computed.head())

    # Cross-check against the eager path so we know the Dask result matches.
    print("\n3. Cross-checking against the eager pandas path...")
    eager = subset_and_return_dataframe(**BASE_REQUEST)
    print(f"   -> eager rows: {len(eager.index)}")
    print(f"   -> dask  rows: {len(computed.index)}")
    assert len(computed.index) == len(eager.index), (
        "Row count mismatch between the Dask and eager paths!"
    )
    assert set(computed.columns) == set(eager.columns), (
        "Column mismatch between the Dask and eager paths!"
    )
    print("   -> OK: row counts and columns match. 🎉")


if __name__ == "__main__":
    main()
