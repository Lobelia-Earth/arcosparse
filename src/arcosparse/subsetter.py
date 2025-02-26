from pathlib import Path
from typing import Optional

import pandas as pd
import pystac

from arcosparse.chunk_selector import select_best_asset_and_get_chunks
from arcosparse.downloader import download_and_convert_to_pandas
from arcosparse.models import UserConfiguration, UserRequest
from arcosparse.sessions import ConfiguredRequestsSession
from arcosparse.utils import run_concurrently

MAX_CONCURRENT_REQUESTS = 10


def subset(
    request: UserRequest,
    user_configuration: UserConfiguration,
    url_metadata: str,
    output_directory: Optional[Path],
) -> Optional[pd.DataFrame]:
    metadata = _get_stac_metadata(url_metadata, user_configuration)
    has_platform_ids_requested = bool(request.platform_ids)
    platforms_metadata = None
    if has_platform_ids_requested:
        platforms_asset = metadata.get_assets().get("platforms")
        if platforms_asset is None:
            # TODO: custom error
            raise ValueError(
                "The requested dataset does not have platform information."
            )
        platforms_metadata = _get_platforms_metadata(
            platforms_asset.href, user_configuration
        )
        for platform_id in request.platform_ids:
            if platform_id not in platforms_metadata:
                raise ValueError(
                    f"Platform {platform_id} is not available in the dataset."
                )
    chunks_to_download, asset_url = select_best_asset_and_get_chunks(
        metadata, request, has_platform_ids_requested, platforms_metadata
    )
    tasks = []
    output_filepath = None
    for chunks in chunks_to_download:
        for chunk in chunks.chunks_names:
            if output_directory:
                output_directory.mkdir(parents=True, exist_ok=True)
                if chunks.platform_id:
                    output_filename = (
                        f"{chunks.platform_id}_{chunks.variable_id}_{chunk}"
                        f".parquet"
                    )
                else:
                    output_filename = f"{chunks.variable_id}_{chunk}.parquet"
                output_filepath = output_directory / output_filename
            tasks.append(
                (
                    asset_url,
                    chunks.variable_id,
                    chunk,
                    chunks.platform_id,
                    chunks.output_coordinates,
                    user_configuration,
                    output_filepath,
                )
            )
    results = [
        result
        for result in run_concurrently(
            download_and_convert_to_pandas,
            tasks,
            max_concurrent_requests=8,
        )
        if result is not None
    ]
    if output_directory:
        return None
    if not results:
        return pd.DataFrame()
    return pd.concat([result for result in results if result is not None])


def subset_and_save(
    request: UserRequest,
    user_configuration: UserConfiguration,
    url_metadata: str,
    output_directory: Path,
) -> None:
    """
    To open the result in pandas:

    ```python
    import pandas as pd

    import glob

    # Get all partitioned Parquet files
    parquet_files = glob.glob(f"{output_dir}/*.parquet")

    # Read all files into a single dataframe
    df = pd.concat(pd.read_parquet(file) for file in parquet_files)

    print(df)

    Or with dask:

    ```python

    import dask.dataframe as dd

    df = dd.read_parquet(output_dir, engine="pyarrow")
    print(df.head())  # Works just like pandas but with lazy loading

    Need to have the pyarrow library as a dependency
    """
    output_directory.mkdir(parents=True, exist_ok=True)
    subset(request, user_configuration, url_metadata, output_directory)


def open_dataset(
    request: UserRequest,
    user_configuration: UserConfiguration,
    url_metadata: str,
) -> Optional[pd.DataFrame]:
    subset(request, user_configuration, url_metadata, None)


def _get_stac_metadata(
    url_metadata: str, user_configuration: UserConfiguration
) -> pystac.Item:
    with ConfiguredRequestsSession(
        user_configuration.disable_ssl,
        user_configuration.trust_env,
        user_configuration.ssl_certificate_path,
        user_configuration.extra_params,
    ) as session:
        result = session.get(url_metadata)
        result.raise_for_status()
        metadata_json = result.json()

        return pystac.Item.from_dict(metadata_json)


def _get_platforms_metadata(
    url: str, user_configuration: UserConfiguration
) -> dict[str, str]:
    with ConfiguredRequestsSession(
        user_configuration.disable_ssl,
        user_configuration.trust_env,
        user_configuration.ssl_certificate_path,
        user_configuration.extra_params,
    ) as session:
        result = session.get(url)
        result.raise_for_status()
        return {
            key: value["chunking"]
            for key, value in result.json()["platforms"].items()
        }
