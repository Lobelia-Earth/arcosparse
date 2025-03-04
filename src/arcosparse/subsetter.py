from pathlib import Path
from typing import Optional

import pandas as pd
import pystac

from arcosparse.chunk_selector import (
    get_full_chunks_names,
    select_best_asset_and_get_chunks,
)
from arcosparse.downloader import download_and_convert_to_pandas
from arcosparse.logger import logger
from arcosparse.models import (
    RequestedCoordinate,
    UserConfiguration,
    UserRequest,
)
from arcosparse.sessions import ConfiguredRequestsSession
from arcosparse.utils import run_concurrently

# quite high because a lot of 403
MAX_CONCURRENT_REQUESTS = 50


def _subset(
    minimum_latitude: Optional[float],
    maximum_latitude: Optional[float],
    minimum_longitude: Optional[float],
    maximum_longitude: Optional[float],
    minimum_time: Optional[float],
    maximum_time: Optional[float],
    minimum_elevation: Optional[float],
    maximum_elevation: Optional[float],
    variables: list[str],
    platform_ids: list[str],
    user_configuration: UserConfiguration,
    url_metadata: str,
    output_path: Optional[Path],
    disable_progress_bar: bool,
) -> Optional[pd.DataFrame]:
    request = UserRequest(
        time=RequestedCoordinate(
            minimum=minimum_time, maximum=maximum_time, coordinate_id="time"
        ),
        latitude=RequestedCoordinate(
            minimum=minimum_latitude,
            maximum=maximum_latitude,
            coordinate_id="latitude",
        ),
        longitude=RequestedCoordinate(
            minimum=minimum_longitude,
            maximum=maximum_longitude,
            coordinate_id="longitude",
        ),
        elevation=RequestedCoordinate(
            minimum=minimum_elevation,
            maximum=maximum_elevation,
            coordinate_id="elevation",
        ),
        variables=variables,
        platform_ids=platform_ids,
    )
    has_platform_ids_requested = bool(request.platform_ids)
    metadata, platforms_metadata = _get_metadata(
        url_metadata,
        user_configuration,
        has_platform_ids_requested,
    )
    if has_platform_ids_requested:
        if platforms_metadata is None:
            # TODO: custom error
            raise ValueError(
                "The requested dataset does not have platform information."
            )
        for platform_id in request.platform_ids:
            if platform_id not in platforms_metadata:
                raise ValueError(
                    f"Platform {platform_id} is not available in the dataset."
                )
    logger.info("Selecting the best asset and chunks to download")
    chunks_to_download, asset_url = select_best_asset_and_get_chunks(
        metadata, request, has_platform_ids_requested, platforms_metadata
    )
    tasks = []
    output_filepath = None
    for chunks_range in chunks_to_download:
        logger.debug(f"Downloading chunks for {chunks_range.variable_id}")
        # TODO: Maybe we should do this calculation per batches
        # it would allow for huge downloads and create bigger parquet files?
        for chunk_name in get_full_chunks_names(chunks_range.chunks_ranges):
            if output_path:
                if chunks_range.platform_id:
                    # TODO: maybe need a way to no overwrite the files
                    # also a skip existing option? maybe not
                    output_filename = (
                        f"{chunks_range.platform_id}_"
                        f"{chunks_range.variable_id}_{chunk_name}"
                        f".parquet"
                    )
                else:
                    output_filename = (
                        f"{chunks_range.variable_id}_{chunk_name}.parquet"
                    )
                output_filepath = output_path / output_filename
            tasks.append(
                (
                    asset_url,
                    chunks_range.variable_id,
                    chunk_name,
                    chunks_range.platform_id,
                    chunks_range.output_coordinates,
                    user_configuration,
                    output_filepath,
                )
            )
    logger.info("Downloading and converting to pandas-like dataframes")
    results = [
        result
        for result in run_concurrently(
            download_and_convert_to_pandas,
            tasks,
            max_concurrent_requests=MAX_CONCURRENT_REQUESTS,
            tdqm_bar_configuration={
                "disable": disable_progress_bar,
                "desc": "Downloading files",
            },
        )
        if result is not None
    ]
    if output_path:
        return None
    if not results:
        return pd.DataFrame()
    return pd.concat(results)


# TODO: ask if it's okay that we can actually subset without credentials
# well in the end it's the same as xarray
def subset_and_save(
    minimum_latitude: Optional[float],
    maximum_latitude: Optional[float],
    minimum_longitude: Optional[float],
    maximum_longitude: Optional[float],
    minimum_time: Optional[float],
    maximum_time: Optional[float],
    minimum_elevation: Optional[float],
    maximum_elevation: Optional[float],
    variables: list[str],
    platform_ids: list[str],
    url_metadata: str,
    output_path: Path,
    user_configuration: UserConfiguration = UserConfiguration(),
    disable_progress_bar: bool = False,
) -> None:
    """
    Parameters
    ----------
    minimum_latitude: Optional[float]
        The minimum latitude to subset
    maximum_latitude: Optional[float]
        The maximum latitude to subset
    minimum_longitude: Optional[float]
        The minimum longitude to subset
    maximum_longitude: Optional[float]
        The maximum longitude to subset
    minimum_time: Optional[float]
        The minimum time to subset as a Unix timestamp in seconds
    maximum_time: Optional[float]
        The maximum time to subset as a Unix timestamp in seconds
    minimum_elevation: Optional[float]
        The minimum elevation to subset
    maximum_elevation: Optional[float]
        The maximum elevation to subset
    variables: list[str]
        The variables to subset
    platform_ids: list[str]
        The platform ids to subset. If see will use the platformChunked asset
    url_metadata: str
        The URL to the stac metadata. It will be parsed and use to do the subsetting
    output_path: Path
        The path where to save the subsetted data
    user_configuration: UserConfiguration
        The user configuration to use for the requests
    disable_progress_bar: bool
        Disable the progress bar

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
    """  # noqa
    output_path.mkdir(parents=True, exist_ok=True)
    _subset(
        minimum_latitude=minimum_latitude,
        maximum_latitude=maximum_latitude,
        minimum_longitude=minimum_longitude,
        maximum_longitude=maximum_longitude,
        minimum_time=minimum_time,
        maximum_time=maximum_time,
        minimum_elevation=minimum_elevation,
        maximum_elevation=maximum_elevation,
        variables=variables,
        platform_ids=platform_ids,
        user_configuration=user_configuration,
        url_metadata=url_metadata,
        output_path=output_path,
        disable_progress_bar=disable_progress_bar,
    )


def subset_and_return_dataframe(
    minimum_latitude: Optional[float],
    maximum_latitude: Optional[float],
    minimum_longitude: Optional[float],
    maximum_longitude: Optional[float],
    minimum_time: Optional[float],
    maximum_time: Optional[float],
    minimum_elevation: Optional[float],
    maximum_elevation: Optional[float],
    variables: list[str],
    platform_ids: list[str],
    user_configuration: UserConfiguration,
    url_metadata: str,
    disable_progress_bar: bool = False,
) -> pd.DataFrame:
    """
    Parameters
    ----------
    minimum_latitude: Optional[float]
        The minimum latitude to subset
    maximum_latitude: Optional[float]
        The maximum latitude to subset
    minimum_longitude: Optional[float]
        The minimum longitude to subset
    maximum_longitude: Optional[float]
        The maximum longitude to subset
    minimum_time: Optional[float]
        The minimum time to subset as a Unix timestamp in seconds
    maximum_time: Optional[float]
        The maximum time to subset as a Unix timestamp in seconds
    minimum_elevation: Optional[float]
        The minimum elevation to subset
    maximum_elevation: Optional[float]
        The maximum elevation to subset
    variables: list[str]
        The variables to subset
    platform_ids: list[str]
        The platform ids to subset. If see will use the platformChunked asset
    url_metadata: str
        The URL to the stac metadata. It will be parsed and use to do the subsetting
    user_configuration: UserConfiguration
        The user configuration to use for the requests
    disable_progress_bar: bool
        Disable the progress bar
    """  # noqa
    df = _subset(
        minimum_latitude=minimum_latitude,
        maximum_latitude=maximum_latitude,
        minimum_longitude=minimum_longitude,
        maximum_longitude=maximum_longitude,
        minimum_time=minimum_time,
        maximum_time=maximum_time,
        minimum_elevation=minimum_elevation,
        maximum_elevation=maximum_elevation,
        variables=variables,
        platform_ids=platform_ids,
        user_configuration=user_configuration,
        url_metadata=url_metadata,
        output_path=None,
        disable_progress_bar=disable_progress_bar,
    )
    if df is None:
        return pd.DataFrame()
    return df


def get_platforms_names(
    url_metadata: str,
    user_configuration: UserConfiguration = UserConfiguration(),
) -> list[str]:
    """
    Get the platforms metadata from the metadata URL
    """
    _, platforms_metadata = _get_metadata(
        url_metadata, user_configuration, True
    )
    if platforms_metadata is None:
        return []
    return list(platforms_metadata.keys())


def _get_metadata(
    url_metadata: str,
    user_configuration: UserConfiguration,
    platform_ids_subset: bool,
) -> tuple[pystac.Item, Optional[dict[str, str]]]:
    with ConfiguredRequestsSession(
        user_configuration.disable_ssl,
        user_configuration.trust_env,
        user_configuration.ssl_certificate_path,
        user_configuration.extra_params,
    ) as session:
        result = session.get(url_metadata)
        result.raise_for_status()
        metadata_item = pystac.Item.from_dict(result.json())
        platforms_metadata = None
        if platform_ids_subset:
            platforms_asset = metadata_item.get_assets().get("platforms")
            if platforms_asset is None:
                return metadata_item, platforms_metadata
            result = session.get(platforms_asset.href)
            result.raise_for_status()
            platforms_metadata = {
                key: value["chunking"]
                for key, value in result.json()["platforms"].items()
            }

        return metadata_item, platforms_metadata
