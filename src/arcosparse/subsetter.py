from copy import deepcopy
from pathlib import Path
from typing import Literal, Optional

import pandas as pd
import pystac

from arcosparse.chunk_selector import (
    get_full_chunks_names,
    select_best_asset_and_get_chunks,
)
from arcosparse.downloader import download_and_convert_to_pandas
from arcosparse.logger import logger
from arcosparse.models import (
    ASSETS_NAMES,
    Asset,
    AssetsNames,
    Dataset,
    Entity,
    RequestedCoordinate,
    UserConfiguration,
    UserRequest,
)
from arcosparse.sessions import ConfiguredRequestsSession
from arcosparse.utils import run_concurrently

DEFAULT_COLUMNS_RENAME = {
    "platform_id": "entity_id",
    "platform_type": "entity_type",
}


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
    vertical_axis: Literal["elevation", "depth"],
    user_configuration: UserConfiguration,
    url_metadata: str,
    output_path: Optional[Path],
    disable_progress_bar: bool,
    columns_rename: Optional[dict[str, str]],
) -> Optional[pd.DataFrame]:
    columns_rename = _set_columns_rename(columns_rename)
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
    metadata, raw_platforms_metadata = _get_metadata(
        url_metadata,
        user_configuration,
        has_platform_ids_requested,
    )
    platforms_metadata: Optional[dict[str, str]] = None
    if has_platform_ids_requested:
        if raw_platforms_metadata is None:
            # TODO: custom error
            raise ValueError(
                "The requested dataset does not have platform information."
            )
        platforms_metadata = {
            key: value["chunking"]
            for key, value in raw_platforms_metadata["platforms"].items()
        }
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
                    vertical_axis,
                    columns_rename,
                )
            )
    results = [
        result
        for result in run_concurrently(
            download_and_convert_to_pandas,
            tasks,
            max_concurrent_requests=user_configuration.max_concurrent_requests,
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
    return pd.concat(results, ignore_index=True)


def subset_and_save(
    url_metadata: str,
    minimum_latitude: Optional[float],
    maximum_latitude: Optional[float],
    minimum_longitude: Optional[float],
    maximum_longitude: Optional[float],
    minimum_time: Optional[float],
    maximum_time: Optional[float],
    minimum_elevation: Optional[float],
    maximum_elevation: Optional[float],
    variables: list[str],
    entities: list[str] = [],
    vertical_axis: Literal["elevation", "depth"] = "elevation",
    output_path: Optional[Path] = None,
    user_configuration: UserConfiguration = UserConfiguration(),
    disable_progress_bar: bool = False,
    columns_rename: Optional[dict[str, str]] = None,
) -> None:
    """
    Parameters
    ----------
    url_metadata: str
        The URL to the STAC metadata. It will be parsed and use to do the subsetting.
    minimum_latitude: Optional[float]
        The minimum latitude to subset.
    maximum_latitude: Optional[float]
        The maximum latitude to subset.
    minimum_longitude: Optional[float]
        The minimum longitude to subset.
    maximum_longitude: Optional[float]
        The maximum longitude to subset.
    minimum_time: Optional[float]
        The minimum time to subset as a Unix timestamp in seconds.
    maximum_time: Optional[float]
        The maximum time to subset as a Unix timestamp in seconds.
    minimum_elevation: Optional[float]
        The minimum elevation to subset.
    maximum_elevation: Optional[float]
        The maximum elevation to subset.
    variables: list[str]
        The variables to subset, required.
    entities: list[str], default=[]
        The entities to subset on. If set, it will use the platformChunked asset.
    vertical_axis: Literal["elevation", "depth"], default="elevation"
        If depth selected, we will rename the vertical axis to depth and multiply by -1.
    output_path: Optional[Path], default=None
        The path where to save the subsetted data.
    user_configuration: Optional[UserConfiguration], default=UserConfiguration()
        The user configuration to use for the requests.
    disable_progress_bar: Optional[bool], default=False
        Disable the progress bar.
    columns_rename: Optional[dict[str, str]], default=None
        The columns to rename in the resulting dataframe. Setting two columns with the same name will raise the follwing error:
        "ValueError: Duplicate column names found"

    Returns
    -------

    None, parquet file saved in the output_path.
        The subsetted data in a parquet partitioned folder.
        By default the columns names are:
        'entity_id'
        'entity_type'
        'time'
        'latitude'
        'longitude'
        'elevation'
        'is_approx_elevation'
        'pressure'
        'value'
        'value_qc'
        'variable'

        Can be renamed using the columns_rename parameter.
        Also, 'elevation' will be renamed to 'depth' if vertical_axis is set to 'depth'.

    To open the result in pandas:

    ```python
    import pandas as pd


    # With latest pandas version you can also use directly:
    df = pd.read_parquet(output_dir)

    # In case, it does not work, you can try the following code:
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
    if output_path:
        output_path.mkdir(parents=True, exist_ok=True)
    else:
        output_path = Path(".")
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
        platform_ids=entities,
        vertical_axis=vertical_axis,
        user_configuration=user_configuration,
        url_metadata=url_metadata,
        output_path=output_path,
        disable_progress_bar=disable_progress_bar,
        columns_rename=columns_rename,
    )


def subset_and_return_dataframe(
    url_metadata: str,
    minimum_latitude: Optional[float],
    maximum_latitude: Optional[float],
    minimum_longitude: Optional[float],
    maximum_longitude: Optional[float],
    minimum_time: Optional[float],
    maximum_time: Optional[float],
    minimum_elevation: Optional[float],
    maximum_elevation: Optional[float],
    variables: list[str],
    entities: list[str] = [],
    vertical_axis: Literal["elevation", "depth"] = "elevation",
    user_configuration: UserConfiguration = UserConfiguration(),
    disable_progress_bar: bool = False,
    columns_rename: Optional[dict[str, str]] = None,
) -> pd.DataFrame:
    """
    Parameters
    ----------
    url_metadata: str
        The URL to the STAC metadata. It will be parsed and use to do the subsetting.
    minimum_latitude: Optional[float]
        The minimum latitude to subset.
    maximum_latitude: Optional[float]
        The maximum latitude to subset.
    minimum_longitude: Optional[float]
        The minimum longitude to subset.
    maximum_longitude: Optional[float]
        The maximum longitude to subset.
    minimum_time: Optional[float]
        The minimum time to subset as a Unix timestamp in seconds.
    maximum_time: Optional[float]
        The maximum time to subset as a Unix timestamp in seconds.
    minimum_elevation: Optional[float]
        The minimum elevation to subset.
    maximum_elevation: Optional[float]
        The maximum elevation to subset.
    variables: list[str]
        The variables to subset, required.
    entities: list[str], default=[]
        The entities to subset on. If set, it will use the platformChunked asset.
    vertical_axis: Literal["elevation", "depth"], default="elevation"
        If depth selected, we will rename the vertical axis to depth and multiply by -1.
    user_configuration: Optional[arcosparse.UserConfiguration], default=arcosparse.UserConfiguration()
        The user configuration to use for the requests.
    disable_progress_bar: Optional[bool], default=False
        Disable the progress bar.
    columns_rename: Optional[dict[str, str]], default=None
        The columns to rename in the resulting dataframe. Setting two columns with the same name will raise the follwing error:
        "ValueError: Duplicate column names found"

    Returns
    -------

    pd.DataFrame
        The subsetted data in a pandas DataFrame.
        By default the columns names are:
        'entity_id'
        'entity_type'
        'time'
        'latitude'
        'longitude'
        'elevation'
        'is_approx_elevation'
        'pressure'
        'value'
        'value_qc'
        'variable'

        Can be renamed using the columns_rename parameter.
        Also, 'elevation' will be renamed to 'depth' if vertical_axis is set to 'depth'.

    """  # noqa
    df = _subset(
        url_metadata=url_metadata,
        minimum_latitude=minimum_latitude,
        maximum_latitude=maximum_latitude,
        minimum_longitude=minimum_longitude,
        maximum_longitude=maximum_longitude,
        minimum_time=minimum_time,
        maximum_time=maximum_time,
        minimum_elevation=minimum_elevation,
        maximum_elevation=maximum_elevation,
        variables=variables,
        platform_ids=entities,
        vertical_axis=vertical_axis,
        user_configuration=user_configuration,
        output_path=None,
        disable_progress_bar=disable_progress_bar,
        columns_rename=columns_rename,
    )
    if df is None:
        return pd.DataFrame()
    return df


def get_entities(
    url_metadata: str,
    user_configuration: UserConfiguration = UserConfiguration(),
) -> list[Entity]:
    """
    Retrieve the ids of the entities available in the dataset.
    You can use those ids to subset the data.

    Parameters
    ----------

    url_metadata: str
        The URL to the STAC metadata. It will be parsed and use to do the subsetting.
    user_configuration: Optional[arcosparse.UserConfiguration], default=arcosparse.UserConfiguration()
        The user configuration to use for the requests.

    Returns
    -------

    list[Entity]
        The list of entities available in the dataset. Each entity is an object
        with the following attributes:
        - entity_id: str
            The id of the entity.
        - entity_type: str
            The type of the entity.
        - institution: str, optional
            The institution of the entity.
        - doi: str, optional
            The doi of the entity.
    """  # noqa

    _, platforms_metadata = _get_metadata(
        url_metadata, user_configuration, True
    )
    all_entities = []
    if platforms_metadata is None or "platforms" not in platforms_metadata:
        return []
    institution_mapping = platforms_metadata.get("dicts", {}).get("inst", {})
    doi_mapping = platforms_metadata.get("dicts", {}).get("doi", {})
    for platform_id, platform_info in platforms_metadata["platforms"].items():
        all_entities.append(
            Entity(
                entity_id=platform_id,
                entity_type=platform_info.get("ptype"),
                institution=institution_mapping.get(
                    platform_info.get("inst"), None
                ),
                doi=doi_mapping.get(platform_info.get("doi"), None),
            )
        )
    return all_entities


def get_dataset_metadata(
    url_metadata: str,
    user_configuration: UserConfiguration = UserConfiguration(),
) -> Dataset:
    """
    Retrieve the metadata of the dataset.

    Parameters
    ----------
    url_metadata: str
        The URL to the STAC metadata. It will be parsed and use to do the subsetting.
    user_configuration: Optional[arcosparse.UserConfiguration], default=arcosparse.UserConfiguration()
        The user configuration to use for the requests.

    Returns
    -------
    Dataset
        The metadata of the dataset. See the Dataset class for more details.
    """  # noqa

    metadata_item, _ = _get_metadata(url_metadata, user_configuration, False)
    assets = metadata_item.get_assets()
    assets_names: list[AssetsNames] = [
        asset_name
        for asset_name in assets.keys()
        if asset_name in ASSETS_NAMES
    ]  # type: ignore
    if not assets_names:
        raise ValueError(
            "No assets found in the metadata. "
            "Please check the metadata URL."
        )
    variables = list(metadata_item.properties.get("cube:variables", {}).keys())
    example_asset = Asset.from_metadata_item(
        metadata_item,
        variables,
        assets_names[0],  # type: ignore
    )

    return example_asset.to_dataset(
        asset_names=assets_names,
        dataset_id=metadata_item.id,
    )  # type: ignore


def _get_metadata(
    url_metadata: str,
    user_configuration: UserConfiguration,
    platform_ids_subset: bool,
) -> tuple[pystac.Item, Optional[dict]]:
    with ConfiguredRequestsSession(
        user_configuration=user_configuration
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

        return metadata_item, result.json()


def _set_columns_rename(
    input_columns_rename: Optional[dict[str, str]],
) -> dict[str, str]:
    columns_rename = deepcopy(input_columns_rename)
    if not columns_rename:
        return DEFAULT_COLUMNS_RENAME
    for key, value in DEFAULT_COLUMNS_RENAME.items():
        if key in columns_rename:
            del columns_rename[key]
        if value in columns_rename:
            columns_rename[key] = columns_rename[value]
            del columns_rename[value]
            continue
        columns_rename[key] = value
    return columns_rename
