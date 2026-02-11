import sqlite3
import tempfile
from pathlib import Path
from typing import Literal, Optional
import json

# TODO: if we have performances issues
# check if we could use polars instead of pandas
import pandas as pd

from arcosparse.logger import logger
from arcosparse.models import OutputCoordinate, UserConfiguration
from arcosparse.sessions import ConfiguredRequestsSession


def download_and_convert_to_pandas(
    base_url: str,
    variable_id: str,
    chunk_name: str,
    platform_id: Optional[str],
    output_coordinates: list[OutputCoordinate],
    user_configuration: UserConfiguration,
    output_path: Optional[Path],
    vertical_axis: Literal["elevation", "depth"],
    columns_rename: dict[str, str],
) -> Optional[pd.DataFrame]:
    if platform_id:
        url_to_download = (
            f"{base_url}/{platform_id}/{variable_id}/{chunk_name}.sqlite"
        )
    else:
        url_to_download = f"{base_url}/{variable_id}/{chunk_name}.sqlite"
    logger.debug(f"downloading {url_to_download}")
    # TODO: check if we'd better use boto3 instead of requests
    with ConfiguredRequestsSession(
        user_configuration=user_configuration
    ) as session:
        response = session.get(url_to_download)
        # TODO: check that this is okay to save the file in a temporary file
        # else need to find a way to save it in memory
        # for this we need the encoding of the file:
        # database_content = io.BytesIO(response.content)
        # connection = sqlite3.connect("file::memory:?cache=shared", uri=True)
        # connection.executescript(database_content.read().decode('utf-8'))
        # OR use a thread safe csv writer:
        # https://stackoverflow.com/questions/33107019/multiple-threads-writing-to-the-same-csv-in-python # noqa
        if (
            overflow_chunks := get_num_overflow_chunks(response)
        ) is not None and overflow_chunks > 0:
            df = pd.DataFrame()
            for chunk in range(overflow_chunks + 1):
                if chunk > 0:
                    overflow_url = f"{base_url}/{platform_id}/{variable_id}/{chunk_name}b{chunk}.sqlite"
                else:
                    overflow_url = f"{base_url}/{platform_id}/{variable_id}/{chunk_name}.sqlite"
                logger.debug(f"downloading overflow chunk {overflow_url}")
                overflow_response = session.get(overflow_url)
                overflow_df = read_query_from_sqlite_and_convert_to_df(
                    overflow_response,
                    output_coordinates,
                    variable_id,
                    vertical_axis,
                    columns_rename,
                    output_path,
                )
                logger.debug(f"Appending overflow chunk {chunk} to df")
                if overflow_df is not None:
                    df = pd.concat([df, overflow_df], ignore_index=True)
        else:
            return read_query_from_sqlite_and_convert_to_df(
                response,
                output_coordinates,
                variable_id,
                vertical_axis,
                columns_rename,
                output_path,
            )
        return df


def read_query_from_sqlite_and_convert_to_df(
    response,
    output_coordinates: list[OutputCoordinate],
    variable_id: str,
    vertical_axis: Literal["elevation", "depth"] = "elevation",
    columns_rename: dict[str, str] = {},
    output_path: Optional[Path] = None,
) -> pd.DataFrame | None:
    # means that the chunk does not exist
    if response.status_code == 403:
        logger.debug("Chunk does not exist")
        return None
    response.raise_for_status()

    query = "SELECT * FROM data"
    if output_coordinates:
        query += " WHERE "
        query += " AND ".join(
            [
                f"{coordinate.coordinate_id} >= {coordinate.minimum} "
                f"AND {coordinate.coordinate_id} <= {coordinate.maximum}"
                for coordinate in output_coordinates
            ]
        )
    with tempfile.NamedTemporaryFile(
        suffix=".sqlite", delete=False
    ) as temp_file:
        temp_file.write(response.content)
        temp_file.flush()
    try:
        with sqlite3.connect(temp_file.name) as connection:
            df = pd.read_sql(query, connection)
        connection.close()
        df["variable"] = variable_id
        if df.empty:
            df = None
        else:
            if vertical_axis == "depth" and "elevation" in df.columns:
                df["elevation"] = -df["elevation"]
                columns_rename["elevation"] = "depth"
            df.rename(columns=columns_rename, inplace=True)
        if output_path and df is not None:
            df.to_parquet(output_path)
            df = None
    finally:
        Path(temp_file.name).unlink()
    return df


def get_num_overflow_chunks(response) -> int | None:
    query = "SELECT * FROM meta"

    with tempfile.NamedTemporaryFile(
        suffix=".sqlite", delete=False
    ) as temp_file:
        temp_file.write(response.content)
        temp_file.flush()
    try:
        with sqlite3.connect(temp_file.name) as connection:
            metadata = pd.read_sql(query, connection)
        raw = metadata["metadata"].iloc[0]

        if raw is None:
            logger.debug("metadata is NULL, assuming no overflow chunks")
            return 0
        else:
            meta = json.loads(raw)

        return meta.get("overflow_chunks", 0)
    except Exception as e:
        logger.error(f"Error reading sqlite file metadata: {e}")
        return None
