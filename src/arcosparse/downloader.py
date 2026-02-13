import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Literal, Optional

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
        url_base = f"{base_url}/{platform_id}/{variable_id}"
    else:
        url_base = f"{base_url}/{variable_id}"
    logger.debug(f"downloading {url_base}/{chunk_name}.sqlite")
    # TODO: check if we'd better use boto3 instead of requests
    with ConfiguredRequestsSession(
        user_configuration=user_configuration
    ) as session:
        response = session.get(f"{url_base}/{chunk_name}.sqlite")
        # TODO: check that this is okay to save the file in a temporary file
        # else need to find a way to save it in memory
        # for this we need the encoding of the file:
        # database_content = io.BytesIO(response.content)
        # connection = sqlite3.connect("file::memory:?cache=shared", uri=True)
        # connection.executescript(database_content.read().decode('utf-8'))
        # OR use a thread safe csv writer:
        # https://stackoverflow.com/questions/33107019/multiple-threads-writing-to-the-same-csv-in-python # noqa
        df, overflow_chunks = read_query_from_sqlite_and_convert_to_df(
            response,
            output_coordinates,
            variable_id,
            columns_rename,
            vertical_axis,
        )
        if overflow_chunks is not None and overflow_chunks > 0:
            for chunk in range(overflow_chunks):
                overflow_url = f"{url_base}/{chunk_name}b{chunk+1}.sqlite"
                logger.debug(f"downloading overflow chunk {overflow_url}")

                overflow_response = session.get(overflow_url)

                overflow_df, _ = read_query_from_sqlite_and_convert_to_df(
                    overflow_response,
                    output_coordinates,
                    variable_id,
                    columns_rename,
                    vertical_axis,
                )
                logger.debug(f"Appending overflow chunk {chunk} to df")
                if overflow_df is not None:
                    if df is not None:
                        df = pd.concat([df, overflow_df], ignore_index=True)
                    else:
                        df = overflow_df

    if output_path and df is not None:
        df.to_parquet(output_path)
        return
    return df


def read_query_from_sqlite_and_convert_to_df(
    response,
    output_coordinates: list[OutputCoordinate],
    variable_id: str,
    columns_rename: dict[str, str],
    vertical_axis: Literal["elevation", "depth"] = "elevation",
) -> tuple[Optional[pd.DataFrame], Optional[int]]:
    # means that the chunk does not exist
    if response.status_code == 403:
        logger.debug("Chunk does not exist")
        return None, None
    response.raise_for_status()

    query = "SELECT * FROM data"
    query_metadata = "SELECT * FROM meta"
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
            try:
                metadata = pd.read_sql(query_metadata, connection)
            except (pd.errors.DatabaseError, sqlite3.OperationalError) as e:
                logger.debug(f"No meta table found (or can't read it): {e}")
                metadata = None

        if df.empty:
            df = None
        else:
            df["variable"] = variable_id
            if vertical_axis == "depth" and "elevation" in df.columns:
                df["elevation"] = -df["elevation"]
                columns_rename["elevation"] = "depth"
            df.rename(columns=columns_rename, inplace=True)

        if metadata is None or metadata.empty:
            overflow = 0
        else:
            try:
                raw = metadata["metadata"].iloc[0]
                meta = json.loads(raw)
                overflow = meta.get("overflow_chunks", 0)
            except (
                KeyError,
                IndexError,
                json.JSONDecodeError,
                TypeError,
            ) as e:
                logger.debug(f"Metadata could not be processed: {e}")
                overflow = 0
    finally:
        Path(temp_file.name).unlink()
    return df, overflow
