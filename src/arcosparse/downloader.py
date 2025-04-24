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
        # means that the chunk does not exist
        if response.status_code == 403:
            logger.debug(f"Chunk {chunk_name} does not exist")
            return None
        response.raise_for_status()
        # TODO: check that this is okay to save the file in a temporary file
        # else need to find a way to save it in memory
        # for this we need the encoding of the file:
        # database_content = io.BytesIO(response.content)
        # connection = sqlite3.connect("file::memory:?cache=shared", uri=True)
        # connection.executescript(database_content.read().decode('utf-8'))
        # OR use a thread safe csv writer:
        # https://stackoverflow.com/questions/33107019/multiple-threads-writing-to-the-same-csv-in-python # noqa
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
