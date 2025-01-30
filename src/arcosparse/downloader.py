import sqlite3
import tempfile
from typing import Optional

# TODO: if we have performances issues
# check if we could use polars instead of pandas
import pandas as pd

from src.arcosparse.logger import logger
from src.arcosparse.models import OutputCoordinate, UserConfiguration
from src.arcosparse.sessions import ConfiguredRequestsSession


def download_and_convert_to_pandas(
    base_url: str,
    variable_id: str,
    chunk_name: str,
    output_coordinates: list[OutputCoordinate],
    user_configuration: UserConfiguration,
) -> Optional[pd.DataFrame]:
    logger.debug(f"downloading {base_url}/{variable_id}/{chunk_name}.sqlite")
    # TODO: create a proper request with headers retries etc
    # see the toolbox for examples (sessions.py)
    with ConfiguredRequestsSession(
        user_configuration.disable_ssl,
        user_configuration.trust_env,
        user_configuration.ssl_certificate_path,
        user_configuration.extra_params,
    ) as session:
        response = session.get(
            f"{base_url}/{variable_id}/{chunk_name}.sqlite",
        )
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
            query += " WHERE"
            first_done = False
            for coordinate in output_coordinates:
                if first_done:
                    query += " AND"
                first_done = True
                query += (
                    f" {coordinate.coordinate_id} >= {coordinate.minimum} "
                )
                f"AND {coordinate.coordinate_id} <= {coordinate.maximum}"
        # TODO: add some logger debug here
        with tempfile.NamedTemporaryFile(
            suffix=".sqlite", delete=True
        ) as temp_file:
            temp_file.write(response.content)
            temp_file.flush()
            with sqlite3.connect(temp_file.name) as connection:
                df = pd.read_sql(query, connection)
        logger.debug("Done downloading")
        return df


if __name__ == "__main__":
    import requests

    url_file = "https://s3.waw3-1.cloudferro.com/mdl-arco-time-057/arco/INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311--ext--latest/timeChunked/WDIR/34.0.0.0.sqlite"  # noqa
    response = requests.get(url_file, timeout=600)
    response.raise_for_status()
    # db_path = "your_database.sqlite"
    # with open(db_path, "wb") as f:
    #     f.write(response.content)
    # connection = sqlite3.connect(db_path)

    with tempfile.NamedTemporaryFile(
        suffix=".sqlite", delete=True
    ) as temp_file:
        # Write the downloaded content to the temporary file
        temp_file.write(response.content)
        temp_file.flush()  # Ensure all data is written to the file
        print(f"Database file downloaded to temporary file: {temp_file.name}")
        with sqlite3.connect(temp_file.name) as connection:
            connection = sqlite3.connect(temp_file.name)
            # Show column names
            cursor = connection.cursor()
            cursor.execute("PRAGMA table_info(data)")
            print(cursor.fetchall())

            # Read data from a specific table
            df = pd.read_sql(
                "SELECT * FROM data WHERE time < 1732060800", connection
            )

            print(df)
