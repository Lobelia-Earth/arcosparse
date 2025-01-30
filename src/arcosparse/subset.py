import pandas as pd
import pystac

from src.arcosparse.chunk_calculator import ChunkCalculator
from src.arcosparse.downloader import download_and_convert_to_pandas
from src.arcosparse.models import UserConfiguration, UserRequest
from src.arcosparse.sessions import ConfiguredRequestsSession
from src.arcosparse.utils import run_concurrently

MAX_CONCURRENT_REQUESTS = 10


def subset(
    request: UserRequest,
    user_configuration: UserConfiguration,
    url_metadata: str,
) -> pd.DataFrame:
    metadata = get_stac_metadata(url_metadata, user_configuration)
    if request.platform_ids:
        raise NotImplementedError("Platform subsetting not implemented yet")
    chunk_calculator = ChunkCalculator(metadata, request)
    chunks_to_download, asset_url = (
        chunk_calculator.select_best_asset_and_get_chunks()
    )
    tasks = []
    for variable_id, chunks in chunks_to_download.items():
        output_coordinates = chunks.output_coordinates
        for chunk in chunks.chunks_names:
            tasks.append(
                (
                    asset_url,
                    variable_id,
                    chunk,
                    output_coordinates,
                    user_configuration,
                )
            )
    results = run_concurrently(
        download_and_convert_to_pandas,
        tasks,
        max_concurrent_requests=8,
    )
    return pd.concat([result for result in results if result is not None])


def get_stac_metadata(
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
