import pandas as pd
import pystac

from src.arcosparse.chunk_calculator import ChunkCalculator
from src.arcosparse.downloader import download_and_convert_to_pandas
from src.arcosparse.models import UserRequest
from src.arcosparse.utils import run_concurrently

MAX_CONCURRENT_REQUESTS = 10


def subset(
    request: UserRequest,
    url_metadata: str,
) -> pd.DataFrame:
    metadata = pystac.Item.from_file(url_metadata)
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
                )
            )
    results = run_concurrently(
        download_and_convert_to_pandas,
        tasks,
        max_concurrent_requests=8,
    )
    return pd.concat([result for result in results if result is not None])
