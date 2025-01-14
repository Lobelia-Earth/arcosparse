import pandas as pd
import pystac

from src.sparsub.chunk_calculator import (
    get_chunk_indexes_for_coordinate,
    get_full_chunks_names,
)
from src.sparsub.downloader import download_and_convert_to_pandas
from src.sparsub.models import (
    Asset,
    ChunksToDownload,
    OutputCoordinate,
    RequestedCoordinate,
    Variable,
)
from src.sparsub.utils import run_concurrently

# from utils import run_concurrently

MAX_CONCURRENT_REQUESTS = 10


def subset(
    request: dict[str, RequestedCoordinate],
    variables: list[str],
    url_metadata: str,
) -> pd.DataFrame:
    metadata = pystac.Item.from_file(url_metadata)
    asset = Asset.from_metadata_item(metadata, variables, "timeChunked")
    chunks_to_download = get_chunks_combinations(request, asset.variables)
    print("chunk indexes:", chunks_to_download)
    chunks_to_download_names: dict[str, ChunksToDownload] = (
        get_chunks_all_chunks_names(
            chunks_to_download, request, asset.variables
        )
    )
    print("chunk indexes names :", chunks_to_download_names)
    tasks = []
    for variable_id, chunks in chunks_to_download_names.items():
        output_coordinates = chunks.output_coordinates
        for chunk in chunks.chunks_names:
            tasks.append(
                (
                    asset.url,
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


# TODO: create tests for this function
def get_chunks_combinations(
    request: dict[str, RequestedCoordinate], variables
) -> dict[str, dict[str, tuple[int, int]]]:
    chunks_per_variable = {}
    for variable in variables:
        chunks_per_coordinate = {}
        for coordinate in variable.coordinates:
            requested_subset = request.get(
                coordinate.coordinate_id,
            )
            if requested_subset:
                chunks_per_coordinate[coordinate.coordinate_id] = (
                    get_chunk_indexes_for_coordinate(
                        requested_subset.minimum,
                        requested_subset.maximum,
                        coordinate,
                    )
                )
            else:
                chunks_per_coordinate[coordinate.coordinate_id] = (
                    get_chunk_indexes_for_coordinate(None, None, coordinate)
                )
        chunks_per_variable[variable.variable_id] = chunks_per_coordinate
    return chunks_per_variable


def get_chunks_all_chunks_names(
    chunks_indexes: dict[str, dict[str, tuple[int, int]]],
    request: dict[str, RequestedCoordinate],
    variables: list[Variable],
) -> dict[str, ChunksToDownload]:
    chunks_to_download_names: dict[str, ChunksToDownload] = {}
    for variable_id, chunks in chunks_indexes.items():
        chunks = get_full_chunks_names(chunks)
        variable = [
            variable
            for variable in variables
            if variable.variable_id == variable_id
        ][0]
        output_coordinates = []
        for coordinate in variable.coordinates:
            requested_data = request.get(coordinate.coordinate_id)
            if (
                requested_data
                and requested_data.minimum
                and requested_data.maximum
            ):
                output_coordinates.append(
                    OutputCoordinate(
                        minimum=requested_data.minimum or coordinate.minimum,
                        maximum=requested_data.maximum or coordinate.maximum,
                        coordinate_id=coordinate.coordinate_id,
                    )
                )

        chunks_to_download_names[variable_id] = ChunksToDownload(
            variable_id=variable_id,
            chunks_names=chunks,
            output_coordinates=output_coordinates,
        )
    return chunks_to_download_names


# # TODO: we actually need to subset on each download the tiles
# # and then save them locally so we don't have to save all the data
# # in RAM. Though it would be nice to know what is the format we
# # are going to save the data in.
# def _download_chunks(
#     self, chunks_to_download: list[str], output_path: str
# ) -> None:
#     # TODO: decide if we wanna go with boto3 or requests
#     tasks: list[tuple] = []
#     with get_session as session:
#         for chunk in chunks_to_download:
#             tasks.append(
#                 _build_url(chunk),
#                 output_path / f"{chunk}.json",
#             )
#         run_concurrently(session.get, tasks, MAX_CONCURRENT_REQUESTS)
#     pass

# def _build_url(self, chunk: str) -> str:
#     pass
