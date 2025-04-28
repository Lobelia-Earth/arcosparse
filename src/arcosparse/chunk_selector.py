import math
from itertools import product
from typing import Optional

import pystac

from arcosparse.logger import logger
from arcosparse.models import (
    CHUNK_INDEX_INDICES,
    Asset,
    AssetsNames,
    ChunksRanges,
    ChunkType,
    Coordinate,
    OutputCoordinate,
    RequestedCoordinate,
    UserRequest,
)


def select_best_asset_and_get_chunks(
    metadata: pystac.Item,
    request: UserRequest,
    has_platform_ids_requested: bool,
    platforms_metadata: Optional[dict] = None,
) -> tuple[list[ChunksRanges], str]:
    """
    Selects the best asset by comparing the number
    of chunks needed to download for each asset.
    Then returns the chunks to download and the url.

    if the user wants to subset on platform ids, the platformChunked
    asset is selected.

    Returns:
        tuple[dict[str, ChunksToDownload], str]: the chunks to download
        and the url
    """
    if has_platform_ids_requested:
        chunks_platform_chunked, platform_chunked_url, _ = (
            _get_chunks_to_download(
                metadata,
                request,
                "platformChunked",
                platforms_metadata=platforms_metadata,
            )
        )
        logger.debug("Downloading using platform chunked")
        return chunks_platform_chunked, platform_chunked_url
    chunks_time_chunked, time_chunked_url, number_chunks_time_chunked = (
        _get_chunks_to_download(metadata, request, "timeChunked")
    )
    chunks_geo_chunked, geo_chunked_url, number_chunks_geo_chunked = (
        _get_chunks_to_download(metadata, request, "geoChunked")
    )
    logger.debug(f"score time chunked {number_chunks_time_chunked}")
    logger.debug(f"score geo chunked {2 * number_chunks_geo_chunked}")
    # geo*2 because it's in the code of tero-sparse
    # TODO: ask why this is the case
    if number_chunks_time_chunked <= 2 * number_chunks_geo_chunked:
        logger.debug("Downloading using time chunked")
        return chunks_time_chunked, time_chunked_url
    else:
        logger.debug("Downloading using geo chunked")
        return chunks_geo_chunked, geo_chunked_url


# TODO: create tests for this function
def _get_chunks_to_download(
    metadata: pystac.Item,
    request: UserRequest,
    asset_name: AssetsNames,
    platforms_metadata: Optional[dict[str, str]] = None,
) -> tuple[list[ChunksRanges], str, int]:
    """
    Given the asset name, returns the chunks to download
    and the url, as well as the total number of chunks.
    """
    asset = Asset.from_metadata_item(metadata, request.variables, asset_name)
    chunks_to_download: list[ChunksRanges] = []
    total_number_of_chunks = 0
    for platform_id in request.platform_ids or [None]:
        number_of_chunks_per_variable = 0
        for variable in asset.variables:
            output_coordinates = []
            chunks_ranges: dict[str, tuple[int, int]] = {}
            number_of_chunks = 1
            for coordinate in variable.coordinates:
                requested_subset: Optional[RequestedCoordinate] = getattr(
                    request, coordinate.coordinate_id, None
                )
                if isinstance(coordinate.chunk_length, dict):
                    if not platforms_metadata:
                        raise ValueError(
                            "Platforms metadata is needed when chunk "
                            "length is a dict"
                        )
                    if not platform_id:
                        raise ValueError(
                            "Platform id is needed when chunk length is a dict"
                        )
                    chunk_length = coordinate.chunk_length.get(
                        platforms_metadata[platform_id]
                    )
                else:
                    chunk_length = coordinate.chunk_length
                if requested_subset and chunk_length:
                    chunks_range = _get_chunk_indexes_for_coordinate(
                        requested_minimum=requested_subset.minimum,
                        requested_maximum=requested_subset.maximum,
                        chunk_length=chunk_length,
                        coordinate=coordinate,
                    )
                elif chunk_length:
                    chunks_range = _get_chunk_indexes_for_coordinate(
                        requested_minimum=None,
                        requested_maximum=None,
                        chunk_length=chunk_length,
                        coordinate=coordinate,
                    )
                else:
                    chunks_range = (0, 0)
                chunks_ranges[coordinate.coordinate_id] = chunks_range
                number_of_chunks *= chunks_range[1] - chunks_range[0] + 1
                if requested_subset:
                    output_coordinates.append(
                        OutputCoordinate(
                            minimum=(
                                requested_subset.minimum
                                if requested_subset.minimum is not None
                                else coordinate.minimum
                            ),
                            maximum=(
                                requested_subset.maximum
                                if requested_subset.maximum is not None
                                else coordinate.maximum
                            ),
                            coordinate_id=coordinate.coordinate_id,
                        )
                    )
            chunks_to_download.append(
                ChunksRanges(
                    platform_id=platform_id,
                    variable_id=variable.variable_id,
                    chunks_ranges=chunks_ranges,
                    output_coordinates=output_coordinates,
                )
            )
            logger.debug(
                "Number of chunks after variable "
                f"{variable.variable_id}: {number_of_chunks_per_variable}"
            )
            total_number_of_chunks += number_of_chunks

    return chunks_to_download, asset.url, total_number_of_chunks


# TODO: creates specific tests for this function
def _get_chunk_indexes_for_coordinate(
    requested_minimum: Optional[float],
    requested_maximum: Optional[float],
    chunk_length: int,
    coordinate: Coordinate,
) -> tuple[int, int]:
    """
    Returns the index range of the chunks that needs to be downloaded.
    """
    if requested_minimum is None or requested_minimum < coordinate.minimum:
        requested_minimum = coordinate.minimum
    if requested_maximum is None or requested_maximum > coordinate.maximum:
        requested_maximum = coordinate.maximum
    index_min = 0
    index_max = 0
    if chunk_length:
        logger.debug(
            f"Getting chunks indexes for coordinate"
            f"{coordinate.coordinate_id} of length "
            f"{coordinate.chunk_length}"
        )
        if coordinate.chunk_type == ChunkType.ARITHMETIC:
            logger.debug("Arithmetic chunking")
            index_min = _get_chunks_index_arithmetic(
                requested_minimum,
                coordinate.chunk_reference_coordinate,
                chunk_length,
            )
            index_max = _get_chunks_index_arithmetic(
                requested_maximum,
                coordinate.chunk_reference_coordinate,
                chunk_length,
            )
        elif coordinate.chunk_type == ChunkType.GEOMETRIC:
            logger.debug("Geometric chunking")
            index_min = _get_chunks_index_geometric(
                requested_minimum,
                coordinate.chunk_reference_coordinate,
                chunk_length,
                coordinate.chunk_geometric_factor,
            )
            index_max = _get_chunks_index_geometric(
                requested_maximum,
                coordinate.chunk_reference_coordinate,
                chunk_length,
                coordinate.chunk_geometric_factor,
            )
    return (index_min, index_max)


def _get_chunks_index_arithmetic(
    requested_value: float,
    reference_chunking_step: float,
    chunk_length: int,
) -> int:
    """
    Chunk index calculation for arithmetic chunking.
    """
    return math.floor(
        (requested_value - reference_chunking_step) / chunk_length
    )


def _get_chunks_index_geometric(
    requested_value: float,
    reference_chunking_step: float,
    chunk_length: int,
    factor: float,
) -> int:
    """
    Chunk index calculation for geometric chunking.
    """
    absolute_coordinate = abs(requested_value - reference_chunking_step)
    if absolute_coordinate < chunk_length:
        return 0
    if factor == 1:
        chunk_index = math.floor(absolute_coordinate / chunk_length)
    else:
        chunk_index = math.ceil(
            math.log(absolute_coordinate / chunk_length) / math.log(factor)
        )
    return (
        -chunk_index
        if requested_value < reference_chunking_step
        else chunk_index
    )


# TODO: unit test for this
def get_full_chunks_names(
    chunks_indexes: dict[str, tuple[int, int]],
) -> set[str]:
    """
    Given a list of all the indexes for each coordinate, returns
    the list of all the chunks that need to be downloaded.
    Based on the indices from CHUNK_INDEX_INDICES.

    Example:
    input: {
        "time": (0, 0),
        "depth": (0, 1),
        "latitude": (0, 0),
        "longitude": (4, 7),
    }
    output: [
        "0.0.0.4",
        "0.0.0.5",
        "0.0.0.6",
        ...
        "0.1.0.7",
        ]
    """
    sorted_chunks_indexes = sorted(
        chunks_indexes.items(), key=lambda x: CHUNK_INDEX_INDICES[x[0]]
    )
    ranges = [
        range(start, end + 1) for _, (start, end) in sorted_chunks_indexes
    ]
    combinations = product(*ranges)
    return {".".join(map(str, combination)) for combination in combinations}
