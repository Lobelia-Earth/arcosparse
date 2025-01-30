import math
from itertools import product
from typing import Literal, Optional

import pystac

from src.arcosparse.logger import logger
from src.arcosparse.models import (
    CHUNK_INDEX_INDICES,
    Asset,
    ChunksToDownload,
    ChunkType,
    Coordinate,
    OutputCoordinate,
    RequestedCoordinate,
    UserRequest,
)


class ChunkCalculator:
    metadata: pystac.Item
    request: UserRequest

    def __init__(self, metadata: pystac.Item, request: UserRequest):
        self.metadata = metadata
        self.request = request

    def select_best_asset_and_get_chunks(
        self,
    ) -> tuple[dict[str, ChunksToDownload], str]:
        """
        Selects the best asset by comparing the number
        of chunks needed to download for each asset.
        Then returns the chunks to download and the url.

        Returns:
            tuple[dict[str, ChunksToDownload], str]: the chunks to download
            and the url
        """
        chunks_time_chunked, time_chunked_url, number_chunks_time_chunked = (
            self._get_chunks_to_download("timeChunked")
        )
        chunks_geo_chunked, geo_chunked_url, number_chunks_geo_chunked = (
            self._get_chunks_to_download("geoChunked")
        )
        logger.debug(f"score time chunked {number_chunks_time_chunked}")
        logger.debug(f"score geo chunked {2 * number_chunks_geo_chunked}")
        # geo*2 because it's in the code of tero-sparse
        # TODO: ask why this is the case
        if number_chunks_time_chunked <= 2 * number_chunks_geo_chunked:
            logger.info("Downloading using time chunked")
            return chunks_time_chunked, time_chunked_url
        else:
            logger.info("Downloading using geo chunked")
            return chunks_geo_chunked, geo_chunked_url

    # TODO: create tests for this function
    def _get_chunks_to_download(
        self,
        asset_name: Literal["timeChunked", "geoChunked", "platformChunked"],
    ) -> tuple[dict[str, ChunksToDownload], str, int]:
        """
        Given the asset name, returns the chunks to download
        and the url, as well as the total number of chunks.
        """
        asset = Asset.from_metadata_item(
            self.metadata, self.request.variables, asset_name
        )
        chunks_to_download_names: dict[str, ChunksToDownload] = {}
        total_number_of_chunks = 0
        for variable in asset.variables:
            output_coordinates = []
            chunks_ranges: dict[str, tuple[int, int]] = {}
            number_of_chunks = 1
            for coordinate in variable.coordinates:
                requested_subset: Optional[RequestedCoordinate] = getattr(
                    self.request, coordinate.coordinate_id, None
                )
                if requested_subset:
                    chunks_range = self._get_chunk_indexes_for_coordinate(
                        requested_subset.minimum,
                        requested_subset.maximum,
                        coordinate,
                    )
                else:
                    chunks_range = self._get_chunk_indexes_for_coordinate(
                        None, None, coordinate
                    )
                chunks_ranges[coordinate.coordinate_id] = chunks_range
                number_of_chunks *= chunks_range[1] - chunks_range[0] + 1

                if (
                    requested_subset
                    and requested_subset.minimum
                    and requested_subset.maximum
                ):
                    output_coordinates.append(
                        OutputCoordinate(
                            minimum=requested_subset.minimum
                            or coordinate.minimum,
                            maximum=requested_subset.maximum
                            or coordinate.maximum,
                            coordinate_id=coordinate.coordinate_id,
                        )
                    )
            total_number_of_chunks += number_of_chunks
            chunks_to_download_names[variable.variable_id] = ChunksToDownload(
                variable_id=variable.variable_id,
                chunks_names=self._get_full_chunks_names(chunks_ranges),
                output_coordinates=output_coordinates,
            )
        return chunks_to_download_names, asset.url, total_number_of_chunks

    # TODO: creates specific tests for this function
    def _get_chunk_indexes_for_coordinate(
        self,
        requested_minimum: Optional[float],
        requested_maximum: Optional[float],
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
        if coordinate.chunk_length:
            logger.debug(
                f"Getting chunks indexes for coordinate"
                f"{coordinate.chunk_length}",
            )
            if coordinate.chunk_type == ChunkType.ARITHMETIC:
                logger.debug("Arithmetic chunking")
                index_min = self._get_chunks_index_arithmetic(
                    requested_minimum,
                    coordinate.chunk_reference_coordinate,
                    coordinate.chunk_length,
                )
                index_max = self._get_chunks_index_arithmetic(
                    requested_maximum,
                    coordinate.chunk_reference_coordinate,
                    coordinate.chunk_length,
                )
            elif coordinate.chunk_type == ChunkType.GEOMETRIC:
                logger.debug("Geometric chunking")
                index_min = self._get_chunks_index_geometric(
                    requested_minimum,
                    coordinate.chunk_reference_coordinate,
                    coordinate.chunk_length,
                    coordinate.chunk_geometric_factor,
                )
                index_max = self._get_chunks_index_geometric(
                    requested_maximum,
                    coordinate.chunk_reference_coordinate,
                    coordinate.chunk_length,
                    coordinate.chunk_geometric_factor,
                )
        return (index_min, index_max)

    def _get_chunks_index_arithmetic(
        self,
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
        self,
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
    def _get_full_chunks_names(
        self,
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
        return {
            ".".join(map(str, combination)) for combination in combinations
        }
