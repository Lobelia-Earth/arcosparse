import bisect
import math
from itertools import product
from typing import Optional

# from src.models import (
#     CHUNK_INDEX_INDICES,
#     ChunkType,
#     Coordinate,
# )
from models import (
    CHUNK_INDEX_INDICES,
    ChunkType,
    Coordinate,
)


# TODO: creates specific tests for this function
# TODO: handle geometric chunking
def get_chunks_indexes_for_coordinate_old(
    requested_minimum: Optional[float],
    requested_maximum: Optional[float],
    coordinate: Coordinate,
) -> tuple[int, int]:
    """
    Given the requested data and the metadata about the coordinate,
    returns the indexes of the chunks that need to be downloaded.
    """
    maximum_value = coordinate.maximum
    minimum_value = coordinate.minimum
    # TODO: try to get rid of this type ignores
    # check how bisect works with the time values
    values = coordinate.values
    step_value = coordinate.step
    if not values and (
        maximum_value is not None
        and minimum_value is not None
        and step_value is not None
    ):
        values = [minimum_value]  # type: ignore
        for _ in range(
            0, math.ceil((maximum_value - minimum_value) / step_value)  # type: ignore
        ):
            values.append(values[-1] + step_value)  # type: ignore
    elif not values:
        raise ValueError(
            f"No values could be parsed for coordinate {coordinate}"
        )

    values.sort()
    if requested_minimum is None or requested_minimum < values[0]:  # type: ignore
        requested_minimum = values[0]  # type: ignore
    if requested_maximum is None or requested_maximum > values[-1]:  # type: ignore
        requested_maximum = values[-1]  # type: ignore

    index_left_minimum = bisect.bisect_left(values, requested_minimum)  # type: ignore
    if index_left_minimum == len(values) - 1 or abs(
        values[index_left_minimum] - requested_minimum  # type: ignore
    ) <= abs(
        values[index_left_minimum + 1] - requested_minimum  # type: ignore
    ):
        chunk_of_requested_minimum = math.floor(
            (index_left_minimum) / coordinate.chunk_length
        )
    else:
        chunk_of_requested_minimum = math.floor(
            (index_left_minimum + 1) / coordinate.chunk_length
        )

    index_right_maximum = bisect.bisect_right(values, requested_maximum)  # type: ignore
    index_right_maximum = index_right_maximum - 1
    if (
        index_right_maximum == len(values) - 1
        or index_right_maximum == len(values)
        or abs(values[index_right_maximum] - requested_maximum)  # type: ignore
        <= abs(values[index_right_maximum + 1] - requested_maximum)  # type: ignore
    ):
        chunk_of_requested_maximum = math.floor(
            (index_right_maximum) / coordinate.chunk_length
        )
    else:
        chunk_of_requested_maximum = math.floor(
            (index_right_maximum + 1) / coordinate.chunk_length
        )
    return chunk_of_requested_minimum, chunk_of_requested_maximum


# TODO: creates specific tests for this function
def get_chunk_indexes_for_coordinate(
    requested_minimum: Optional[float],
    requested_maximum: Optional[float],
    coordinate: Coordinate,
) -> tuple[int, int]:
    """
    Given the requested data and the metadata about the coordinate,
    returns the indexes of the chunks that need to be downloaded.
    """
    if requested_minimum is None or requested_minimum < coordinate.minimum:
        requested_minimum = coordinate.minimum
    if requested_maximum is None or requested_maximum > coordinate.maximum:
        requested_maximum = coordinate.maximum
    index_min = 0
    index_max = 0
    if coordinate.chunk_length:
        print("Getting chunks indexes for coordinate", coordinate.chunk_length)
        if coordinate.chunk_type == ChunkType.ARITHMETIC:
            print("Arithmetic chunking")
            index_min = get_chunks_index_arithmetic(
                requested_minimum,
                coordinate.chunk_reference_coordinate,
                coordinate.chunk_length,
            )
            index_max = get_chunks_index_arithmetic(
                requested_maximum,
                coordinate.chunk_reference_coordinate,
                coordinate.chunk_length,
            )
        elif coordinate.chunk_type == ChunkType.GEOMETRIC:
            print("Geometric chunking")
            index_min = get_chunks_index_geometric(
                requested_minimum,
                coordinate.chunk_reference_coordinate,
                coordinate.chunk_length,
                coordinate.chunk_geometric_factor,
            )
            index_max = get_chunks_index_geometric(
                requested_maximum,
                coordinate.chunk_reference_coordinate,
                coordinate.chunk_length,
                coordinate.chunk_geometric_factor,
            )
    return (index_min, index_max)


def get_chunks_index_arithmetic(
    requested_value: float,
    reference_chunking_step: float,
    chunk_length: int,
) -> int:
    """
    Given a value and the chunking information, returns the index
    of the chunk that contains the value.
    """
    return math.floor(
        (requested_value - reference_chunking_step) / chunk_length
    )


def get_chunks_index_geometric(
    requested_value: float,
    reference_chunking_step: float,
    chunk_length: int,
    factor: float,
) -> int:
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
    Based on the indices from SPARSE_SAMPLE_INDICES.

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
