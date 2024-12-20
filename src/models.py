from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ChunkType(Enum):
    ARITHMETIC = "default"
    GEOMETRIC = "symmetricGeometric"


@dataclass
class Coordinate:
    minimum: float
    maximum: float
    step: float
    values: list
    coordinate_id: str
    unit: str
    chunk_length: int
    chunk_type: ChunkType
    chunk_reference_coordinate: float
    chunk_geometric_factor: float


@dataclass
class Variable:
    """
    Variables can have different coordinate chunking in theory.
    At least they don't have always the same coordinates.

    For example, some variable of the dataset might have depth
    others no.
    """

    variable_id: str
    coordinates: list[Coordinate]
    unit: str


SQL_FIELDS = [
    "platform_id",
    "platform_type",
    "time",
    "longitude",
    "latitude",
    "elevation",
    "pressure",
    "value",
    "value_qc",
]

SQL_COLUMNS = {
    "platformId": 0,
    "platformType": 1,
    "time": 2,
    "longitude": 3,
    "latitude": 4,
    "elevation": 5,
    "pressure": 6,
    "value": 7,
    "valueQc": 8,
}

CHUNK_INDEX_INDICES = {
    "time": 0,
    "elevation": 1,
    "longitude": 2,
    "latitude": 3,
}


@dataclass
class RequestedCoordinate:
    minimum: Optional[float]
    maximum: Optional[float]
    coodinate_id: str


# @dataclass
# class UserRequest:
#     time: RequestedCoordinate
#     depth: RequestedCoordinate
#     latitude: RequestedCoordinate
#     longitude: RequestedCoordinate


@dataclass
class OutputCoordinate:
    """
    Class useful to know what data we need
    contrary to the RequestedCoordinate class
    None type is not allowed here.
    """

    minimum: float
    maximum: float
    coordinate_id: str


@dataclass
class ChunksToDownload:
    """
    This class is used to store the chunking information
    for the subset we want to download.
    """

    variable_id: str
    # TODO: subset on platform_id
    # platform_id: str
    chunks_names: set[str]
    output_coordinates: list[OutputCoordinate]
