from src.sparsub.chunk_calculator import get_chunk_indexes_for_coordinate
from src.sparsub.models import ChunkType, Coordinate

EXAMPLE_COORDINATE_ARITHMETIC = Coordinate(
    coordinate_id="time",
    maximum=1734516000,
    minimum=1731888000,
    step=0,
    unit="seconds since 1970-01-01 00:00:00",
    values=[],
    chunk_length=1728000,
    chunk_type=ChunkType.ARITHMETIC,
    chunk_reference_coordinate=1706400000,
    chunk_geometric_factor=0,
)

EXAMPLE_COORDINATE_GEOMETRIC = Coordinate(
    coordinate_id="elevation",
    maximum=12000.0,
    minimum=-4349.0,
    step=0,
    unit="m",
    values=[],
    chunk_length=5,
    chunk_type=ChunkType.GEOMETRIC,
    chunk_reference_coordinate=0,
    chunk_geometric_factor=1,
)


class TestChunkCalculator:
    def test_arithmetic_chunking_returns_expected_chunks(self):
        requested_minimum = 1731888000
        requested_maximum = 1734516000
        index_min, index_max = get_chunk_indexes_for_coordinate(
            requested_minimum, requested_maximum, EXAMPLE_COORDINATE_ARITHMETIC
        )
        assert index_min == 14
        assert index_max == 16

    def test_geometric_chunking_returns_expected_chunks(self):
        requested_minimum = -4349
        requested_maximum = 12000
        index_min, index_max = get_chunk_indexes_for_coordinate(
            requested_minimum, requested_maximum, EXAMPLE_COORDINATE_GEOMETRIC
        )
        assert index_min == -869
        assert index_max == 2400
