from arcosparse.chunk_selector import _get_chunk_indexes_for_coordinate
from arcosparse.models import ChunkType, Coordinate

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

EXAMPLE_COORDINATE_CHUNK_LENGTH_PER_TYPE = Coordinate(
    coordinate_id="time",
    maximum=1738367997,
    minimum=-4063996800,
    step=0,
    unit="seconds since 1970-01-01 00:00:00",
    values=[],
    chunk_length={
        "timeSeries": 86400000,
        "timeSeriesHF2": 25920000,
        "timeSeriesHF": 8640000,
        "timeSeriesUHF": 864000,
        "profileLF": 17280000,
        "profile": 1728000,
        "profileHF2": 864000,
        "profileHF": 172800,
    },
    chunk_type=ChunkType.ARITHMETIC,
    chunk_reference_coordinate=-4063996800,
    chunk_geometric_factor=0,
)

EXAMPLE_COORDINATE_CHUNK_LENGTH_PER_TYPE_NONE = Coordinate(
    coordinate_id="longitude",
    maximum=179.99998474121094,
    minimum=-180,
    step=0,
    unit="m",
    values=[],
    chunk_length={
        "timeSeries": None,
        "timeSeriesHF2": None,
        "timeSeriesHF": None,
        "timeSeriesUHF": None,
        "profileLF": None,
        "profile": None,
        "profileHF": None,
    },
    chunk_type=ChunkType.ARITHMETIC,
    chunk_reference_coordinate=-180,
    chunk_geometric_factor=0,
)

requested_platform_id = "F-Vartdalsfjorden___MO"
platforms_metadata = {"F-Vartdalsfjorden___MO": "timeSeriesUHF"}


class TestChunkSelector:
    def test_arithmetic_chunking_returns_expected_chunks(self):
        requested_minimum = 1731888000
        requested_maximum = 1734516000
        index_min, index_max = _get_chunk_indexes_for_coordinate(
            requested_minimum,
            requested_maximum,
            EXAMPLE_COORDINATE_ARITHMETIC.chunk_length,  # type: ignore
            EXAMPLE_COORDINATE_ARITHMETIC,
        )
        assert index_min == 14
        assert index_max == 16

    def test_geometric_chunking_returns_expected_chunks(self):
        requested_minimum = -4349
        requested_maximum = 12000
        index_min, index_max = _get_chunk_indexes_for_coordinate(
            requested_minimum,
            requested_maximum,
            EXAMPLE_COORDINATE_GEOMETRIC.chunk_length,  # type: ignore
            EXAMPLE_COORDINATE_GEOMETRIC,
        )
        assert index_min == -869
        assert index_max == 2400

    def test_chunks_with_chunk_length_per_type(self):
        requested_minimum = 1700888000
        requested_maximum = 1720516000
        index_min, index_max = _get_chunk_indexes_for_coordinate(
            requested_minimum,
            requested_maximum,
            EXAMPLE_COORDINATE_CHUNK_LENGTH_PER_TYPE.chunk_length[  # type: ignore
                platforms_metadata[requested_platform_id]
            ],
            EXAMPLE_COORDINATE_CHUNK_LENGTH_PER_TYPE,
        )
        assert index_min == 6672
        assert index_max == 6695

    def test_chunks_with_chunk_length_per_type_none(self):
        requested_minimum = -180
        requested_maximum = 179.99998474121094
        index_min, index_max = _get_chunk_indexes_for_coordinate(
            requested_minimum,
            requested_maximum,
            EXAMPLE_COORDINATE_CHUNK_LENGTH_PER_TYPE_NONE.chunk_length[  # type: ignore
                platforms_metadata[requested_platform_id]
            ],
            EXAMPLE_COORDINATE_CHUNK_LENGTH_PER_TYPE_NONE,
        )
        assert index_min == 0
        assert index_max == 0

    def test_basic_num_overflow_chunks_test(self):
        from arcosparse.chunk_selector import get_full_chunks_names

        input_dict = {
            "longitude": (4, 7),
            "time": (0, 0),
            "elevation": (0, 1),
            "latitude": (0, 0),
        }
        out = get_full_chunks_names(input_dict, num_overflow_chunks=1)
        assert "0.0.7.0-1" in out
