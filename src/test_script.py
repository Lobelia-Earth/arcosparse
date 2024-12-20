from models import ChunkType, Coordinate, RequestedCoordinate, Variable
from src.main import subset

if __name__ == "__main__":
    url_file = "https://s3.waw3-1.cloudferro.com/mdl-arco-time-057/arco/INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311--ext--latest/timeChunked"

    variables = [
        Variable(
            variable_id="ATMP",
            coordinates=[
                Coordinate(
                    coordinate_id="time",
                    maximum=1734516000,  # Need to convert to timestamp in seconds
                    minimum=1731888000,
                    step=0,
                    unit="ISO8601",
                    values=[],
                    chunk_length=1728000,
                    chunk_type=ChunkType.ARITHMETIC,
                    chunk_reference_coordinate=1706400000,
                    chunk_geometric_factor=0,
                ),
                Coordinate(
                    coordinate_id="latitude",
                    maximum=-63.900001525878906,
                    minimum=90.0,
                    step=0,
                    unit="degrees_north",
                    values=[],
                    chunk_length=0,
                    chunk_type=ChunkType.ARITHMETIC,
                    chunk_reference_coordinate=56.54330062866211,
                    chunk_geometric_factor=0,
                ),
                Coordinate(
                    coordinate_id="longitude",
                    maximum=-179.99998474121094,
                    minimum=179.99937438964844,
                    step=0,
                    unit="degrees_east",
                    values=[],
                    chunk_length=0,
                    chunk_type=ChunkType.ARITHMETIC,
                    chunk_reference_coordinate=-180,
                    chunk_geometric_factor=0,
                ),
                Coordinate(
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
                ),
            ],
            unit="degrees",
        ),
    ]

    # TODO: we should also be able to requests variables
    request = {
        "time": RequestedCoordinate(
            minimum=1731888000, maximum=1734516000, coodinate_id="time"
        ),
        "latitude": RequestedCoordinate(
            minimum=90.0, maximum=-63.900001525878906, coodinate_id="latitude"
        ),
        "longitude": RequestedCoordinate(
            minimum=179.99937438964844,
            maximum=-179.99998474121094,
            coodinate_id="longitude",
        ),
        # TODO: handle the elevation and depth problem if needed
        # TODO: fix the problem with the elevation, cannot request the min and max
        "elevation": RequestedCoordinate(
            maximum=120, minimum=-10, coodinate_id="elevation"
        ),
    }
    # should download 3 chunks
    # 2024-12-12 06:52:43     147456 14.0.0.0.sqlite
    # 2024-12-16 08:58:00     258048 15.0.0.0.sqlite
    # 2024-12-18 19:49:59      77824 16.0.0.0.sqlite
    pandas = subset(request, variables, url_file)
    print(pandas)
