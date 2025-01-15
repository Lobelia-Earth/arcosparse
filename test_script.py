from src.arcosparse.main import subset
from src.arcosparse.models import RequestedCoordinate

if __name__ == "__main__":
    url_file = "https://s3.waw3-1.cloudferro.com/mdl-arco-time-057/arco/INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311--ext--latest/timeChunked"  # noqa

    # TODO: we should also be able to requests variables
    request = {
        "time": RequestedCoordinate(
            minimum=1731888000, maximum=1734516000, coodinate_id="time"
        ),
        "latitude": RequestedCoordinate(
            minimum=-63.900001525878906, maximum=90.0, coodinate_id="latitude"
        ),
        "longitude": RequestedCoordinate(
            minimum=-146.99937438964844,
            maximum=179.99998474121094,
            coodinate_id="longitude",
        ),
        # TODO: handle the elevation and depth problem if needed
        # TODO: fix the problem with the elevation,
        # cannot request the min and max two many chunks to create
        "elevation": RequestedCoordinate(
            maximum=120, minimum=-10, coodinate_id="elevation"
        ),
    }
    url_metadata = "https://stac.marine.copernicus.eu/metadata/INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311--ext--latest/dataset.stac.json"  # noqa

    # should download 3 chunks
    # 2024-12-12 06:52:43     147456 14.0.0.0.sqlite
    # 2024-12-16 08:58:00     258048 15.0.0.0.sqlite
    # 2024-12-18 19:49:59      77824 16.0.0.0.sqlite
    pandas = subset(request, ["ATMP", "CDOM"], url_metadata)
    print(pandas)
