import pystac

from src.arcosparse.main import choose_best_asset
from src.arcosparse.models import RequestedCoordinate, UserRequest

url_metadata = "https://stac.marine.copernicus.eu/metadata/INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311--ext--latest/dataset.stac.json"  # noqa

# TODO: check other insitus with more points along time and space
# so that we can also design more hybrid tests

LONG_GEO_RANGE_REQUEST = UserRequest(
    time=RequestedCoordinate(
        minimum=1731888000, maximum=1734516000, coodinate_id="time"
    ),
    latitude=RequestedCoordinate(
        minimum=-63.900001525878906, maximum=90.0, coodinate_id="latitude"
    ),
    longitude=RequestedCoordinate(
        minimum=-146.99937438964844,
        maximum=179.99998474121094,
        coodinate_id="longitude",
    ),
    # TODO: handle the elevation and depth problem if needed
    # TODO: fix the problem with the elevation,
    # cannot request the min and max two many chunks to create
    elevation=RequestedCoordinate(
        maximum=120, minimum=-10, coodinate_id="elevation"
    ),
    variables=["ATMP", "CDOM"],
    platform_ids=[],
)

LONG_TIME_RANGE_REQUEST = UserRequest(
    time=RequestedCoordinate(
        minimum=1716308800, maximum=1736958400, coodinate_id="time"
    ),
    latitude=RequestedCoordinate(
        minimum=69, maximum=70.0, coodinate_id="latitude"
    ),
    longitude=RequestedCoordinate(
        minimum=-146.99937438964844,
        maximum=-146.0,
        coodinate_id="longitude",
    ),
    # TODO: handle the elevation and depth problem if needed
    # TODO: fix the problem with the elevation,
    # cannot request the min and max two many chunks to create
    elevation=RequestedCoordinate(
        maximum=120, minimum=-10, coodinate_id="elevation"
    ),
    variables=["TEMP", "PSAL"],
    platform_ids=[],
)

PLATFORM_REQUEST = UserRequest(
    time=RequestedCoordinate(
        minimum=1716308800, maximum=1736958400, coodinate_id="time"
    ),
    latitude=RequestedCoordinate(
        minimum=69, maximum=70.0, coodinate_id="latitude"
    ),
    longitude=RequestedCoordinate(
        minimum=-146.99937438964844,
        maximum=-146.0,
        coodinate_id="longitude",
    ),
    elevation=RequestedCoordinate(
        maximum=120, minimum=-10, coodinate_id="elevation"
    ),
    variables=["TEMP", "PSAL"],
    platform_ids=["something"],
)


class TestChoosingArcoAsset:
    def test_choose_time_chunked_for_long_geo_range(self):
        metadata = pystac.Item.from_file(url_metadata)
        _, asset_url = choose_best_asset(metadata, LONG_GEO_RANGE_REQUEST)
        assert "timeChunked" in asset_url

    def test_choose_geo_chunked_for_long_time_range(self):
        metadata = pystac.Item.from_file(url_metadata)
        _, asset_url = choose_best_asset(metadata, LONG_TIME_RANGE_REQUEST)
        assert "geoChunked" in asset_url

    def test_choose_platform_chunked_for_long_time_range(self):
        metadata = pystac.Item.from_file(url_metadata)
        try:
            _, _ = choose_best_asset(metadata, PLATFORM_REQUEST)
        except Exception as e:
            assert "Platform subsetting not implemented yet" in str(e)
