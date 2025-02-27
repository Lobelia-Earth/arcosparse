from arcosparse import subset
from arcosparse.models import (
    RequestedCoordinate,
    UserConfiguration,
    UserRequest,
)

URL_METADATA = "https://stac.marine.copernicus.eu/metadata/INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311--ext--history/dataset.stac.json"  # noqa
USER_CONFIGURATION = UserConfiguration(
    extra_params={
        "x-cop-client": "copernicus-marine-toolbox",
        "x-cop-client-version": "1.3.4",
        "x-cop-user": "rjester",
    }
)
REQUEST = UserRequest(
    time=RequestedCoordinate(
        minimum=1700888000, maximum=1701516000, coodinate_id="time"
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
    variables=["ATMP", "PSAL"],
    platform_ids=[
        "F-Vartdalsfjorden___MO",
        "B-Sulafjorden___MO",
    ],
)


class TestPlatformSubsetting:
    def test_platform_subsetting(self):
        df = subset(REQUEST, USER_CONFIGURATION, URL_METADATA)
        values = df["platform_id"].values
        for platform_id in REQUEST.platform_ids:
            assert platform_id in values
