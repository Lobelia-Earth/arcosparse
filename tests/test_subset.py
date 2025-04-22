from arcosparse import subset_and_return_dataframe
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
        minimum=1700888000, maximum=1701516000, coordinate_id="time"
    ),
    latitude=RequestedCoordinate(
        minimum=-63.900001525878906, maximum=90.0, coordinate_id="latitude"
    ),
    longitude=RequestedCoordinate(
        minimum=-146.99937438964844,
        maximum=179.99998474121094,
        coordinate_id="longitude",
    ),
    # TODO: handle the elevation and depth problem if needed
    # TODO: fix the problem with the elevation,
    # cannot request the min and max two many chunks to create
    elevation=RequestedCoordinate(
        maximum=120, minimum=-10, coordinate_id="elevation"
    ),
    variables=["TEMP", "PSAL"],
    platform_ids=[
        "F-Vartdalsfjorden___MO",
        "B-Sulafjorden___MO",
    ],
)


class TestSubsetting:
    def test__subsetting(self):
        df = subset_and_return_dataframe(
            url_metadata=URL_METADATA,
            minimum_latitude=REQUEST.latitude.minimum,
            maximum_latitude=REQUEST.latitude.maximum,
            minimum_longitude=REQUEST.longitude.minimum,
            maximum_longitude=REQUEST.longitude.maximum,
            minimum_time=REQUEST.time.minimum,
            maximum_time=REQUEST.time.maximum,
            minimum_elevation=REQUEST.elevation.minimum,
            maximum_elevation=REQUEST.elevation.maximum,
            variables=REQUEST.variables,
            entities=REQUEST.platform_ids,
        )
        assert df is not None
        assert not df.empty
        assert len(df.index) == len(set(df.index)) == 41452

    def test_can_request_with_limits_outside_extent(self):
        df = subset_and_return_dataframe(
            url_metadata=URL_METADATA,
            minimum_latitude=REQUEST.latitude.minimum,
            maximum_latitude=REQUEST.latitude.maximum,
            minimum_longitude=REQUEST.longitude.minimum,
            maximum_longitude=REQUEST.longitude.maximum,
            minimum_time=-5063996800,  # real min time is -4063996800
            maximum_time=-4053996800,
            minimum_elevation=REQUEST.elevation.minimum,
            maximum_elevation=REQUEST.elevation.maximum,
            variables=REQUEST.variables,
        )
        assert df is not None
        assert not df.empty
        assert min(df["time"]) > -4063996800
