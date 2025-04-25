from copy import deepcopy

from arcosparse import subset_and_return_dataframe
from arcosparse.models import (
    UserConfiguration,
)

URL_METADATA = "https://stac.marine.copernicus.eu/metadata/INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311--ext--history/dataset.stac.json"  # noqa
USER_CONFIGURATION = UserConfiguration(
    extra_params={
        "x-cop-client": "copernicus-marine-toolbox",
        "x-cop-client-version": "1.3.4",
        "x-cop-user": "rjester",
    }
)


BASE_REQUEST = {
    "url_metadata": URL_METADATA,
    "minimum_latitude": -63.900001525878906,
    "maximum_latitude": 90.0,
    "minimum_longitude": -146.99937438964844,
    "maximum_longitude": 179.99998474121094,
    "minimum_time": 1700888000,
    "maximum_time": 1701516000,
    "minimum_elevation": -10,
    "maximum_elevation": 120,
    "variables": ["TEMP", "PSAL"],
    "entities": [
        "F-Vartdalsfjorden___MO",
        "B-Sulafjorden___MO",
    ],
    "user_configuration": USER_CONFIGURATION,
}


class TestSubsetting:
    def test_subsetting(self):
        df = subset_and_return_dataframe(
            **BASE_REQUEST,
        )
        assert df is not None
        assert not df.empty
        assert len(df.index) == len(set(df.index)) == 41452

    def test_can_request_with_limits_outside_extent(self):
        request = deepcopy(BASE_REQUEST)
        request["maximum_time"] = -4053996800
        request["minimum_time"] = -5063996800
        del request["entities"]
        df = subset_and_return_dataframe(
            **request,
        )
        assert df is not None
        assert not df.empty
        assert min(df["time"]) > -4063996800

    def test_can_get_depth_instead_of_elevation(self):
        df = subset_and_return_dataframe(
            **BASE_REQUEST,
            vertical_axis="depth",
            columns_rename={
                "entity_id": "platform_id",
            },
        )
        assert df is not None
        assert not df.empty
        assert "depth" in df.columns
        assert "platform_id" in df.columns

    def test_can_explicit_elevation_and_rename(self):
        df = subset_and_return_dataframe(
            **BASE_REQUEST,
            vertical_axis="elevation",
            columns_rename={
                "entity_id": "platform_id",
            },
        )
        assert df is not None
        assert not df.empty
        assert "elevation" in df.columns
        assert "platform_id" in df.columns
