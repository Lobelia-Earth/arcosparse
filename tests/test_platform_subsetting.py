from copy import deepcopy

from arcosparse import subset_and_return_dataframe, subset_and_save
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
    variables=["ATMP", "PSAL"],
    platform_ids=[
        "F-Vartdalsfjorden___MO",
        "B-Sulafjorden___MO",
    ],
)

REQUEST_WITH_WRONG_IDS = deepcopy(REQUEST)
REQUEST_WITH_WRONG_IDS.platform_ids = ["wrong_id"]


class TestPlatformSubsetting:
    def test_platform_subsetting(self):
        df = subset_and_return_dataframe(
            minimum_latitude=REQUEST.latitude.minimum,
            maximum_latitude=REQUEST.latitude.maximum,
            minimum_longitude=REQUEST.longitude.minimum,
            maximum_longitude=REQUEST.longitude.maximum,
            minimum_time=REQUEST.time.minimum,
            maximum_time=REQUEST.time.maximum,
            minimum_elevation=REQUEST.elevation.minimum,
            maximum_elevation=REQUEST.elevation.maximum,
            variables=REQUEST.variables,
            platform_ids=REQUEST.platform_ids,
            user_configuration=USER_CONFIGURATION,
            url_metadata=URL_METADATA,
        )
        values = df["platform_id"].values
        for platform_id in REQUEST.platform_ids:
            assert platform_id in values

    def test_platform_subsetting_save_locally(self, tmp_path):
        subset_and_save(
            minimum_latitude=REQUEST.latitude.minimum,
            maximum_latitude=REQUEST.latitude.maximum,
            minimum_longitude=REQUEST.longitude.minimum,
            maximum_longitude=REQUEST.longitude.maximum,
            minimum_time=REQUEST.time.minimum,
            maximum_time=REQUEST.time.maximum,
            minimum_elevation=REQUEST.elevation.minimum,
            maximum_elevation=REQUEST.elevation.maximum,
            variables=REQUEST.variables,
            platform_ids=REQUEST.platform_ids,
            user_configuration=USER_CONFIGURATION,
            url_metadata=URL_METADATA,
            output_path=tmp_path,
        )
        expected_files = [
            "B-Sulafjorden___MO_PSAL_6672.0.0.0.parquet",
            "B-Sulafjorden___MO_PSAL_6673.0.0.0.parquet",
            "F-Vartdalsfjorden___MO_PSAL_6672.0.0.0.parquet",
            "F-Vartdalsfjorden___MO_PSAL_6673.0.0.0.parquet",
        ]
        for file in expected_files:
            assert (tmp_path / file).exists()

    def test_wrong_platform_ids(self):
        try:
            _ = subset_and_return_dataframe(
                minimum_latitude=REQUEST_WITH_WRONG_IDS.latitude.minimum,
                maximum_latitude=REQUEST_WITH_WRONG_IDS.latitude.maximum,
                minimum_longitude=REQUEST_WITH_WRONG_IDS.longitude.minimum,
                maximum_longitude=REQUEST_WITH_WRONG_IDS.longitude.maximum,
                minimum_time=REQUEST_WITH_WRONG_IDS.time.minimum,
                maximum_time=REQUEST_WITH_WRONG_IDS.time.maximum,
                minimum_elevation=REQUEST_WITH_WRONG_IDS.elevation.minimum,
                maximum_elevation=REQUEST_WITH_WRONG_IDS.elevation.maximum,
                variables=REQUEST_WITH_WRONG_IDS.variables,
                platform_ids=REQUEST_WITH_WRONG_IDS.platform_ids,
                user_configuration=USER_CONFIGURATION,
                url_metadata=URL_METADATA,
            )
            assert False
        except ValueError as e:
            assert (
                f"Platform {REQUEST_WITH_WRONG_IDS.platform_ids[0]} is not available in the dataset."
                in str(e)
            )
