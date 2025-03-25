import logging
from pathlib import Path

import pandas as pd

from arcosparse.models import (
    RequestedCoordinate,
    UserConfiguration,
    UserRequest,
)
from arcosparse.subsetter import _subset

logging.getLogger("arcosparse").setLevel(logging.DEBUG)

if __name__ == "__main__":
    url_file = "https://s3.waw3-1.cloudferro.com/mdl-arco-time-057/arco/INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311--ext--latest/timeChunked"  # noqa
    user_configuration = UserConfiguration(
        extra_params={
            "x-cop-client": "copernicus-marine-toolbox",
            "x-cop-client-version": "1.3.4",
            "x-cop-user": "rjester",
        }
    )
    request = UserRequest(
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
        # platform_ids=[
        #     "F-Vartdalsfjorden___MO",
        #     "B-Sulafjorden___MO",
        # ],  # [20726 rows x 10 columns]
        platform_ids=[],  # [100938 rows x 10 columns]
    )
    url_metadata = "https://stac.marine.copernicus.eu/metadata/INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311--ext--history/dataset.stac.json"  # noqa
    output_path = Path("todelete")
    output_path.mkdir(parents=True, exist_ok=True)
    # should download 3 chunks
    # 2024-12-12 06:52:43     147456 14.0.0.0.sqlite
    # 2024-12-16 08:58:00     258048 15.0.0.0.sqlite
    # 2024-12-18 19:49:59      77824 16.0.0.0.sqlite
    _subset(
        minimum_latitude=request.latitude.minimum,
        maximum_latitude=request.latitude.maximum,
        minimum_longitude=request.longitude.minimum,
        maximum_longitude=request.longitude.maximum,
        minimum_time=request.time.minimum,
        maximum_time=request.time.maximum,
        minimum_elevation=request.elevation.minimum,
        maximum_elevation=request.elevation.maximum,
        variables=request.variables,
        platform_ids=request.platform_ids,
        # vertical_axis="depth",
        vertical_axis="depth",
        user_configuration=user_configuration,
        url_metadata=url_metadata,
        output_path=output_path,
        # output_directory=None,
        disable_progress_bar=False,
        columns_rename={
            "platform_id": "entity_id",
            "platform_type": "entity_type",
            # # does not raise if doesn't exist
            "lksdjf": "lkshdf",
            # raises cannot have the same name as a column
            # "time": "entity_type",
            # also works with the same name
            # "platform_id": "platform_id",
        },
    )

    # open parquet file
    df = pd.read_parquet("todelete")
    print(df)
    try:
        print(df["elevation"])
    except KeyError:
        print("elevation not in the dataframe")
        print(df["depth"])
