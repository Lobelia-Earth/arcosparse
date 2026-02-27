import os
from dataclasses import asdict

from dotenv import load_dotenv

import arcosparse

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".test.env"))

URL_METADATA_1 = "https://stac.marine.copernicus.eu/metadata/INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311--ext--history/dataset.stac.json"  # noqa
URL_METADATA_2 = "https://object-store.os-api.cci2.ecmwf.int/cadl-metadata/metadata/satellite_lake_water_level/multi-track/dataset.stac.json"  # noqa
USER_CONFIGURATION_1 = arcosparse.UserConfiguration(
    auth_token=None,
)
USER_CONFIGURATION_2 = arcosparse.UserConfiguration(
    auth_token=os.getenv("ARCOSPARSE_ECMWF_TOKEN")
)


class TestGetEntities:
    def test_get_entities(self, snapshot):
        for i, (url, user_configuration) in enumerate(
            [
                (URL_METADATA_1, USER_CONFIGURATION_1),
                (URL_METADATA_2, USER_CONFIGURATION_2),
            ],
            start=1,
        ):
            entities = arcosparse.get_entities(
                url, user_configuration=user_configuration
            )
            assert entities is not None
            assert len(entities) > 0
            assert snapshot(name=f"url{i}") == [
                asdict(entity) for entity in entities
            ]
