from dataclasses import asdict

import arcosparse

URL_METADATA_1 = "https://stac.marine.copernicus.eu/metadata/INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311--ext--history/dataset.stac.json"  # noqa
URL_METADATA_2 = "https://object-store.os-api.cci2.ecmwf.int/cadl-metadata/metadata/satellite_lake_water_level/multi-track/dataset.stac.json"  # noqa


class TestGetMetadata:
    def test_get_metadata(self, snapshot):
        for i, url in enumerate([URL_METADATA_1, URL_METADATA_2], start=1):
            metadata = arcosparse.get_dataset_metadata(url)
            assert snapshot(name=f"url{i}") == asdict(metadata)
