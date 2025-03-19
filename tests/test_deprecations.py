from arcosparse import get_platforms_names

URL_METADATA = "https://stac.marine.copernicus.eu/metadata/INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311--ext--history/dataset.stac.json"  # noqa


class TestDepecations:

    def test_get_platforms_names_deprecated(self, caplog):
        result = get_platforms_names(URL_METADATA)
        assert "WARNING" in caplog.text
        assert "'get_platforms_names' is deprecated" in caplog.text
        assert result
        assert "F-Vartdalsfjorden___MO" in result
