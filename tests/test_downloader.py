from arcosparse.downloader import download_and_convert_to_pandas
from tests.test_platform_subsetting import USER_CONFIGURATION

URL_METADATA_1 = "https://s3.waw3-1.cloudferro.com/mdl-arco-time-059/arco/INSITU_MED_PHYBGCWAV_DISCRETE_MYNRT_013_035/cmems_obs-ins_med_phybgcwav_mynrt_na_irr_202311--ext--monthly/platformChunked"


class TestDownloader:
    def test_downloader(self):
        df = download_and_convert_to_pandas(
            base_url=URL_METADATA_1,
            variable_id="EWCT",
            chunk_name="1.0.0.0",
            platform_id="FMCY___AD",
            output_coordinates=[],
            user_configuration=USER_CONFIGURATION,
            output_path=None,
            vertical_axis="depth",
            columns_rename={},
        )
        assert df is not None
        assert not df.empty
        assert df.size > 62728000
