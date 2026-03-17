import gzip
import io
import json
from unittest.mock import MagicMock, patch

import certifi
import pytest
import requests
from botocore.exceptions import ClientError

from arcosparse.models import S3Credentials, UserConfiguration
from arcosparse.sessions import ConfiguredBoto3Session

# Example URLs from test_get_entities.py
URL_COPERNICUS = (
    "https://stac.marine.copernicus.eu/metadata/"
    "INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/"
    "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311"
    "--ext--history/dataset.stac.json"
)
URL_ECMWF = (
    "https://object-store.os-api.cci2.ecmwf.int/"
    "cadl-metadata/metadata/"
    "satellite_lake_water_level/multi-track/dataset.stac.json"
)


def _make_session(
    url=URL_COPERNICUS,
    user_configuration=None,
    token_authenticated=True,
):
    """Create a ConfiguredBoto3Session with boto3.Session mocked out."""
    if user_configuration is None:
        user_configuration = UserConfiguration()
    with patch("arcosparse.sessions.boto3.Session") as mock_boto3:
        mock_client = MagicMock()
        mock_boto3.return_value.client.return_value = mock_client
        session = ConfiguredBoto3Session(
            url=url,
            user_configuration=user_configuration,
            token_authenticated=token_authenticated,
        )
    return session, mock_boto3, mock_client


# ── URL parsing ─────────────────────────────────────────────────


class TestParseAccessDatasetUrl:
    def test_copernicus_url(self):
        session, _, _ = _make_session(url=URL_COPERNICUS)
        assert session.enpoint_url == "https://stac.marine.copernicus.eu"
        assert session.bucket_name == "metadata"
        assert session.prefix == (
            "INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/"
            "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311"
            "--ext--history/dataset.stac.json"
        )

    def test_ecmwf_url(self):
        session, _, _ = _make_session(url=URL_ECMWF)
        assert (
            session.enpoint_url == "https://object-store.os-api.cci2.ecmwf.int"
        )
        assert session.bucket_name == "cadl-metadata"
        assert session.prefix == (
            "metadata/satellite_lake_water_level/"
            "multi-track/dataset.stac.json"
        )

    def test_url_with_port(self):
        url = "https://my-host.example.com:9443/bucket/path/to/object.json"
        session, _, _ = _make_session(url=url)
        assert session.enpoint_url == "https://my-host.example.com"
        assert session.bucket_name == "bucket"
        assert session.prefix == "path/to/object.json"

    def test_http_url(self):
        url = "http://localhost/mybucket/foo/bar"
        session, _, _ = _make_session(url=url)
        assert session.enpoint_url == "http://localhost"
        assert session.bucket_name == "mybucket"
        assert session.prefix == "foo/bar"

    def test_invalid_url_raises(self):
        with pytest.raises(ValueError, match="Invalid data path"):
            _make_session(url="not-a-url")

    def test_only_dataset_root_path(self):
        session, _, _ = _make_session(url=URL_COPERNICUS)
        endpoint, bucket, path = session._parse_access_dataset_url(
            URL_COPERNICUS, only_dataset_root_path=True
        )
        assert endpoint == "https://stac.marine.copernicus.eu"
        assert bucket == "metadata"
        # only_dataset_root_path keeps segments[2:5] + "/"
        assert path == (
            "INSITU_ARC_PHYBGCWAV_DISCRETE_MYNRT_013_031/"
            "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr_202311"
            "--ext--history/dataset.stac.json/"
        )


# ── Query-param construction ────────────────────────────────────


class TestConstructUrlWithQueryParams:
    def _session(self):
        session, _, _ = _make_session()
        return session

    def test_adds_params_to_bare_url(self):
        session = self._session()
        result = session._construct_url_with_query_params(
            "https://example.com/path", {"foo": "1", "bar": "2"}
        )
        assert result is not None
        assert "foo=1" in result
        assert "bar=2" in result

    def test_merges_with_existing_params(self):
        session = self._session()
        result = session._construct_url_with_query_params(
            "https://example.com/path?existing=yes", {"new": "val"}
        )
        assert result is not None
        assert "existing=yes" in result
        assert "new=val" in result

    def test_overrides_existing_params(self):
        session = self._session()
        result = session._construct_url_with_query_params(
            "https://example.com/path?key=old", {"key": "new"}
        )
        assert result is not None
        assert "key=new" in result
        assert "key=old" not in result

    def test_empty_params_preserves_url(self):
        session = self._session()
        result = session._construct_url_with_query_params(
            "https://example.com/path?a=1", {}
        )
        assert result is not None
        assert "a=1" in result


# ── Response helpers ────────────────────────────────────────────


class TestRaiseForStatus:
    def _session(self):
        s, _, _ = _make_session()
        return s

    def test_200_does_not_raise(self):
        session = self._session()
        session._raise_for_status(
            {"ResponseMetadata": {"HTTPStatusCode": 200}}
        )

    def test_299_does_not_raise(self):
        session = self._session()
        session._raise_for_status(
            {"ResponseMetadata": {"HTTPStatusCode": 299}}
        )

    def test_400_raises_http_error(self):
        session = self._session()
        with pytest.raises(requests.HTTPError, match="400"):
            session._raise_for_status(
                {"ResponseMetadata": {"HTTPStatusCode": 400}}
            )

    def test_500_raises_http_error(self):
        session = self._session()
        with pytest.raises(requests.HTTPError, match="500"):
            session._raise_for_status(
                {"ResponseMetadata": {"HTTPStatusCode": 500}}
            )

    def test_missing_status_raises_value_error(self):
        session = self._session()
        with pytest.raises(ValueError, match="missing HTTPStatusCode"):
            session._raise_for_status({})


class TestResponseToJson:
    def _session(self):
        s, _, _ = _make_session()
        return s

    def test_plain_json(self):
        session = self._session()
        payload = {"hello": "world"}
        body = io.BytesIO(json.dumps(payload).encode())
        response = {"Body": body}
        assert session._response_to_json(response) == payload

    def test_gzip_json(self):
        session = self._session()
        payload = {"compressed": True}
        compressed = gzip.compress(json.dumps(payload).encode())
        body = io.BytesIO(compressed)
        response = {"Body": body, "ContentEncoding": "gzip"}
        assert session._response_to_json(response) == payload


# ── Constructor / UserConfiguration variants ────────────────────


class TestDefaultConfiguration:
    def test_default_uses_certifi_and_unsigned(self):
        with patch("arcosparse.sessions.boto3.Session") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.return_value.client.return_value = mock_client

            session = ConfiguredBoto3Session(
                url=URL_COPERNICUS,
                user_configuration=UserConfiguration(),
            )

            client_call = mock_boto3.return_value.client
            client_call.assert_called_once()
            kwargs = client_call.call_args
            assert kwargs.kwargs["verify"] == certifi.where()
            assert kwargs.kwargs["endpoint_url"] == (
                "https://stac.marine.copernicus.eu"
            )
            assert kwargs.kwargs["aws_access_key_id"] is None
            assert kwargs.kwargs["aws_secret_access_key"] is None
            assert kwargs.kwargs["aws_session_token"] is None
            assert session.use_threads is True


class TestDisableSsl:
    def test_verify_is_false_when_ssl_disabled(self):
        with patch("arcosparse.sessions.boto3.Session") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.return_value.client.return_value = mock_client

            ConfiguredBoto3Session(
                url=URL_COPERNICUS,
                user_configuration=UserConfiguration(disable_ssl=True),
            )

            kwargs = mock_boto3.return_value.client.call_args
            assert kwargs.kwargs["verify"] is False


class TestSslCertificatePath:
    def test_custom_cert_path(self):
        with patch("arcosparse.sessions.boto3.Session") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.return_value.client.return_value = mock_client

            ConfiguredBoto3Session(
                url=URL_COPERNICUS,
                user_configuration=UserConfiguration(
                    ssl_certificate_path="/custom/cert.pem"
                ),
            )

            kwargs = mock_boto3.return_value.client.call_args
            assert kwargs.kwargs["verify"] == "/custom/cert.pem"


class TestTrustEnv:
    def test_trust_env_false_sets_empty_proxies(self):
        with patch("arcosparse.sessions.boto3.Session") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.return_value.client.return_value = mock_client

            ConfiguredBoto3Session(
                url=URL_COPERNICUS,
                user_configuration=UserConfiguration(trust_env=False),
            )

            kwargs = mock_boto3.return_value.client.call_args
            config = kwargs.kwargs["config"]
            assert config.proxies == {"http": "", "https": ""}


class TestHttpsRetries:
    def test_custom_retries(self):
        with patch("arcosparse.sessions.boto3.Session") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.return_value.client.return_value = mock_client

            ConfiguredBoto3Session(
                url=URL_COPERNICUS,
                user_configuration=UserConfiguration(https_retries=10),
            )

            kwargs = mock_boto3.return_value.client.call_args
            config = kwargs.kwargs["config"]
            assert config.retries["max_attempts"] == 10
            assert config.retries["mode"] == "adaptive"


class TestUseThreads:
    def test_use_threads_false(self):
        session, _, _ = _make_session(
            user_configuration=UserConfiguration(use_threads=False)
        )
        assert session.use_threads is False

    def test_use_threads_true(self):
        session, _, _ = _make_session(
            user_configuration=UserConfiguration(use_threads=True)
        )
        assert session.use_threads is True


class TestS3Credentials:
    def test_credentials_passed_to_client(self):
        creds = S3Credentials(
            access_key="AKID",
            secret_key="SECRET",
            session_token="TOKEN",
        )
        with patch("arcosparse.sessions.boto3.Session") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.return_value.client.return_value = mock_client

            ConfiguredBoto3Session(
                url=URL_COPERNICUS,
                user_configuration=UserConfiguration(s3_credentials=creds),
            )

            kwargs = mock_boto3.return_value.client.call_args
            assert kwargs.kwargs["aws_access_key_id"] == "AKID"
            assert kwargs.kwargs["aws_secret_access_key"] == "SECRET"
            assert kwargs.kwargs["aws_session_token"] == "TOKEN"

    def test_credentials_without_session_token(self):
        creds = S3Credentials(access_key="AKID", secret_key="SECRET")
        with patch("arcosparse.sessions.boto3.Session") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.return_value.client.return_value = mock_client

            ConfiguredBoto3Session(
                url=URL_COPERNICUS,
                user_configuration=UserConfiguration(s3_credentials=creds),
            )

            kwargs = mock_boto3.return_value.client.call_args
            assert kwargs.kwargs["aws_access_key_id"] == "AKID"
            assert kwargs.kwargs["aws_secret_access_key"] == "SECRET"
            assert kwargs.kwargs["aws_session_token"] is None


class TestAuthToken:
    def test_token_registered_as_header(self):
        """When token_authenticated=True, the event handler should
        inject an Authorization header."""
        with patch("arcosparse.sessions.boto3.Session") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.return_value.client.return_value = mock_client

            session = ConfiguredBoto3Session(
                url=URL_COPERNICUS,
                user_configuration=UserConfiguration(
                    auth_token="my-secret-token"
                ),
                token_authenticated=True,
            )

            # Simulate what the event handler does
            handler = session._create_custom_query_function(
                extra_params={},
                extra_headers={
                    "Authorization": "Bearer my-secret-token",
                },
            )
            params = {
                "url": "https://example.com/path",
                "headers": {},
            }
            handler(params, context={})
            assert params["headers"]["Authorization"] == (
                "Bearer my-secret-token"
            )

    def test_token_not_registered_when_not_authenticated(self):
        """When token_authenticated=False, no Authorization header."""
        with patch("arcosparse.sessions.boto3.Session") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.return_value.client.return_value = mock_client

            session = ConfiguredBoto3Session(
                url=URL_COPERNICUS,
                user_configuration=UserConfiguration(
                    auth_token="my-secret-token"
                ),
                token_authenticated=False,
            )

            # The handler created without extra_headers should not set auth
            handler = session._create_custom_query_function(
                extra_params={},
                extra_headers=None,
            )
            params = {
                "url": "https://example.com/path",
                "headers": {},
            }
            handler(params, context={})
            assert "Authorization" not in params["headers"]


class TestExtraParams:
    def test_extra_params_injected_via_event_handler(self):
        session, _, _ = _make_session(
            user_configuration=UserConfiguration(
                extra_params={"x-cop-client": "test", "x-version": "1.0"}
            )
        )
        handler = session._create_custom_query_function(
            extra_params={"x-cop-client": "test", "x-version": "1.0"},
        )
        params = {
            "url": "https://example.com/path",
            "headers": {},
        }
        handler(params, context={})
        assert "x-cop-client=test" in params["url"]
        assert "x-version=1.0" in params["url"]


# ── Validation ──────────────────────────────────────────────────


class TestAuthConflict:
    def test_raises_when_both_token_and_credentials(self):
        creds = S3Credentials(access_key="AK", secret_key="SK")
        with pytest.raises(ValueError, match="Cannot use both"):
            _make_session(
                user_configuration=UserConfiguration(
                    auth_token="token",
                    s3_credentials=creds,
                )
            )


# ── Context manager ─────────────────────────────────────────────


class TestContextManager:
    def test_enter_returns_self(self):
        session, _, _ = _make_session()
        assert session.__enter__() is session

    def test_exit_calls_close(self):
        session, _, mock_client = _make_session()
        session.__exit__(None, None, None)
        mock_client.close.assert_called_once()

    def test_with_statement(self):
        with patch("arcosparse.sessions.boto3.Session") as mock_boto3:
            mock_client = MagicMock()
            mock_boto3.return_value.client.return_value = mock_client

            with ConfiguredBoto3Session(
                url=URL_COPERNICUS,
                user_configuration=UserConfiguration(),
            ) as session:
                assert isinstance(session, ConfiguredBoto3Session)

            mock_client.close.assert_called_once()


# ── download_file / get_object ──────────────────────────────────


class TestDownloadFile:
    def test_calls_s3_download(self, tmp_path):
        session, _, mock_client = _make_session()
        session.download_file("chunk.parquet", str(tmp_path / "out.parquet"))
        mock_client.download_file.assert_called_once()
        args = mock_client.download_file.call_args
        assert args.args[0] == session.bucket_name
        assert "chunk.parquet" in args.args[1]
        assert str(tmp_path / "out.parquet") == args.args[2]

    def test_403_error_is_silenced(self, tmp_path):
        session, _, mock_client = _make_session()
        error_response = {"Error": {"Code": "403", "Message": "Forbidden"}}
        mock_client.download_file.side_effect = ClientError(
            error_response, "GetObject"
        )
        # Should not raise
        session.download_file("missing.parquet", str(tmp_path / "out"))

    def test_404_error_is_silenced(self, tmp_path):
        session, _, mock_client = _make_session()
        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_client.download_file.side_effect = ClientError(
            error_response, "GetObject"
        )
        session.download_file("missing.parquet", str(tmp_path / "out"))

    def test_other_client_error_is_raised(self, tmp_path):
        session, _, mock_client = _make_session()
        error_response = {
            "Error": {"Code": "500", "Message": "Internal Error"}
        }
        mock_client.download_file.side_effect = ClientError(
            error_response, "GetObject"
        )
        with pytest.raises(ClientError):
            session.download_file("fail.parquet", str(tmp_path / "out"))


class TestGetObject:
    def _mock_s3_response(self, payload: dict, gzipped: bool = False) -> dict:
        raw = json.dumps(payload).encode()
        if gzipped:
            raw = gzip.compress(raw)
            return {
                "ResponseMetadata": {"HTTPStatusCode": 200},
                "ContentEncoding": "gzip",
                "Body": io.BytesIO(raw),
            }
        return {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "Body": io.BytesIO(raw),
        }

    def test_get_object_returns_json(self):
        session, _, mock_client = _make_session()
        payload = {"data": [1, 2, 3]}
        mock_client.get_object.return_value = self._mock_s3_response(payload)

        result = session.get_object("index.json")
        assert result == payload

    def test_get_object_with_gzip(self):
        session, _, mock_client = _make_session()
        payload = {"compressed": True}
        mock_client.get_object.return_value = self._mock_s3_response(
            payload, gzipped=True
        )

        result = session.get_object("index.json.gz")
        assert result == payload

    def test_get_object_empty_key_uses_prefix(self):
        session, _, mock_client = _make_session()
        payload = {"root": True}
        mock_client.get_object.return_value = self._mock_s3_response(payload)

        session.get_object("")
        call_kwargs = mock_client.get_object.call_args.kwargs
        assert call_kwargs["Key"] == session.prefix

    def test_get_object_raises_on_error_status(self):
        session, _, mock_client = _make_session()
        mock_client.get_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 403},
            "Body": io.BytesIO(b""),
        }
        with pytest.raises(requests.HTTPError, match="403"):
            session.get_object("secret.json")
