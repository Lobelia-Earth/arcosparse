import gzip
import json
import logging
from pathlib import PurePosixPath
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import boto3
import botocore
import botocore.config
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from arcosparse.models import UserConfiguration

logger = logging.getLogger("copernicusmarine")


class ConfiguredBoto3Session:
    def __init__(
        self,
        url: str,
        user_configuration: UserConfiguration,
        token_authenticated: bool = True,
    ):
        self.endpoint_url, self.bucket_name, self.prefix = (
            self._parse_access_dataset_url(url)
        )
        if user_configuration.auth_token and user_configuration.s3_credentials:
            raise ValueError(
                "Cannot use both auth_token and "
                "s3_credentials for authentication"
            )
        self.s3_client = self._get_configured_boto3_session(
            self.endpoint_url,
            user_configuration,
            token_authenticated,
        )
        self.use_threads = user_configuration.use_threads

    def close(self):
        self.s3_client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def download_file(self, object_key: str, file_path: str) -> Optional[str]:
        """
        If the file is not found, returns None,
        else returns the path to the file.
        """
        try:
            self.s3_client.download_file(
                self.bucket_name,
                str(PurePosixPath(self.prefix) / object_key),
                file_path,
                Config=TransferConfig(use_threads=self.use_threads),
            )
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code in ["403", "404"]:
                logger.debug(
                    f"File {object_key} not found in bucket {self.bucket_name}"
                )
                return None
            logger.error(f"Error downloading file {object_key}: {e}")
            raise
        return file_path

    def get_object(self, object_key: str) -> dict:
        full_object_key = self.prefix
        if object_key:
            full_object_key = str(PurePosixPath(self.prefix) / object_key)
        response = self.s3_client.get_object(
            Bucket=self.bucket_name, Key=full_object_key
        )
        self._raise_for_status(response)
        return self._response_to_json(response)

    def _raise_for_status(self, response: Any) -> None:
        status_code = response.get("ResponseMetadata", {}).get(
            "HTTPStatusCode"
        )
        if status_code is None:
            raise ValueError("Invalid response object: missing HTTPStatusCode")
        if not (200 <= status_code < 300):
            raise ValueError(f"HTTP error {status_code} for S3 operation")

    def _response_to_json(self, response: Any) -> dict:
        result = response["Body"].read()
        if response.get("ContentEncoding") == "gzip":
            result = gzip.decompress(result)
        return json.loads(result)

    def _get_configured_boto3_session(
        self,
        endpoint_url: str,
        user_configuration: UserConfiguration,
        token_authenticated: bool,
    ) -> Any:
        extra_config = {}
        if not user_configuration.trust_env:
            extra_config["proxies"] = {"http": "", "https": ""}
        if not user_configuration.s3_credentials:
            extra_config["signature_version"] = botocore.UNSIGNED

        extra_client_arguments = {}
        if user_configuration.disable_ssl:
            extra_client_arguments["verify"] = False
        elif user_configuration.ssl_certificate_path:
            extra_client_arguments["verify"] = (
                user_configuration.ssl_certificate_path
            )

        config_boto3 = botocore.config.Config(
            retries={
                "max_attempts": user_configuration.https_retries,
                "mode": "adaptive",
            },
            **extra_config,
        )
        s3_session = boto3.Session()
        s3_client = s3_session.client(
            "s3",
            config=config_boto3,
            endpoint_url=endpoint_url,
            aws_access_key_id=(
                user_configuration.s3_credentials.access_key
                if user_configuration.s3_credentials
                else None
            ),
            aws_secret_access_key=(
                user_configuration.s3_credentials.secret_key
                if user_configuration.s3_credentials
                else None
            ),
            aws_session_token=(
                user_configuration.s3_credentials.session_token
                if user_configuration.s3_credentials
                else None
            ),
            **extra_client_arguments,
        )
        extra_headers = None
        if user_configuration.auth_token and token_authenticated:
            extra_headers = {
                "Authorization": f"Bearer {user_configuration.auth_token}"
            }
        # Register the botocore event handler for adding custom params
        s3_client.meta.events.register(
            "before-call.s3.*",
            self._create_custom_query_function(
                user_configuration.extra_params,
                extra_headers,
            ),
        )

        return s3_client

    def _create_custom_query_function(
        self,
        extra_params: dict[str, str],
        extra_headers: dict[str, str] | None = None,
    ) -> Callable:
        def _add_custom_query_param(params, context, **kwargs):
            params["url"] = self._construct_url_with_query_params(
                params["url"], extra_params
            )
            if extra_headers:
                params["headers"].update(extra_headers)

        return _add_custom_query_param

    def _construct_url_with_query_params(
        self, url: str, query_params: dict[str, str]
    ) -> str:
        parsed = urlparse(url)

        existing_params = parse_qs(parsed.query, keep_blank_values=True)
        flat_existing = {k: v[0] for k, v in existing_params.items()}
        merged_params = {**flat_existing, **query_params}

        new_query = urlencode(merged_params)
        new_parsed = parsed._replace(query=new_query)
        return urlunparse(new_parsed)

    def _parse_access_dataset_url(
        self, data_path: str
    ) -> tuple[str, str, str]:
        parsed = urlparse(data_path)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.netloc
            or not parsed.path
        ):
            raise ValueError(f"Invalid data path: {data_path}")
        endpoint_url = f"{parsed.scheme}://{parsed.netloc}"
        # Path is expected to be of the form /bucket/optional/extra/path
        path_without_leading_slash = parsed.path.lstrip("/")
        segments = path_without_leading_slash.split("/", 1)
        if not segments[0]:
            raise ValueError(f"Invalid data path: {data_path}")
        bucket = segments[0]
        path = segments[1] if len(segments) > 1 else ""
        return endpoint_url, bucket, path
