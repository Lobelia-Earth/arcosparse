import logging
from typing import Optional

import certifi
import requests
import requests.auth
from requests.adapters import HTTPAdapter, Retry

from src.arcosparse.environment_variables import PROXY_HTTP, PROXY_HTTPS

logger = logging.getLogger("copernicusmarine")

PROXIES = {}
if PROXY_HTTP:
    PROXIES["http"] = PROXY_HTTP
if PROXY_HTTPS:
    PROXIES["https"] = PROXY_HTTPS

HTTPS_TIMEOUT = 60
HTTPS_RETRIES = 5


# TODO: add tests
# example: with https://httpbin.org/delay/10 or
# https://medium.com/@mpuig/testing-robust-requests-with-python-a06537d97771
class ConfiguredRequestsSession(requests.Session):
    def __init__(
        self,
        disable_ssl_context: bool,
        trust_env: bool,
        ssl_certificate_path: Optional[str],
        extra_params: dict[str, str] = {},
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.trust_env = trust_env
        if disable_ssl_context:
            self.verify = False
        else:
            self.verify = ssl_certificate_path or certifi.where()
        self.proxies = PROXIES
        if HTTPS_RETRIES:
            self.mount(
                "https://",
                HTTPAdapter(
                    max_retries=Retry(
                        total=HTTPS_RETRIES,
                        backoff_factor=1,
                        status_forcelist=[408, 429, 500, 502, 503, 504],
                    )
                ),
            )
        self.params = extra_params

    def request(self, *args, **kwargs):
        kwargs.setdefault("timeout", HTTPS_TIMEOUT)
        return super().request(*args, **kwargs)
