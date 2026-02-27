import logging

import certifi
import requests
import requests.auth
from requests.adapters import HTTPAdapter, Retry

from arcosparse.environment_variables import PROXY_HTTP, PROXY_HTTPS
from arcosparse.models import UserConfiguration

logger = logging.getLogger("copernicusmarine")

PROXIES = {}
if PROXY_HTTP:
    PROXIES["http"] = PROXY_HTTP
if PROXY_HTTPS:
    PROXIES["https"] = PROXY_HTTPS


# TODO: add tests
# example: with https://httpbin.org/delay/10 or
# https://medium.com/@mpuig/testing-robust-requests-with-python-a06537d97771
class ConfiguredRequestsSession(requests.Session):
    def __init__(
        self,
        user_configuration: UserConfiguration,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.trust_env = user_configuration.trust_env
        if user_configuration.disable_ssl:
            self.verify = False
        else:
            self.verify = (
                user_configuration.ssl_certificate_path or certifi.where()
            )
        self.proxies = PROXIES
        if user_configuration.https_retries:
            self.mount(
                "https://",
                HTTPAdapter(
                    max_retries=Retry(
                        total=user_configuration.https_retries,
                        backoff_factor=1,
                        status_forcelist=[408, 429, 500, 502, 503, 504],
                    )
                ),
            )
        self.params = user_configuration.extra_params
        self.https_timeout = user_configuration.https_timeout
        self.bearer = (
            BearerAuth(user_configuration.auth_token)
            if user_configuration.auth_token
            else None
        )

    def request(self, *args, authenticated=True, **kwargs):
        kwargs.setdefault("timeout", self.https_timeout)
        if self.bearer and authenticated:
            kwargs.setdefault("auth", self.bearer)
        return super().request(*args, **kwargs)


# from https://stackoverflow.com/questions/29931671/making-an-api-call-in-python-with-an-api-that-requires-a-bearer-token
class BearerAuth(requests.auth.AuthBase):
    """
    Allow to pass the bearer as "auth" argument to the requests.get method and not as a header.

    Hence the headers are not overwritten by the netrc file as stated here:
    https://requests.readthedocs.io/en/latest/user/authentication/#netrc-authentication
    """  # noqa

    def __init__(self, token: str):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r
