import logging

import requests
from requests.adapters import HTTPAdapter, Retry

from utils.common import timer

DEFAULT_TIMEOUT = 5  # seconds
RETRIES_NUM = 5
RETRY_BACKOFF_FACTOR = 1
RETRY_STATUS_FORCELIST = [429, 500, 502, 503, 504]


class TimeoutHTTPAdapter(HTTPAdapter):
    """Set default timeout of each request"""

    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


class HTTPRequestsDataExtract:
    """Wrapper of requests library"""

    def __init__(
        self,
        url: str,
        method: str = "GET",
        data: dict = None,
        json: dict = None,
        params: dict = None,
        headers=None,
        auth=None,
        timeout=None,
        **kwargs,
    ):
        self.url = url
        self.method = method
        self.data = data
        self.json = json
        self.params = params
        self.headers = headers
        self.auth = auth

        self.session = requests.Session()

        self.timeout = timeout or DEFAULT_TIMEOUT
        self.retries_config = {
            "total": RETRIES_NUM,
            "backoff_factor": RETRY_BACKOFF_FACTOR,
            "status_forcelist": RETRY_STATUS_FORCELIST,
        }
        self.set_up_retries_time_out()

    def set_up_retries_time_out(self):
        logging.info("Config timeout and number of retries")
        retries = Retry(**self.retries_config)

        adapter = TimeoutHTTPAdapter(max_retries=retries, timeout=self.timeout)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    @timer
    def make_request(self):
        logging.info(f"Requesting to {self.url} ...")
        response = self.session.request(
            url=self.url,
            method=self.method,
            data=self.data,
            json=self.json,
            params=self.params,
            headers=self.headers,
            auth=self.auth,
        )
        response.raise_for_status()

        logging.info(
            f"Request to {response.url} complete with status code {response.status_code}"
        )

        return response
