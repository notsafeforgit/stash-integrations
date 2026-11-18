"""Bounded application requests for reviewed source-management operations."""

from http.client import HTTPException
import os
from urllib.error import HTTPError, URLError
from urllib.request import Request

from .backfill_import import ImportClient
from .client import Unavailable
from .encoding import InvalidData, decode, encode
from .events import sha256 as valid_sha256


def sha256(value):
    if not valid_sha256(value):
        raise InvalidData("Expected a SHA-256 digest")
    return value


def integer(value, minimum=0, maximum=(1 << 63) - 1):
    if type(value) is not int or not minimum <= value <= maximum:
        raise InvalidData("Invalid management counter")
    return value


class ManagementClient(ImportClient):
    """Authenticated JSON requests to the Stash archive API; domain subclasses validate."""

    def request(self, method, path, value=None):
        key = os.environ.get(self.key_env, "")
        if not key or any(ord(c) <= 32 or ord(c) >= 127 for c in key):
            raise Unavailable("stash_application_key_missing")
        request = Request(self.endpoint + "/api/v3/archive" + path, method=method,
                          data=None if value is None else encode(value, self.max_http_bytes),
                          headers={"ApiKey": key, "Accept": "application/json", "Content-Type": "application/json"})
        try:
            with self.opener.open(request, timeout=60) as response:
                if (response.status != 200 or response.headers.get_content_type() != "application/json"
                        or response.headers.get("Content-Encoding") is not None):
                    raise Unavailable(f"invalid_{self.kind}_activation_response")
                return decode(response.read(self.max_http_bytes + 1), self.max_http_bytes)
        except HTTPError as error:
            code = error.code
            error.close()
            raise Unavailable(f"{self.kind}_activation_rejected", code) from None
        except (HTTPException, URLError, OSError, TimeoutError):
            raise Unavailable("network_unavailable") from None
        except InvalidData:
            raise Unavailable(f"invalid_{self.kind}_activation_response") from None
