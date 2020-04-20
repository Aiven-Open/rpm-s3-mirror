import hashlib

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry


class InvalidChecksumError(Exception):
    pass


def validate_checksum(path, checksum_type, checksum) -> None:
    if checksum_type != "sha256":
        raise ValueError("Only sha256 checksums are currently supported")
    with open(path, "rb") as f:
        local_checksum = hashlib.sha256(f.read()).hexdigest()
        if checksum != local_checksum:
            raise InvalidChecksumError(f"{path}: expected {checksum} found {local_checksum}")


def get_requests_session() -> Session:
    session = requests.session()
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session
