from collections import namedtuple
from datetime import datetime
from typing import Optional, Iterator, Dict
from urllib.parse import urlparse

from lxml.etree import fromstring, Element
from lxml.etree import XMLParser
from dateutil.parser import parse
from os.path import join
from tempfile import TemporaryDirectory

import gzip

from requests import Response

from fedora_s3_mirror.util import download_repodata_section, get_requests_session

namespaces = {
    "common": "http://linux.duke.edu/metadata/common",
    "repo": "http://linux.duke.edu/metadata/repo",
    "rpm": "http://linux.duke.edu/metadata/rpm"
}


def safe_parse_xml(xml_string: bytes) -> Element:
    safe_parser = XMLParser(resolve_entities=False)
    return fromstring(xml_string, parser=safe_parser)


class Package:
    __slots__ = [
        "base_url",
        "name",
        "checksum",
        "location",
        "destination",
        "version",
        "epoch",
        "package_size",
        "release",
    ]

    def __init__(self, base_url: str, destination_path: str, package_element: Element):
        if not destination_path.endswith("/"):
            destination_path += "/"
        self.base_url = base_url
        self.name = package_element.findtext("common:name", namespaces=namespaces)
        self.checksum = package_element.findtext("common:checksum", namespaces=namespaces)
        self.location = package_element.find("common:location", namespaces=namespaces).get("href")
        self.destination = f"{destination_path}{self.location}"
        version_data = package_element.find("common:version", namespaces=namespaces)
        self.version = version_data.get("ver")
        self.epoch = version_data.get("epoch")
        self.package_size = int(package_element.find("common:size", namespaces=namespaces).get("package"))
        self.release = version_data.get("rel")

    @property
    def url(self) -> str:
        return f"{self.base_url}{self.location}"

    def __eq__(self, other) -> bool:
        if not isinstance(other, Package):
            return False
        return repr(self) == repr(other)

    def __repr__(self) -> str:
        return f"Package(name='{self.name}', version='{self.version}', epoch='{self.epoch}', checksum='{self.checksum}')"

    def __hash__(self) -> int:
        return hash(self.__repr__())


class PackageList:
    def __init__(self, base_url: str, packages_xml: bytes):
        self.base_url = base_url
        self.path = urlparse(base_url).path
        self.root = safe_parse_xml(packages_xml)

    def __len__(self) -> int:
        return int(self.root.get('packages'))

    def __iter__(self) -> Iterator[Package]:
        for package_element in self.root:
            yield Package(base_url=self.base_url, destination_path=self.path, package_element=package_element)


Metadata = namedtuple("Metadata", ["package_list", "repodata", "base_url"])
RepodataSection = namedtuple("RepodataSection", ["url", "location", "destination", "checksum_type", "checksum"])


class YUMRepository:
    def __init__(self, base_url: str):
        if not base_url.endswith("/"):
            base_url += "/"
        self.base_url = base_url
        self.path = urlparse(base_url).path
        self.session = get_requests_session()

    def has_updates(self, since: datetime) -> bool:
        response = self._req(self.session.head, "repodata/repomd.xml")
        last_modified_header = response.headers.get("Last-Modified")
        if not last_modified_header:
            return True
        return parse(last_modified_header) > since

    def parse_metadata(self) -> Metadata:
        response = self._req(self.session.get, "repodata/repomd.xml")
        xml = safe_parse_xml(response.content)
        repodata = self.parse_repomd(xml)
        package_list = self._extract_package_list(primary=repodata["primary"])
        return Metadata(package_list=package_list, repodata=repodata, base_url=self.base_url)

    def _extract_package_list(self, primary: RepodataSection) -> PackageList:
        with self._req(self.session.get, path=primary.location, stream=True) as request:
            with TemporaryDirectory(prefix="/var/tmp/") as temp_dir:
                local_path = download_repodata_section(primary, request, temp_dir)
                with gzip.open(local_path) as f:
                    return PackageList(base_url=self.base_url, packages_xml=f.read())

    def parse_repomd(self, xml: Element) -> Dict[str, RepodataSection]:
        sections = {}
        for data_element in xml.findall(f'repo:data', namespaces=namespaces):
            section_type = data_element.attrib["type"]
            section = {}
            for element in xml.findall(f'repo:data[@type="{section_type}"]/repo:*', namespaces=namespaces):
                # Strip the namespace from the tag as it is annoying
                _, _, key = element.tag.partition('}')
                value = element.text
                if key == "location":
                    value = element.get("href")
                elif "checksum" in key:
                    value = {"hash": value}
                    value.update(element.attrib)
                section[key] = value
            url = join(self.base_url, section["location"])
            checksum_type, checksum = section["checksum"]["type"], section["checksum"]["hash"]
            location = section["location"]
            sections[section_type] = RepodataSection(
                url=url,
                location=location,
                destination=f"{self.path}{location}",
                checksum_type=checksum_type,
                checksum=checksum,
            )
        return sections

    def _req(self, method, path, *, json=None, params=None, **kwargs) -> Response:
        url = f"{self.base_url}{path}"
        response = method(url, json=json, params=params, **kwargs)
        if response.status_code != 200:
            response.raise_for_status()
        return response
