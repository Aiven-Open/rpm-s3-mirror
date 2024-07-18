# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
import dataclasses
import lzma
import zstandard
import re
import subprocess
from abc import abstractmethod
from collections import namedtuple
from datetime import datetime
from typing import Iterator, Dict, Optional, Tuple
from urllib.parse import urlparse
from xml.etree.ElementTree import ElementTree

from lxml.etree import fromstring, Element, tostring  # pylint: disable=no-name-in-module
from lxml.etree import XMLParser  # pylint: disable=no-name-in-module
from dateutil.parser import parse
from tempfile import TemporaryDirectory
import os
import shutil
from pathlib import Path
from os.path import join, basename

import gzip

from requests import Response

from rpm_s3_mirror.util import get_requests_session, validate_checksum, sha256

namespaces = {
    "common": "http://linux.duke.edu/metadata/common",
    "repo": "http://linux.duke.edu/metadata/repo",
    "rpm": "http://linux.duke.edu/metadata/rpm",
}


def safe_parse_xml(xml_bytes: bytes) -> Element:
    safe_parser = XMLParser(resolve_entities=False)
    return fromstring(xml_bytes, parser=safe_parser)


def download_repodata_section(section, request, destination_dir) -> str:
    local_path = join(destination_dir, os.path.basename(section.location))
    with open(local_path, "wb") as out:
        shutil.copyfileobj(request.raw, out)
    validate_checksum(path=local_path, checksum_type=section.checksum_type, checksum=section.checksum)
    return local_path


class Package:
    __slots__ = [
        "base_url",
        "name",
        "checksum",
        "checksum_type",
        "location",
        "destination",
        "version",
        "epoch",
        "package_size",
        "release",
        "url",
    ]

    def __init__(self, base_url: str, destination_path: str, package_element: Element):
        if not destination_path.endswith("/"):
            destination_path += "/"
        self.base_url = base_url
        self.name = package_element.findtext("common:name", namespaces=namespaces)
        checksum_data = package_element.find("common:checksum", namespaces=namespaces)
        self.checksum = checksum_data.text
        self.checksum_type = checksum_data.get("type")
        self.location = package_element.find("common:location", namespaces=namespaces).get("href")
        self.destination = f"{destination_path}{self.location}"
        version_data = package_element.find("common:version", namespaces=namespaces)
        self.version = version_data.get("ver")
        self.epoch = version_data.get("epoch")
        self.release = version_data.get("rel")
        self.package_size = int(package_element.find("common:size", namespaces=namespaces).get("package"))
        self.release = version_data.get("rel")
        self.url = f"{self.base_url}{self.location}"

    def to_dict(self):
        return {key: getattr(self, key) for key in self.__slots__}

    def __eq__(self, other) -> bool:
        if not isinstance(other, Package):
            return False
        return self._key() == other._key()

    def _key(self):
        return self.name, self.version, self.epoch, self.release, self.checksum

    def __repr__(self) -> str:
        return f"Package(name='{self.name}', \
            version='{self.version}',\
            epoch='{self.epoch}',\
            release='{self.release}', \
            checksum='{self.checksum}')"

    def __hash__(self) -> int:
        return hash(self._key())


class PackageList:
    def __init__(self, base_url: str, packages_xml: bytes):
        self.base_url = base_url
        self.path = urlparse(base_url).path
        self.root = safe_parse_xml(packages_xml)

    def __len__(self) -> int:
        return int(self.root.get("packages"))

    def __iter__(self) -> Iterator[Package]:
        for package_element in self.root:
            yield Package(base_url=self.base_url, destination_path=self.path, package_element=package_element)


Metadata = namedtuple(
    "Metadata",
    [
        "package_list",
        "repodata",
        "base_url",
    ],
)
RepodataSection = namedtuple(
    "RepodataSection",
    [
        "url",
        "location",
        "destination",
        "checksum_type",
        "checksum",
    ],
)

RepoDataFiles = namedtuple("RepoDataFiles", ["sync_files", "upload_files"])


@dataclasses.dataclass
class SectionMetadata:
    size: int
    open_size: int
    open_checksum: str
    checksum: str
    local_path: str
    location: str
    checksum_type: str = "sha256"
    header_checksum: Optional[str] = None
    header_size: Optional[int] = None


class UpdateInfoSection:
    def __init__(self, path: str, scratch_dir):
        self.path = path
        self.scratch_dir = scratch_dir

    @classmethod
    def from_path(cls, path: str, scratch_dir):
        if path.endswith(".zck"):
            return ZCKUpdateInfoSection(path, scratch_dir)
        elif path.endswith(".xz"):
            return XZUpdateInfoSection(path, scratch_dir)
        else:
            raise ValueError("Only xz and zck files supported")

    @abstractmethod
    def _read(self) -> bytes:
        pass

    @abstractmethod
    def _compress(self, root, open_size, open_checksum):
        pass

    def strip_to_arches(self, arches):
        xml_bytes = self._read()
        root = safe_parse_xml(xml_bytes)
        self._strip(root, arches)
        open_size = len(xml_bytes)
        open_checksum = sha256(xml_bytes)
        return self._compress(root, open_size, open_checksum)

    def _strip(self, root, arches):
        for update_element in root:
            for collection in update_element.find("pkglist"):
                for package in collection.getchildren():
                    arch = package.get("arch")
                    if arch is not None and arch not in arches:
                        collection.remove(package)


class XZUpdateInfoSection(UpdateInfoSection):
    def _read(self) -> bytes:
        with lzma.open(self.path, mode="rb") as f:
            return f.read()

    def _compress(self, root, open_size, open_checksum):
        compressed_xml = lzma.compress(tostring(root, encoding="utf-8"))
        compressed_sha256 = sha256(compressed_xml)
        compressed_size = len(compressed_xml)

        local_path = os.path.join(self.scratch_dir, f"{compressed_sha256}-updateinfo.xml.xz")
        with open(local_path, "wb+") as out:
            out.write(compressed_xml)
        return SectionMetadata(
            open_checksum=open_checksum,
            checksum=compressed_sha256,
            checksum_type="sha256",
            size=compressed_size,
            open_size=open_size,
            local_path=local_path,
            location=f"repodata/{basename(local_path)}",
        )


def decompress(filename: Path | str) -> bytes:
    try:
        with zstandard.open(filename) as f:
            return f.read()
    except zstandard.ZstdError:
        with gzip.open(filename) as f:
            return f.read()


class ZCKUpdateInfoSection(UpdateInfoSection):
    def _read(self):
        return subprocess.check_output(["unzck", self.path, "--stdout"])

    def _compress(self, root, open_size, open_checksum):
        stripped_path = os.path.join(self.scratch_dir, "stripped.xml")
        ElementTree(root).write(stripped_path)

        # Now compress and take compressed checksum,size.
        compressed_stripped_path = os.path.join(self.scratch_dir, "stripped.xml.zck")
        subprocess.check_call(["zck", stripped_path, "-o", compressed_stripped_path])
        sha256_compressed_out = subprocess.check_output(["sha256sum", compressed_stripped_path], text=True)
        checksum = sha256_compressed_out.split()[0]
        size = os.path.getsize(compressed_stripped_path)

        # We also need some ZChunk specific metadata.
        header_out = subprocess.check_output(["zck_read_header", compressed_stripped_path], text=True)
        header_checksum, header_size = self._parse_zck_read_header(output=header_out)
        final_path = os.path.join(self.scratch_dir, f"{checksum}-updateinfo.xml.zck")

        # Rename it in the same format as the other sections.
        os.rename(compressed_stripped_path, final_path)

        return SectionMetadata(
            size=size,
            open_size=open_size,
            header_size=header_size,
            header_checksum=header_checksum,
            open_checksum=open_checksum,
            checksum=checksum,
            local_path=final_path,
            location=f"repodata/{os.path.basename(final_path)}",
        )

    def _parse_zck_read_header(self, output):
        checksum_match = re.search("Header checksum: (?P<checksum>.*$)", output, flags=re.MULTILINE)
        if not checksum_match:
            raise ValueError(f"Failed to locate checksum in output: {output}")
        size_match = re.search("Header size:(?P<size>.*$)", output, flags=re.MULTILINE)
        if not size_match:
            raise ValueError(f"Failed to locate size in output: {output}")
        return checksum_match.groupdict()["checksum"], int(size_match.groupdict()["size"])


class RPMRepository:
    """Upstream repository. This MAY NOT be a S3 bucket."""

    def __init__(self, base_url: str):
        if not base_url.startswith("https://"):
            raise ValueError("Only https upstream repositories can be synced from")
        if not base_url.endswith("/"):
            base_url += "/"
        self.base_url = base_url
        self.path = urlparse(base_url).path
        if self.path.find("//") != -1:
            raise ValueError("Consecutive slashes detected in URL path")
        self.session = get_requests_session()

    def has_updates(self, since: datetime) -> bool:
        response = self._req(self.session.head, "repodata/repomd.xml")
        last_modified_header = response.headers.get("Last-Modified")
        if not last_modified_header:
            return True
        return parse(last_modified_header) > since

    def parse_metadata(self) -> Metadata:
        repodata = self.get_repodata()
        package_list = self._extract_package_list(primary=repodata["primary"])
        return Metadata(package_list=package_list, repodata=repodata, base_url=self.base_url)

    def exists(self):
        # S3 will respond with HTTP 403 (Access Denied) if the object does not exist when we tried to retreive it using
        # HTTP GET (public access). Also handle 404 for "normal" web server behaviour.
        response = self._req(self.session.get, "repodata/repomd.xml", acceptible_status_code={200, 403, 404})
        if response.status_code == 200:
            return True
        return False

    def get_repodata(self, xml_bytes=None):
        if xml_bytes is None:
            xml_bytes = self._req(self.session.get, "repodata/repomd.xml").content
        repodata = self.parse_repomd(xml=safe_parse_xml(xml_bytes))
        return repodata

    def strip_metadata(
        self,
        xml_bytes: bytes,
        target_arches: Tuple[str],
        scratch_dir: str,
    ):
        sync_files, upload_files = [], []
        repomd_xml = safe_parse_xml(xml_bytes)
        repodata = self.parse_repomd(xml=repomd_xml)
        for key, section in repodata.items():
            if key.startswith("updateinfo"):
                with self._req(self.session.get, path=section.location, stream=True) as request:
                    local_path = download_repodata_section(section, request, destination_dir=scratch_dir)
                    update_section = UpdateInfoSection.from_path(path=local_path, scratch_dir=scratch_dir)
                    rewritten_section = update_section.strip_to_arches(arches=target_arches)
                    self._rewrite_repomd(repomd_xml=repomd_xml, snapshot=rewritten_section, section_name=key)
                    upload_files.append(rewritten_section.local_path)
        repomd_xml_path = join(scratch_dir, "repomd.xml")
        with open(repomd_xml_path, "wb+") as out:
            out.write(tostring(repomd_xml, encoding="utf-8"))
        upload_files.append(repomd_xml_path)

        return RepoDataFiles(
            sync_files=sync_files,
            upload_files=upload_files,
        )

    def create_snapshot(self, scratch_dir):
        response = self._req(self.session.get, "repodata/repomd.xml")
        repomd_xml = safe_parse_xml(response.content)
        repodata = self.parse_repomd(xml=repomd_xml)
        snapshot_primary = self._rewrite_primary(temp_dir=scratch_dir, primary=repodata["primary"])
        self._rewrite_repomd(repomd_xml=repomd_xml, snapshot=snapshot_primary, section_name="primary")
        repomd_xml_path = join(scratch_dir, "repomd.xml")
        with open(repomd_xml_path, "wb+") as out:
            out.write(tostring(repomd_xml, encoding="utf-8"))

        sync_files = []
        for section in repodata.values():
            if (
                section.location.endswith(".xml.gz")
                or section.location.endswith("updateinfo.xml.xz")
                or section.location.endswith("modules.yaml.gz")
            ):
                sync_files.append(urlparse(join(self.base_url, section.location)).path)
        return RepoDataFiles(
            sync_files=sync_files,
            upload_files=[repomd_xml_path, snapshot_primary.local_path],
        )

    def _rewrite_primary(self, temp_dir, primary: RepodataSection):
        with self._req(self.session.get, path=primary.location, stream=True) as request:
            local_path = download_repodata_section(primary, request, temp_dir)
            file_bytes = decompress(local_path)
            primary_xml = safe_parse_xml(xml_bytes=file_bytes)
            open_checksum = sha256(content=file_bytes)
            open_size = len(file_bytes)
            for package_element in primary_xml:
                location = package_element.find("common:location", namespaces=namespaces)
                # As our S3 structure is https://<base-repo>/snapshots/<snapshot-uuid>/, and the "location"
                # attribute of the packages in primary.xml references a path relative to the root like:
                # "Packages/v/vim.rmp", we need to rewrite this location to point to back a few directories
                # from our snapshot root.
                relative_location = f"../../{location.get('href')}"
                location.set("href", relative_location)

            # Now we have rewritten our XML file the checksums no longer match, so calculate some new ones (along with
            # size etc from above).
            compressed_xml = gzip.compress(tostring(primary_xml, encoding="utf-8"))
            compressed_sha256 = sha256(compressed_xml)
            compressed_size = len(compressed_xml)
            local_path = f"{temp_dir}/{compressed_sha256}-primary.xml.gz"
            with open(local_path, "wb+") as out:
                out.write(compressed_xml)

            return SectionMetadata(
                open_checksum=open_checksum,
                checksum=compressed_sha256,
                checksum_type="sha256",
                size=compressed_size,
                open_size=open_size,
                local_path=local_path,
                location=f"repodata/{basename(local_path)}",
            )

    def _rewrite_repomd(self, repomd_xml: Element, snapshot: SectionMetadata, section_name: str):
        # Rewrite the XML with correct metadata for our changed primary.xml
        for element in repomd_xml.find(f"repo:data[@type='{section_name}']", namespaces=namespaces):
            _, _, key = element.tag.partition("}")
            if key == "checksum":
                element.text = snapshot.checksum
            elif key == "open-checksum":
                element.text = snapshot.open_checksum
            elif key == "location":
                element.set("href", snapshot.location)
            elif key == "size":
                element.text = str(snapshot.size)
            elif key == "open-size":
                element.text = str(snapshot.open_size)
            elif key == "header-size" and snapshot.header_size is not None:
                element.text = str(snapshot.header_size)
            elif key == "header-checksum" and snapshot.header_checksum is not None:
                element.text = snapshot.header_checksum

    def _extract_package_list(self, primary: RepodataSection) -> PackageList:
        with self._req(self.session.get, path=primary.location, stream=True) as request:
            with TemporaryDirectory(prefix="/var/tmp/") as temp_dir:
                local_path = download_repodata_section(primary, request, temp_dir)
                return PackageList(base_url=self.base_url, packages_xml=decompress(local_path))

    def parse_repomd(self, xml: Element) -> Dict[str, RepodataSection]:
        sections = {}
        for data_element in xml.findall("repo:data", namespaces=namespaces):
            section_type = data_element.attrib["type"]
            section = {}
            for element in xml.findall(f"repo:data[@type='{section_type}']/repo:*", namespaces=namespaces):
                # Strip the namespace from the tag as it is annoying
                _, _, key = element.tag.partition("}")
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

    def _req(self, method, path, *, json=None, params=None, acceptible_status_code=None, **kwargs) -> Response:
        acceptible_status_code = acceptible_status_code or {200}
        url = f"{self.base_url}{path}"
        response = method(url, json=json, params=params, **kwargs)
        if response.status_code not in acceptible_status_code:
            response.raise_for_status()
        return response
