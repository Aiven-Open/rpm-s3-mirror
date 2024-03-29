# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

import base64
import functools
import hashlib
import json
import logging
import os
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from tempfile import TemporaryDirectory, NamedTemporaryFile
from typing import Collection, Union, BinaryIO, Dict, Iterable
from urllib.parse import urlparse

import botocore.session
import botocore.exceptions
import time

from rpm_s3_mirror.repository import RepodataSection, Package
from rpm_s3_mirror.statsd import StatsClient
from rpm_s3_mirror.util import get_requests_session, validate_checksum, download_file

lock = threading.RLock()


def md5_string(string):
    return hashlib.md5(string.encode("utf-8")).hexdigest()


class S3DirectoryNotFound(Exception):
    def __init__(self, response):
        super().__init__()
        self.response = response


class S3:
    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        bucket_name: str,
        bucket_region: str,
        stats: StatsClient,
        max_workers: int = 8,
        scratch_dir: str = "/var/tmp/",
    ):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket_name = bucket_name
        self.bucket_region = bucket_region
        self.stats = stats
        self.max_workers = max_workers
        self.scratch_dir = scratch_dir
        self._s3 = None
        self.session = get_requests_session()
        self.log = logging.getLogger(type(self).__name__)

    def sync_packages(
        self,
        base_url: str,
        upstream_repodata: Dict[str, RepodataSection],
        upstream_packages: Collection[Package],
        skip_existing: bool = False,
    ):
        with TemporaryDirectory(prefix=self.scratch_dir) as temp_dir:
            self._sync_objects(temp_dir, upstream_packages, skip_existing=skip_existing)
            synced_bytes = sum((package.package_size for package in upstream_packages))
            self.stats.gauge(
                metric="s3_mirror_sync_bytes",
                value=synced_bytes,
                tags={"repo": urlparse(base_url).path},
            )
            self.stats.gauge(
                metric="s3_mirror_sync_packages",
                value=len(upstream_packages),
                tags={"repo": urlparse(base_url).path},
            )
            self._sync_objects(temp_dir=temp_dir, repo_objects=upstream_repodata.values(), skip_existing=skip_existing)

    def overwrite_repomd(self, repomd_xml_local_path, base_url):
        url = f"{base_url}repodata/repomd.xml"
        remote_path = urlparse(url).path
        self.put_object(repomd_xml_local_path, remote_path, cache_age=0)

    def archive_repomd(self, base_url, location):
        self.log.debug("Archiving repomd.xml to %s", location)
        url = f"{base_url}repodata/repomd.xml"
        self.copy_object(source=urlparse(url).path, destination=location)

    def put_manifest(self, location, manifest):
        self.log.info("Writing manifest to: %s", location)
        manifest_json = json.dumps(manifest._asdict(), default=lambda x: x.isoformat(), indent=2)
        with NamedTemporaryFile(prefix=self.scratch_dir) as f:
            f.write(manifest_json.encode("utf-8"))
            f.flush()
            self.put_object(local_path=f.name, key=location)

    def repomd_update_time(self, base_url: str) -> datetime:
        url = f"{base_url}repodata/repomd.xml"
        response = self._head_object(key=self._trim_key(remote_path=urlparse(url).path))
        return response["LastModified"]

    def _sync_objects(self, temp_dir: str, repo_objects: Iterable[Package], skip_existing: bool):
        sync = functools.partial(self._sync_object, temp_dir, skip_existing)
        start = time.time()
        self.log.info("Beginning sync of %s objects.", len(repo_objects))
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # We iterate through the generator to pick up and propagate any Exceptions
            for _ in executor.map(sync, repo_objects):
                pass
        elapsed = int(time.time() - start)
        self.log.info("Completed syncing %s objects in %s seconds", len(repo_objects), elapsed)

    # pylint: disable=unsubscriptable-object
    def _sync_object(self, temp_dir: str, skip_existing: bool, repo_object: Union[Package, RepodataSection]):
        # When bootstrapping, support backfilling two versions of problematic packages (see below)
        workaround_destination = repo_object.destination.replace("+", " ")
        if skip_existing:
            if ("+" in repo_object.destination and self._object_exists(workaround_destination)) and self._object_exists(
                repo_object.destination
            ):
                self.log.debug("SKIP: %s", repo_object.destination)
                return

        package_path = download_file(temp_dir=temp_dir, url=repo_object.url, session=self.session)
        validate_checksum(package_path, checksum_type=repo_object.checksum_type, checksum=repo_object.checksum)
        self.put_object(package_path, repo_object.destination)
        if "+" in repo_object.destination:
            # Old versions of DNF did not urlencode plus signs in urls, and s3 always does
            # so we need to upload two versions of these packages, one with the + sign unmodified
            # for newer versions of DNF, and one with the + sign replaced with a space for older
            # versions as s3 interprets a space as a + sign.
            # https://bugzilla.redhat.com/show_bug.cgi?id=1817130
            # https://forums.aws.amazon.com/thread.jspa?threadID=55746
            self.log.debug(
                "Uploading workaround version of package: %s -> %s", repo_object.destination, workaround_destination
            )
            self.put_object(package_path, key=workaround_destination)
        try:
            os.unlink(package_path)
        except Exception as e:  # pylint: disable=broad-except
            self.log.debug("Failed to unlink %s: %s", package_path, e)

    def put_object(self, local_path: str, key: str, cache_age=31536000):
        with open(local_path, "rb") as package_fp:
            # We need to seek after this call so boto gets the file pointer at the beginning
            md5_header = self._build_md5_header(fp=package_fp)
            package_fp.seek(0)

            key = self._trim_key(key)
            self.log.debug("PUT: %s", key)
            self._client.put_object(
                ACL="public-read",
                Bucket=self.bucket_name,
                CacheControl=f"max-age={cache_age}",
                Key=key,
                Body=package_fp,
                ContentMD5=md5_header,
            )

    def delete_subdirectory(self, subdir):
        objects = []
        for s3_object in self.list(subdir):
            objects.append({"Key": s3_object["Key"]})
        self._client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": objects, "Quiet": True})

    def exists(self, prefix):
        try:
            self.list(prefix)
        except S3DirectoryNotFound:
            return False
        return True

    def list(self, prefix):
        response = self._client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        if response.get("KeyCount", 0) == 0:
            raise S3DirectoryNotFound(response=response)
        return response["Contents"]

    def copy_object(self, source, destination):
        source, destination = self._trim_key(source), self._trim_key(destination)
        self.log.debug("COPY: %s -> %s", source, destination)
        self._client.copy_object(
            Bucket=self.bucket_name,
            CopySource={
                "Bucket": self.bucket_name,
                "Key": source,
            },
            ACL="public-read",
            Key=destination,
            CacheControl="max-age=0",
        )

    def _object_exists(self, key: str) -> bool:
        try:
            self._head_object(key=self._trim_key(key))
            return True
        except botocore.exceptions.ClientError as e:
            if int(e.response["Error"]["Code"]) != 404:
                raise
        return False

    def _head_object(self, key: str):
        self.log.debug("HEAD: %s", key)
        return self._client.head_object(
            Bucket=self.bucket_name,
            Key=key,
        )

    def _trim_key(self, remote_path: str) -> str:
        # Strip the leading / if present otherwise we end up
        # with an extra root directory in s3 which we don't want.
        if remote_path.startswith("/"):
            remote_path = remote_path[1:]
        return remote_path

    @property
    def _client(self):
        if self._s3 is None:
            # The boto3 client call is not threadsafe, so only allow calling it from a singe thread at a time
            with lock:
                botocore_session = botocore.session.get_session()
                self._s3 = botocore_session.create_client(
                    "s3",
                    region_name=self.bucket_region,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                )
        return self._s3

    def _build_md5_header(self, fp: BinaryIO) -> str:
        """
        ContentMD5 (string) -- The base64-encoded 128-bit MD5 digest of the message (without the headers)
        according to RFC 1864. This header can be used as a message integrity check to verify that the data is the same
        data that was originally sent
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
        """
        h = hashlib.md5()
        data = fp.read(1000000)
        while data:
            h.update(data)
            data = fp.read(1000000)
        return base64.b64encode(h.digest()).decode("utf-8")
