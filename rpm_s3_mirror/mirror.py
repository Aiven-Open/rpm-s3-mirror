# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

import logging
import re
from contextlib import suppress
from os.path import join
from tempfile import TemporaryDirectory

import time
from collections import namedtuple, defaultdict
from urllib.parse import urlparse

from rpm_s3_mirror.repository import RPMRepository, safe_parse_xml
from rpm_s3_mirror.s3 import S3, S3DirectoryNotFound
from rpm_s3_mirror.statsd import StatsClient
from rpm_s3_mirror.util import get_requests_session, now, get_snapshot_directory, get_snapshot_path, download_file, \
    validate_checksum, primary_xml_checksums_equal

Manifest = namedtuple("Manifest", ["update_time", "upstream_repository", "previous_repomd", "synced_packages"])
MANIFEST_DIRECTORY = "manifests"

VALID_SNAPSHOT_REGEX = r"^[A-Za-z0-9_-]+$"
SNAPSHOT_REGEX = re.compile(r"/snapshots/(?P<snapshot_id>[A-Za-z0-9_-]+)/repodata")


class InvalidSnapshotID(ValueError):
    pass


class Mirror:
    def __init__(self, config):
        self.config = config
        self.session = get_requests_session()
        self.log = logging.getLogger(type(self).__name__)
        self.stats = StatsClient()
        self.s3 = S3(
            aws_secret_access_key=self.config.aws_secret_access_key,
            aws_access_key_id=self.config.aws_access_key_id,
            bucket_name=self.config.bucket_name,
            bucket_region=self.config.bucket_region,
            stats=self.stats,
            max_workers=self.config.max_workers,
            scratch_dir=self.config.scratch_dir,
        )
        self.repositories = [RPMRepository(base_url=url) for url in config.upstream_repositories]

    def sync(self, bootstrap=False):
        """ Sync upstream repositories to our s3 mirror """
        start = time.monotonic()
        sync_success = True
        for upstream_repository in self.repositories:
            try:
                self._sync_repository(bootstrap, upstream_repository)
                self.stats.gauge(metric="s3_mirror_failed", value=0, tags={"repo": upstream_repository.path})
            except Exception as e:  # pylint: disable=broad-except
                self.log.exception("Failed to sync: %s", upstream_repository.base_url, exc_info=e)
                self.stats.gauge(metric="s3_mirror_failed", value=1, tags={"repo": upstream_repository.path})
                sync_success = False
                continue

        self.log.info("Synced %s repos in %s seconds", len(self.repositories), time.monotonic() - start)
        self.stats.gauge(metric="s3_mirror_sync_seconds_total", value=time.monotonic() - start)
        return sync_success

    def _sync_repository(self, bootstrap, upstream_repository):
        mirror_start = time.monotonic()
        update_time = now()
        upstream_metadata = upstream_repository.parse_metadata()
        if bootstrap:
            self.log.info("Bootstrapping repository: %s", upstream_repository.base_url)
            new_packages = upstream_metadata.package_list
        else:
            self.log.info("Syncing repository: %s", upstream_repository.base_url)
            # If the upstream repomd.xml file was updated after the last time we updated our
            # mirror repomd.xml file then there is probably some work to do.
            mirror_repository = RPMRepository(base_url=self._build_s3_url(upstream_repository.base_url))
            last_check_time = self.s3.repomd_update_time(base_url=mirror_repository.base_url)
            if not upstream_repository.has_updates(since=last_check_time):
                self.log.info("Skipping repository with no updates since: %s", last_check_time)
                self.stats.gauge(
                    metric="s3_mirror_sync_seconds",
                    value=time.monotonic() - mirror_start,
                    tags={"repo": upstream_repository.path},
                )
                return

            # Extract our metadata and detect any new/updated packages.
            mirror_metadata = mirror_repository.parse_metadata()
            new_packages = set(upstream_metadata.package_list).difference(set(mirror_metadata.package_list))

        # Sync our mirror with upstream.
        self.s3.sync_packages(
            base_url=upstream_metadata.base_url,
            upstream_repodata=upstream_metadata.repodata,
            upstream_packages=new_packages,
            # If we are bootstrapping the s3 repo, it is worth checking if the package already exists as if the
            # process is interrupted halfway through we would have to do a lot of potentially useless work. Once
            # we have completed bootstrapping and are just running a sync we don't benefit from checking as it
            # slows things down for no good reason (we expect the packages to be there already and if not
            # it is a bug of some kind).
            skip_existing=bootstrap
        )

        # If we are not bootstrapping, store a manifest that describes the changes synced in this run
        if not bootstrap:
            manifest_location = self._build_manifest_location(base_url=upstream_repository.base_url)
            repomd_path = join(manifest_location, "repomd.xml")
            self.s3.archive_repomd(base_url=upstream_repository.base_url, location=repomd_path)
            manifest = Manifest(
                update_time=update_time,
                upstream_repository=upstream_repository.base_url,
                previous_repomd=repomd_path,
                synced_packages=[package.to_dict() for package in new_packages],
            )
            manifest_path = join(manifest_location, "manifest.json")
            self.s3.put_manifest(location=manifest_path, manifest=manifest)

        # Finally, overwrite the repomd.xml file to make our changes live
        self.s3.overwrite_repomd(base_url=upstream_repository.base_url)
        self.log.info("Updated mirror with %s packages", len(new_packages))
        self.stats.gauge(
            metric="s3_mirror_sync_seconds",
            value=time.monotonic() - mirror_start,
            tags={"repo": upstream_repository.path},
        )

    def _build_manifest_location(self, base_url):
        sync_directory = now(microsecond=True).replace(tzinfo=None).isoformat()
        manifest_location = join(urlparse(base_url).path, MANIFEST_DIRECTORY, sync_directory)
        return manifest_location

    def snapshot(self, snapshot_id):
        """ Create a named snapshot of upstream repositories at a point in time """
        self.log.info("Creating snapshot: %s", snapshot_id)
        self._validate_snapshot_id(snapshot_id)
        with TemporaryDirectory(prefix=self.config.scratch_dir) as temp_dir:
            for upstream_repository in self.repositories:
                try:
                    self._snapshot_repository(
                        snapshot_id=snapshot_id,
                        temp_dir=temp_dir,
                        upstream_repository=upstream_repository,
                    )
                except Exception as e:
                    self._try_remove_snapshots(snapshot_id=snapshot_id)
                    raise Exception("Failed to snapshot repositories") from e

    def list_snapshots(self):
        snapshots = defaultdict(dict)
        for repository in self.repositories:
            repo_path = urlparse(repository.base_url).path
            prefix = join(repo_path, "snapshots")
            results = self.s3.list(prefix=prefix[1:])
            for result in results:
                key, last_modified = result["Key"], result["LastModified"]
                # We write the repomd.xml file last so this time
                # is when the snapshot is considered complete.
                if key.endswith("repomd.xml"):
                    match = SNAPSHOT_REGEX.search(key)
                    if match:
                        group = match.groupdict()
                        snapshot_id = group["snapshot_id"]
                        snapshots[repo_path][snapshot_id] = last_modified
        return snapshots

    def sync_snapshot(self, snapshot_id):
        """ Sync a snapshot from one s3 mirror to another """
        self.log.info("Syncing snapshot: %s", snapshot_id)
        self._validate_snapshot_id(snapshot_id)
        for upstream_repo in self.config.upstream_repositories:
            snapshot_root = join(upstream_repo, "snapshots", snapshot_id)
            metadata_url = join(snapshot_root, "repodata", "repomd.xml")
            with TemporaryDirectory(prefix=self.config.scratch_dir) as temp_dir:
                snapshot_path = urlparse(snapshot_root).path
                repomd_path = download_file(temp_dir=temp_dir, url=metadata_url, session=self.session)
                with open(repomd_path, "rb") as f:
                    repomd_xml = safe_parse_xml(xml_bytes=f.read())

                repository = RPMRepository(base_url=snapshot_root)
                repodata = repository.parse_repomd(xml=repomd_xml)
                for repo_object in repodata.values():
                    local_path = download_file(temp_dir=temp_dir, url=repo_object.url, session=self.session)
                    validate_checksum(local_path, checksum_type=repo_object.checksum_type, checksum=repo_object.checksum)
                    destination = join(snapshot_path, repo_object.location)
                    self.s3.put_object(local_path=local_path, key=destination)

                repomd_destination = join(snapshot_path, "repodata", "repomd.xml")
                self.s3.put_object(local_path=local_path, key=repomd_destination)

    def diff_snapshots(self, old_snapshot, new_snapshot):
        def create_mirror_repo(repo, snapshot):
            snapshot_root = join(repo, "snapshots", snapshot)
            mirror_url = self._build_s3_url(snapshot_root)
            return RPMRepository(base_url=mirror_url)

        diff = defaultdict(dict)
        for upstream_repo in self.config.upstream_repositories:
            repo1 = create_mirror_repo(upstream_repo, new_snapshot)
            repo2 = create_mirror_repo(upstream_repo, old_snapshot)

            # If the checksums of the primary.xml.gz files match then there are no
            # changes. This saves quite a bit of time as it is expensive downloading
            # and parsing these large XML blobs.
            if primary_xml_checksums_equal(repo1=repo1, repo2=repo2):
                self.log.debug("Skipping %s as primary xml is identical", upstream_repo)
                continue

            repo1_metadata = repo1.parse_metadata()
            repo2_metadata = repo2.parse_metadata()

            repo1_snapshot_list = repo1_metadata.package_list
            repo2_snapshot_list = repo2_metadata.package_list
            changed_packages = set(repo2_snapshot_list).difference(set(repo1_snapshot_list))

            new_packages = {package.name: package for package in repo1_snapshot_list}
            repo_path = urlparse(upstream_repo).path
            diff[repo_path]["updated"] = {}
            for package in sorted(changed_packages, key=lambda x: x.name):
                orig_package = new_packages.get(package.name)
                if orig_package:
                    diff[repo_path]["updated"][package.name] = [
                        [package.version, package.release],
                        [orig_package.version, orig_package.release],
                    ]
        return diff

    def _validate_snapshot_id(self, snapshot_id):
        if not re.match(VALID_SNAPSHOT_REGEX, snapshot_id):
            raise InvalidSnapshotID(f"Snapshot id must match regex: {VALID_SNAPSHOT_REGEX}")
        elif "\n" in snapshot_id:
            raise InvalidSnapshotID("Snapshot id cannot contain newlines")
        for repository in self.repositories:
            base_path = repository.path[1:]
            snapshot_dir = get_snapshot_directory(base_path=base_path, snapshot_id=snapshot_id)
            if self.s3.exists(prefix=snapshot_dir):
                raise InvalidSnapshotID(f"Cannot overwrite existing snapshot: {snapshot_dir}")

    def _snapshot_repository(self, snapshot_id, temp_dir, upstream_repository):
        self.log.debug("Snapshotting repository: %s", upstream_repository.base_url)
        repository = RPMRepository(base_url=self._build_s3_url(upstream_repository.base_url))
        snapshot = repository.create_snapshot(scratch_dir=temp_dir)
        base_path = urlparse(repository.base_url).path[1:]  # need to strip the leading slash
        for file_path in snapshot.sync_files:
            self.s3.copy_object(
                source=file_path,
                destination=get_snapshot_path(base_path, snapshot_id, file_path),
            )
        for file_path in snapshot.upload_files:
            self.s3.put_object(
                local_path=file_path,
                key=get_snapshot_path(base_path, snapshot_id, file_path),
            )

    def _try_remove_snapshots(self, snapshot_id):
        for repository in self.repositories:
            snapshot_dir = get_snapshot_directory(base_path=repository.path, snapshot_id=snapshot_id)
            try:
                with suppress(S3DirectoryNotFound):
                    self.s3.delete_subdirectory(subdir=snapshot_dir)
                    self.log.info("Deleted: %s", snapshot_dir)
            except Exception as e:  # pylint: disable=broad-except
                self.log.warning("Failed to remove snapshot: %s - %s", snapshot_dir, e)

    def _build_s3_url(self, base_url) -> str:
        dest_path = urlparse(base_url).path
        # For some reason, s3 buckets in us-east-1 have a different URL structure to all the rest...
        if self.config.bucket_region == "us-east-1":
            s3_mirror_url = f"https://{self.config.bucket_name}.s3.amazonaws.com{dest_path}"
        else:
            s3_mirror_url = f"https://{self.config.bucket_name}.s3-{self.config.bucket_region}.amazonaws.com{dest_path}"
        return s3_mirror_url
