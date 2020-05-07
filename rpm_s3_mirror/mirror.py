# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

import logging
import uuid
from os.path import basename, join
from tempfile import TemporaryDirectory

import time
from collections import namedtuple
from urllib.parse import urlparse

from rpm_s3_mirror.repository import RPMRepository
from rpm_s3_mirror.s3 import S3
from rpm_s3_mirror.statsd import StatsClient
from rpm_s3_mirror.util import get_requests_session, now

Manifest = namedtuple("Manifest", ["update_time", "upstream_repository", "previous_repomd", "synced_packages"])
MANIFEST_LOCATION = "manifests"


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
        start = time.monotonic()
        for upstream_repository in self.repositories:
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
                mirror_repository = RPMRepository(base_url=self._build_s3_url(upstream_repository))
                last_check_time = self.s3.repomd_update_time(base_url=mirror_repository.base_url)
                if not upstream_repository.has_updates(since=last_check_time):
                    self.log.info("Skipping repository with no updates since: %s", last_check_time)
                    continue

                # Extract our metadata and detect any new/updated packages.
                mirror_metadata = mirror_repository.parse_metadata()
                new_packages = set(upstream_metadata.package_list).difference(set(mirror_metadata.package_list))

            # Sync our mirror with upstream.
            if new_packages:
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
                    archive_location = self.s3.archive_repomd(
                        update_time=update_time,
                        base_url=upstream_repository.base_url,
                        manifest_location=MANIFEST_LOCATION,
                    )
                    manifest = Manifest(
                        update_time=update_time,
                        upstream_repository=upstream_repository.base_url,
                        previous_repomd=archive_location,
                        synced_packages=[package.to_dict() for package in new_packages],
                    )
                    self.s3.put_manifest(manifest_location=MANIFEST_LOCATION, manifest=manifest)

                # Finally, overwrite the repomd.xml file to make our changes live
                self.s3.overwrite_repomd(base_url=upstream_repository.base_url)

            self.log.info("Updated mirror with %s packages", len(new_packages))
            self.stats.gauge(
                metric="s3_mirror_sync_seconds",
                value=time.monotonic() - mirror_start,
                tags={"repo": upstream_metadata.base_url},
            )

        self.log.info("Synced %s repos in %s seconds", len(self.repositories), time.monotonic() - start)
        self.stats.gauge(metric="s3_mirror_sync_seconds_total", value=time.monotonic() - start)

    def snapshot(self):
        snapshot_id = uuid.uuid4()
        self.log.debug("Creating snapshot: %s", snapshot_id)
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
        return snapshot_id

    def _snapshot_repository(self, snapshot_id, temp_dir, upstream_repository):
        self.log.debug("Snapshotting repository: %s", upstream_repository.base_url)
        repository = RPMRepository(base_url=self._build_s3_url(upstream_repository))
        snapshot = repository.create_snapshot(scratch_dir=temp_dir)
        base_path = urlparse(repository.base_url).path[1:]  # need to strip the leading slash
        for file_path in snapshot.sync_files:
            self.s3.copy_object(
                source=file_path,
                destination=self._snapshot_path(base_path, snapshot_id, file_path),
            )
        for file_path in snapshot.upload_files:
            self.s3.put_object(
                local_path=file_path,
                key=self._snapshot_path(base_path, snapshot_id, file_path),
            )

    def _try_remove_snapshots(self, snapshot_id):
        for repository in self.repositories:
            snapshot_dir = self._snapshot_directory(base_path=repository.base_url, snapshot_id=snapshot_id)
            try:
                self.s3.delete_subdirectory(subdir=snapshot_dir)
                self.log.debug("Deleted: %s", snapshot_dir)
            except:  # pylint: disable=bare-except
                self.log.warning("Failed to remove snapshot: %s", snapshot_dir)

    def _snapshot_path(self, base_path, snapshot_id, file_path):
        return join(self._snapshot_directory(base_path, snapshot_id), "repodata", basename(file_path))

    def _snapshot_directory(self, base_path, snapshot_id):
        return join(base_path, "snapshots", str(snapshot_id))

    def _build_s3_url(self, upstream_repository) -> str:
        dest_path = urlparse(upstream_repository.base_url).path
        # For some reason, s3 buckets in us-east-1 have a different URL structure to all the rest...
        if self.config.bucket_region == "us-east-1":
            s3_mirror_url = f"https://{self.config.bucket_name}.s3.amazonaws.com{dest_path}"
        else:
            s3_mirror_url = f"https://{self.config.bucket_name}.s3-{self.config.bucket_region}.amazonaws.com{dest_path}"
        return s3_mirror_url
