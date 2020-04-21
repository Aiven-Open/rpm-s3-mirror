import json
import logging
import time
from collections import namedtuple
from urllib.parse import urlparse

from fedora_s3_mirror.repository import YUMRepository
from fedora_s3_mirror.s3 import S3
from fedora_s3_mirror.statsd import StatsClient
from fedora_s3_mirror.util import get_requests_session, now

Manifest = namedtuple("Manifest", ["update_time", "upstream_repository", "previous_repomd", "synced_packages"])
MANIFEST_LOCATION = "manifests"


class YUMMirror:
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
        self.repositories = [YUMRepository(base_url=url) for url in config.upstream_repositories]

    def sync(self):
        start = time.monotonic()
        for upstream_repository in self.repositories:
            mirror_start = time.monotonic()
            update_time = now()
            upstream_metadata = upstream_repository.parse_metadata()

            if self.config.bootstrap:
                self.log.info("Bootstrapping repository: %s", upstream_repository.base_url)
                new_packages = upstream_metadata.package_list
            else:
                self.log.info("Syncing repository: %s", upstream_repository.base_url)
                # If the upstream repomd.xml file was updated after the last time we updated our
                # mirror repomd.xml file then there is probably some work to do.
                mirror_repository = YUMRepository(base_url=self._build_s3_url(upstream_repository))
                last_check_time = self.s3.repomd_update_time(base_url=mirror_repository.base_url)
                if not upstream_repository.has_updates(since=last_check_time):
                    self.log.info(f"Skipping repository with no updates since: {last_check_time}")
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
                    skip_existing=self.config.bootstrap
                )

                if not self.config.bootstrap:
                    # Store the previous repomd.xml file so if we have any issues we can easily restore it.
                    repomd_archive_location = self.s3.overwrite_repomd(
                        update_time=update_time,
                        base_url=upstream_repository.base_url,
                        manifest_location=MANIFEST_LOCATION,
                    )

                    # Store a manifest that describes the changes synced in this run
                    manifest = Manifest(
                        update_time=update_time,
                        upstream_repository=upstream_repository.base_url,
                        previous_repomd=repomd_archive_location,
                        synced_packages=[package.to_dict() for package in new_packages],
                    )
                    self.s3.put_manifest(manifest_location=MANIFEST_LOCATION, manifest=manifest)

            self.log.info("Updated mirror with %s packages", len(new_packages))
            self.stats.gauge(
                metric="s3_mirror_sync_seconds",
                value=time.monotonic() - mirror_start,
                tags={"repo": upstream_metadata.base_url},
            )

        self.stats.gauge(metric="s3_mirror_sync_seconds_total", value=time.monotonic() - start)

    def _build_s3_url(self, upstream_repository) -> str:
        dest_path = urlparse(upstream_repository.base_url).path
        s3_mirror_url = f"https://{self.config.bucket_name}.s3-{self.config.bucket_region}.amazonaws.com{dest_path}"
        return s3_mirror_url
