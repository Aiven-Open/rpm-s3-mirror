import logging
from urllib.parse import urlparse

from fedora_s3_mirror.repository import YUMRepository
from fedora_s3_mirror.s3 import S3
from fedora_s3_mirror.util import get_requests_session


class YUMMirror:
    def __init__(self, config):
        self.config = config
        self.s3 = S3(
            aws_secret_access_key=self.config.aws_secret_access_key,
            aws_access_key_id=self.config.aws_access_key_id,
            bucket_name=self.config.bucket_name,
            bucket_region=self.config.bucket_region,
            max_workers=self.config.max_workers,
            scratch_dir=self.config.scratch_dir,
        )
        self.repositories = [YUMRepository(base_url=url) for url in config.upstream_repositories]
        self.session = get_requests_session()
        self.log = logging.getLogger(type(self).__name__)

    def sync(self):
        for upstream_repository in self.repositories:
            self.log.info("Syncing repository: %s", upstream_repository.base_url)

            # If the upstream repomd.xml file was updated after the last time we updated our
            # mirror repomd.xml file then there is probably some work to do.
            mirror_repository = YUMRepository(base_url=self._build_s3_url(upstream_repository))
            last_check_time = self.s3.repomd_update_time(base_url=mirror_repository.base_url)
            if not upstream_repository.has_updates(since=last_check_time):
                self.log.info(f"Skipping repository with no updates since: {last_check_time}")
                continue

            # Extract our metadata and detect any new/updated packages.
            upstream_metadata = upstream_repository.parse_metadata()
            mirror_metadata = mirror_repository.parse_metadata()
            new_packages = set(upstream_metadata.package_list).difference(set(mirror_metadata.package_list))

            # Sync upstream with our mirror.
            if new_packages:
                self.s3.sync_packages(
                    base_url=upstream_repository.base_url,
                    upstream_repodata=upstream_metadata.repodata,
                    upstream_packages=new_packages,
                )
            self.log.info("Updated mirror with %s packages", len(new_packages))

    def _build_s3_url(self, upstream_repository) -> str:
        dest_path = urlparse(upstream_repository.base_url).path
        s3_mirror_url = f"https://{self.config.bucket_name}.s3-{self.config.bucket_region}.amazonaws.com{dest_path}"
        return s3_mirror_url
