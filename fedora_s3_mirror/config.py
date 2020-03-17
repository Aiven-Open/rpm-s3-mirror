import json

REQUIRED = {
    "aws_access_key_id",
    "aws_secret_access_key",
    "bucket_name",
    "bucket_region",
    "max_workers",
    "upstream_repositories",
}

DEFAULTS = {
    "scratch_dir": "/var/tmp/",
    "max_workers": 4,
}


class ConfigError(ValueError):
    pass


class Config:
    aws_access_key_id = None
    aws_secret_access_key = None
    bucket_name = None
    bucket_region = None
    scratch_dir = None
    max_workers = None
    upstream_repositories = None

    def __init__(self, path):
        self.config = DEFAULTS
        with open(path) as f:
            self.config.update(json.load(f))
        missing = REQUIRED.difference(set(self.config))
        if missing:
            raise ConfigError(f"Missing required items: {missing}")
        for key, value in self.config.items():
            setattr(self, key, value)
