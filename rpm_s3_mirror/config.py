# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

import json
import os
from abc import abstractmethod

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
    _config = DEFAULTS

    def __init__(self):
        self.load()

    def load(self):
        self._config.update(DEFAULTS)
        self._populate_required()
        missing = REQUIRED.difference(set(self._config))
        if missing:
            raise ConfigError(f"Missing required items: {missing}")
        for key, value in self._config.items():
            setattr(self, key, value)

    @abstractmethod
    def _populate_required(self):
        pass

    def __repr__(self):
        return f"{type(self).__name__}<{repr(self._config)}>"


class ENVConfig(Config):
    def _populate_required(self):
        config_options = list(REQUIRED) + list(DEFAULTS)
        for key in sorted(config_options):
            value = os.environ.get(key.upper())
            if not value:
                if key not in DEFAULTS:
                    raise ConfigError(f"Missing required environment variable: {key.upper()}")
                else:
                    continue
            elif key == "upstream_repositories":
                value = value.split(",")
            elif key == "max_workers":
                value = int(value)
            self._config[key] = value


class JSONConfig(Config):
    def __init__(self, path):
        self.path = path
        super().__init__()

    def _populate_required(self):
        with open(self.path) as f:
            self._config.update(json.load(f))


class DictConfig(Config):
    def __init__(self, config_dict):
        self._config.update(config_dict)
        super().__init__()

    def _populate_required(self):
        pass
