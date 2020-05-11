# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

import os
import pytest
from rpm_s3_mirror.config import DictConfig


def load_resource_xml(filename):
    root_dir = os.path.dirname(os.path.abspath(__file__))
    package_xml_path = os.path.join(root_dir, "resources", filename)
    with open(package_xml_path, "rb") as f:
        return f.read()


PACKAGE_XML = load_resource_xml(filename="primary.xml")
PACKAGE_CHANGED_XML = load_resource_xml(filename="primary-one-changed.xml")
REPOMD_XML = load_resource_xml(filename="repomd.xml")


@pytest.fixture(name="package_list_xml")
def package_list_xml():
    return PACKAGE_XML


@pytest.fixture(name="package_list_changed_xml")
def package_list_changed_xml():
    return PACKAGE_CHANGED_XML


@pytest.fixture(name="repomd_xml")
def repomd_xml():
    return REPOMD_XML


@pytest.fixture(name="mirror_config")
def mirror_config():
    return DictConfig(
        config_dict={
            "aws_access_key_id": "***",
            "aws_secret_access_key": "***",
            "bucket_name": "some-bucket",
            "bucket_region": "ap-southeast-2",
            "upstream_repositories": ["https://someupstreamrepo/os"]
        }
    )
