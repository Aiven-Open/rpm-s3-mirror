# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

import os
import pytest


@pytest.fixture(name="test_package_list_xml")
def test_package_list_xml():
    root_dir = os.path.dirname(os.path.abspath(__file__))
    package_xml_path = os.path.join(root_dir, "resources", "primary.xml")
    with open(package_xml_path, "rb") as f:
        return f.read()
