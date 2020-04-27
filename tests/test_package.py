# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from rpm_s3_mirror.repository import Package, PackageList


def test_package_list(test_package_list_xml):
    package_list = PackageList(base_url="https://some.repo/some/path", packages_xml=test_package_list_xml)
    packages = list(package_list)
    assert len(package_list) > 0
    assert len(package_list) == len(packages)
    assert all((isinstance(package, Package) for package in packages))
