# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
import pytest
import tempfile
from rpm_s3_mirror.repository import Package, PackageList, RPMRepository, safe_parse_xml, decompress

TEST_BASE_URL = "https://some.repo/some/path"
CHANGED_PACKAGE_NAME = "GMT"

EXPECTED_REPOMD_KEYS = [
    "primary",
    "filelists",
    "other",
    "primary_db",
    "filelists_db",
    "other_db",
    "primary_zck",
    "filelists_zck",
    "other_zck",
    "group",
    "group_gz",
    "group_zck",
    "prestodelta",
    "prestodelta_zck",
    "updateinfo",
    "updateinfo_zck",
]

EXPECTED_REPOMD_CHECKSUMS = {
    "primary": "df0a31f7ab547ef2231eaedac2c00280ea4512dfa697da4c6f21c2eeee2ca653",
    "filelists": "6957f0f984e4bf67998529227f9b790efa58deb3c1699e4c4852c4dbff04fc59",
    "other": "36b4d77da655b84f886c2aba829d22fa05545e069f3b1d734fdc5cf5fed30648",
    "primary_db": "9af4005c4e601da4141822c4e4eaa9deda09b5e5bc75a086c2acd101c57345ee",
    "filelists_db": "09f119eb1b87875eccde402ce5a813a7d3f91b5d3d03cd62c34583506a9bfa27",
    "other_db": "2241e176091360cbb8fb8cb5fa6d7d6851f4958755a9aac606f8c507352eff3c",
    "primary_zck": "876b3081bbb63c4835563a448d85570fdae9eebb7d2efcb37d2d540957a8b1d3",
    "filelists_zck": "9cfa6d492833ca9c5fe7392476b1b2586dc1462025bebfd178b12017471cdbf6",
    "other_zck": "3abe0789c2e0369b4432b1b25a001064d59ee9cfe6e8e3685a8dee2a5aaa2201",
    "group": "9831064bb0ea0bce4c0c454053ab778481ebf87fc1cc7c9b0a393ad7b4a6a4f2",
    "group_gz": "6ab0e9a7ad06dc195a1d003d3fa1ba6682da051d3bfb7612b6551f4246003f17",
    "group_zck": "f2509a1cccdef24dc2b873bc537a4c76da2ee4cb60160c908d7410e519bdfab4",
    "prestodelta": "c39046ec3ed2813ef94b07b3664c556f40b72ef5988872511e8e64716f1a25b7",
    "prestodelta_zck": "37ef7542fb5233d40749c8e3455b181c513682a840493e8f63162e34da72fd02",
    "updateinfo": "4fc1cd2996f92d0c32f61e83c2f927f171abae4bf6e8013e58bbb012bcbec12e",
    "updateinfo_zck": "9a9ac8e15d2fd91c1789ba9f6811db121f5e6916a0f3b518ca1211e6d6e02f43",
}


def test_package_list(package_list_xml):
    packages = PackageList(base_url=TEST_BASE_URL, packages_xml=package_list_xml)
    package_list = list(packages)
    assert len(packages) > 0
    assert len(packages) == len(package_list)
    assert all((isinstance(package, Package) for package in package_list))
    assert set(packages).difference(packages) == set()


def test_package_list_comparison(package_list_xml, package_list_changed_xml):
    packages1 = PackageList(base_url=TEST_BASE_URL, packages_xml=package_list_xml)
    packages2 = PackageList(base_url=TEST_BASE_URL, packages_xml=package_list_changed_xml)

    assert set(packages1).difference(set(packages1)) == set()

    changed_packages = set(packages1).difference(set(packages2))
    assert len(changed_packages) == 1

    changed_package = list(changed_packages)[0]
    assert changed_package.name == CHANGED_PACKAGE_NAME


def test_package_equality(package_list_xml, package_list_changed_xml):
    package_list = list(PackageList(base_url=TEST_BASE_URL, packages_xml=package_list_xml))
    package1 = package_list[0]
    package2 = package_list[1]
    assert package1 != package2

    package_list2 = list(PackageList(base_url=TEST_BASE_URL, packages_xml=package_list_changed_xml))
    assert package_list[1] == package_list2[1]


def test_parse_repomd_xml(repomd_xml):
    repository = RPMRepository(base_url=TEST_BASE_URL)
    repomd = repository.parse_repomd(safe_parse_xml(repomd_xml))

    assert list(repomd.keys()) == EXPECTED_REPOMD_KEYS
    assert {k: v.checksum for k, v in repomd.items()} == EXPECTED_REPOMD_CHECKSUMS

    for repodata_section in repomd.values():
        assert all(attr is not None for attr in repodata_section._asdict().values())


def test_reject_http_upstream_repository():
    with pytest.raises(ValueError):
        RPMRepository(base_url="http://dangerdanger")


GZIP_CONTENT = b"\x1f\x8b\x08\x08\xe0\x84\x84f\x00\x03content\x00+\xc8/I,\xc9\xe7\x02\x00I:&V\x07\x00\x00\x00"
ZSTD_CONTENT = b"(\xb5/\xfd$\x079\x00\x00potato\nE.\xa8%"
UNCOMPRESSED_CONTENT = b"potato\n"


@pytest.mark.parametrize(
    ["content", "expected"],
    [
        pytest.param(GZIP_CONTENT, UNCOMPRESSED_CONTENT, id="gzip"),
        pytest.param(ZSTD_CONTENT, UNCOMPRESSED_CONTENT, id="zstd"),
    ],
)
def test_decompress(content: bytes, expected: bytes):
    with tempfile.NamedTemporaryFile() as f:
        f.write(content)
        f.flush()
        actual = decompress(f.name)
        assert actual == expected
