# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from setuptools import setup
import version

version = version.get_project_version("rpm_s3_mirror/version.py")

setup(
    name="rpm_s3_mirror",
    packages=["rpm_s3_mirror"],
    version=version,
    description="Tool for syncing RPM repositories with S3",
    license="Apache 2.0",
    author="Aiven",
    author_email="willcoe@aiven.io",
    url="https://github.com/aiven/rpm-s3-mirror",
    install_requires=[
        "defusedxml",
        "requests",
        "python-dateutil",
        "botocore",
        "lxml",
    ],
    entry_points={
        "console_scripts": [
            "rpm_s3_mirror = rpm_s3_mirror.__main__:main",
        ],
    },
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "Programming Language :: Python :: 3.7",
        "Natural Language :: English",
    ],
)
