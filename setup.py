from setuptools import setup
import setuptools

setup(
    name="rpm_s3_mirror",
    packages=setuptools.find_packages(),
    version="0.1",
    description="Tool for syncing RPM repositories with S3",
    license="Apache 2.0",
    author="Aiven",
    author_email="willcoe@aiven.io",
    url="https://github.com/aiven/rpm-s3-mirror",
    install_requires=[
        "defusedxml",
        "requests",
        "python-dateutil",
        "boto3",
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
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Natural Language :: English",
    ],
)
