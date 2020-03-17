from setuptools import setup
import setuptools

setup(
    name="fedora_s3_mirror",
    packages=setuptools.find_packages(),
    version="0.1",
    description="Tool for syncing YUM repositories with S3",
    license="Apache 2.0",
    author="Aiven",
    author_email="willcoe@aiven.io",
    url="https://github.com/aiven/fedora_s3_mirror",
    install_requires=[
        "defusedxml>=0.6.0",
        "requests>=2.23.0",
        "python-dateutil>=2.8.1",
        "boto3>=1.12.21",
        "lxml>=4.5.0",
    ],
    entry_points={
        "console_scripts": [
            "fedora_s3_mirror = fedora_s3_mirror.__main__:main",
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
