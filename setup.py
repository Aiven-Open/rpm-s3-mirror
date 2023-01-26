# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

# a setup.py shim which is required for an editable install
# see pyproject.toml for build configuration
# https://peps.python.org/pep-0518
import setuptools

if __name__ == "__main__":
    setuptools.setup()
