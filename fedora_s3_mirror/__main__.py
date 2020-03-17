import argparse
import logging

from fedora_s3_mirror.config import Config
from fedora_s3_mirror.mirror import YUMMirror

logging.getLogger('boto').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.basicConfig(level=logging.DEBUG)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="Path to config file", required=True)
    args = parser.parse_args()

    mirror = YUMMirror(config=Config(path=args.config))
    mirror.sync()
