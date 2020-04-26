import argparse
import logging

from rpm_s3_mirror.config import JSONConfig, ENVConfig
from rpm_s3_mirror.mirror import Mirror

logging.getLogger('boto').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.basicConfig(level=logging.DEBUG)


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--config", help="Path to config file")
    group.add_argument("--env", help="Read configuration from environment variables", action="store_true")
    args = parser.parse_args()
    if args.config:
        config = JSONConfig(path=args.config)
    elif args.env:
        config = ENVConfig()

    mirror = Mirror(config=config)
    mirror.sync()


if __name__ == "__main__":
    main()
