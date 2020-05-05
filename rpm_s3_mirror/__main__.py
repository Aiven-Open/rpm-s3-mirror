# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

import argparse
import logging

from rpm_s3_mirror.config import JSONConfig, ENVConfig
from rpm_s3_mirror.mirror import Mirror

logging.getLogger("boto").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--snapshot",
        help="Create a snapshot of current repository state",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--verbose",
        help="Verbose logging",
        action="store_true",
        default=False,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--config", help="Path to config file")
    group.add_argument("--env", help="Read configuration from environment variables", action="store_true")

    args = parser.parse_args()
    if args.config:
        config = JSONConfig(path=args.config)
    elif args.env:
        config = ENVConfig()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    mirror = Mirror(config=config)
    if args.snapshot:
        mirror.snapshot()
    else:
        mirror.sync()


if __name__ == "__main__":
    main()
