import argparse
import logging

from fedora_s3_mirror.config import Config, JSONConfig, ENVConfig
from fedora_s3_mirror.mirror import YUMMirror

logging.getLogger('boto').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.basicConfig(level=logging.DEBUG)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="Path to config file")
    parser.add_argument("--env", help="Read configuration from environment variables", action="store_true")
    args = parser.parse_args()
    if not args.config and not args.env:
        raise Exception("--config or --env are required")
    elif args.config and args.env:
        raise Exception("--config and --env are mutually exclusive")
    elif args.config:
        config = JSONConfig(path=args.config)
    elif args.env:
        config = ENVConfig()

    config.load()
    mirror = YUMMirror(config=config)
    mirror.sync()


if __name__ == "__main__":
    main()
