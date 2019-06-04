#!/usr/bin/env python3
"""
Predict hypocenter location
"""
from spp.utils.cli import CLI

__module_name__ = "nlloc"


def main():
    cli = CLI(__module_name__)
    cli.run_module()


if __name__ == "__main__":
    main()
