#!/usr/bin/env python3
"""
Predict Picks
"""

from spp.utils.cli import CLI
from spp.pipeline.picker import process

__module_name__ = "picker"


def main():
    """
    Run the picking module
    """

    cli = CLI(__module_name__, callback=process)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
