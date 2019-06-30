#!/usr/bin/env python3
"""
Predict Picks
"""

from spp.utils.cli import CLI

__module_name__ = "picker"


def main():
    """
    Run the picking module
    """

    cli = CLI(__module_name__)
    cli.run_module()


if __name__ == "__main__":
    main()
