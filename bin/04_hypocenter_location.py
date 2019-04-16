#!/usr/bin/env python3
"""
Predict hypocenter location
"""
from spp.utils.cli import CLI
from spp.pipeline.hypocenter_location import prepare, process

__module_name__ = "hypocenter_location"


def main():
    cli = CLI(__module_name__, callback=process, prepare=prepare)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
