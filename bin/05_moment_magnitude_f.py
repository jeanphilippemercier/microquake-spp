#!/usr/bin/env python3

from spp.utils.cli import CLI
from spp.pipeline.magnitude_f import process
from spp.pipeline.utils import prepare_velocities

__module_name__ = "magnitude_f"


def main():
    cli = CLI(__module_name__, callback=process, prepare=prepare_velocities)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
