#!/usr/bin/env python3

from spp.utils.cli import CLI
from spp.pipeline.measure_smom import process

__module_name__ = "measure_smom"


def main():
    cli = CLI(__module_name__, callback=process)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
