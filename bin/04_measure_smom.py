#!/usr/bin/env python3

from spp.utils.cli import CLI

__module_name__ = "measure_smom"


def main():
    cli = CLI(__module_name__)
    cli.run_module()


if __name__ == "__main__":
    main()
