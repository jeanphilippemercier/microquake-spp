#!/usr/bin/env python3

from spp.utils.cli import CLI

__module_name__ = "magnitude"


def main():
    cli = CLI(module_name = "magnitude", module_type = "magnitude")
    cli.run_module()


if __name__ == "__main__":
    main()
