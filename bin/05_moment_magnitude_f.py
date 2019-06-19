#!/usr/bin/env python3

from spp.utils.cli import CLI


def main():
    cli = CLI(module_name = "magnitude", module_type = "magnitude_f")
    cli.run_module()


if __name__ == "__main__":
    main()
