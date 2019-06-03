#!/usr/bin/env python3
"""
Predict hypocenter location
"""
from spp.utils.cli import CLI
from spp.pipeline.nlloc import process
from spp.pipeline.utils import prepare_nll

__module_name__ = "nlloc"


def main():
    cli = CLI(__module_name__, callback=process, prepare=prepare_nll)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
