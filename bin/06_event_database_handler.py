#!/usr/bin/env python3
# This modules expect to receive a message containing the following:
# [catalog, stream, context_stream, event_id]

from spp.pipeline.event_database import process
from spp.utils.cli import CLI

__module_name__ = 'event_database'


def main():
    cli = CLI(__module_name__, callback=process)
    cli.prepare_module()
    cli.run_module()


if __name__ == "__main__":
    main()
