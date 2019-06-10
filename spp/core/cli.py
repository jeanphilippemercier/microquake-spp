#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: spp.core.cli
.. moduleauthor:: my_name <my_email>

This is the entry point for the command-line interface (CLI) application.  It
can be used as a handy facility for running the task from a command line.

.. note::

    To learn more about Click visit the
    `project website <http://click.pocoo.org/5/>`_.  There is also a very
    helpful `tutorial video <https://www.youtube.com/watch?v=kNke39OZ2k0>`_.

    To learn more about running Luigi, visit the Luigi project's
    `Read-The-Docs <http://luigi.readthedocs.io/en/stable/>`_ page.
"""
import importlib.util
import logging

import click

from loguru import logger

from .. import __version__
from ..utils.kafka_redis_application import KafkaRedisApplication

LOGGING_LEVELS = {
    0: logging.NOTSET,
    1: logging.ERROR,
    2: logging.WARN,
    3: logging.INFO,
    4: logging.DEBUG
}  #: a mapping of `verbose` option counts to logging levels


class Info(object):
    """
    This is an information object that can be used to pass data between CLI
    functions.
    """

    # Note that this object must have an empty constructor.
    def __init__(self):
        self.verbose: int = 0
        self.once: bool = False
        self.processing_flow: str = "automatic"
        self.module_name: str


# pass_info is a decorator for functions that pass 'Info' objects.
#: pylint: disable=invalid-name
pass_info = click.make_pass_decorator(
    Info,
    ensure=True
)


@click.group()
@click.option('--verbose', '-v', count=True, is_eager=True, help="Enable verbose output.")
@click.option('--module_name', '-m', required=True, default="interloc", help="Module name")
@click.option('--processing_flow', '-p', default="automatic", is_eager=True, help="Processing flow")
@click.option('--once', '-1', is_flag=True, default=False, help="Run loop only once")
@pass_info
def cli(info: Info,
        verbose: int,
        module_name: str,
        processing_flow: str,
        once: bool
        ):
    """
    Run seismic platform.
    """
    # Use the verbosity count to determine the logging level...

    if verbose > 0:
        logging.basicConfig(
            level=LOGGING_LEVELS[verbose]

            if verbose in LOGGING_LEVELS
            else logging.DEBUG
        )
        click.echo(
            click.style(
                f'Verbose logging is enabled. '
                f'(LEVEL={logging.getLogger().getEffectiveLevel()})',
                fg='yellow'
            )
        )
    info.verbose = verbose
    info.module_name = module_name
    info.processing_flow = processing_flow
    info.once = once


@cli.command()
@pass_info
def run(info: Info):
    """
    Run module
    """
    click.echo(f"Running '{info.module_name}'")

    app = KafkaRedisApplication(
        module_name=info.module_name,
        processing_flow_name=info.processing_flow,
    )

    spec = importlib.util.spec_from_file_location(
        "spp.pipeline." + info.module_name, "./spp/pipeline/" + info.module_name + ".py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    processor = getattr(mod, 'Processor')
    processor = processor(app=app, module_name=info.module_name)

    for msg_in in app.consumer_msg_iter():
        try:
            cat, st = app.receive_message(
                msg_in,
                processor,
            )
        except Exception as e:
            logger.error(e, exc_info=True)

            continue
        app.send_message(cat, st)

        if info.once:
            app.close()

            return


@cli.command()
def version():
    """
    Get the library version.
    """
    click.echo(click.style(f'{__version__}', bold=True))
