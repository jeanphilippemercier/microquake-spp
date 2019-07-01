#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: spp.client.cli
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
import logging

import click

from loguru import logger

from .. import __version__
from .api import SeismicClient
from .api import Cable

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
        self.base_url = None
        self.token = None


# pass_info is a decorator for functions that pass 'Info' objects.
#: pylint: disable=invalid-name
pass_info = click.make_pass_decorator(
    Info,
    ensure=True
)


@click.group()
@click.option('--verbose', '-v', count=True, is_eager=True, help="Enable verbose output.")
@click.option('--function', '-f', help="")
@click.option('--token', '-t', required=True, help="")
@click.option('--url', '-u', help="api url")
@pass_info
def cli(info: Info,
        verbose: int,
        function: str,
        token: str,
        url: str,
        ):
    """
    Run seismic api client
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
    info.token = token
    info.base_url = url


@cli.command()
@pass_info
def run(info: Info):
    """
    Run module
    """
    sc = SeismicClient(info.token, base_url=info.base_url)

    # print(sc.get_cables())
    # Cable(x="1.0"
    # print(sc.post_ray([]))
    print(sc.post_cables([]))


@cli.command()
def version():
    """
    Get the library version.
    """
    click.echo(click.style(f'{__version__}', bold=True))
