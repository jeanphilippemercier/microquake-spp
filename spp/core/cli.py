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
import msgpack
import traceback
import sys

from loguru import logger
from microquake.nlloc import NLL
from redis import Redis

from .. import __version__
from ..pipeline.automatic_pipeline import automatic_pipeline
from ..pipeline.interactive_pipeline import interactive_pipeline
from ..pipeline.ray_tracer import ray_tracer_pipeline
from ..utils.kafka_redis_application import KafkaRedisApplication
from .hdf5 import write_ttable_h5
from .nlloc import nll_sensors, nll_velgrids
from .settings import settings
from .velocity import create_velocities
from .connectors import connect_redis

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
        self.step: int


# pass_info is a decorator for functions that pass 'Info' objects.
#: pylint: disable=invalid-name
pass_info = click.make_pass_decorator(
    Info,
    ensure=True
)


@click.group()
@click.option('--verbose', '-v', count=True, is_eager=True, help="Enable verbose output.")
@click.option('--module_name', '-m', help="Module name")
@click.option('--step', '-s', type=int, help="Step number, starts with 1", default=1)
@click.option('--processing_flow', '-p', default="automatic", is_eager=True, help="Processing flow")
@click.option('--once', '-1', is_flag=True, default=False, help="Run loop only once")
@pass_info
def cli(info: Info,
        verbose: int,
        module_name: str,
        step: int,
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
    info.step = step-1
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
        step_number=info.step,
    )

    module_name = app.processing_flow_steps[info.step]['module']
    module_type = app.processing_flow_steps[info.step]['type']

    spec = importlib.util.spec_from_file_location(
        "spp.pipeline." + module_name, "./spp/pipeline/" + module_name + ".py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    processor = getattr(mod, 'Processor')
    processor = processor(app=app, module_type=module_type)

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


def run_pipeline(pipeline, setting_name):
    redis = connect_redis()
    message_queue = settings.get(
        setting_name).message_queue

    logger.info('initialization successful')

    while 1:
        try:
            logger.info('waiting for message on channel %s' % message_queue)
            message_queue, message = redis.blpop(message_queue)
            logger.info('message received')

            tmp = msgpack.loads(message)
            data = {}

            for key in tmp.keys():
                data[key.decode('utf-8')] = tmp[key]

            pipeline(**data)

        except Exception as e:
            logger.exception("Error occured while processing message from redis queue due to: %s" % e)



@cli.command()
@pass_info
def automatic(info: Info):
    """
    Run automatic pipeline
    """
    click.echo(f"Running automatic pipeline")
    setting_name = 'processing_flow.automatic'
    run_pipeline(automatic_pipeline, setting_name)


@cli.command()
@pass_info
def interactive(info: Info):
    """
    Run interactive pipeline
    """
    click.echo(f"Running interactive pipeline")
    setting_name = 'processing_flow.interactive'
    run_pipeline(interactive_pipeline, setting_name)


@cli.command()
@pass_info
def raytracer(info: Info):
    """
    Run raytracer module
    """
    click.echo(f"Running raytracer module")
    setting_name = 'processing_flow.ray_tracing'
    run_pipeline(ray_tracer_pipeline, setting_name)


@cli.command()
def version():
    """
    Get the library version.
    """
    click.echo(click.style(f'{__version__}', bold=True))


@cli.command()
def prepare():
    """
    Prepare project and run NonLinLoc
    """
    project_code = settings.PROJECT_CODE
    base_folder = settings.nll_base
    gridpar = nll_velgrids()
    sensors = nll_sensors()
    params = settings.get('nlloc')

    nll = NLL(project_code, base_folder=base_folder, gridpar=gridpar,
              sensors=sensors, params=params)

    # creating NLL base project including travel time grids
    logger.info('Preparing NonLinLoc')
    nll.prepare()
    logger.info('Done preparing NonLinLoc')

    # creating H5 grid from NLL grids
    logger.info('Writing h5 travel time table')
    write_ttable_h5()


@cli.command()
def velocities():
    """
    Create velocity files
    """
    logger.info('Create velocity model')
    create_velocities()
