import datetime as dt
import json
import logging
import sys

import click
import pytz

from beemeteo.sources.darksky import DarkSky
from beemeteo.sources.soda import SODA

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    pass


@cli.command()
@click.argument("name", type=click.File("rb"))
def darksky(name):
    """
    Import forecast from darksky

    :param file name: configuration file
    """

    config = json.load(name)
    data = DarkSky(config["darksky"]["api_key"]).get_data(
        41.29, 2.19, pytz.UTC, dt.datetime(2021, 9, 1)
    )
    _print(data)


@cli.command()
@click.argument("name", type=click.File("rb"))
def soda(name):
    """
    Import forecast from SODA

    :param file name: configuration file
    """

    config = json.load(name)
    data = SODA(config["soda"]["registered_emails"]).get_data(
        41.29, 2.19, pytz.UTC, dt.datetime(2021, 9, 1)
    )
    _print(data)


def _print(data):
    data.to_csv(sys.stdout, index=False)


if __name__ == "__main__":
    cli(obj={})
