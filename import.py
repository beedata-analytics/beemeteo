import datetime as dt
import json
import logging
import sys

import click
import pytz

from beemeteo.sources.darksky import DarkSky
from beemeteo.sources.meteogalicia import MeteoGalicia
from beemeteo.sources.soda import SODA

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    pass


@cli.command()
@click.argument("filename", type=click.File("rb"))
def darksky(filename):
    """
    Import forecast from darksky

    :param file filename: configuration file
    """

    config = json.load(filename)
    source = DarkSky(config)
    data = source.get_data(
        41.29, 2.19, pytz.timezone("Europe/Madrid"), dt.datetime(2021, 9, 1)
    )
    source.save(data, config["darksky"]["hbase-table"])
    _print(data)


@cli.command()
@click.argument("filename", type=click.File("rb"))
def meteogalicia(filename):
    """
    Import forecast from meteogalicia

    :param file name: configuration file
    """

    config = json.load(filename)
    source = MeteoGalicia(config)
    data = source.get_data(
        41.29, 2.19, pytz.timezone("Europe/Madrid"), dt.datetime(2021, 9, 1)
    )
    source.save(data, config["meteogalicia"]["hbase-table"])
    _print(data)


@cli.command()
@click.argument("filename", type=click.File("rb"))
def soda(filename):
    """
    Import forecast from SODA

    :param file filename: configuration file
    """

    config = json.load(filename)
    source = SODA(config)
    data = source.get_data(
        41.29, 2.19, pytz.timezone("Europe/Madrid"), dt.datetime(2021, 9, 1)
    )
    source.save(data, config["soda"]["hbase-table"])
    _print(data)


def _print(data):
    data.to_csv(sys.stdout, index=False)


if __name__ == "__main__":
    cli(obj={})
