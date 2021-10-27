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
@click.option("--hbase-table", type=str)
def darksky(name, hbase_table):
    """
    Import forecast from darksky

    :param file name: configuration file
    :param str hbase_table: HBase table name
    """

    config = json.load(name)
    source = DarkSky(config)
    data = source.get_data(
        41.29, 2.19, pytz.timezone("Europe/Madrid"), dt.datetime(2021, 9, 1)
    )
    source.save(data, hbase_table)
    _print(data)


@cli.command()
@click.argument("name", type=click.File("rb"))
@click.option("--hbase-table", type=str)
def soda(name, hbase_table):
    """
    Import forecast from SODA

    :param file name: configuration file
    :param str hbase_table: HBase table name
    """

    config = json.load(name)
    source = SODA(config)
    data = source.get_data(
        41.29, 2.19, pytz.timezone("Europe/Madrid"), dt.datetime(2021, 9, 1)
    )
    source.save(data, hbase_table)
    _print(data)


def _print(data):
    data.to_csv(sys.stdout, index=False)


if __name__ == "__main__":
    cli(obj={})
