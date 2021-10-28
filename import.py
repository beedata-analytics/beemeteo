import datetime
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


def get_raw_data(
    source, latitude, longitude, timezone, date_from, date_to, hbase_table
):
    """
    Retrieve data from source

    :param source: raw data source
    :param float latitude: station's latitude
    :param float longitude: station's longitude
    :param str timezone: station's timezone
    :param datetime date_from: start date
    :param datetime date_to: end date
    :param str hbase_table: HBase table for source
    :return:
    """
    data = source.get_data(
        latitude, longitude, pytz.timezone(timezone), date_from, date_to, hbase_table
    )
    if hbase_table is not None:
        source.save(data, hbase_table)
    _print(data)


@cli.command()
@click.argument("filename", type=click.File("rb"))
@click.argument("latitude", type=float)
@click.argument("longitude", type=float)
@click.argument("timezone", type=str)
@click.argument(
    "date_from",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(datetime.datetime.now().date()),
)
@click.argument(
    "date_to",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(datetime.datetime.now().date()),
)
@click.option("--hbase-table", type=str)
def darksky(filename, latitude, longitude, timezone, date_from, date_to, hbase_table):
    """
    Gets raw data from darksky

    :param file filename: configuration file
    :param float latitude: station's latitude
    :param float longitude: station's longitude
    :param str timezone: station's timezone
    :param datetime date_from: start date
    :param datetime date_to: end date
    :param str hbase_table: HBase table for source
    """

    get_raw_data(
        DarkSky(json.load(filename)),
        latitude,
        longitude,
        timezone,
        date_from,
        date_to,
        hbase_table,
    )


@cli.command()
@click.argument("filename", type=click.File("rb"))
@click.argument("latitude", type=float)
@click.argument("longitude", type=float)
@click.argument("timezone", type=str)
@click.argument(
    "date_from",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(datetime.datetime.now().date()),
)
@click.argument(
    "date_to",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(datetime.datetime.now().date()),
)
@click.option("--hbase-table", type=str)
def meteogalicia(filename, latitude, longitude, timezone, date_from, date_to, hbase_table):
    """
    Gets raw data from meteogalicia

    :param file filename: configuration file
    :param float latitude: station's latitude
    :param float longitude: station's longitude
    :param str timezone: station's timezone
    :param datetime date_from: start date
    :param datetime date_to: end date
    :param str hbase_table: HBase table for source
    """

    get_raw_data(
        MeteoGalicia(json.load(filename)),
        latitude,
        longitude,
        timezone,
        date_from,
        date_to,
        hbase_table,
    )


@cli.command()
@click.argument("filename", type=click.File("rb"))
@click.argument("latitude", type=float)
@click.argument("longitude", type=float)
@click.argument("timezone", type=str)
@click.argument(
    "date_from",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(datetime.datetime.now().date()),
)
@click.argument(
    "date_to",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(datetime.datetime.now().date()),
)
@click.option("--hbase-table", type=str)
def soda(filename, latitude, longitude, timezone, date_from, date_to, hbase_table):
    """
    Gets raw data from SODA

    :param file filename: configuration file
    :param float latitude: station's latitude
    :param float longitude: station's longitude
    :param str timezone: station's timezone
    :param datetime date_from: start date
    :param datetime date_to: end date
    :param str hbase_table: HBase table for source
    """

    get_raw_data(
        SODA(json.load(filename)),
        latitude,
        longitude,
        timezone,
        date_from,
        date_to,
        hbase_table,
    )


def _print(data):
    data.to_csv(sys.stdout, index=False)


if __name__ == "__main__":
    cli(obj={})
