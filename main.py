import click
import json
import pytz

import datetime as dt

from beemeteo.darksky import DarkSky

import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.argument("name", type=click.File("rb"))
def main(name):
    """
    Import forecast from darksky

    :param file name: configuration file
    """

    config = json.load(name)
    darksky = DarkSky(config["darksky"]["api_key"])
    hourly_forecast = darksky.hourly_forecast(41.29, 2.19, pytz.UTC, dt.datetime(2021, 9, 1))
    pass


if __name__ == "__main__":
    main()
