import json
import logging
import sys

import click
import pytz

from beemeteo.sources.cams import CAMS
from beemeteo.sources.darksky import DarkSky
from beemeteo.sources.meteogalicia import MeteoGalicia


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.option("--name", type=str, help="Raw data source name")
@click.option("--filename", type=click.File("rb"), help="Configuration filename")
@click.option("--latitude", type=float, help="Station's latitude")
@click.option("--longitude", type=float, help="Station's longitude")
@click.option("--timezone", type=str, help="Station's timezone")
@click.option(
    "--date-from", type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date"
)
@click.option("--date-to", type=click.DateTime(formats=["%Y-%m-%d"]), help="End date")
@click.option("--hbase-table", type=str, help="Source HBase table name for raw data")
def main(
    name, filename, latitude, longitude, timezone, date_from, date_to, hbase_table
):
    """
    Gets raw data from source
    """

    sources = {
        "cams": CAMS,
        "darksky": DarkSky,
        "meteogalicia": MeteoGalicia,
    }

    source = sources.get(name)(json.load(filename))
    data = source.get_data(
        latitude, longitude, pytz.timezone(timezone), date_from, date_to, hbase_table
    )
    if hbase_table is not None:
        source.save(data, hbase_table)
    data.to_csv(sys.stdout, index=False)


if __name__ == "__main__":
    main()
