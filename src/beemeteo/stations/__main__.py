import logging

import click

from src.beemeteo.stations.postal_code import PostalCode
from src.beemeteo.stations.stations import Stations


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.option("--stations", type=click.File("rb"))
@click.option("--postal-code", type=str, help="Postal code")
@click.option("--country", type=str, default="ES", help="Country")
def main(stations, postal_code, country):
    """
    Gets the closest station for a given postal code.
    """

    (latitude, longitude, distance) = PostalCode(
        country, postal_code
    ).find_closest(Stations.load(stations))
    logger.info(
        "Closest station ({latitude}, {longitude}) is {distance} "
        "kilometers from postal code {postal_code})".format(
            latitude=latitude,
            longitude=longitude,
            distance=distance,
            postal_code=postal_code,
        )
    )


if __name__ == "__main__":
    main()
