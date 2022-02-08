import json
from datetime import datetime
import logging
from beemeteo.sources.cams import CAMS
from beemeteo.sources.darksky import DarkSky
from beemeteo.sources.meteogalicia import MeteoGalicia
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(source, conf, latitude, longitude, date_from, date_to, data_file):
    """
    Gets raw data from source
    """
    sources = {
        "CAMS": CAMS,
        "DarkSky": DarkSky,
        "MeteoGalicia": MeteoGalicia,
    }
    source_ = sources.get(source)(conf)
    data = source_.get_historical_data(
        float(latitude),
        float(longitude),
        datetime.fromisoformat(date_from),
        datetime.fromisoformat(date_to)
    )
    data.to_csv(data_file, index=False)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-s", "--source", required=True,
                    help="The source to get data from one of [DarkSky, CAMS, MeteoGalicia]")
    ap.add_argument("-c", "--config", help="The configuration file path", required=True)
    ap.add_argument("-lat", "--latitude", help="The latitude", required=True)
    ap.add_argument("-lon", "--longitude", required=True, help="The longitude")
    ap.add_argument("-d1", "--date_from", required=True, help="The date to get weather from")
    ap.add_argument("-d2", "--date_to", required=True, help="The date to get weather to (included)")
    ap.add_argument("-f", "--file", required=True, help="The file to store the data")
    args = ap.parse_args()
    with open(args.config) as config_f:
        config = json.load(config_f)
    main(args.source, config, args.latitude, args.longitude, args.date_from, args.date_to, args.file)
