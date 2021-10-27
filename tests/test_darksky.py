import datetime as dt
import os

import pytz

from beemeteo.sources.darksky import DarkSky


def test_darksky():
    api_key = os.environ.get("DARKSKY_API_KEY")
    DarkSky(api_key).get_data(41.29, 2.19, pytz.UTC, dt.datetime(2021, 9, 1))