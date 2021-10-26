import datetime as dt
import os

import pytz

from beemeteo.darksky import DarkSky


def test_darksky():
    API_KEY = os.environ.get("DARKSKY_API_KEY")
    darksky = DarkSky(API_KEY)
    darksky.hourly_forecast(41.29, 2.19, pytz.UTC, dt.datetime(2021, 9, 1))
