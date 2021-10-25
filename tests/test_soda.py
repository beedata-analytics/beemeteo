import datetime as dt
import os

import pytz

from beemeteo.soda import SODA


def test_soda():
    username = os.environ.get("SODA_USERNAME")
    soda = SODA([username])
    soda.solar_radiation(41.29, 2.19, pytz.UTC, dt.datetime(2021, 1, 1))
