import datetime as dt
import os

import pytz

from beemeteo.sources.soda import SODA


def test_soda():
    username = os.environ.get("SODA_USERNAME")
    SODA([username]).get_data(41.29, 2.19, pytz.UTC, dt.datetime(2021, 1, 1))
