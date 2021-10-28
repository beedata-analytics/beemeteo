import datetime as dt
import os

import pytz

from beemeteo.sources.soda import SODA


def test_soda():
    username = os.environ.get("SODA_USERNAME")
    source = SODA({"soda": {"cams-registered-mails": [username]}})
    source.get_data(41.29, 2.19, pytz.UTC, dt.datetime(2021, 1, 1))
