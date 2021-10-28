import datetime as dt
import os

import pytz

from beemeteo.sources.cams import CAMS


def test_cams():
    username = os.environ.get("SODA_USERNAME")
    source = CAMS({"cams": {"cams-registered-mails": [username]}})
    source.get_data(41.29, 2.19, pytz.UTC, dt.datetime(2021, 1, 1))
