import datetime as dt
import os

import pytz

from beemeteo.sources.darksky import DarkSky


def test_darksky():
    api_key = os.environ.get("DARKSKY_API_KEY")
    source = DarkSky({"darksky": {"api-key": api_key}})
    source.get_data(
        41.29,
        2.19,
        pytz.timezone("Europe/Madrid"),
        dt.datetime(2021, 9, 1),
        dt.datetime(2021, 9, 5),
    )
