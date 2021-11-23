import datetime
import os

import pandas as pd
import pytz

from beemeteo.sources.darksky import DarkSky
from pandas.util.testing import assert_frame_equal


def test_darksky():
    api_key = os.environ.get("DARKSKY_API_KEY")
    source = DarkSky({"darksky": {"api-key": api_key}})
    data = source.get_data(
        41.29,
        2.19,
        pytz.timezone("Europe/Madrid"),
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
    )
    expected = pd.read_csv("tests/b2back/darksky.csv")
    columns_to_compare = [
        "latitude",
        "longitude",
        "ts",
        "windBearing",
        "windGust",
        "windSpeed",
    ]
    assert_frame_equal(data[columns_to_compare], expected[columns_to_compare])
