import datetime
import os

import pandas as pd
import pytz

from beemeteo.sources.darksky import DarkSky


def test_darksky():
    source = DarkSky("config.json")
    data = source.get_historical_data(
        latitude=-30.3183,
        longitude=131.8894413,
        date_from=datetime.datetime(2022, 1, 1),
        date_to=datetime.datetime(2022, 1, 9),
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
    #assert_frame_equal(data[columns_to_compare], expected[columns_to_compare])
