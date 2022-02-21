import datetime

import pandas as pd
import json
from beemeteo.sources.meteogalicia import MeteoGalicia


def test_meteogalicia():
    source = MeteoGalicia(json.load(open("config.json")))
    data = source.get_historical_data(
        latitude=41.29,
        longitude=2.19,
        date_from=datetime.datetime(2020, 12, 25),
        date_to=datetime.datetime(2021, 1, 12),
    )
    expected = pd.read_csv("tests/b2back/meteogalicia.csv")
    assert data.equals(expected)