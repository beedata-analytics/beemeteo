import datetime
import pandas as pd
from beemeteo.sources.cams import CAMS
import json

def test_cams():
    source = CAMS(json.load(open("config.json")))
    data = source.get_historical_data(
        latitude=41.29,
        longitude=2.19,
        date_from=datetime.datetime(2021, 1, 1),
        date_to=datetime.datetime(2022, 1, 7),
    )
    expected = pd.read_csv("tests/b2back/cams.csv")
    assert data.equals(expected)
