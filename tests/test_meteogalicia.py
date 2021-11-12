import datetime

import pandas as pd
import pytz

from beemeteo.sources.meteogalicia import MeteoGalicia


def test_meteogalicia():
    source = MeteoGalicia({})
    data = source.get_data(
        41.29,
        2.19,
        pytz.timezone("Europe/Madrid"),
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
    )
    expected = pd.read_csv("tests/b2back/meteogalicia.csv")
    assert data.equals(expected)
