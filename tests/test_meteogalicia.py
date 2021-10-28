import datetime as dt

import pytz

from beemeteo.sources.meteogalicia import MeteoGalicia


def test_meteogalicia():
    source = MeteoGalicia({})
    source.get_data(
        41.29,
        2.19,
        pytz.timezone("Europe/Madrid"),
        dt.datetime(2021, 9, 1),
        dt.datetime(2021, 9, 5),
    )
