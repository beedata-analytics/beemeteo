import datetime
import os

import pandas as pd
import pytz

from beemeteo.sources.cams import CAMS


def test_cams():
    username = os.environ.get("SODA_USERNAME")
    source = CAMS({"cams": {"cams-registered-mails": [username]}})
    data = source.get_data(
        41.29,
        2.19,
        pytz.timezone("Europe/Madrid"),
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
    )
    expected = pd.read_csv("tests/b2back/cams.csv")
    assert data.equals(expected)
