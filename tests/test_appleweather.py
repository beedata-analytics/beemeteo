import datetime
import pandas as pd
import json

from beemeteo.sources.appleweather import AppleWeather


def test_apple_weather():
    source = AppleWeather(json.load(open("config.json")))
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

    source.collect_forecasting(41.6100308, 0.6307409)
    x = source.get_forecasting_data(41.6100308, 0.6307409, datetime.datetime(2022, 1, 1), datetime.datetime(2022, 3, 1))
    #assert_frame_equal(data[columns_to_compare], expected[columns_to_compare])
