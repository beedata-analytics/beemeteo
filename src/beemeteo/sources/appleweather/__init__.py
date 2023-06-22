import datetime
import requests
import pandas as pd
import pytz
import json

from beemeteo.sources import Source, logger

# curl

# htps://weatherkit.apple.com/api/v1/ + weather/en_US/ + _latitude + _longitude + ?dataSets= +
# ["currentWeather","forecastDaily","forecastHourly","forecastNextHour","weatherAlerts"]

# -H 'Authorization: Bearer TOKEN'

class AppleWeather(Source):
    requests.get()
    hbase_table_historical = "appleweather_historical"
    hbase_table_forecasting = "appleweather_forecasting"

    def __init__(self, config):
        super(AppleWeather, self).__init__(config)
        self.api_key = self.config["A"][]

    # returning to main
    def _collect_forecasting(self, latitude, longitude):
        return forecasting

    # returning to main
    def _get_historical_data(self, latitude, longitude, gaps, local_tz):
        return missing_data

    # returning to main
    def _get_forecasting_data(self, latitude, longitude, date_from, date_to):

    # aux functions under
    def _request_server(
        lat,
        long,git
        service,
        day_from = ,
        day_to = ,
    ):

        headers = {'Authorization': 'Bearer {}'.format(TOKEN)}
        payload = {
            "dataSets" = service,
        "dailyStart" =,  #
        "dailyEnd" =,
        "hourlyStart" = 00:00,  # If this parameter is absent, hourly forecasts start on the current hour
        "hourlyEnd" = 23:59,  # If this parameter is absent, hourly forecasts run 24 hours or the length of the daily forecast, whichever is longer
        "timezone" =,
        }

        response = requests.get(url, headers=headers, params=payload)
        if response.status_code != 200:
            raise Exception
        return self._parse_request(response.text)

    @staticmethod
    def _parse_request(response):
        """Parse the request output into a pandas DataFrame.
        :param response: api response
        :return: dataframe
        """