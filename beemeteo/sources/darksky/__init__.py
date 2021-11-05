import datetime

import forecastio
import pandas as pd
import pytz

from beemeteo.sources import Source


class DarkSky(Source):
    def __init__(self, config):
        super(DarkSky, self).__init__(config)
        self.api_key = self.config["darksky"]["api-key"]

    def _get_data(
        self, latitude, longitude, timezone, date_from, date_to, hbase_table
    ):
        data = None
        days = pd.date_range(
            date_from, date_to - datetime.timedelta(days=1), freq="d"
        )
        for day in days:
            daily_data = self._get_from_hbase(
                latitude, longitude, timezone, day, hbase_table
            )
            if len(daily_data) < 24:
                daily_data = self._get_data_day(
                    latitude, longitude, timezone, day
                )
                data = (
                    pd.merge(data, daily_data, how="outer")
                    if data is not None
                    else daily_data
                )
        return data

    def _get_data_day(self, latitude, longitude, timezone, day):
        """
        Gets 24 hours of forecast data from darksky

        :param latitude: station's latitude
        :param longitude: station's longitude
        :param timezone: station's timezone
        :param day: day to retrieve data from
        :return: all raw data for a given day
        """
        hourly = []
        for item in (
            forecastio.load_forecast(
                self.api_key,
                latitude,
                longitude,
                time=timezone.localize(day),
                units="si",
            )
            .hourly()
            .data
        ):
            d = item.d
            d.update({"time": pytz.UTC.localize(item.time)})
            hourly.append(d)
        return pd.DataFrame.from_dict(hourly)
