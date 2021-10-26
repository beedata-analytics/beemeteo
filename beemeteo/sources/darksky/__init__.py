import forecastio
import pandas as pd
import pytz

from beemeteo.sources import Source


class DarkSky(Source):
    def __init__(self, api_key):
        self.api_key = api_key

    def get_data(self, latitude, longitude, timezone, day):
        """
        Gets 24 hours of forecast from darksky

        :param float latitude: latitude
        :param float longitude: longitude
        :param timezone: timezone
        :param datetime.datetime day: day to forecast
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
