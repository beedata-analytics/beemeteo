import forecastio
import pandas as pd

from beemeteo.sources import Source


class DarkSky(Source):
    def __init__(self, config):
        super(DarkSky, self).__init__(config)
        self.api_key = self.config["darksky"]["api-key"]

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
            hourly.append(d)
        data = pd.DataFrame(hourly)
        data["ts"] = data["time"]
        data.drop(["time"], axis=1, inplace=True)
        return data
