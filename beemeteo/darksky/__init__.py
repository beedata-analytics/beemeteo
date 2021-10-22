import forecastio
import pytz


class DarkSky:
    def __init__(self, api_key):
        self.api_key = api_key

    def hourly_forecast(self, latitude, longitude, timezone, day):
        """
        Gets 24 hours of forecast from darksky

        :param float latitude: latitude
        :param float longitude: longitude
        :param timezone: timezone
        :param datetime.datetime day: day to forecast
        """

        hourly = []
        for item in forecastio.load_forecast(self.api_key, latitude, longitude, time=day, units="si").hourly().data:
            d = item.d
            d.update({
                'time': pytz.UTC.localize(item.time).astimezone(timezone)
            })
            hourly.append(d)
        return hourly
