import datetime

import forecastio
import pandas as pd

from src.beemeteo.sources import Source, logger


class DarkSky(Source):
    hbase_table = "darksky_historical"

    def __init__(self, config):
        super(DarkSky, self).__init__(config)
        self.api_key = self.config["dark_sky"]["api_key"]

    def _get_historical_data_source(self, latitude, longitude, gaps, local_tz):
        # Darksky will return the data starting from 00 in local time and finishing at 23 local
        missing_data = pd.DataFrame()
        for ts_ini, ts_end in gaps:
            data_period = pd.DataFrame()
            # for each gap, get the date at instant 0 (we will always download the full day) the ts_end at 00,
            # is also downloaded at full
            ts_ini = local_tz.localize(datetime.datetime.combine(ts_ini.date(), datetime.datetime.min.time()))
            ts_end = local_tz.localize(datetime.datetime.combine(ts_end.date(), datetime.datetime.min.time()))
            logger.debug("No data for period {ts_ini} {ts_end}, downloading".format(ts_ini=ts_ini, ts_end=ts_end))
            for day in pd.date_range(ts_ini, ts_end, freq="1d"):
                logger.debug("downloading for day {}".format(day))
                daily_data = self._get_data_day(latitude, longitude, day, local_tz)
                data_period = pd.concat([data_period, daily_data])
            missing_data = pd.concat([missing_data, data_period])
        missing_data = missing_data.sort_values(by=["ts"])
        missing_data['latitude'] = latitude
        missing_data['longitude'] = longitude
        missing_data.drop_duplicates(subset=['latitude', 'longitude', 'ts'], inplace=True)
        key_cols = ["latitude", "longitude", "ts"]
        missing_data = missing_data.set_index(key_cols)[
            sorted(missing_data.columns[~missing_data.columns.isin(key_cols)])
        ].reset_index() if not missing_data.empty else missing_data
        return missing_data

    def _get_data_day(self, latitude, longitude, day, local_tz):
        """
        Gets 24 hours of forecast data from darksky

        :param latitude: station's latitude
        :param longitude: station's longitude
        :param day: day to retrieve data from
        :return: all raw data for a given day
        """
        hourly = []
        for item in (
            forecastio.load_forecast(
                self.api_key,
                latitude,
                longitude,
                time=day,
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
