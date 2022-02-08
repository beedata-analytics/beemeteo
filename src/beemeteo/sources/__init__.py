import datetime
import logging
from abc import abstractmethod
from datetime import timedelta

import pandas as pd
import pytz
from timezonefinder import TimezoneFinder

from beemeteo.utils import save_to_hbase, read_config, get_hbase_data_batch, _datetime_dt_to_ts_utc, _pandas_ts_to_dt, \
    _datetime_to_tz

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)


class Source:
    hbase_table = None

    def __init__(self, config):
        self.config = read_config(config)

    @abstractmethod
    def _get_historical_data_source(self, latitude, longitude, gaps, local_tz):
        """
        Gets one day of forecast data from source

        :param latitude: station's latitude
        :param longitude: station's longitude
        :param ts_ini: day to retrieve data from
        :param ts_end: day to retrieve data to
        :return: all raw data for a given day
        """
        pass

    def get_historical_data(self, latitude, longitude, date_from, date_to):
        """
        Gets forecast data from source

        :param latitude: station's latitude
        :param longitude: station's longitude
        :param date_from: start date
        :param date_to: end date
        """
        # all dates must be in the timezone of the location
        tz_find = TimezoneFinder()
        tz_in_location = pytz.timezone(tz_find.timezone_at(lat=float(latitude), lng=float(longitude)))
        date_from_local = _datetime_to_tz(date_from, tz_in_location)
        date_to_local = _datetime_to_tz(date_to, tz_in_location)

        # get the current time in location
        now = _datetime_to_tz(pytz.UTC.localize(datetime.datetime.utcnow()), tz_in_location)
        if date_to_local > now:
            logger.info("Warning: requested date {} is newer than current date {}".format(date_to_local, now))

        g_ts_ini_utc = _datetime_dt_to_ts_utc(date_from_local)
        g_ts_end_utc = _datetime_dt_to_ts_utc(date_to_local)

        # set 3 dots in location
        latitude = format(latitude, '.3f')
        longitude = format(longitude, '.3f')

        # get data from hbase, the date end must be fully included
        data = self._get_from_hbase(latitude, longitude, g_ts_ini_utc, g_ts_end_utc + 86400)
        key_cols = ["latitude", "longitude", "ts"]
        data = data.set_index(key_cols)[
            sorted(data.columns[~data.columns.isin(key_cols)])
        ].reset_index() if not data.empty else data
        # check if the data contains all hours, if not, get the missing days
        gaps = []
        if data.empty:
            gaps = [(date_from_local, date_to_local)]
        else:
            data['ts2_py'] = _pandas_ts_to_dt(data['ts'], tz_in_location)
            if date_from_local < data['ts2_py'].min():
                gaps.append((date_from_local, data['ts2_py'].min().to_pydatetime()))
            if min(date_to_local, now) - timedelta(hours=1) > data['ts2_py'].max():
                gaps.append((data['ts2_py'].max().to_pydatetime(), min(date_to_local, now)))
            deltas = data['ts2_py'].diff()[1:]
            gaps_tmp = deltas[deltas > timedelta(hours=1)]
            for i, _ in gaps_tmp.iteritems():
                gap_start = data['ts2_py'][i - 1]
                gap_end = data['ts2_py'][i]
                gaps.append((gap_start, gap_end))
            data = data.drop('ts2_py', axis=1)
        if gaps:
            missing_data = self._get_historical_data_source(latitude, longitude, gaps, tz_in_location)
            missing_data = missing_data.query("ts <= {}".format(_datetime_dt_to_ts_utc(now))).sort_values(by=["ts"])
            save_to_hbase(missing_data.to_dict(orient="records"), self.hbase_table,
                          self.config['hbase_weather_data'], [("info", "all")],
                          row_fields=["latitude", "longitude", "ts"])
            data = pd.concat([data, missing_data]) if not data.empty else missing_data
        data = data.query("ts >= {} and ts <= {}".format(g_ts_ini_utc,
                                                         min(_datetime_dt_to_ts_utc(now),
                                                             g_ts_end_utc + 82800))).sort_values(by=["ts"])
        data.drop_duplicates(subset=['latitude', 'longitude', 'ts'], inplace=True)
        data.ts = _pandas_ts_to_dt(data.ts, tz_in_location)
        return data

    def _get_from_hbase(self, latitude, longitude, ts_ini, ts_end):
        """
        Gets all raw data from HBase for a given day

        :param latitude: station's latitude
        :param longitude: station's longitude
        :param ts_ini: a given timestamp to start getting data
        :param ts_end: a given timestamp to stop getting data
        :return:
        """
        measures = []
        try:
            for data_tmp in get_hbase_data_batch(self.config['hbase_weather_data'], self.hbase_table, columns=["info"],
                                                 row_start="%s~%s~%d" % (latitude, longitude, ts_ini),
                                                 row_stop="%s~%s~%d" % (latitude, longitude, ts_end)):
                for row_key, data in data_tmp:
                    new_data = data.copy()
                    for key, n_key in \
                            zip(data.keys(), [key.decode("utf-8").replace("info:", "") for key in data.keys()]):
                        new_data[n_key] = new_data.pop(key).decode("utf-8")
                    new_data["latitude"] = latitude
                    new_data["longitude"] = longitude
                    new_data["ts"] = int(row_key.decode("UTF-8").split("~")[2])
                    measures.append(new_data)
            return pd.DataFrame(measures)
        except Exception as e:
            return pd.DataFrame({})

    def save(self, data, hbase_table):
        """
        Save source raw data to HBase

        :param data: source raw data
        :param hbase_table: HBase table for source
        :return:
        """
        table = self.hbase.get_table(hbase_table, {"info": {}})
        self.hbase.save(
            table,
            data.to_dict("records"),
            [("info", "all")],
            row_fields=["latitude", "longitude", "ts"],
        )
