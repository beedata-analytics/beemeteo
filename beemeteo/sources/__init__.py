import datetime

from abc import abstractmethod
from datetime import timedelta

import pandas as pd
import pytz

from beemeteo.hbase import HBase


def _to_tz(ts, timezone):
    return (
        ts.astimezone(pytz.UTC)
        if ts.tzinfo is not None
        else timezone.localize(ts).astimezone(pytz.UTC)
    )


def _dt_to_ts(dt, timezone=pytz.UTC):
    ts_init = pd.Timestamp("1970-01-01")
    if dt.dt.tz is not None:
        ts_init = ts_init.tz_localize(timezone)
    return ((dt - ts_init) / pd.Timedelta("1s")).astype(int)


def _local_dt_to_ts(dt):
    return int((dt - datetime.datetime(1970, 1, 1)) / timedelta(seconds=1))


class Source:
    def __init__(self, config):
        self.config = config

    @property
    def hbase(self):
        return (
            HBase(
                self.config["hbase"]["host"],
                self.config["hbase"]["port"],
                self.config["hbase"]["db"],
            )
            if "hbase" in self.config
            else None
        )

    @abstractmethod
    def _get_data_day(self, latitude, longitude, timezone, day):
        """
        Gets one day of forecast data from source

        :param latitude: station's latitude
        :param longitude: station's longitude
        :param timezone: station's timezone
        :param day: day to retrieve data from
        :return: all raw data for a given day
        """
        pass

    def get_data(
        self,
        latitude,
        longitude,
        timezone,
        date_from,
        date_to,
        hbase_table=None,
    ):
        """
        Gets forecast data from source

        :param latitude: station's latitude
        :param longitude: station's longitude
        :param timezone: station's timezone
        :param date_from: start date
        :param date_to: end date
        :param hbase_table: HBase table for source
        """
        data = None
        days = pd.date_range(
            date_from, date_to - datetime.timedelta(days=1), freq="d"
        )
        ts_from = _local_dt_to_ts(date_from)
        ts_to = _local_dt_to_ts(date_to)
        for day in days:
            ts = _local_dt_to_ts(day.to_pydatetime())
            if (
                data is not None
                and len(data.query("ts >= {}".format(ts_to))) > 0
            ):
                continue
            daily_data = self._get_from_hbase(
                latitude, longitude, ts, hbase_table
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
        data["latitude"] = latitude
        data["longitude"] = longitude
        data = data.query(
            "ts >= {} and ts <= {}".format(ts_from, ts_to)
        ).sort_values(by=["ts"])

        key_cols = ["latitude", "longitude", "ts"]
        data = data.set_index(key_cols)[
            sorted(data.columns[~data.columns.isin(key_cols)])
        ].reset_index()

        return data

    def _get_from_hbase(self, latitude, longitude, ts, hbase_table):
        """
        Gets all raw data from HBase for a given day

        :param latitude: station's latitude
        :param longitude: station's longitude
        :param ts: a given timestamp
        :param hbase_table: HBase table for source
        :return:
        """
        if hbase_table is not None:
            table = self.hbase.get_table(hbase_table, {"info": {}})
            measures = []
            for row_key, data in table.scan(
                columns=["info"],
                row_start="%s~%s~%d" % (latitude, longitude, ts),
            ):
                new_data = data.copy()
                for key, n_key in zip(
                    data.keys(),
                    [
                        key.decode("utf-8").replace("info:", "")
                        for key in data.keys()
                    ],
                ):
                    new_data[n_key] = new_data.pop(key).decode("utf-8")
                new_data["latitude"] = latitude
                new_data["longitude"] = longitude
                new_data["ts"] = int(row_key.decode("UTF-8").split("~")[2])
                measures.append(new_data)
            return pd.DataFrame(measures)
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
