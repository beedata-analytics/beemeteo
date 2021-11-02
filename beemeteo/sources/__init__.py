from abc import abstractmethod

import pandas as pd
import pytz

from beemeteo.hbase import HBase


def _to_tz(ts, timezone):
    return (
        ts.astimezone(pytz.UTC)
        if ts.tzinfo is not None
        else timezone.localize(ts).astimezone(pytz.UTC)
    )


def _dt_to_ts(dt):
    return (
        (dt - pd.Timestamp("1970-01-01").tz_localize(pytz.UTC)) / pd.Timedelta("1s")
    ).astype(int)


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
    def _get_data(self, latitude, longitude, timezone, date_from, date_to, hbase_table):
        """
        Gets forecast data from source

        :param float latitude: latitude
        :param float longitude: longitude
        :param timezone: timezone
        :param datetime.datetime day: day to forecast
        :param str hbase_table: HBase table for source
        """
        pass

    def get_data(
        self, latitude, longitude, timezone, date_from, date_to, hbase_table=None
    ):
        data = self._get_data(
            latitude, longitude, timezone, date_from, date_to, hbase_table
        )
        data["ts"] = _dt_to_ts(data["time"])
        data["latitude"] = latitude
        data["longitude"] = longitude
        data = data.sort_values(by=["ts"])
        return data

    def _get_from_hbase(self, day, hbase_table):
        """
        Gets all raw data from HBase for a given day

        :param day: a given day
        :param hbase_table: HBase table for source
        :return:
        """
        # TODO:
        if hbase_table is None:
            return pd.DataFrame({})
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
