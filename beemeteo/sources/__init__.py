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
    return (dt - pd.Timestamp("1970-01-01").tz_localize(pytz.UTC)) / pd.Timedelta("1s")


class Source:
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def _get_data(self, latitude, longitude, timezone, day):
        pass

    def get_data(self, latitude, longitude, timezone, day):
        data = self._get_data(latitude, longitude, timezone, day)
        data["ts"] = _dt_to_ts(data["time"])
        return data

    def save(self, data, hbase_table):
        hbase = HBase(
            self.config["hbase"]["host"],
            self.config["hbase"]["port"],
            self.config["hbase"]["db"],
        )
        table = hbase.get_table(hbase_table, {"info": {}})
        hbase.save(
            table, data.to_dict("records"), [("info", "all")], row_fields=["time"]
        )
