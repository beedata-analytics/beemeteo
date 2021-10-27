from abc import abstractmethod

import pytz

from beemeteo.hbase import HBase


def to_tz(ts, timezone):
    return (
        ts.astimezone(pytz.UTC)
        if ts.tzinfo is not None
        else timezone.localize(ts).astimezone(pytz.UTC)
    )


class Source:
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def get_data(self, latitude, longitude, timezone, day):
        pass

    def save(self, data, hbase_table):
        hbase = HBase(
            self.config["hbase"]["host"],
            self.config["hbase"]["port"],
            self.config["hbase"]["db"],
        )
        table = hbase.get_table(hbase_table, {"info": {}})
        hbase.save(table, data.to_dict("records"), [("info", "all")], row_fields=["time"])
