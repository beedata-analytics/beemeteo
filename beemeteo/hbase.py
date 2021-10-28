import logging
import time

import happybase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HBase:
    def __init__(self, host, port, db):
        self.host = host
        self.port = port
        self.db = db

    @property
    def connection(self):
        _connection = happybase.Connection(
            self.host, self.port, table_prefix=self.db, table_prefix_separator=":"
        )
        _connection.open()
        return _connection

    def get_table(self, table_name, cf=None):
        try:
            if not cf:
                cf = {"cf": {}}
            self.connection.create_table(table_name, cf)
        except Exception as e:
            if str(e.__class__) == "<class 'Hbase_thrift.AlreadyExists'>":
                pass
            else:
                logger.error(e)
        return self.connection.table(table_name)

    @staticmethod
    def save(
        table,
        documents,
        cf_mapping,
        row_fields=None,
        version=int(time.time()),
        batch_size=1000,
    ):
        batch = table.batch(timestamp=version, batch_size=batch_size)
        row_auto = 0
        for d in documents:
            if not row_fields:
                row = row_auto
                row_auto += 1
            else:
                row = "~".join([str(d.pop(f)) if f in d else "" for f in row_fields])
            values = {}
            for cf, fields in cf_mapping:
                if fields == "all":
                    for c, v in d.items():
                        values["{cf}:{c}".format(cf=cf, c=c)] = str(v)
                else:
                    for c in fields:
                        if c in d:
                            values["{cf}:{c}".format(cf=cf, c=c)] = str(d[c])
                batch.put(str(row), values)
        batch.send()
