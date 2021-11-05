import datetime
import io
import logging

import pandas as pd
import pytz
import requests

from beemeteo.sources import Source
from beemeteo.sources import _to_tz
from dateutil.relativedelta import relativedelta


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VERSION = "1.0.0"
SODA_SERVER_SERVICE = "http://www.soda-is.com/service/wps"
SODA_SERVER_MIRROR_SERVICE = "http://pro.soda-is.com/service/wps"


class CAMS(Source):
    def __init__(self, config):
        super(CAMS, self).__init__(config)
        self.cams_registered_mails = self.config["cams"][
            "cams-registered-mails"
        ]
        assert len(self.cams_registered_mails) > 0

    def _get_data(
        self, latitude, longitude, timezone, date_from, date_to, hbase_table
    ):
        data = None
        days = pd.date_range(
            date_from, date_to - datetime.timedelta(days=1), freq="d"
        )
        for day in days:
            daily_data = self._get_from_hbase(
                latitude, longitude, timezone, day, hbase_table
            )
            if len(daily_data) < 24:
                daily_data = self._get_data_day(
                    latitude, longitude, timezone, day
                )
                if daily_data is None:
                    logger.info(
                        "[CAMS] could not retrieve info for %s" % (day)
                    )
                else:
                    data = (
                        pd.merge(data, daily_data, how="outer")
                        if data is not None
                        else daily_data
                    )
        return data

    def _get_data_day(self, latitude, longitude, timezone, day):
        """
        Gets solar radiation information for a location on a given day
        http://www.soda-pro.com/web-services/radiation/cams-radiation-service/info

        :param float latitude: longitude
        :param float longitude: latitude
        :param timezone: timezone
        :param datetime.datetime day:
        :return:
        """
        date_begin = _to_tz(day, timezone)
        date_end = _to_tz(
            day + relativedelta(days=1) - relativedelta(seconds=1), timezone
        )
        for mail in self.cams_registered_mails:
            data = self._request(
                mail, latitude, longitude, date_begin, date_end
            )
            if data is not None:
                logger.info(
                    "[CAMS] %s retrieved info for %s" % (mail, date_begin)
                )
                return data
            else:
                logger.info(
                    "[CAMS] maximum number of daily requests reached for %s"
                    % mail
                )
                self.cams_registered_mails.remove(mail)

    def _request_server(
        self,
        server,
        username,
        latitude,
        longitude,
        date_begin,
        date_end,
        altitude,
        time_ref,
        summarization,
    ):
        payload = {
            "Service": "WPS",
            "Request": "Execute",
            "Identifier": "get_cams_radiation",
            "version": VERSION,
            "DataInputs": "latitude={};longitude={};altitude={};"
            "date_begin={};date_end={};time_ref={};"
            "summarization={};username={}".format(
                latitude,
                longitude,
                altitude,
                datetime.datetime.strftime(date_begin, "%Y-%m-%d %H:%M:%S")[
                    :10
                ],
                datetime.datetime.strftime(date_end, "%Y-%m-%d %H:%M:%S")[:10],
                time_ref,
                summarization,
                username.replace("@", "%2540"),
            ),
            "RawDataOutput": "irradiation",
        }

        params = "&".join("%s=%s" % (k, v) for k, v in payload.items())
        response = requests.get(server, params=params)
        if response.status_code != 200:
            raise Exception
        return self._parse_request(response)

    def _request(
        self,
        username,
        latitude,
        longitude,
        date_begin,
        date_end,
        altitude=-999,
        time_ref="UT",
        summarization="PT01H",
    ):
        for service in [SODA_SERVER_SERVICE, SODA_SERVER_MIRROR_SERVICE]:
            try:
                response = self._request_server(
                    service,
                    username,
                    latitude,
                    longitude,
                    date_begin,
                    date_end,
                    altitude,
                    time_ref,
                    summarization,
                )
                return response
            except Exception:
                pass

    @staticmethod
    def _parse_request(response):
        """Parse the request output into a pandas DataFrame.
        :param response: api response
        :return: dataframe
        """

        data = io.StringIO(response.text.split("#")[-1])
        df = pd.read_csv(data, delimiter=";")
        time_column_name = " Observation period"
        df[time_column_name] = pd.to_datetime(
            df[time_column_name].str[:19]
        ).dt.tz_localize(pytz.UTC)
        df = df.rename({time_column_name: "time"}, axis=1)
        return df
