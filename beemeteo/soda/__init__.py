import datetime as dt
import io

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

VERSION = "1.0.0"
SODA_SERVER_SERVICE = "http://www.soda-is.com/service/wps"
SODA_SERVER_MIRROR_SERVICE = "http://pro.soda-is.com/service/wps"


class SODA:
    def __init__(self, cams_registered_mails):
        self.cams_registered_mails = cams_registered_mails
        assert len(self.cams_registered_mails) > 0

    def solar_radiation(self, latitude, longitude, timezone, day):
        date_begin = day.astimezone(timezone)
        date_end = (day + relativedelta(days=1)).astimezone(timezone)
        for mail in self.cams_registered_mails:
            username = mail.replace("@", "%2540")
            try:
                response = self.request(
                    username, latitude, longitude, date_begin, date_end
                )
                return response
            except Exception:
                continue

    def request_server(
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
        params = {
            "Service": "WPS",
            "Request": "Execute",
            "Identifier": "get_cams_radiation",
            "version": VERSION,
            "DataInputs": "latitude={};longitude={};altitude={};date_begin={};date_end={};time_ref={};summarization={};username={}".format(
                latitude,
                longitude,
                altitude,
                dt.datetime.strftime(date_begin, "%Y-%m-%d %H:%M:%S")[:10],
                dt.datetime.strftime(date_end, "%Y-%m-%d %H:%M:%S")[:10],
                time_ref,
                summarization,
                username,
            ),
            "RawDataOutput": "irradiation",
        }
        response = requests.get(server, params=params)
        if response.status_code != 200:
            raise Exception
        return self._parse_request(response)

    def request(
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
        try:
            response = self.request_server(
                SODA_SERVER_SERVICE,
                username,
                latitude,
                longitude,
                date_begin,
                date_end,
                altitude,
                time_ref,
                summarization,
            )
        except:
            response = self.request_server(
                SODA_SERVER_MIRROR_SERVICE,
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

    def _parse_request(self, response):
        """Parse the request output into a pandas DataFrame.
        :param response: api response
        :return: dataframe
        """

        data = io.StringIO(response.text.split("#")[-1])
        df = pd.read_csv(data, delimiter=";")
        return df
