import datetime
import io
import logging
import pandas as pd
import pytz
import requests
from beemeteo.sources import Source
from beemeteo.utils import _pandas_dt_to_ts_utc
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VERSION = "1.0.0"
SODA_SERVER_SERVICE = "http://www.soda-is.com/service/wps"
SODA_SERVER_MIRROR_SERVICE = "http://pro.soda-is.com/service/wps"


class CAMS(Source):
    hbase_table_historical = "cams_historical"
    hbase_table_forecasting = None

    def __init__(self, config):
        super(CAMS, self).__init__(config)
        self.cams_registered_mails = self.config["cams"][
            "registered_emails"
        ]
        assert len(self.cams_registered_mails) > 0

    def _collect_forecasting(self, latitude, longitude, now, tz_local):
        raise NotImplementedError("CAMS can't get forecasting")

    def get_forecasting_data(self, latitude, longitude, date_from, date_to):
        raise NotImplementedError("CAMS can't get forecasting")

    def _get_historical_data_source(self, latitude, longitude, gaps, local_tz):
        # In cams, we can download as many data as we want, but we have limited number of requests. So, if we detect
        # a gap we will try to download the full period again.
        # if the gaps are 1 year apart, we will download them by separate.
        gaps_df = pd.DataFrame(gaps)
        gaps_df.columns = ["ini", "end"]
        gaps_df.sort_values(by=["ini"], inplace=True)
        gaps_df['next_start'] = gaps_df['ini'].shift(-1)
        gaps_df['between_gaps'] = gaps_df.apply(lambda x: x["next_start"] - x['end'], axis=1)
        gaps_df['join'] = (gaps_df.between_gaps < datetime.timedelta(days=365)
                           if isinstance(gaps_df.between_gaps, bool) else False)
        new_gaps = []
        current_gap = [None, None]
        init = False
        # join the gaps that are less than 1 year apart
        for i, g in gaps_df.iterrows():
            if not init:
                current_gap[0] = g.ini
                init = True
            current_gap[1] = g.end
            if not g.join:
                new_gaps.append(tuple(current_gap))
                init = False
                current_gap = [None, None]

        missing_data = pd.DataFrame()
        for ts_ini, ts_end in new_gaps:
            # for each gap, get the date at instant 0 (we will always download the full day) the ts_end at 00,
            # is also downloaded at full
            ts_ini = local_tz.localize(datetime.datetime.combine(ts_ini.date(), datetime.datetime.min.time()))
            ts_end = local_tz.localize(datetime.datetime.combine(ts_end.date(), datetime.datetime.max.time()))
            logger.debug("No data for period {ts_ini} {ts_end}, downloading".format(ts_ini=ts_ini, ts_end=ts_end))
            data_period = self._get_historic_period(latitude, longitude, ts_ini, ts_end, local_tz)
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

    def _get_historic_period(self, latitude, longitude, date_begin, date_end, local_tz):
        """
        Gets solar radiation information for a location on a given day
        http://www.soda-pro.com/web-services/radiation/cams-radiation-service/info

        :param latitude: station's latitude
        :param longitude: station's longitude
        :param date_begin: day to retrieve data from
        :param date_end: day to retrieve data to
        :param local_tz: the timezone to return the timestamp
        :return: all raw data for a given day
        """
        date_begin = date_begin.astimezone(pytz.UTC)
        date_end = date_end.astimezone(pytz.UTC)
        for mail in self.cams_registered_mails:
            data = self._request(
                mail, latitude, longitude, date_begin, date_end
            )
            if data is not None:
                logger.info(
                    "[CAMS] %s retrieved info for %s to  %s" % (mail, date_begin, date_end)
                )
                data["ts"] = _pandas_dt_to_ts_utc(data["time"])
                data.drop(["time"], axis=1, inplace=True)
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
