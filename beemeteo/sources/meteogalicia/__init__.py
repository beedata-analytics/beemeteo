import datetime
import logging

from io import StringIO

import pandas as pd
import pytz
import requests

from beemeteo.sources import Source


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MeteoGalicia(Source):
    def __init__(self, config):
        super(MeteoGalicia, self).__init__(config)

    def _get_data(self, latitude, longitude, timezone, date_from, date_to, hbase_table):
        data = None
        days = pd.date_range(date_from, date_to - datetime.timedelta(days=1), freq="d")
        for day in days:
            daily_data = self._get_from_hbase(day, hbase_table)
            if len(daily_data) < 24:
                daily_data = self._get_data_day(latitude, longitude, timezone, day)
                data = (
                    pd.merge(data, daily_data, how="outer")
                    if data is not None
                    else daily_data
                )
        return data

    def _get_data_day(self, latitude, longitude, timezone, day):
        run = 0
        days_forecasted = 1
        for resolution in [(4, 2), (12, 2), (12, 1), (36, 2), (36, 1)]:
            try:
                # Last 14 days of operational forecasts
                # http://mandeo.meteogalicia.es/
                # thredds/
                # ncss/
                # grid/
                # wrf_2d_04km/
                # fmrc/
                # files/
                # 20180423/
                # wrf_arw_det_history_d02_20180423_0000.nc4?
                # var=swflx&
                # point=true&
                # accept=csv&
                # longitude=0.62&
                # latitude=41.62&
                # temporal=all
                if (datetime.datetime.utcnow() - day).days <= 14:
                    url_mg = (
                        "http://mandeo.meteogalicia.es/"
                        "thredds/"
                        "ncss/"
                        "grid/"
                        "wrf_2d_%02ikm/"
                        "fmrc/"
                        "files/"
                        "%s/"
                        "wrf_arw_det_history_d0%s_%s_%02i00.nc4?"
                        "var=swflx&"
                        "point=true&"
                        "accept=csv&"
                        "longitude=%s&"
                        "latitude=%s&"
                        "temporal=all"
                        % (
                            resolution[0],
                            datetime.datetime.strftime(day, "%Y%m%d"),
                            resolution[1],
                            datetime.datetime.strftime(day, "%Y%m%d"),
                            run,
                            longitude,
                            latitude,
                        )
                    )
                else:
                    # Historical forecasts. Only run 00 is available
                    # http://mandeo.meteogalicia.es/
                    # thredds/
                    # ncss/
                    # grid/
                    # modelos/
                    # WRF_HIST/
                    # d02/
                    # 2018/
                    # 01/
                    # wrf_arw_det_history_d02_20180122_0000.nc4?
                    # var=swflx&
                    # point=true&
                    # accept=csv&
                    # longitude=41.62&
                    # latitude=0.62&
                    # temporal=all
                    url_mg = (
                        "http://mandeo.meteogalicia.es/"
                        "thredds/"
                        "ncss/"
                        "grid/"
                        "modelos/"
                        "WRF_HIST/"
                        "d0%s"
                        "/%s"
                        "/%s"
                        "/wrf_arw_det_history_d0%s_%s_0000.nc4?"
                        "var=swflx&"
                        "point=true&"
                        "accept=csv&"
                        "longitude=%s&"
                        "latitude=%s&"
                        "temporal=all"
                        % (
                            resolution[1],
                            datetime.datetime.strftime(day, "%Y"),
                            datetime.datetime.strftime(day, "%m"),
                            resolution[1],
                            datetime.datetime.strftime(day, "%Y%m%d"),
                            longitude,
                            latitude,
                        )
                    )
                r = requests.get(url_mg)
                solar_data = pd.read_csv(StringIO(r.text), sep=",")
                if len(solar_data) == 0:
                    raise Exception(
                        "Location out of the bounding box, trying with another resolution..."
                        "(Actual: " + str(resolution) + "km)"
                    )
                else:
                    solar_data = solar_data.rename(
                        columns={"date": "time", 'swflx[unit="W m-2"]': "GHI"}
                    )
                    solar_data["time"] = pd.to_datetime(
                        solar_data["time"]
                    ).dt.tz_convert(pytz.UTC)
                    solar_data = solar_data[["time", "GHI"]]
                    solar_data = solar_data.reset_index(drop=True)
                    return solar_data[: (days_forecasted * 24)]
            except Exception as e:
                logger.error(e)
        return pd.DataFrame({})
