import datetime
import logging

from io import StringIO

import pandas as pd
import requests

from beemeteo.sources import Source
from beemeteo.sources import _dt_to_ts


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MeteoGalicia(Source):
    def __init__(self, config):
        super(MeteoGalicia, self).__init__(config)

    def _get_data_day(self, latitude, longitude, timezone, day):
        """
        Gets solar radiation information for a location on a given day

        :param latitude: station's latitude
        :param longitude: station's longitude
        :param timezone: station's timezone
        :param day: day to retrieve data from
        :return: all raw data for a given day
        """
        run = 0
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
                        "Location out of the bounding box, "
                        "trying with another resolution..."
                        "(Actual: " + str(resolution) + "km)"
                    )
                else:
                    solar_data = solar_data.rename(
                        columns={"date": "time", 'swflx[unit="W m-2"]': "GHI"}
                    )
                    solar_data["ts"] = _dt_to_ts(
                        pd.to_datetime(solar_data["time"]).dt.tz_convert(
                            timezone
                        ),
                        timezone,
                    )
                    solar_data = solar_data[["ts", "GHI"]]
                    solar_data = solar_data.reset_index(drop=True)

                    return solar_data
            except Exception as e:
                logger.error(e)
        return pd.DataFrame({})
