from datetime import datetime
from io import StringIO

import pandas as pd
import pytz
import requests

from beemeteo.sources import Source


class MeteoGalicia(Source):
    def __init__(self, config):
        super(MeteoGalicia, self).__init__(config)

    def get_data(self, latitude, longitude, timezone, day, run=0, ndays_forecasted=1):
        for resolution in [(4, 2), (12, 2), (12, 1), (36, 2), (36, 1)]:
            try:
                # meteogalicia stores 14 days of operational forecasts
                # After 14 days the forecasts are moved to the WRF_HIST folder
                # http://mandeo.meteogalicia.es/thredds/ncss/grid//wrf_2d_04km/fmrc/files/20180423/wrf_arw_det_history_d02_20180423_0000.nc4?var=swflx&point=true&accept=csv&longitude=0.62&latitude=41.62&temporal=all
                if (datetime.utcnow() - day).days <= 14:
                    url_mg = (
                        "http://mandeo.meteogalicia.es/thredds/ncss/grid/wrf_2d_%02ikm/fmrc/files/%s/wrf_arw_det_"
                        "history_d0%s_%s_%02i00.nc4?var=swflx&point=true&accept=csv&longitude=%s&latitude=%s"
                        "&temporal=all"
                        % (
                            resolution[0],
                            datetime.strftime(day, "%Y%m%d"),
                            resolution[1],
                            datetime.strftime(day, "%Y%m%d"),
                            run,
                            longitude,
                            latitude,
                        )
                    )
                else:
                    # http://mandeo.meteogalicia.es/thredds/ncss/grid/modelos/WRF_HIST/d02/2018/01/wrf_arw_det_history_d02_20180122_0000.nc4?var=swflx&point=true&accept=csv&longitude=41.62&latitude=0.62&temporal=all
                    # Historical forecasts. Only run 00 is available
                    url_mg = "http://mandeo.meteogalicia.es/thredds/ncss/grid/modelos/WRF_HIST/d0%s/%s/%s/wrf_arw_det_history_d0%s_" "%s_0000.nc4?var=swflx&point=true&accept=csv&longitude=%s&latitude=%s&temporal=all" % (
                        resolution[1],
                        datetime.strftime(day, "%Y"),
                        datetime.strftime(day, "%m"),
                        resolution[1],
                        datetime.strftime(day, "%Y%m%d"),
                        longitude,
                        latitude,
                    )
                r = requests.get(url_mg)
                solar_data = pd.read_csv(StringIO(r.text), sep=",")
                if len(solar_data) == 0:
                    raise Exception(
                        "Location out of the bounding box, trying with another resolution..."
                        "(Actual: " + str(resolution) + "km)"
                    )
                else:
                    for colname in [solar_data.columns[1], solar_data.columns[2]]:
                        del solar_data[colname]
                    solar_data = solar_data.rename(
                        columns={"date": "time", 'swflx[unit="W m-2"]': "GHI"}
                    )
                    solar_data["time"] = [
                        pytz.UTC.localize(datetime.strptime(i, "%Y-%m-%dT%H:%M:%SZ"))
                        for i in solar_data.time
                    ]
                    solar_data = pd.concat(
                        [
                            pd.DataFrame(
                                [
                                    {
                                        "time": pytz.UTC.localize(
                                            datetime.strptime(
                                                "%sT00:00:00Z" % day,
                                                "%Y-%m-%dT%H:%M:%SZ",
                                            )
                                        ),
                                        "GHI": 0.0,
                                    }
                                ]
                            ),
                            solar_data,
                        ]
                    )
                    solar_data = solar_data.reset_index(drop=True)
                    return solar_data[: (ndays_forecasted * 24)]
            except Exception as e:
                print(e)
