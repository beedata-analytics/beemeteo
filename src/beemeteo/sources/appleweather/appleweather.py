import requests
import json
import datetime
import pytz
import pandas as pd
# from beemeteo.utils import _datetime_to_tz

# TODO:
# token is retrieved from a GREATER file in the __init__ function (see darksky)
# Delete _datetime_to_tz, uncomment import
WEATHER = 'https://weatherkit.apple.com/api/v1/weather/en_US/'

"""
def _datetime_to_tz(dt, timezone):
    return (
        dt.astimezone(timezone)
        if dt.tzinfo is not None
        else timezone.localize(dt)
    )
"""
def _datetime_to_api(date):
    # day_aux.strftime("%Y-%m-%dT00:00:00Z") 
    return date.strftime("%Y-%m-%dT%H:%M:%SZ") 

def _api_to_datetime(date_str):
    return datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")

"""
def __init__(self, config):
    super(AppleWeather, self).__init__(config)
    self.api_key = self.config["AppleWeather"]["Token"]

def _get_historical_data():
    raise NotImplementedError("AppleWeather can't get historical data")
    # https://developer.apple.com/forums/thread/708727

def _collect_forecasting(latitude, longitude, now, local_tz):
    # Dates are ignored
    df = _request_server(latitude, longitude, "currentWeather", )
    forecasted_data = 
    return df
"""

def _get_forecasting_data():
    # returned in UTC
    # https://developer.apple.com/forums/thread/722722
    datetime.datetime.fromisoformat("2023-02-02"), 
    
    return df

def _request_server(
        lat, 
        long, 
        service,
        day_from,
        day_to
        ):
  
    # TODO: Delete until COMMON once it's included in the main code
    day_from = datetime.datetime.fromisoformat(day_from)
    day_to = datetime.datetime.fromisoformat(day_to) 
    
    ## COMMON ##
    # From given to UTC (functions from utils of beemeteo)
    # day_from = _datetime_to_tz(day_from, datetime.timezone.utc) 
    # day_to = _datetime_to_tz(day_to, datetime.timezone.utc)
    # TODO: this converts from our machine's local time zone
    day_from = day_from.astimezone(datetime.timezone.utc)
    day_to = day_to.astimezone(datetime.timezone.utc)

    if(day_from < datetime.datetime.fromisoformat('2021-08-01').astimezone(datetime.timezone.utc)):
        raise NotImplementedError("Apple Weather Kit's historical data is currently available back to Aug 1, 2021.")
    
    url = WEATHER + str(lat) +'/'+ str(long)
    config = json.load(open('config.json'))
    headers = {'Authorization': 'Bearer {}'.format(config['TOKEN'])}
    
    payload = {
            "dataSets" : service,
            "hourlyStart" : [], # If this parameter is absent, hourly forecasts start on the current hour
            "hourlyEnd" : [], # If this parameter is absent, hourly forecasts run 24 hours or the length of the daily forecast, whichever is longer
            "timezone": "Europe/Madrid", # API only uses this to determine day boundaries for daily forecasts
            }

    data_list = []
    if service=="currentWeather":
        day_aux = day_to
    else: 
        day_aux = day_from
    day_to += datetime.timedelta(days=1) # To include up to 23:59 we must set the end date to the follwing day, 00:00
    
    while((day_to - day_aux).days > 9): # Send requests of max 10 days
        payload["hourlyStart"] = _datetime_to_api(day_aux) 
        day_aux += datetime.timedelta(days=10)
        payload["hourlyEnd"] = _datetime_to_api(day_aux) 
        response = requests.get(url, headers = headers, params = payload)
        if response.status_code != 200:
            raise Exception
        
        data_list += _parse_request(response.text) # TODO: it's self._parse_request once we incorporate this code
  
    if((day_to - day_aux).days >= 1):
        payload["hourlyStart"] = _datetime_to_api(day_aux)
        payload["hourlyEnd"] = _datetime_to_api(day_to)
        response = requests.get(url, headers = headers, params = payload)
        if response.status_code != 200:
            raise Exception
       
        data_list += _parse_request(response.text) # TODO: it's self._parse_request once we incorporate this code

    # transform date to dateime object and to local tz
    for d in data_list:
        d.update((k, _api_to_datetime(v)) for k, v in d.items() if (k == 'forecastStart' or k == 'asOf'))
        # TODO: convert from utc to local

    return pd.DataFrame.from_records(data_list)

@staticmethod
def _parse_request(response):
    """
    Parse the request output into a pandas DataFrame.
    :param response: api response in txt format
    :return: python list
    """

    data = json.loads(response)
    service = next(iter(data))
    data = data[service]
    del data['name'], data['metadata']
    return(
            data['hours']
            if(service != 'currentWeather')
            else [data]
            )

#### MAIN
# Dallas, TX
lat = 32.779167
long = -96.808891
# Bcn
lat = 41.390205
long = 2.154007
# now = 
# local_tz = 
r = _request_server(lat, long, 'forecastHourly', '2023-06-01', '2023-06-12') 
print(r)
