# beemeteo

[![CI](https://github.com/beedata-analytics/beemeteo/actions/workflows/main.yml/badge.svg)](https://github.com/beedata-analytics/beemeteo/actions/workflows/main.yml)
[![Python version](https://img.shields.io/badge/python-2.7-blue)](https://img.shields.io/badge/python-2.7-blue)
[![Python version](https://img.shields.io/badge/python-3.6-blue)](https://img.shields.io/badge/python-3.6-blue)

A python package and command line tool to download weather data from different sources

## Sources

- [CAMS Radiation Service](http://www.soda-pro.com/web-services/radiation/cams-radiation-service/info)
- [Dark Sky API](https://darksky.net/dev)
- [MeteoGalicia](http://mandeo.meteogalicia.es)

## Package installation

```bash
make install
```

## For developers

```bash
make build
```

## Run tests

```bash
make test
```

## Usage

### command line

```bash
$ python -m beemeteo --help

Usage: python -m beemeteo [OPTIONS]

  Gets raw data from source

Options:
  --name TEXT             Raw data source name
  --filename FILENAME     Configuration filename
  --latitude FLOAT        Station's latitude
  --longitude FLOAT       Station's longitude
  --timezone TEXT         Station's timezone
  --date-from [%Y-%m-%d]  Start date
  --date-to [%Y-%m-%d]    End date
  --hbase-table TEXT      Source HBase table name for raw data
  --help                  Show this message and exit

Example: python -m beemeteo \
--name darksky \
--filename config.json \
--latitude 41.29 \
--longitude 2.19 \
--timezone Europe/Madrid \
--date-from 2021-09-01 \
--date-to 2021-09-05
```

### python package

#### cams

```python
import datetime
import os

import pytz

from beemeteo.sources.cams import CAMS

username = os.environ["SODA_USERNAME"]
source = CAMS({"cams": {"cams-registered-mails": [username]}})
source.get_data(
    41.29,
    2.19,
    pytz.timezone("Europe/Madrid"),
    datetime.datetime(2021, 9, 1),
    datetime.datetime(2021, 9, 5),
)
```

#### darksky

```python
import datetime
import os

import pytz

from beemeteo.sources.darksky import DarkSky

api_key = os.environ["DARKSKY_API_KEY"]
source = DarkSky({"darksky": {"api-key": api_key}})
source.get_data(
    41.29,
    2.19,
    pytz.timezone("Europe/Madrid"),
    datetime.datetime(2021, 9, 1),
    datetime.datetime(2021, 9, 5),
)
```

#### meteogalicia

```python
import datetime

import pytz

from beemeteo.sources.meteogalicia import MeteoGalicia

source = MeteoGalicia({})
source.get_data(
    41.29,
    2.19,
    pytz.timezone("Europe/Madrid"),
    datetime.datetime(2021, 9, 1),
    datetime.datetime(2021, 9, 5),
)
```
