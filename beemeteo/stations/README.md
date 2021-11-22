# beemeteo.stations

Command line tool to get the closest station to a given postal code.

## Usage

### command line

```console
$ python -m beemeteo.stations --help

Usage: python -m beemeteo.stations [OPTIONS]

  Gets the closest station for a given postal code.

Options:
  --stations FILENAME
  --postal-code TEXT   Postal code
  --country TEXT       Country
  --help               Show this message and exit.

Example: python -m beemeteo.stations \
--stations stations.txt \
--postal-code 08221 \
--country ES
```

### python package

```python

from beemeteo.stations.postal_code import PostalCode
from beemeteo.stations.stations import Stations

postal_code = PostalCode("ES", "08211")
(latitude, longitude, distance) = postal_code.find_closest(
    Stations.load("stations.txt")
)
```
