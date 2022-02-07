import pandas as pd

from src.beemeteo.stations.coordinates import Coordinates


class PostalCode:
    def __init__(self, country, postal_code):
        self.country = country
        self.postal_code = postal_code
        self.data = self._get()
        self.latitude = self.data["latitude"].values[0]
        self.longitude = self.data["longitude"].values[0]

    def __str__(self):
        return "{country} {postal_code}".format(
            country=self.country, postal_code=self.postal_code
        )

    def _get(self):
        data = pd.read_table(
            "beemeteo/stations/postalCode_geoLoc",
            sep="\t",
            names=[
                "postalCode",
                "latitude",
                "longitude",
                "altitude",
                "country",
                "UNK1",
                "UNK2",
                "UNK3",
                "UNK4",
                "UNK5",
                "UNK6",
            ],
            dtype={"postalCode": str, "country": str},
        )
        return data.query(
            "country == @self.country & postalCode == @self.postal_code"
        )

    def find_closest(self, stations):
        building = Coordinates(self.latitude, self.longitude)
        return building.find_closest(stations)[0]
