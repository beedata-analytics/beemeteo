import numpy as np


class Coordinates:
    def __init__(self, latitude, longitude):
        self.latitude = latitude
        self.longitude = longitude

    def __str__(self):
        return "({latitude}, {longitude})".format(
            latitude=self.latitude,
            longitude=self.longitude,
        )

    def find_closest(self, stations):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)

        All args must be of equal length.
        """
        lon1 = np.radians(self.longitude)
        lat1 = np.radians(self.latitude)
        lon2 = np.radians(stations.longitude)
        lat2 = np.radians(stations.latitude)

        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = (
            np.sin(dlat / 2.0) ** 2
            + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
        )

        c = 2 * np.arcsin(np.sqrt(a))
        km = 6367 * c
        index = range(km.shape[0])
        return [
            (stations.latitude[i], stations.longitude[i], k)
            for i, k in sorted(zip(index, km), key=lambda x: x[1])
        ]
