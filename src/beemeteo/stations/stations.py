import pandas as pd


class Stations:
    @staticmethod
    def load(filename):
        return pd.read_table(
            filename,
            names=[
                "latitude",
                "longitude",
            ],
        )
