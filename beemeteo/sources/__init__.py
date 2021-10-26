from abc import abstractmethod


class Source:
    @abstractmethod
    def get_data(self, latitude, longitude, timezone, day):
        pass
