from abc import abstractmethod

import pytz


def to_tz(ts, timezone):
    return (
        ts.astimezone(pytz.UTC)
        if ts.tzinfo is not None
        else timezone.localize(ts).astimezone(pytz.UTC)
    )


class Source:
    @abstractmethod
    def get_data(self, latitude, longitude, timezone, day):
        pass
