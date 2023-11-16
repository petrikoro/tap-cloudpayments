import pendulum
from typing import Iterable, tim


def get_date_range(start: pendulum.DateTime, end: pendulum.DateTime) -> Iterable[list]:
        """
        Get a list of dates between a start_date and the current datetime.

        RETURNS:
           Tuple of datetimes.
        """

        period = list()
        
        if str(start) == str(end):

            for dt in pendulum.period(start, pendulum.now('UTC')):
                period.append(dt)
        else:
            period.append(start)
        
        return period