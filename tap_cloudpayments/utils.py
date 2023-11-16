import pendulum
from typing import Iterable

def date_range(self, context: dict | None) -> Iterable[tuple]:
        """
        Get a list of dates between a start_date and the current datetime.

        RETURNS:
           Tuple of datetimes.
        """

        period = []
        
        if str(self.get_starting_timestamp(context)) == str(self.config.get('start_date')):
            for dt in pendulum.period(self.get_starting_timestamp(context), pendulum.now('UTC')):
                period.append(dt)
            return period
        
        return period.append(self.get_starting_timestamp(context))