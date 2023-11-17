import pendulum
from typing import Iterable


def get_date_range(starting_replication_ts: pendulum.DateTime, 
                   replication_key_ts: pendulum.DateTime,
                   time_zone: str) -> Iterable[list]:
        """
        Get a list of dates between a start_timestamp/replication_timestamp and the current datetime.

        RETURNS:

           List of datetimes
        """
        date_range = []
        
        if starting_replication_ts == replication_key_ts:
            for dt in pendulum.period(starting_replication_ts, pendulum.now(time_zone)):
                date_range.append(dt.to_w3c_string())
        
        else:
             for dt in pendulum.period(replication_key_ts, pendulum.now(time_zone)):
                date_range.append(dt.to_w3c_string())

        return date_range
