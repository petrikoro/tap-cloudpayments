"""CloudPayments pagination handling."""

import requests
import pendulum
import typing as t

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator


class DateOffsetPaginator(BaseAPIPaginator):
    def __init__(
        self,
        start_value: pendulum.DateTime,
        offset_days: int,
        timezone: str,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        """Create a new paginator.

        Args:
            start_value: Initial value.
            offset_days: Constant days number.
            args: Paginator positional arguments.
            kwargs: Paginator keyword arguments.
        """
        super().__init__(start_value, *args, **kwargs)
        self._offset_days = offset_days
        self._timezone = timezone

    def has_more(self, response: requests.Response) -> bool:
        """Override this method to check if the endpoint has any pages left.

        Args:
            response: API response object.

        Returns:
            Boolean flag used to indicate if the endpoint has more pages.
        """
        return self.current_value < pendulum.today(tz=self._timezone)

    def get_next(self, response: requests.Response) -> int | None:  # noqa: ARG002
        """Get the next page offset.

        Args:
            response: API response object.

        Returns:
            The next page offset.
        """
        return self._value + pendulum.duration(days=self._offset_days)
