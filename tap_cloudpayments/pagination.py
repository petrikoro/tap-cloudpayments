"""CloudPayments pagination handling."""

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BasePageNumberPaginator


class EmptyPageNumberPaginator(BasePageNumberPaginator):

    def __init__(self, 
                 start_value: int,
                 records_jsonpath: str | None, 
                 *args, 
                 **kwargs):
        super().__init__(1)
        self._start_value = start_value
        self._records_jsonpath = records_jsonpath

    def has_more(self, response: requests.Response):
        """
        Return True if there are more pages to fetch.

        Args:
            response: The most recent response object.
            jsonpath: An optional jsonpath to where the tokens are located in
                      the response, defaults to `hasMore` in the response.

        Returns:
            Whether there are more pages to fetch.

        """

        if self._records_jsonpath:
            return bool(len(list(extract_jsonpath(self._records_jsonpath, input=response.json()))))
        else:
            return bool(len(response.json()))
        
    def get_next(self, response: requests.Response) -> int | None:  # noqa: ARG002
        """
        Get the next page number.

        Args:
            response: API response object.

        Returns:
            The next page number.
        """

        return self._start_value + 1
