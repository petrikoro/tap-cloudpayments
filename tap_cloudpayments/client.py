"""REST client handling, including CloudPaymentsStream base class."""

from __future__ import annotations

import backoff
import pendulum
from typing import Any, Callable, Iterable, Generator

import requests
from memoization import cached
from singer_sdk import metrics
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import BasicAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_cloudpayments.helpers import get_date_range
from tap_cloudpayments.pagination import EmptyPageNumberPaginator


_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]


class CloudPaymentsStream(RESTStream):
    """CloudPayments stream class."""

    def backoff_wait_generator(self) -> Generator[float, None, None]:
        """
        The wait generator used by the backoff decorator on request failure.

        RETURNS:
            The wait generator
        """
        
        return backoff.constant(120)

    def backoff_max_tries(self) -> int:
        """
        The number of attempts before giving up when retrying requests.

        RETURNS:
            Number of max retries.
        """
        
        return 30
    
    def get_new_paginator(self):
        """
        Get a fresh paginator for this API endpoint.

        RETURNS:
            A paginator instance.
        """
        
        return EmptyPageNumberPaginator(start_value=1, 
                                        records_jsonpath=self.records_jsonpath)
    
    @property
    def timeout(self) -> int:
        """
        Return the request timeout limit in seconds.

        The default timeout is 300 seconds, or as defined by DEFAULT_REQUEST_TIMEOUT.

        RETURNS:
            The request timeout limit as number of seconds.
        """

        return 300

    @property
    def url_base(self) -> str:
        """Return the base url, e.g. https://api.mysite.com/v3/."""

        return "https://api.cloudpayments.ru/v2"

    @property
    @cached
    def authenticator(self) -> BasicAuthenticator:
        """
        Return a new authenticator object.

        Returns:
            An authenticator instance.
        """

        return BasicAuthenticator.create_for_stream(
            self,
            username=self.config.get("public_id", ""),
            password=self.config.get("api_secret", ""),
        )

    @property
    def http_headers(self) -> dict:
        """
        Return headers dict to be used for HTTP requests.

        If an authenticator is also specified, the authenticator's headers will be combined with http_headers when making HTTP requests.

        RETURNS:
            Dictionary of HTTP headers to use as a base for every request.
        """

        headers: dict = {}

        headers["Content-type"] = 'application/json'

        return headers

    @property
    def requests_session(self) -> requests.Session:
        """
        Get requests session.

        RETURNS:
            The requests.Session object for HTTP requests.
        """

        if not self._requests_session:
            self._requests_session = requests.Session()
            self._requests_session.stream = True

        return self._requests_session


    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict | None:
        """
        Construct and return request body for HTTP request.

        Args:
            context: Stream context.
            next_page_token: Pagination token to retrieve next page.

        Returns:
            Dictionary to pass as JSON body in the HTTP request.
        """

        payload: dict = {}
        
        payload["PageNumber"] = next_page_token['pagination_token']
        payload["CreatedDateGte"] = str(next_page_token['date_token'])
        payload["CreatedDateLte"] = str(next_page_token['date_token'] + pendulum.duration(hours=24))
        payload["TimeZone"] = "UTC"

        return payload
    
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """

        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
    
    def request_records(self, context: dict | None) -> Iterable[dict]:
        """
        Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """

        period = get_date_range(self.get_starting_timestamp(context), self.config.get('start_date'))

        with metrics.http_request_counter(self.name, self.path) as request_counter:
            request_counter.context = context
            
            for dt in period:

                paginator = self.get_new_paginator()
                decorated_request = self.request_decorator(self._request)

                while not paginator.finished:
                    prepared_request = self.prepare_request(
                        context,
                        next_page_token={"pagination_token": paginator.current_value,
                                            "date_token": dt},
                    )
                    resp = decorated_request(prepared_request, context)
                    request_counter.increment()
                    self.update_sync_costs(prepared_request, resp, context)

                    yield from self.parse_response(resp)

                    self.finalize_state_progress_markers
                    self._write_state_message()

                    paginator.advance(resp)
