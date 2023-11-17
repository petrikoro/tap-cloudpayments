"""REST client handling, including CloudPaymentsStream base class."""

from __future__ import annotations

import backoff
import pendulum
from typing import Any, Callable, Iterable, Generator

import requests
from memoization import cached
from singer_sdk import metrics
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError
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
        payload["CreatedDateGte"] = pendulum.parse(next_page_token['date_token'], tz=self.config.get('time_zone')).to_w3c_string()
        payload["CreatedDateLte"] = (pendulum.parse(next_page_token['date_token'], tz=self.config.get('time_zone')) + pendulum.duration(hours=24)).to_w3c_string()
        payload["TimeZone"] = self.config.get('time_zone')

        return payload
    
    def request_records(self, context: dict | None) -> Iterable[dict]:
        """
        Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """
        date_range = get_date_range(
            starting_replication_ts=pendulum.parse(self.config.get('start_date'), tz=self.config.get('time_zone')), 
            replication_key_ts=self.get_starting_timestamp(context),
            time_zone=self.config.get('time_zone')
            )

        with metrics.http_request_counter(self.name, self.path) as request_counter:
            request_counter.context = context
            
            for dt in date_range:
                
                paginator = self.get_new_paginator()
                decorated_request = self.request_decorator(self._request)

                while not paginator.finished:
                    prepared_request = self.prepare_request(
                        context,
                        next_page_token={
                            "pagination_token": paginator.current_value,
                            "date_token": dt
                            },
                    )

                    resp = decorated_request(prepared_request, context)
                    request_counter.increment()
                    self.update_sync_costs(prepared_request, resp, context)

                    yield from self.parse_response(resp)

                    self.finalize_state_progress_markers
                    self._write_state_message()

                    paginator.advance(resp)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """

        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def validate_response(self, response: requests.Response) -> None:
        """
        Validate HTTP response.

        Checks for error status codes and whether they are fatal or retriable.
        In case an error is deemed transient and can be safely retried, then this method should raise an singer_sdk.exceptions.RetriableAPIError. By default this applies to 5xx error codes, along with values set in: extra_retry_statuses
        In case an error is unrecoverable raises a singer_sdk.exceptions.FatalAPIError. By default, this applies to 4xx errors, excluding values found in: extra_retry_statuses
        Tap developers are encouraged to override this method if their APIs use HTTP status codes in non-conventional ways, or if they communicate errors differently (e.g. in the response body).

        PARAMETERS:
            response – A requests.Response object.

        RAISES:
            FatalAPIError – If the request is not retriable.

            RetriableAPIError – If the request is retriable.
        """
        try:
            super().validate_response(response)
        except FatalAPIError as fatal_error:
            if response.json().get('Success') is False:
                raise fatal_error
