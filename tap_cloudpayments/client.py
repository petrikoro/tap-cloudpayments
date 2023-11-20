"""REST client handling, including CloudPaymentsStream base class."""

from __future__ import annotations

import backoff
import pendulum
from http import HTTPStatus
from typing import Any, Iterable, Generator

import requests
from memoization import cached
from singer_sdk import metrics
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.authenticators import BasicAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_cloudpayments.pagination import DateOffsetPaginator


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

        return DateOffsetPaginator(
            start_value=self._starting_timestamp,
            offset_days=1,
            timezone=self._timezone,
        )

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

        return "https://api.cloudpayments.ru"

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

        headers["Content-type"] = "application/json"

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

        payload["Date"] = pendulum.instance(next_page_token).to_date_string()
        payload["TimeZone"] = self.config.get("timezone")

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
        # Set custom properties to the stream.
        self._starting_timestamp = self.get_starting_timestamp(context)
        self._timezone = self.config.get("timezone")

        self.logger.info(msg=f"_starting_timestamp: {self.get_starting_timestamp(context)}")
        self.logger.info(msg=f"get_context_state: {self.get_context_state(context)}")
        self.logger.info(msg=f"get_replication_key_signpost: {self.get_replication_key_signpost(context)}")
        self.logger.info(msg=f"get_starting_replication_key_value: {self.get_starting_replication_key_value(context)}")

        with metrics.http_request_counter(self.name, self.path) as request_counter:
            request_counter.context = context
            paginator = self.get_new_paginator()

            while not paginator.finished:
                decorated_request = self.request_decorator(self._request)
                prepared_request = self.prepare_request(
                    context, next_page_token=paginator.current_value
                )

                resp = decorated_request(prepared_request, context)
                request_counter.increment()
                self.update_sync_costs(prepared_request, resp, context)

                yield from self.parse_response(resp)

                self.finalize_state_progress_markers()
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

        if (
            response.status_code in self.extra_retry_statuses
            or HTTPStatus.INTERNAL_SERVER_ERROR
            <= response.status_code
            <= max(HTTPStatus)
        ):
            msg = self.response_error_message(response)
            raise FatalAPIError(msg, response)

        if (
            HTTPStatus.BAD_REQUEST
            <= response.status_code
            < HTTPStatus.INTERNAL_SERVER_ERROR
        ):
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

        if (response.json()).get("Success", False) is False:
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)
