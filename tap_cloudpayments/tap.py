"""CloudPayments tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_cloudpayments import streams


class TapCloudPayments(Tap):
    """CloudPayments tap class."""

    name = "tap-cloudpayments"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "public_id",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "api_secret",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The secret to authenticate against the API service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="The earliest record date to sync",
        ),
        th.Property(
            "time_zone",
            th.StringType,
            default="UTC",
            description="Time zone code for CreatedDateIso",
        ),
    ).to_dict()


    def discover_streams(self) -> list[streams.CloudPaymentsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.PaymentsStream(self),
        ]


if __name__ == "__main__":
    TapCloudPayments.cli()
