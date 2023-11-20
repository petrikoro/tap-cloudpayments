"""Stream type classes for tap-cloudpayments."""

from __future__ import annotations

from pathlib import Path
from tap_cloudpayments.client import CloudPaymentsStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class PaymentsStream(CloudPaymentsStream):
    """Stream for CloudPayments transactions"""

    name = "payments"
    rest_method = "POST"
    path = "/payments/list"
    records_jsonpath = "$.Model[*]"

    primary_keys = ["TransactionId"]
    replication_key = "CreatedDateIso"
    is_sorted = False

    schema_filepath = SCHEMAS_DIR / "payments.json"
