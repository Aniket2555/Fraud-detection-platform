"""
Unit tests for the event mapper.
"""
import pytest
import json
from datetime import datetime, timezone
from jsonschema import validate
from event_mapper import map_kaggle_row_to_event

with open("schemas/transaction_event_v1.json") as f:
    EVENT_SCHEMA = json.load(f)

TIME_ANCHOR = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _make_kaggle_row(time=0, amount=100.0, fraud=0, **kwargs):
    row = {"Time": time, "Amount": amount, "Class": fraud}
    for i in range(1, 29):
        row[f"V{i}"] = kwargs.get(f"V{i}", float(i) * 0.1)
    return row


class TestEventMapper:
    def test_schema_compliance(self):
        row = _make_kaggle_row()
        event = map_kaggle_row_to_event(row, 0, TIME_ANCHOR)
        validate(instance=event, schema=EVENT_SCHEMA)

    def test_deterministic_ids(self):
        row = _make_kaggle_row()
        event1 = map_kaggle_row_to_event(row, 0, TIME_ANCHOR)
        event2 = map_kaggle_row_to_event(row, 0, TIME_ANCHOR)
        assert event1["customer_id"] == event2["customer_id"]
        assert event1["card_id"] == event2["card_id"]
        assert event1["merchant_id"] == event2["merchant_id"]

    def test_unique_transaction_ids(self):
        row = _make_kaggle_row()
        event1 = map_kaggle_row_to_event(row, 0, TIME_ANCHOR)
        event2 = map_kaggle_row_to_event(row, 0, TIME_ANCHOR)
        assert event1["transaction_id"] != event2["transaction_id"]

    def test_fraud_label_mapping(self):
        fraud_row = _make_kaggle_row(fraud=1)
        legit_row = _make_kaggle_row(fraud=0)
        assert map_kaggle_row_to_event(fraud_row, 0, TIME_ANCHOR)["is_fraud"] is True
        assert map_kaggle_row_to_event(legit_row, 1, TIME_ANCHOR)["is_fraud"] is False
