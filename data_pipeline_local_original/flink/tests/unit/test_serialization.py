"""
Unit tests for serialization utilities
"""

import json
import pytest
from datetime import datetime, timezone
from decimal import Decimal

import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from flink.common.serialization import OrderEvent, JsonDecoder, JsonEncoder


class TestOrderEvent:
    """Tests for OrderEvent data class"""

    def test_from_json_valid(self):
        """Test parsing valid JSON"""
        json_str = json.dumps({
            "event_id": "123e4567-e89b-12d3-a456-426614174000",
            "event_time": "2024-01-15T10:30:00+00:00",
            "order_id": "ORD-12345678",
            "user_id": "USR-87654321",
            "order_amount": "150.50",
            "currency": "USD",
            "order_status": "CREATED"
        })

        event = OrderEvent.from_json(json_str)

        assert event is not None
        assert event.event_id == "123e4567-e89b-12d3-a456-426614174000"
        assert event.order_id == "ORD-12345678"
        assert event.user_id == "USR-87654321"
        assert event.order_amount == Decimal("150.50")
        assert event.currency == "USD"
        assert event.order_status == "CREATED"
        assert event.processed_at is None

    def test_from_json_invalid(self):
        """Test parsing invalid JSON returns None"""
        invalid_json = "not valid json"
        event = OrderEvent.from_json(invalid_json)
        assert event is None

    def test_from_json_missing_field(self):
        """Test parsing JSON with missing required field"""
        json_str = json.dumps({
            "event_id": "123e4567-e89b-12d3-a456-426614174000",
            # Missing event_time
            "order_id": "ORD-12345678",
        })

        event = OrderEvent.from_json(json_str)
        assert event is None

    def test_to_json(self):
        """Test serializing to JSON"""
        event = OrderEvent(
            event_id="123e4567-e89b-12d3-a456-426614174000",
            event_time=datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            order_id="ORD-12345678",
            user_id="USR-87654321",
            order_amount=Decimal("150.50"),
            currency="USD",
            order_status="CREATED"
        )

        json_str = event.to_json()
        parsed = json.loads(json_str)

        assert parsed["event_id"] == "123e4567-e89b-12d3-a456-426614174000"
        assert parsed["order_id"] == "ORD-12345678"
        assert parsed["order_amount"] == "150.50"
        assert parsed["processed_at"] is None

    def test_with_processed_at(self):
        """Test adding processed_at timestamp"""
        event = OrderEvent(
            event_id="test-id",
            event_time=datetime.now(timezone.utc),
            order_id="ORD-123",
            user_id="USR-456",
            order_amount=Decimal("100.00"),
            currency="USD",
            order_status="CREATED"
        )

        processed = event.with_processed_at()

        assert processed.processed_at is not None
        assert processed.event_id == event.event_id
        assert processed.order_id == event.order_id


class TestJsonDecoder:
    """Tests for JsonDecoder"""

    def test_decode_valid(self):
        """Test decoding valid JSON bytes"""
        data = b'{"key": "value", "number": 42}'
        result = JsonDecoder.decode(data)

        assert result is not None
        assert result["key"] == "value"
        assert result["number"] == 42

    def test_decode_invalid(self):
        """Test decoding invalid JSON returns None"""
        data = b'not valid json'
        result = JsonDecoder.decode(data)
        assert result is None


class TestJsonEncoder:
    """Tests for JsonEncoder"""

    def test_encode(self):
        """Test encoding dictionary to JSON bytes"""
        data = {"key": "value", "number": 42}
        result = JsonEncoder.encode(data)

        assert isinstance(result, bytes)
        parsed = json.loads(result.decode("utf-8"))
        assert parsed["key"] == "value"
        assert parsed["number"] == 42


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
