"""
Serialization Utilities

JSON serialization/deserialization for Flink events.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class OrderEvent:
    """
    Order event data class for Flink processing.
    
    Matches the schema from producer/schemas/order_created_v1.py
    """
    event_id: str
    event_time: datetime
    order_id: str
    user_id: str
    order_amount: Decimal
    currency: str
    order_status: str
    processed_at: Optional[datetime] = None

    @classmethod
    def from_json(cls, json_str: str) -> Optional["OrderEvent"]:
        """
        Parse an order event from JSON string.
        
        Args:
            json_str: JSON string representation
            
        Returns:
            OrderEvent or None if parsing fails
        """
        try:
            data = json.loads(json_str)
            return cls(
                event_id=data["event_id"],
                event_time=datetime.fromisoformat(data["event_time"].replace("Z", "+00:00")),
                order_id=data["order_id"],
                user_id=data["user_id"],
                order_amount=Decimal(data["order_amount"]),
                currency=data["currency"],
                order_status=data["order_status"],
            )
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Failed to parse order event: {e}")
            return None

    def to_json(self) -> str:
        """
        Serialize order event to JSON string.
        
        Returns:
            JSON string representation
        """
        return json.dumps({
            "event_id": self.event_id,
            "event_time": self.event_time.isoformat(),
            "order_id": self.order_id,
            "user_id": self.user_id,
            "order_amount": str(self.order_amount),
            "currency": self.currency,
            "order_status": self.order_status,
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
        })

    def with_processed_at(self, timestamp: Optional[datetime] = None) -> "OrderEvent":
        """
        Create a copy with processed_at timestamp set.
        
        Args:
            timestamp: Processing timestamp (defaults to now)
            
        Returns:
            New OrderEvent with processed_at set
        """
        return OrderEvent(
            event_id=self.event_id,
            event_time=self.event_time,
            order_id=self.order_id,
            user_id=self.user_id,
            order_amount=self.order_amount,
            currency=self.currency,
            order_status=self.order_status,
            processed_at=timestamp or datetime.utcnow(),
        )


class JsonDecoder:
    """JSON decoder for Flink deserialization schema"""

    @staticmethod
    def decode(data: bytes) -> Optional[dict]:
        """
        Decode JSON bytes to dictionary.
        
        Args:
            data: JSON bytes
            
        Returns:
            Dictionary or None if decoding fails
        """
        try:
            return json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Failed to decode JSON: {e}")
            return None


class JsonEncoder:
    """JSON encoder for Flink serialization schema"""

    @staticmethod
    def encode(data: dict) -> bytes:
        """
        Encode dictionary to JSON bytes.
        
        Args:
            data: Dictionary to encode
            
        Returns:
            JSON bytes
        """
        return json.dumps(data, default=str).encode("utf-8")
