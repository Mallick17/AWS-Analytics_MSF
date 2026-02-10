"""
Order Created Event Schema (v1)

This schema defines the structure of order creation events.
It is versioned and must remain backward compatible.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class OrderStatus(str, Enum):
    """Valid order statuses for creation events"""
    CREATED = "CREATED"


class OrderCreatedV1(BaseModel):
    """
    Order Creation Event Schema (Version 1)
    
    Represents a successful order placement by a user.
    
    Attributes:
        event_id: Unique identifier for this event
        event_time: ISO-8601 timestamp when the event occurred (event-time for Flink)
        order_id: Unique identifier for the order
        user_id: Identifier of the user who placed the order
        order_amount: Total order amount as decimal
        currency: Currency code (e.g., USD, EUR)
        order_status: Status of the order (always CREATED for this event type)
    """
    
    event_id: UUID = Field(
        ...,
        description="Unique identifier for this event"
    )
    event_time: datetime = Field(
        ...,
        description="ISO-8601 timestamp when the event occurred"
    )
    order_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="Unique identifier for the order"
    )
    user_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="Identifier of the user who placed the order"
    )
    order_amount: Decimal = Field(
        ...,
        gt=0,
        decimal_places=2,
        description="Total order amount"
    )
    currency: str = Field(
        ...,
        min_length=3,
        max_length=3,
        description="ISO 4217 currency code"
    )
    order_status: OrderStatus = Field(
        default=OrderStatus.CREATED,
        description="Status of the order"
    )

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        """Ensure currency is uppercase"""
        return v.upper()

    @field_validator("order_amount", mode="before")
    @classmethod
    def convert_amount(cls, v):
        """Convert amount to Decimal with 2 decimal places"""
        if isinstance(v, (int, float)):
            return Decimal(str(v)).quantize(Decimal("0.01"))
        return v

    def to_json(self) -> dict:
        """Convert to JSON-serializable dictionary"""
        return {
            "event_id": str(self.event_id),
            "event_time": self.event_time.isoformat(),
            "order_id": self.order_id,
            "user_id": self.user_id,
            "order_amount": str(self.order_amount),
            "currency": self.currency,
            "order_status": self.order_status.value if hasattr(self.order_status, 'value') else str(self.order_status)
        }

    class Config:
        """Pydantic model configuration"""
        json_encoders = {
            UUID: str,
            datetime: lambda v: v.isoformat(),
            Decimal: str,
        }
        use_enum_values = True
