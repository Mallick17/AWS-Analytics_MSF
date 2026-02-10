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
        order_id: Unique identifier for the order
        user_id: Identifier of the user who placed the order
        order_amount: Total order amount as decimal
        currency: Currency code (e.g., USD, EUR)
        order_status: Status of the order (always CREATED for this event type)
        timestamp_ms: Unix timestamp in milliseconds
    """
    
    event_id: str = Field(
        ...,
        description="Unique identifier for this event"
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
    timestamp_ms: int = Field(
        ...,
        description="Unix timestamp in milliseconds"
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
            "event_id": self.event_id,
            "order_id": self.order_id,
            "user_id": self.user_id,
            "order_amount": str(self.order_amount),
            "currency": self.currency,
            "order_status": self.order_status.value,
            "timestamp_ms": self.timestamp_ms
        }

    class Config:
        """Pydantic model configuration"""
        json_encoders = {
            Decimal: str,
        }
        use_enum_values = True
