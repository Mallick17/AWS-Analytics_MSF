"""
Bid Event Schema

This schema defines the structure of bid events.
"""

from pydantic import BaseModel, Field


class BidEvent(BaseModel):
    """
    Bid Event Schema
    
    Represents a bid action by a user.
    
    Attributes:
        event_name: Name of the bid event
        user_id: Identifier of the user
        city_id: City identifier
        platform: Platform where bid was placed
        session_id: Session identifier
        timestamp_ms: Unix timestamp in milliseconds
    """
    
    event_name: str = Field(
        ...,
        description="Name of the bid event"
    )
    user_id: int = Field(
        ...,
        description="User identifier"
    )
    city_id: int = Field(
        ...,
        description="City identifier"
    )
    platform: str = Field(
        ...,
        description="Platform (web, android, ios, mobile, desktop)"
    )
    session_id: str = Field(
        ...,
        description="Session identifier"
    )
    timestamp_ms: int = Field(
        ...,
        description="Unix timestamp in milliseconds"
    )

    def to_json(self) -> dict:
        """Convert to JSON-serializable dictionary"""
        return {
            "event_name": self.event_name,
            "user_id": self.user_id,
            "city_id": self.city_id,
            "platform": self.platform,
            "session_id": self.session_id,
            "timestamp_ms": self.timestamp_ms
        }
