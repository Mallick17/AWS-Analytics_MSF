"""
User Event Schema

This schema defines the structure of user activity events.
"""

from pydantic import BaseModel, Field


class UserEvent(BaseModel):
    """
    User Event Schema
    
    Represents a user activity event.
    
    Attributes:
        event_type: Type of user event
        user_id: Identifier of the user
        page_url: URL of the page
        device_type: Type of device
        session_id: Session identifier
        timestamp_ms: Unix timestamp in milliseconds
    """
    
    event_type: str = Field(
        ...,
        description="Type of event (PAGE_VIEW, BUTTON_CLICK, etc.)"
    )
    user_id: int = Field(
        ...,
        description="User identifier"
    )
    page_url: str = Field(
        ...,
        description="URL of the page"
    )
    device_type: str = Field(
        ...,
        description="Device type (desktop, mobile, tablet)"
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
            "event_type": self.event_type,
            "user_id": self.user_id,
            "page_url": self.page_url,
            "device_type": self.device_type,
            "session_id": self.session_id,
            "timestamp_ms": self.timestamp_ms
        }
