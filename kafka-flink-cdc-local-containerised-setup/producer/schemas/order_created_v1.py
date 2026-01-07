from pydantic import BaseModel
from datetime import datetime
import uuid


class OrderCreatedV1(BaseModel):
    """
    Schema for order-created events.
    """

    event_id: str = str(uuid.uuid4())
    event_time: datetime = datetime.utcnow()

    order_id: str
    user_id: str
    order_amount: float
    currency: str
    order_status: str

    def to_json(self) -> dict:
        """
        Serialize event to JSON-compatible dict.
        """
        return self.model_dump()
