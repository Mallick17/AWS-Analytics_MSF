"""
FastAPI Business Logic Service
This service receives events from the Flink job and applies custom Python logic
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Flink Python Logic API")

# ============ REQUEST/RESPONSE MODELS ============

class EventRequest(BaseModel):
    event_id: str
    user_id: str
    event_type: str
    timestamp: int
    ride_id: str
    surge_multiplier: float
    estimated_wait_minutes: int
    fare_amount: float
    driver_rating: float

class EventResponse(BaseModel):
    event_id: str
    fraud_detected: bool
    risk_score: float
    recommendations: str
    adjusted_fare: Optional[float] = None
    processing_time_ms: Optional[float] = None

# ============ BUSINESS LOGIC FUNCTIONS ============

def detect_fraud(event: EventRequest) -> bool:
    """
    Fraud detection logic
    Example: Flag if surge is too high or wait time is suspicious
    """
    if event.surge_multiplier > 3.0:
        logger.warning(f"High surge detected for event {event.event_id}: {event.surge_multiplier}x")
        return True
    
    if event.estimated_wait_minutes > 60:
        logger.warning(f"Suspicious wait time for event {event.event_id}: {event.estimated_wait_minutes} min")
        return True
    
    return False

def calculate_risk_score(event: EventRequest) -> float:
    """
    Calculate risk score based on multiple factors
    Scale: 0.0 (low risk) to 1.0 (high risk)
    """
    risk = 0.0
    
    # Factor 1: Surge multiplier
    if event.surge_multiplier > 2.5:
        risk += 0.3
    elif event.surge_multiplier > 2.0:
        risk += 0.2
    elif event.surge_multiplier > 1.5:
        risk += 0.1
    
    # Factor 2: Wait time
    if event.estimated_wait_minutes > 45:
        risk += 0.3
    elif event.estimated_wait_minutes > 30:
        risk += 0.2
    
    # Factor 3: Fare amount (unusual fares)
    if event.fare_amount > 1000:
        risk += 0.2
    elif event.fare_amount < 5:
        risk += 0.1
    
    # Factor 4: Driver rating
    if event.driver_rating < 3.0:
        risk += 0.2
    
    return min(risk, 1.0)  # Cap at 1.0

def generate_recommendations(event: EventRequest, fraud_detected: bool, risk_score: float) -> str:
    """
    Generate recommendations based on event analysis
    """
    recommendations = []
    
    if fraud_detected:
        recommendations.append("ALERT: Manual review required")
    
    if risk_score > 0.7:
        recommendations.append("High risk - apply additional verification")
    elif risk_score > 0.4:
        recommendations.append("Moderate risk - monitor closely")
    
    if event.surge_multiplier > 2.0:
        recommendations.append(f"High surge pricing ({event.surge_multiplier}x)")
    
    if event.driver_rating < 3.5:
        recommendations.append(f"Low driver rating ({event.driver_rating})")
    
    if event.estimated_wait_minutes > 30:
        recommendations.append(f"Long wait time ({event.estimated_wait_minutes} min)")
    
    return " | ".join(recommendations) if recommendations else "Normal"

def adjust_fare_if_needed(event: EventRequest, fraud_detected: bool) -> Optional[float]:
    """
    Adjust fare based on business rules
    Returns None if no adjustment needed
    """
    if fraud_detected and event.fare_amount > 500:
        # Cap fare for fraudulent high-value rides
        return 500.0
    
    if event.surge_multiplier > 3.0 and event.fare_amount > 200:
        # Apply discount for extreme surge
        return event.fare_amount * 0.9
    
    return None

# ============ API ENDPOINTS ============

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "Flink Python Logic API",
        "status": "healthy",
        "version": "1.0"
    }

@app.get("/health")
async def health():
    """Detailed health check"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/process", response_model=EventResponse)
async def process_event(event: EventRequest):
    """
    Main processing endpoint called by Flink
    Applies all business logic and returns enriched event
    """
    start_time = datetime.utcnow()
    
    try:
        logger.info(f"Processing event {event.event_id} for user {event.user_id}")
        
        # Apply business logic
        fraud_detected = detect_fraud(event)
        risk_score = calculate_risk_score(event)
        recommendations = generate_recommendations(event, fraud_detected, risk_score)
        adjusted_fare = adjust_fare_if_needed(event, fraud_detected)
        
        # Calculate processing time
        processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        response = EventResponse(
            event_id=event.event_id,
            fraud_detected=fraud_detected,
            risk_score=risk_score,
            recommendations=recommendations,
            adjusted_fare=adjusted_fare,
            processing_time_ms=processing_time
        )
        
        logger.info(f"Event {event.event_id} processed in {processing_time:.2f}ms - "
                   f"Fraud: {fraud_detected}, Risk: {risk_score:.2f}")
        
        return response
        
    except Exception as e:
        logger.error(f"Error processing event {event.event_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/batch-process")
async def batch_process(events: list[EventRequest]):
    """
    Batch processing endpoint for multiple events
    """
    results = []
    
    for event in events:
        try:
            result = await process_event(event)
            results.append(result)
        except Exception as e:
            logger.error(f"Error in batch processing event {event.event_id}: {str(e)}")
            # Continue processing other events
            continue
    
    return {
        "total": len(events),
        "processed": len(results),
        "results": results
    }

# ============ CUSTOM LOGIC EXAMPLES ============

@app.post("/custom/fraud-check")
async def custom_fraud_check(event: EventRequest):
    """
    Specialized fraud check endpoint
    """
    fraud_indicators = {
        "high_surge": event.surge_multiplier > 3.0,
        "long_wait": event.estimated_wait_minutes > 60,
        "unusual_fare": event.fare_amount > 1000 or event.fare_amount < 5,
        "low_driver_rating": event.driver_rating < 3.0
    }
    
    fraud_detected = any(fraud_indicators.values())
    
    return {
        "event_id": event.event_id,
        "fraud_detected": fraud_detected,
        "indicators": fraud_indicators,
        "recommendation": "BLOCK" if fraud_detected else "ALLOW"
    }

@app.post("/custom/pricing-optimization")
async def pricing_optimization(event: EventRequest):
    """
    Specialized pricing optimization endpoint
    """
    optimal_fare = event.fare_amount
    
    # Apply pricing rules
    if event.surge_multiplier > 2.5:
        optimal_fare = event.fare_amount * 0.95  # 5% discount for high surge
    
    if event.estimated_wait_minutes > 45:
        optimal_fare = optimal_fare * 0.9  # 10% discount for long waits
    
    savings = event.fare_amount - optimal_fare
    
    return {
        "event_id": event.event_id,
        "original_fare": event.fare_amount,
        "optimal_fare": round(optimal_fare, 2),
        "savings": round(savings, 2),
        "discount_percent": round((savings / event.fare_amount) * 100, 1) if event.fare_amount > 0 else 0
    }

# ============ RUN SERVER ============

if __name__ == "__main__":
    import uvicorn
    
    # Run with: python fastapi_logic.py
    # Or: uvicorn fastapi_logic:app --host 0.0.0.0 --port 8000 --reload
    
    uvicorn.run(
        "fastapi_logic:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )