package com.amazonaws.services.msf;

import java.io.Serializable;

/**
 * Event record data model
 * This class represents the structure of events flowing through the pipeline
 */
public class EventRecord implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    public String eventId;
    public String userId;
    public String eventType;
    public long timestamp;
    public String rideId;
    public double surgeMultiplier;
    public int estimatedWaitMinutes;
    public double fareAmount;
    public double driverRating;
    
    // Additional fields that can be set by Python logic
    public String processedBy;  // Can be set to "python" or "java"
    public boolean fraudDetected;
    public double riskScore;
    public String recommendations;
    
    public EventRecord() {
        this.processedBy = "java";
        this.fraudDetected = false;
        this.riskScore = 0.0;
        this.recommendations = "";
    }
    
    @Override
    public String toString() {
        return "EventRecord{" +
                "eventId='" + eventId + '\'' +
                ", userId='" + userId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                ", rideId='" + rideId + '\'' +
                ", surgeMultiplier=" + surgeMultiplier +
                ", estimatedWaitMinutes=" + estimatedWaitMinutes +
                ", fareAmount=" + fareAmount +
                ", driverRating=" + driverRating +
                ", processedBy='" + processedBy + '\'' +
                ", fraudDetected=" + fraudDetected +
                ", riskScore=" + riskScore +
                '}';
    }
}