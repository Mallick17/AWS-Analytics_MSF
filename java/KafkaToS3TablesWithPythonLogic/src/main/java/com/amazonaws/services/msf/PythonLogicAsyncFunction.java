package com.amazonaws.services.msf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Async function that calls Python FastAPI service for business logic
 * Implements retry logic and proper error handling
 */
public class PythonLogicAsyncFunction extends RichAsyncFunction<EventRecord, EventRecord> {
    
    private static final Logger LOG = LoggerFactory.getLogger(PythonLogicAsyncFunction.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    
    private final String pythonApiUrl;
    private final int timeoutMs;
    private final int maxRetries;
    
    private transient CloseableHttpAsyncClient httpClient;
    
    public PythonLogicAsyncFunction(String pythonApiUrl, int timeoutMs, int maxRetries) {
        this.pythonApiUrl = pythonApiUrl;
        this.timeoutMs = timeoutMs;
        this.maxRetries = maxRetries;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Create HTTP client with connection pooling and timeouts
        RequestConfig requestConfig = RequestConfig.custom()
            .setResponseTimeout(Timeout.of(timeoutMs, TimeUnit.MILLISECONDS))
            .setConnectionRequestTimeout(Timeout.of(timeoutMs, TimeUnit.MILLISECONDS))
            .build();
        
        httpClient = HttpAsyncClients.custom()
            .setDefaultRequestConfig(requestConfig)
            .setMaxConnTotal(100)
            .setMaxConnPerRoute(20)
            .build();
        
        httpClient.start();
        
        LOG.info("Python Logic Async Function initialized with URL: {}", pythonApiUrl);
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        if (httpClient != null) {
            httpClient.close();
        }
    }
    
    @Override
    public void asyncInvoke(EventRecord record, ResultFuture<EventRecord> resultFuture) throws Exception {
        // Call Python API with retry logic
        callPythonApiWithRetry(record, resultFuture, 0);
    }
    
    /**
     * Call Python API with exponential backoff retry
     */
    private void callPythonApiWithRetry(
        EventRecord record, 
        ResultFuture<EventRecord> resultFuture, 
        int attemptNumber
    ) {
        try {
            // Prepare request payload
            String requestBody = createRequestPayload(record);
            
            SimpleHttpRequest request = SimpleRequestBuilder.post(pythonApiUrl)
                .setBody(requestBody, ContentType.APPLICATION_JSON)
                .build();
            
            // Execute async request
            httpClient.execute(request, new FutureCallback<SimpleHttpResponse>() {
                
                @Override
                public void completed(SimpleHttpResponse response) {
                    try {
                        int statusCode = response.getCode();
                        
                        if (statusCode == 200) {
                            // Success - parse response
                            String responseBody = response.getBodyText();
                            EventRecord enrichedRecord = parseResponse(record, responseBody);
                            resultFuture.complete(Collections.singleton(enrichedRecord));
                            
                            LOG.debug("Successfully processed event {} via Python API", record.eventId);
                            
                        } else if (statusCode >= 500 && attemptNumber < maxRetries) {
                            // Server error - retry
                            LOG.warn("Python API returned {}, retrying (attempt {}/{})", 
                                statusCode, attemptNumber + 1, maxRetries);
                            
                            scheduleRetry(record, resultFuture, attemptNumber + 1);
                            
                        } else {
                            // Client error or max retries - use fallback
                            LOG.error("Python API failed with status {}, using fallback", statusCode);
                            resultFuture.complete(Collections.singleton(record));
                        }
                        
                    } catch (Exception e) {
                        LOG.error("Error processing Python API response", e);
                        resultFuture.complete(Collections.singleton(record));
                    }
                }
                
                @Override
                public void failed(Exception ex) {
                    if (attemptNumber < maxRetries) {
                        LOG.warn("Python API call failed, retrying (attempt {}/{}): {}", 
                            attemptNumber + 1, maxRetries, ex.getMessage());
                        
                        scheduleRetry(record, resultFuture, attemptNumber + 1);
                    } else {
                        LOG.error("Python API call failed after {} retries, using fallback: {}", 
                            maxRetries, ex.getMessage());
                        resultFuture.complete(Collections.singleton(record));
                    }
                }
                
                @Override
                public void cancelled() {
                    LOG.warn("Python API call cancelled for event {}", record.eventId);
                    resultFuture.complete(Collections.singleton(record));
                }
            });
            
        } catch (Exception e) {
            LOG.error("Error calling Python API", e);
            resultFuture.complete(Collections.singleton(record));
        }
    }
    
    /**
     * Schedule retry with exponential backoff
     */
    private void scheduleRetry(EventRecord record, ResultFuture<EventRecord> resultFuture, int nextAttempt) {
        long backoffMs = (long) Math.pow(2, nextAttempt) * 100; // 200ms, 400ms, 800ms...
        
        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        callPythonApiWithRetry(record, resultFuture, nextAttempt);
    }
    
    /**
     * Create JSON request payload for Python API
     */
    private String createRequestPayload(EventRecord record) throws Exception {
        return mapper.writeValueAsString(new Object() {
            public final String event_id = record.eventId;
            public final String user_id = record.userId;
            public final String event_type = record.eventType;
            public final long timestamp = record.timestamp;
            public final String ride_id = record.rideId;
            public final double surge_multiplier = record.surgeMultiplier;
            public final int estimated_wait_minutes = record.estimatedWaitMinutes;
            public final double fare_amount = record.fareAmount;
            public final double driver_rating = record.driverRating;
        });
    }
    
    /**
     * Parse Python API response and enrich the record
     */
    private EventRecord parseResponse(EventRecord originalRecord, String responseBody) throws Exception {
        JsonNode response = mapper.readTree(responseBody);
        
        // Enrich the original record with Python logic results
        originalRecord.processedBy = "python";
        
        if (response.has("fraud_detected")) {
            originalRecord.fraudDetected = response.get("fraud_detected").asBoolean();
        }
        
        if (response.has("risk_score")) {
            originalRecord.riskScore = response.get("risk_score").asDouble();
        }
        
        if (response.has("recommendations")) {
            originalRecord.recommendations = response.get("recommendations").asText();
        }
        
        // You can also update existing fields if Python logic modifies them
        if (response.has("adjusted_fare")) {
            originalRecord.fareAmount = response.get("adjusted_fare").asDouble();
        }
        
        return originalRecord;
    }
    
    @Override
    public void timeout(EventRecord input, ResultFuture<EventRecord> resultFuture) throws Exception {
        LOG.warn("Python API call timed out for event {}, using fallback", input.eventId);
        resultFuture.complete(Collections.singleton(input));
    }
}