package com.example.opensearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Objects;

/**
 * Event model class representing data to be stored in OpenSearch
 */
public class Event {
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("timestamp")
    private String timestamp;
    
    @JsonProperty("eventType")
    private String eventType;
    
    @JsonProperty("userId")
    private String userId;
    
    @JsonProperty("data")
    private String data;
    
    @JsonProperty("processed")
    private boolean processed;

    public Event() {
        this.timestamp = Instant.now().toString();
        this.processed = false;
    }

    public Event(String id, String eventType, String userId, String data) {
        this();
        this.id = id;
        this.eventType = eventType;
        this.userId = userId;
        this.data = data;
    }

    // Getters and Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(id, event.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", eventType='" + eventType + '\'' +
                ", userId='" + userId + '\'' +
                ", data='" + data + '\'' +
                ", processed=" + processed +
                '}';
    }
}
