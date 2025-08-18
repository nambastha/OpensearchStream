package com.streaming.opensearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Objects;

/**
 * Offset record model for Kafka-like offset management in Elasticsearch
 * Stores consumer offset information in a separate consumer_offsets index
 */
public class OffsetRecord {
    @JsonProperty("consumerId")
    private String consumerId;
    
    @JsonProperty("indexName")
    private String indexName;
    
    @JsonProperty("lastProcessedTimestamp")
    private String lastProcessedTimestamp;
    
    @JsonProperty("lastProcessedDocId")
    private String lastProcessedDocId;
    
    @JsonProperty("lastUpdated")
    private String lastUpdated;
    
    @JsonProperty("totalProcessed")
    private long totalProcessed;

    public OffsetRecord() {
        this.lastUpdated = Instant.now().toString();
        this.totalProcessed = 0L;
    }

    public OffsetRecord(String consumerId, String indexName) {
        this();
        this.consumerId = consumerId;
        this.indexName = indexName;
        this.lastProcessedTimestamp = "1970-01-01T00:00:00Z";
        this.lastProcessedDocId = null;
    }

    public OffsetRecord(String consumerId, String indexName, String lastProcessedTimestamp, 
                       String lastProcessedDocId, long totalProcessed) {
        this();
        this.consumerId = consumerId;
        this.indexName = indexName;
        this.lastProcessedTimestamp = lastProcessedTimestamp;
        this.lastProcessedDocId = lastProcessedDocId;
        this.totalProcessed = totalProcessed;
    }

    // Getters and Setters
    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getLastProcessedTimestamp() {
        return lastProcessedTimestamp;
    }

    public void setLastProcessedTimestamp(String lastProcessedTimestamp) {
        this.lastProcessedTimestamp = lastProcessedTimestamp;
        this.lastUpdated = Instant.now().toString();
    }

    public String getLastProcessedDocId() {
        return lastProcessedDocId;
    }

    public void setLastProcessedDocId(String lastProcessedDocId) {
        this.lastProcessedDocId = lastProcessedDocId;
        this.lastUpdated = Instant.now().toString();
    }

    public String getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(String lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public long getTotalProcessed() {
        return totalProcessed;
    }

    public void setTotalProcessed(long totalProcessed) {
        this.totalProcessed = totalProcessed;
        this.lastUpdated = Instant.now().toString();
    }

    public void incrementTotalProcessed() {
        this.totalProcessed++;
        this.lastUpdated = Instant.now().toString();
    }

    /**
     * Generate unique document ID for this offset record
     * Format: {consumerId}_{indexName}
     */
    public String generateDocumentId() {
        return consumerId + "_" + indexName;
    }

    /**
     * Update offset information after processing an event
     */
    public void updateOffset(String timestamp, String docId) {
        this.lastProcessedTimestamp = timestamp;
        this.lastProcessedDocId = docId;
        this.totalProcessed++;
        this.lastUpdated = Instant.now().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffsetRecord that = (OffsetRecord) o;
        return Objects.equals(consumerId, that.consumerId) && 
               Objects.equals(indexName, that.indexName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerId, indexName);
    }

    @Override
    public String toString() {
        return "OffsetRecord{" +
                "consumerId='" + consumerId + '\'' +
                ", indexName='" + indexName + '\'' +
                ", lastProcessedTimestamp='" + lastProcessedTimestamp + '\'' +
                ", lastProcessedDocId='" + lastProcessedDocId + '\'' +
                ", lastUpdated='" + lastUpdated + '\'' +
                ", totalProcessed=" + totalProcessed +
                '}';
    }
}