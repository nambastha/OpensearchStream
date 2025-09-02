package com.streaming.opensearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;

/**
 * Offset record model for Kafka-like offset management in Elasticsearch
 * Stores consumer offset information in a separate consumer_offsets index
 */
public class OffsetRecord {
    
    // Elasticsearch timestamp format: "Aug 11, 2025 @ 14:20:05.648"
    private static final DateTimeFormatter ELASTICSEARCH_FORMATTER = 
        DateTimeFormatter.ofPattern("MMM d, yyyy @ HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
    
    // ISO 8601 format for internal storage: "1970-01-01T00:00:00Z"
    private static final DateTimeFormatter ISO_FORMATTER = 
        DateTimeFormatter.ISO_INSTANT;
    
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
    
    @JsonProperty("queryStartTimestamp")
    private String queryStartTimestamp;

    public OffsetRecord() {
        this.lastUpdated = Instant.now().toString();
        this.totalProcessed = 0L;
        this.queryStartTimestamp = null;
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
        this.queryStartTimestamp = null;
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

    public String getQueryStartTimestamp() {
        return queryStartTimestamp;
    }

    public void setQueryStartTimestamp(String queryStartTimestamp) {
        this.queryStartTimestamp = queryStartTimestamp;
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

    /**
     * Convert Elasticsearch timestamp format to ISO 8601 format
     * From: "Aug 11, 2025 @ 14:20:05.648" 
     * To: "2025-08-11T14:20:05.648Z"
     */
    public static String convertElasticsearchToISO(String elasticsearchTimestamp) {
        if (elasticsearchTimestamp == null || elasticsearchTimestamp.isEmpty()) {
            return "1970-01-01T00:00:00Z";
        }
        
        try {
            // Parse the Elasticsearch format
            ZonedDateTime zonedDateTime = ZonedDateTime.parse(elasticsearchTimestamp, ELASTICSEARCH_FORMATTER);
            // Convert to UTC and format as ISO
            return zonedDateTime.toInstant().toString();
        } catch (DateTimeParseException e) {
            // If parsing fails, try to parse as ISO format (already correct)
            try {
                Instant.parse(elasticsearchTimestamp);
                return elasticsearchTimestamp; // Already in ISO format
            } catch (DateTimeParseException e2) {
                // If both fail, return epoch
                return "1970-01-01T00:00:00Z";
            }
        }
    }
    
    /**
     * Convert ISO 8601 format to Elasticsearch timestamp format
     * From: "2025-08-11T14:20:05.648Z"
     * To: "Aug 11, 2025 @ 14:20:05.648"
     */
    public static String convertISOToElasticsearch(String isoTimestamp) {
        if (isoTimestamp == null || isoTimestamp.isEmpty()) {
            return "Jan 1, 1970 @ 00:00:00.000";
        }
        
        try {
            // Parse ISO format
            Instant instant = Instant.parse(isoTimestamp);
            // Convert to system timezone and format for Elasticsearch
            ZonedDateTime zonedDateTime = instant.atZone(ZoneId.systemDefault());
            return ELASTICSEARCH_FORMATTER.format(zonedDateTime);
        } catch (DateTimeParseException e) {
            // If parsing fails, try to parse as Elasticsearch format (already correct)
            try {
                ZonedDateTime.parse(isoTimestamp, ELASTICSEARCH_FORMATTER);
                return isoTimestamp; // Already in Elasticsearch format
            } catch (DateTimeParseException e2) {
                // If both fail, return epoch
                return "Jan 1, 1970 @ 00:00:00.000";
            }
        }
    }
    
    /**
     * Get timestamp in Elasticsearch format for queries
     */
    public String getLastProcessedTimestampForQuery() {
        return convertISOToElasticsearch(lastProcessedTimestamp);
    }
    
    /**
     * Get query start timestamp in Elasticsearch format for queries  
     */
    public String getQueryStartTimestampForQuery() {
        return convertISOToElasticsearch(queryStartTimestamp);
    }
    
    /**
     * Set last processed timestamp from Elasticsearch format
     */
    public void setLastProcessedTimestampFromElasticsearch(String elasticsearchTimestamp) {
        this.lastProcessedTimestamp = convertElasticsearchToISO(elasticsearchTimestamp);
        this.lastUpdated = Instant.now().toString();
    }
    
    /**
     * Set query start timestamp from Elasticsearch format
     */
    public void setQueryStartTimestampFromElasticsearch(String elasticsearchTimestamp) {
        this.queryStartTimestamp = convertElasticsearchToISO(elasticsearchTimestamp);
        this.lastUpdated = Instant.now().toString();
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
                ", queryStartTimestamp='" + queryStartTimestamp + '\'' +
                '}';
    }
}