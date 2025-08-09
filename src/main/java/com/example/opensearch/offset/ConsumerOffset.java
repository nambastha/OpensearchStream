package com.example.opensearch.offset;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Objects;

/**
 * Consumer offset model for tracking consumer position using OpenSearch sequence numbers
 */
public class ConsumerOffset {
    @JsonProperty("consumerGroupId")
    private String consumerGroupId;
    
    @JsonProperty("indexName") 
    private String indexName;
    
    @JsonProperty("lastSeqNo")
    private long lastSeqNo;
    
    @JsonProperty("lastPrimaryTerm")
    private long lastPrimaryTerm;
    
    @JsonProperty("lastUpdated")
    private String lastUpdated;
    
    public ConsumerOffset() {
        this.lastSeqNo = -1L;
        this.lastPrimaryTerm = 0L;
        this.lastUpdated = Instant.now().toString();
    }
    
    public ConsumerOffset(String consumerGroupId, String indexName) {
        this();
        this.consumerGroupId = consumerGroupId;
        this.indexName = indexName;
    }
    
    // Getters and Setters
    public String getConsumerGroupId() {
        return consumerGroupId;
    }
    
    public void setConsumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
    }
    
    public String getIndexName() {
        return indexName;
    }
    
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }
    
    public long getLastSeqNo() {
        return lastSeqNo;
    }
    
    public void setLastSeqNo(long lastSeqNo) {
        this.lastSeqNo = lastSeqNo;
    }
    
    public long getLastPrimaryTerm() {
        return lastPrimaryTerm;
    }
    
    public void setLastPrimaryTerm(long lastPrimaryTerm) {
        this.lastPrimaryTerm = lastPrimaryTerm;
    }
    
    public String getLastUpdated() {
        return lastUpdated;
    }
    
    public void setLastUpdated(String lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
    
    public void updatePosition(long seqNo, long primaryTerm) {
        this.lastSeqNo = seqNo;
        this.lastPrimaryTerm = primaryTerm;
        this.lastUpdated = Instant.now().toString();
    }
    
    public String getOffsetId() {
        return consumerGroupId + "_" + indexName;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerOffset that = (ConsumerOffset) o;
        return Objects.equals(consumerGroupId, that.consumerGroupId) &&
               Objects.equals(indexName, that.indexName);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(consumerGroupId, indexName);
    }
    
    @Override
    public String toString() {
        return "ConsumerOffset{" +
                "consumerGroupId='" + consumerGroupId + '\'' +
                ", indexName='" + indexName + '\'' +
                ", lastSeqNo=" + lastSeqNo +
                ", lastPrimaryTerm=" + lastPrimaryTerm +
                ", lastUpdated='" + lastUpdated + '\'' +
                '}';
    }
}