package com.example.opensearch.offset;

import com.example.opensearch.model.Event;

/**
 * Wrapper class that includes Event data along with OpenSearch metadata
 * Provides access to _seq_no and _primary_term for offset management
 */
public class EventWithMetadata {
    private final Event event;
    private final long seqNo;
    private final long primaryTerm;
    private final String documentId;
    
    public EventWithMetadata(Event event, long seqNo, long primaryTerm, String documentId) {
        this.event = event;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.documentId = documentId;
    }
    
    public Event getEvent() {
        return event;
    }
    
    public long getSeqNo() {
        return seqNo;
    }
    
    public long getPrimaryTerm() {
        return primaryTerm;
    }
    
    public String getDocumentId() {
        return documentId;
    }
    
    @Override
    public String toString() {
        return "EventWithMetadata{" +
                "event=" + event +
                ", seqNo=" + seqNo +
                ", primaryTerm=" + primaryTerm +
                ", documentId='" + documentId + '\'' +
                '}';
    }
}