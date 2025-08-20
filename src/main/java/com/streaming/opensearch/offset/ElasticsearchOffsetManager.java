package com.streaming.opensearch.offset;

import com.streaming.opensearch.config.ElasticsearchConfig;
import com.streaming.opensearch.model.OffsetRecord;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Elasticsearch-based offset manager for Kafka-like consumer offset management
 * Uses a separate 'consumer_offsets' index to store offset information
 */
public class ElasticsearchOffsetManager {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchOffsetManager.class);
    
    private final ElasticsearchClient client;
    private final String offsetIndexName;
    private final ReentrantLock offsetLock;
    
    public static final String DEFAULT_OFFSET_INDEX = "consumer_offsets";
    
    public ElasticsearchOffsetManager() {
        this(DEFAULT_OFFSET_INDEX);
    }
    
    public ElasticsearchOffsetManager(String offsetIndexName) {
        this.client = ElasticsearchConfig.createClient();
        this.offsetIndexName = offsetIndexName;
        this.offsetLock = new ReentrantLock();
        
        // Create offset index if it doesn't exist
        createOffsetIndexIfNotExists();
    }
    
    /**
     * Create the consumer_offsets index with only your specified fields
     * Structure: consumerId, indexName, lastProcessedTimestamp, lastProcessedDocId, lastUpdated, totalProcessed
     */
    private void createOffsetIndexIfNotExists() {
        try {
            ExistsRequest existsRequest = ExistsRequest.of(e -> e.index(offsetIndexName));
            boolean exists = client.indices().exists(existsRequest).value();
            
            if (!exists) {
                logger.info("Creating offset index: {}", offsetIndexName);
                CreateIndexRequest createIndexRequest = CreateIndexRequest.of(c -> c
                    .index(offsetIndexName)
                    .mappings(m -> m
                        .properties("consumerId", p -> p.text(t -> t))
                        .properties("indexName", p -> p.text(t -> t))
                        .properties("lastProcessedTimestamp", p -> p.text(t -> t))
                        .properties("lastProcessedDocId", p -> p.text(t -> t))
                        .properties("lastUpdated", p -> p.text(t -> t))
                        .properties("totalProcessed", p -> p.long_(l -> l))
                        .properties("queryStartTimestamp", p -> p.text(t -> t))
                    )
                );
                
                client.indices().create(createIndexRequest);
                logger.info("Offset index {} created successfully", offsetIndexName);
            } else {
                logger.info("Offset index {} already exists", offsetIndexName);
            }
        } catch (Exception e) {
            logger.error("Error creating offset index: {}", offsetIndexName, e);
            throw new RuntimeException("Failed to create offset index", e);
        }
    }
    
    /**
     * Load offset record for a specific consumer and index
     */
    public OffsetRecord loadOffset(String consumerId, String indexName) {
        offsetLock.lock();
        try {
            String documentId = generateDocumentId(consumerId, indexName);
            
            GetRequest getRequest = GetRequest.of(g -> g
                .index(offsetIndexName)
                .id(documentId)
            );
            
            GetResponse<OffsetRecord> response = client.get(getRequest, OffsetRecord.class);
            
            if (response.found() && response.source() != null) {
                OffsetRecord offset = response.source();
                logger.info("Loaded offset for consumer {}, index {}: {}", 
                    consumerId, indexName, offset);
                return offset;
            } else {
                logger.info("No existing offset found for consumer {}, index {}. Creating new record.", 
                    consumerId, indexName);
                return new OffsetRecord(consumerId, indexName);
            }
            
        } catch (Exception e) {
            logger.warn("Error loading offset for consumer {}, index {}: {}", 
                consumerId, indexName, e.getMessage());
            // Return new offset record on error
            return new OffsetRecord(consumerId, indexName);
        } finally {
            offsetLock.unlock();
        }
    }
    
    /**
     * Save offset record to Elasticsearch
     */
    public void saveOffset(OffsetRecord offsetRecord) {
        offsetLock.lock();
        try {
            String documentId = offsetRecord.generateDocumentId();
            
            // Update the lastUpdated timestamp
            offsetRecord.setLastUpdated(Instant.now().toString());
            
            IndexRequest<OffsetRecord> indexRequest = IndexRequest.of(i -> i
                .index(offsetIndexName)
                .id(documentId)
                .document(offsetRecord)
            );
            
            IndexResponse response = client.index(indexRequest);
            
            if (response.result().jsonValue().equals("created") || 
                response.result().jsonValue().equals("updated")) {
                logger.debug("Offset saved successfully for consumer {}, index {}: total processed = {}", 
                    offsetRecord.getConsumerId(), offsetRecord.getIndexName(), offsetRecord.getTotalProcessed());
            } else {
                logger.warn("Unexpected response saving offset: {}", response.result());
            }
            
        } catch (ElasticsearchException e) {
            logger.error("Elasticsearch error saving offset for consumer {}, index {}: {}", 
                offsetRecord.getConsumerId(), offsetRecord.getIndexName(), e.getMessage());
            throw e;
        } catch (Exception e) {
            logger.error("Error saving offset for consumer {}, index {}", 
                offsetRecord.getConsumerId(), offsetRecord.getIndexName(), e);
            throw new RuntimeException("Failed to save offset", e);
        } finally {
            offsetLock.unlock();
        }
    }
    
    /**
     * Update offset after processing an event
     */
    public void updateOffset(String consumerId, String indexName, String timestamp, String docId) {
        OffsetRecord offset = loadOffset(consumerId, indexName);
        offset.updateOffset(timestamp, docId);
        saveOffset(offset);
    }
    
    /**
     * Mark the start of a query batch to prevent missing events during long-running queries
     * This timestamp will be used as the starting point for the next query
     */
    public void markQueryStart(String consumerId, String indexName) {
        offsetLock.lock();
        try {
            OffsetRecord offset = loadOffset(consumerId, indexName);
            String currentTime = Instant.now().toString();
            offset.setQueryStartTimestamp(currentTime);
            saveOffset(offset);
            logger.debug("Marked query start time {} for consumer {}, index {}", 
                currentTime, consumerId, indexName);
        } finally {
            offsetLock.unlock();
        }
    }
    
    /**
     * Get the effective starting timestamp for the next query
     * Uses queryStartTimestamp if available (to handle long-running queries),
     * otherwise falls back to lastProcessedTimestamp
     */
    public String getEffectiveStartTimestamp(String consumerId, String indexName) {
        OffsetRecord offset = loadOffset(consumerId, indexName);
        
        if (offset.getQueryStartTimestamp() != null && !offset.getQueryStartTimestamp().isEmpty()) {
            logger.debug("Using queryStartTimestamp {} for consumer {}, index {}", 
                offset.getQueryStartTimestamp(), consumerId, indexName);
            return offset.getQueryStartTimestamp();
        } else {
            logger.debug("Using lastProcessedTimestamp {} for consumer {}, index {}", 
                offset.getLastProcessedTimestamp(), consumerId, indexName);
            return offset.getLastProcessedTimestamp();
        }
    }
    
    /**
     * Clear the query start timestamp after successfully completing a query
     * This ensures the next query uses the regular lastProcessedTimestamp
     */
    public void clearQueryStart(String consumerId, String indexName) {
        offsetLock.lock();
        try {
            OffsetRecord offset = loadOffset(consumerId, indexName);
            offset.setQueryStartTimestamp(null);
            saveOffset(offset);
            logger.debug("Cleared query start timestamp for consumer {}, index {}", 
                consumerId, indexName);
        } finally {
            offsetLock.unlock();
        }
    }
    
    /**
     * Increment total processed count
     */
    public void incrementProcessedCount(String consumerId, String indexName) {
        offsetLock.lock();
        try {
            String documentId = generateDocumentId(consumerId, indexName);
            
            // Use update API for atomic increment
            UpdateRequest<OffsetRecord, OffsetRecord> updateRequest = UpdateRequest.of(u -> u
                .index(offsetIndexName)
                .id(documentId)
                .script(s -> s
                    .inline(i -> i
                        .source("ctx._source.totalProcessed++; ctx._source.lastUpdated = params.timestamp")
                        .params("timestamp", co.elastic.clients.json.JsonData.of(Instant.now().toString()))
                    )
                )
                .upsert(new OffsetRecord(consumerId, indexName))
            );
            
            client.update(updateRequest, OffsetRecord.class);
            logger.debug("Incremented processed count for consumer {}, index {}", consumerId, indexName);
            
        } catch (Exception e) {
            logger.error("Error incrementing processed count for consumer {}, index {}", 
                consumerId, indexName, e);
            // Fallback to load-modify-save pattern
            OffsetRecord offset = loadOffset(consumerId, indexName);
            offset.incrementTotalProcessed();
            saveOffset(offset);
        } finally {
            offsetLock.unlock();
        }
    }
    
    /**
     * Delete offset record for a consumer
     */
    public void deleteOffset(String consumerId, String indexName) {
        offsetLock.lock();
        try {
            String documentId = generateDocumentId(consumerId, indexName);
            
            client.delete(d -> d
                .index(offsetIndexName)
                .id(documentId)
            );
            
            logger.info("Deleted offset record for consumer {}, index {}", consumerId, indexName);
            
        } catch (Exception e) {
            logger.error("Error deleting offset for consumer {}, index {}", 
                consumerId, indexName, e);
        } finally {
            offsetLock.unlock();
        }
    }
    
    /**
     * Check if offset exists for a consumer
     */
    public boolean offsetExists(String consumerId, String indexName) {
        try {
            String documentId = generateDocumentId(consumerId, indexName);
            
            GetRequest getRequest = GetRequest.of(g -> g
                .index(offsetIndexName)
                .id(documentId)
            );
            
            GetResponse<OffsetRecord> response = client.get(getRequest, OffsetRecord.class);
            return response.found();
            
        } catch (Exception e) {
            logger.warn("Error checking offset existence for consumer {}, index {}: {}", 
                consumerId, indexName, e.getMessage());
            return false;
        }
    }
    
    /**
     * Generate document ID for offset record
     */
    private String generateDocumentId(String consumerId, String indexName) {
        return consumerId + "_" + indexName;
    }
    
    /**
     * Get the offset index name
     */
    public String getOffsetIndexName() {
        return offsetIndexName;
    }
    
    /**
     * Close the offset manager
     */
    public void close() {
        try {
            ElasticsearchConfig.closeClient(client);
            logger.info("ElasticsearchOffsetManager closed successfully");
        } catch (Exception e) {
            logger.error("Error closing ElasticsearchOffsetManager", e);
        }
    }
    
    /**
     * Get statistics about the offset index
     */
    public void printOffsetStats() {
        try {
            var statsResponse = client.indices().stats(s -> s.index(offsetIndexName));
            logger.info("=== OFFSET INDEX STATS ===");
            logger.info("Index: {}", offsetIndexName);
            logger.info("Documents: {}", statsResponse.total().docs().count());
            logger.info("Size: {} bytes", statsResponse.total().store().sizeInBytes());
            logger.info("========================");
        } catch (Exception e) {
            logger.warn("Could not retrieve offset index stats: {}", e.getMessage());
        }
    }
}