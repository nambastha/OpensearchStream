package com.example.opensearch.offset;

import com.example.opensearch.config.OpenSearchConfig;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Result;
import org.opensearch.client.opensearch.core.*;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Manages consumer offsets using a dedicated OpenSearch index
 * Provides persistent offset storage that survives container restarts
 */
public class OffsetManager {
    private static final Logger logger = LoggerFactory.getLogger(OffsetManager.class);
    
    private static final String OFFSET_INDEX = "consumer-offsets";
    private final OpenSearchClient client;
    
    public OffsetManager() {
        this.client = OpenSearchConfig.createClient();
        createOffsetIndexIfNotExists();
    }
    
    public OffsetManager(OpenSearchClient client) {
        this.client = client;
        createOffsetIndexIfNotExists();
    }
    
    /**
     * Load the last committed offset for a consumer group
     */
    public Optional<ConsumerOffset> loadOffset(String consumerGroupId, String indexName) {
        try {
            String offsetId = new ConsumerOffset(consumerGroupId, indexName).getOffsetId();
            
            GetRequest getRequest = GetRequest.of(g -> g
                .index(OFFSET_INDEX)
                .id(offsetId)
            );
            
            GetResponse<ConsumerOffset> response = client.get(getRequest, ConsumerOffset.class);
            
            if (response.found() && response.source() != null) {
                ConsumerOffset offset = response.source();
                logger.info("Loaded offset for {}: seq_no={}, primary_term={}", 
                    offsetId, offset.getLastSeqNo(), offset.getLastPrimaryTerm());
                return Optional.of(offset);
            } else {
                logger.info("No existing offset found for {}, starting from beginning", offsetId);
                return Optional.empty();
            }
            
        } catch (Exception e) {
            logger.error("Error loading offset for consumerGroup={}, index={}", 
                consumerGroupId, indexName, e);
            return Optional.empty();
        }
    }
    
    /**
     * Commit the current offset position atomically
     */
    public boolean commitOffset(ConsumerOffset offset) {
        try {
            offset.updatePosition(offset.getLastSeqNo(), offset.getLastPrimaryTerm());
            
            IndexRequest<ConsumerOffset> indexRequest = IndexRequest.of(i -> i
                .index(OFFSET_INDEX)
                .id(offset.getOffsetId())
                .document(offset)
            );
            
            IndexResponse response = client.index(indexRequest);
            
            boolean success = response.result() == Result.Created || response.result() == Result.Updated;
            
            if (success) {
                logger.debug("Committed offset for {}: seq_no={}, primary_term={}", 
                    offset.getOffsetId(), offset.getLastSeqNo(), offset.getLastPrimaryTerm());
            } else {
                logger.warn("Unexpected response when committing offset: {}", response.result());
            }
            
            return success;
            
        } catch (Exception e) {
            logger.error("Error committing offset for {}", offset.getOffsetId(), e);
            return false;
        }
    }
    
    /**
     * Update and commit offset in one operation
     */
    public boolean commitOffset(String consumerGroupId, String indexName, long seqNo, long primaryTerm) {
        ConsumerOffset offset = new ConsumerOffset(consumerGroupId, indexName);
        offset.updatePosition(seqNo, primaryTerm);
        return commitOffset(offset);
    }
    
    /**
     * Delete offset for a consumer group (useful for resetting)
     */
    public boolean deleteOffset(String consumerGroupId, String indexName) {
        try {
            String offsetId = new ConsumerOffset(consumerGroupId, indexName).getOffsetId();
            
            DeleteRequest deleteRequest = DeleteRequest.of(d -> d
                .index(OFFSET_INDEX)
                .id(offsetId)
            );
            
            DeleteResponse response = client.delete(deleteRequest);
            
            boolean success = response.result() == Result.Deleted;
            
            if (success) {
                logger.info("Deleted offset for {}", offsetId);
            } else {
                logger.warn("Offset not found for deletion: {}", offsetId);
            }
            
            return success;
            
        } catch (Exception e) {
            logger.error("Error deleting offset for consumerGroup={}, index={}", 
                consumerGroupId, indexName, e);
            return false;
        }
    }
    
    private void createOffsetIndexIfNotExists() {
        try {
            ExistsRequest existsRequest = ExistsRequest.of(e -> e.index(OFFSET_INDEX));
            boolean exists = client.indices().exists(existsRequest).value();
            
            if (!exists) {
                logger.info("Creating offset index: {}", OFFSET_INDEX);
                
                CreateIndexRequest createIndexRequest = CreateIndexRequest.of(c -> c
                    .index(OFFSET_INDEX)
                    .mappings(m -> m
                        .properties("consumerGroupId", p -> p.keyword(k -> k))
                        .properties("indexName", p -> p.keyword(k -> k))
                        .properties("lastSeqNo", p -> p.long_(l -> l))
                        .properties("lastPrimaryTerm", p -> p.long_(l -> l))
                        .properties("lastUpdated", p -> p.date(d -> d))
                    )
                    .settings(s -> s
                        .numberOfShards("1")
                        .numberOfReplicas("0") // Single replica for offset storage
                    )
                );
                
                client.indices().create(createIndexRequest);
                logger.info("Offset index {} created successfully", OFFSET_INDEX);
            } else {
                logger.info("Offset index {} already exists", OFFSET_INDEX);
            }
            
        } catch (Exception e) {
            logger.error("Error creating offset index: {}", OFFSET_INDEX, e);
            throw new RuntimeException("Failed to create offset index", e);
        }
    }
    
    public void close() {
        // Don't close the shared client
        logger.debug("OffsetManager closed");
    }
}