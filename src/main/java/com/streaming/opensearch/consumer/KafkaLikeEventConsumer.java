package com.streaming.opensearch.consumer;

import com.streaming.opensearch.config.ElasticsearchConfig;
import com.streaming.opensearch.model.Event;
import com.streaming.opensearch.model.OffsetRecord;
import com.streaming.opensearch.offset.ElasticsearchOffsetManager;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Kafka-like event consumer that uses Elasticsearch-based offset management
 * Uses a separate 'consumer_offsets' index to track processing state
 */
public class KafkaLikeEventConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaLikeEventConsumer.class);
    
    private final ElasticsearchClient client;
    private final String sourceIndexName;
    private final String consumerId;
    private final ElasticsearchOffsetManager offsetManager;
    private final Set<String> recentlyProcessedIds;
    private final AtomicLong sessionProcessedCount;
    private volatile boolean running;
    private OffsetRecord currentOffset;
    
    private static final int BATCH_SIZE = 50;
    private static final long POLL_INTERVAL_MS = 2000;
    private static final int RECENT_CACHE_SIZE = 1000;
    private static final int OFFSET_COMMIT_INTERVAL = 10; // Commit offset every 10 events
    private static final int SAFETY_WINDOW_SECONDS = 30; // Safety window to catch late-arriving events
    
    public KafkaLikeEventConsumer(String sourceIndexName) {
        this(sourceIndexName, generateConsumerId());
    }
    
    public KafkaLikeEventConsumer(String sourceIndexName, String consumerId) {
        this(sourceIndexName, consumerId, new ElasticsearchOffsetManager());
    }
    
    public KafkaLikeEventConsumer(String sourceIndexName, String consumerId, 
                                  ElasticsearchOffsetManager offsetManager) {
        this.client = ElasticsearchConfig.createClient();
        this.sourceIndexName = sourceIndexName;
        this.consumerId = consumerId;
        this.offsetManager = offsetManager;
        this.recentlyProcessedIds = new HashSet<>();
        this.sessionProcessedCount = new AtomicLong(0);
        this.running = false;
        
        // Load existing offset or create new one
        this.currentOffset = offsetManager.loadOffset(consumerId, sourceIndexName);
        
        logger.info("Initialized KafkaLikeEventConsumer: consumerId={}, sourceIndex={}, " +
                   "lastProcessedTimestamp={}, totalProcessed={}", 
                   consumerId, sourceIndexName, currentOffset.getLastProcessedTimestamp(), 
                   currentOffset.getTotalProcessed());
    }
    
    private static String generateConsumerId() {
        return "consumer-" + UUID.randomUUID().toString().substring(0, 8);
    }
    
    public void start() {
        if (running) {
            logger.warn("Consumer {} is already running", consumerId);
            return;
        }
        
        running = true;
        logger.info("Starting Kafka-like event consumer: {}", consumerId);
        logger.info("Source index: {}", sourceIndexName);
        logger.info("Offset index: {}", offsetManager.getOffsetIndexName());
        logger.info("Starting from timestamp: {}", currentOffset.getLastProcessedTimestamp());
        logger.info("Total previously processed: {}", currentOffset.getTotalProcessed());
        
        Thread consumerThread = new Thread(this::consumeEvents);
        consumerThread.setName("KafkaLikeEventConsumer-" + consumerId);
        consumerThread.setDaemon(false);
        consumerThread.start();
    }
    
    public void stop() {
        logger.info("Stopping Kafka-like event consumer: {}", consumerId);
        running = false;
    }
    
    private void consumeEvents() {
        long lastOffsetCommit = 0;
        
        while (running) {
            try {
                // Mark query execution start to protect against long-running query gaps
                offsetManager.markQueryExecutionStart(consumerId, sourceIndexName);
                
                List<Event> events = fetchUnprocessedEvents();
                
                if (!events.isEmpty()) {
                    logger.info("Fetched {} unprocessed events for consumer {}", events.size(), consumerId);
                    
                    // Process all events in the batch
                    boolean batchProcessedSuccessfully = true;
                    
                    for (Event event : events) {
                        // Double-check to avoid reprocessing recent events
                        if (!recentlyProcessedIds.contains(event.getId())) {
                            try {
                                processEvent(event);
                                updateOffsetInMemory(event);
                                recentlyProcessedIds.add(event.getId());
                                
                                long sessionCount = sessionProcessedCount.incrementAndGet();
                                long totalCount = currentOffset.getTotalProcessed();
                                
                                if (sessionCount % 50 == 0) {
                                    logger.info("Consumer {} processed {} events this session (total: {})", 
                                        consumerId, sessionCount, totalCount);
                                }
                                
                                // Commit offset periodically
                                if (sessionCount - lastOffsetCommit >= OFFSET_COMMIT_INTERVAL) {
                                    commitOffset();
                                    lastOffsetCommit = sessionCount;
                                }
                            } catch (Exception eventError) {
                                logger.error("Error processing event {} in consumer {}", event.getId(), consumerId, eventError);
                                batchProcessedSuccessfully = false;
                                break; // Don't continue processing if an event fails
                            }
                        } else {
                            logger.debug("Consumer {} skipping recently processed event: {}", 
                                consumerId, event.getId());
                        }
                    }
                    
                    // Only clear query execution start if batch was processed successfully
                    if (batchProcessedSuccessfully) {
                        offsetManager.clearQueryExecutionStart(consumerId, sourceIndexName);
                        logger.debug("Consumer {} completed batch processing successfully, cleared query execution start", consumerId);
                    } else {
                        logger.warn("Consumer {} batch processing failed, keeping query execution start protection", consumerId);
                    }
                } else {
                    logger.debug("Consumer {} found no new events", consumerId);
                    // No events found, safe to clear query execution start
                    offsetManager.clearQueryExecutionStart(consumerId, sourceIndexName);
                }
                
                // Manage memory usage of recent cache
                if (recentlyProcessedIds.size() > RECENT_CACHE_SIZE) {
                    logger.debug("Consumer {} clearing recent cache (size: {})", 
                        consumerId, recentlyProcessedIds.size());
                    recentlyProcessedIds.clear();
                }
                
                Thread.sleep(POLL_INTERVAL_MS);
                
            } catch (InterruptedException e) {
                logger.info("Consumer {} thread interrupted", consumerId);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in consumer {} event loop", consumerId, e);
                try {
                    Thread.sleep(5000); // Wait before retrying
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        // Final offset commit
        try {
            commitOffset();
            // Clear query execution start on shutdown
            offsetManager.clearQueryExecutionStart(consumerId, sourceIndexName);
            logger.info("Consumer {} stopped. Session events processed: {}, Total: {}", 
                consumerId, sessionProcessedCount.get(), currentOffset.getTotalProcessed());
        } catch (Exception e) {
            logger.error("Error during final offset commit for consumer {}", consumerId, e);
        }
    }
    
    private List<Event> fetchUnprocessedEvents() {
        try {
            // Use effective start timestamp which handles long-running queries properly
            String effectiveStartTimestamp = offsetManager.getEffectiveStartTimestampISO(consumerId, sourceIndexName);
            
            // Add safety window to catch late-arriving events
            String safetyTimestamp = subtractSeconds(effectiveStartTimestamp, SAFETY_WINDOW_SECONDS);
            
            logger.debug("Consumer {} querying with effective start timestamp: effective={}, safety={}, lastDocId={}", 
                consumerId, effectiveStartTimestamp, safetyTimestamp, currentOffset.getLastProcessedDocId());
            
            // Build query with safety window and exclude already processed document
            Query.Builder queryBuilder = new Query.Builder();
            
            if (currentOffset.getLastProcessedDocId() != null) {
                // Use bool query to combine range and exclusion
                queryBuilder.bool(b -> b
                    .must(m -> m.range(r -> r.field("timestamp")
                        .gt(co.elastic.clients.json.JsonData.of(safetyTimestamp))))
                    .mustNot(mn -> mn.term(t -> t.field("_id")
                        .value(currentOffset.getLastProcessedDocId())))
                );
            } else {
                // Simple range query if no last processed doc ID
                queryBuilder.range(r -> r.field("timestamp")
                    .gt(co.elastic.clients.json.JsonData.of(safetyTimestamp)));
            }
            
            SearchRequest searchRequest = SearchRequest.of(s -> s
                .index(sourceIndexName)
                .query(queryBuilder.build())
                .sort(so -> so.field(f -> f.field("timestamp").order(SortOrder.Asc)))
                .size(BATCH_SIZE)
            );

            SearchResponse<Event> response = client.search(searchRequest, Event.class);
            
            return response.hits().hits().stream()
                .map(Hit::source)
                .filter(event -> event != null && !recentlyProcessedIds.contains(event.getId()))
                .collect(Collectors.toList());
                
        } catch (Exception e) {
            logger.error("Error fetching unprocessed events for consumer {}", consumerId, e);
            return List.of();
        }
    }
    
    private void processEvent(Event event) {
        try {
            // Simulate event processing logic
            logger.info("Consumer {} processing event: {} - Type: {} - User: {} - Data: {}", 
                consumerId, event.getId(), event.getEventType(), event.getUserId(), event.getData());
            
            // Add your actual business logic here
            // For example: validate data, transform data, send to another system, etc.
            
            // Simulate processing time
            Thread.sleep(50 + (int)(Math.random() * 100));
            
            logger.debug("Consumer {} successfully processed event: {}", consumerId, event.getId());
            
        } catch (Exception e) {
            logger.error("Consumer {} error processing event: {}", consumerId, event.getId(), e);
            throw new RuntimeException("Failed to process event", e);
        }
    }
    
    /**
     * Update offset in memory (not yet committed to Elasticsearch)
     * Uses smart update strategy to prevent race conditions:
     * - Only advance timestamp if event is newer (prevents going backwards)
     * - Always update doc ID to prevent duplicates
     */
    private void updateOffsetInMemory(Event event) {
        // Only advance timestamp if event is newer (prevents race condition)
        if (event.getTimestamp().compareTo(currentOffset.getLastProcessedTimestamp()) > 0) {
            currentOffset.setLastProcessedTimestamp(event.getTimestamp());
            logger.debug("Consumer {} advanced timestamp to: {}", consumerId, event.getTimestamp());
        } else {
            logger.debug("Consumer {} keeping current timestamp (event not newer): current={}, event={}", 
                consumerId, currentOffset.getLastProcessedTimestamp(), event.getTimestamp());
        }
        
        // Always update last processed doc ID to prevent duplicates
        currentOffset.setLastProcessedDocId(event.getId());
        currentOffset.incrementTotalProcessed();
        
        logger.debug("Consumer {} updated offset in memory: timestamp={}, docId={}, total={}", 
            consumerId, currentOffset.getLastProcessedTimestamp(), event.getId(), currentOffset.getTotalProcessed());
    }
    
    /**
     * Subtract seconds from a timestamp string to create a safety window
     */
    private String subtractSeconds(String timestamp, int seconds) {
        try {
            Instant instant = Instant.parse(timestamp);
            return instant.minus(seconds, ChronoUnit.SECONDS).toString();
        } catch (Exception e) {
            logger.warn("Error parsing timestamp {}, using original: {}", timestamp, e.getMessage());
            return timestamp;
        }
    }
    
    /**
     * Commit current offset to Elasticsearch
     */
    public void commitOffset() {
        try {
            offsetManager.saveOffset(currentOffset);
            logger.debug("Consumer {} committed offset: timestamp={}, docId={}, total={}", 
                consumerId, currentOffset.getLastProcessedTimestamp(), 
                currentOffset.getLastProcessedDocId(), currentOffset.getTotalProcessed());
        } catch (Exception e) {
            logger.error("Consumer {} error committing offset", consumerId, e);
            throw new RuntimeException("Failed to commit offset", e);
        }
    }
    
    /**
     * Seek to a specific timestamp (useful for replay scenarios)
     */
    public void seekToTimestamp(String timestamp) {
        currentOffset.setLastProcessedTimestamp(timestamp);
        currentOffset.setLastProcessedDocId(null);
        commitOffset();
        recentlyProcessedIds.clear();
        logger.info("Consumer {} seeked to timestamp: {}", consumerId, timestamp);
    }
    
    /**
     * Reset offset to beginning
     */
    public void resetToBeginning() {
        seekToTimestamp("1970-01-01T00:00:00Z");
        logger.info("Consumer {} reset to beginning", consumerId);
    }
    
    public void close() {
        stop();
        
        // Final offset commit
        try {
            commitOffset();
        } catch (Exception e) {
            logger.error("Error during final offset commit for consumer {}", consumerId, e);
        }
        
        // Close resources
        offsetManager.close();
        ElasticsearchConfig.closeClient(client);
        
        logger.info("Consumer {} closed successfully", consumerId);
    }
    
    // Getters for monitoring
    public String getConsumerId() {
        return consumerId;
    }
    
    public String getSourceIndexName() {
        return sourceIndexName;
    }
    
    public long getSessionProcessedCount() {
        return sessionProcessedCount.get();
    }
    
    public long getTotalProcessedCount() {
        return currentOffset.getTotalProcessed();
    }
    
    public boolean isRunning() {
        return running;
    }
    
    public String getLastProcessedTimestamp() {
        return currentOffset.getLastProcessedTimestamp();
    }
    
    public String getLastProcessedDocId() {
        return currentOffset.getLastProcessedDocId();
    }
    
    public OffsetRecord getCurrentOffset() {
        return currentOffset;
    }
    
    public void printConsumerStats() {
        logger.info("=== CONSUMER {} STATS ===", consumerId);
        logger.info("Source Index: {}", sourceIndexName);
        logger.info("Running: {}", running);
        logger.info("Session Processed: {}", sessionProcessedCount.get());
        logger.info("Total Processed: {}", currentOffset.getTotalProcessed());
        logger.info("Last Timestamp: {}", currentOffset.getLastProcessedTimestamp());
        logger.info("Last Doc ID: {}", currentOffset.getLastProcessedDocId());
        logger.info("Recent Cache Size: {}", recentlyProcessedIds.size());
        logger.info("========================");
    }
}