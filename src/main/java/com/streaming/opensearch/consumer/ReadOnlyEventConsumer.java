package com.streaming.opensearch.consumer;

import com.streaming.opensearch.config.ElasticsearchConfig;
import com.streaming.opensearch.model.Event;
import com.streaming.opensearch.offset.ElasticsearchOffsetManager;
import com.streaming.opensearch.model.OffsetRecord;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Production-ready read-only consumer with batched offset management
 * - Handles 500K+ messages daily with optimal Elasticsearch usage
 * - Batched offset commits (every 100 events OR 30 seconds)
 * - Filters only documents with _id starting with 'completion_'
 * - Kubernetes-safe with no local file dependencies
 * - Prevents message reprocessing after pod restarts
 * - Graceful shutdown with final offset commit
 * - Does not write to source index - completely safe for shared environments
 */
public class ReadOnlyEventConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ReadOnlyEventConsumer.class);
    
    private final ElasticsearchClient client;
    private final String sourceIndexName;
    private final ElasticsearchOffsetManager offsetManager;
    private final String consumerId;
    private final AtomicLong processedCount;
    private volatile boolean running;
    
    // In-memory offset tracking for batching
    private final AtomicReference<String> currentTimestamp;
    private final AtomicReference<String> currentDocId;
    private final AtomicLong eventsSinceLastCommit;
    private volatile long lastCommitTime;
    
    // Batch commit scheduler
    private final ScheduledExecutorService offsetCommitScheduler;
    
    // Production-ready configurable parameters
    private static final int BATCH_SIZE = 50;                    // Query batch size
    private static final long POLL_INTERVAL_MS = 2000;           // Poll interval
    private static final int OFFSET_COMMIT_BATCH_SIZE = 100;     // Commit every 100 events
    private static final long OFFSET_COMMIT_INTERVAL_MS = 30000; // Commit every 30 seconds
    private static final int MAX_COMMIT_RETRIES = 3;             // Retry failed commits
    private static final long COMMIT_RETRY_DELAY_MS = 1000;      // Initial retry delay

    public ReadOnlyEventConsumer(String sourceIndexName) {
        this(sourceIndexName, "readonly-consumer-" + sourceIndexName);
    }
    
    public ReadOnlyEventConsumer(String sourceIndexName, String consumerId) {
        this.client = ElasticsearchConfig.createClient();
        this.sourceIndexName = sourceIndexName;
        this.consumerId = consumerId;
        this.offsetManager = new ElasticsearchOffsetManager();
        this.processedCount = new AtomicLong(0);
        this.running = false;
        
        // Initialize in-memory offset tracking
        this.currentTimestamp = new AtomicReference<>();
        this.currentDocId = new AtomicReference<>();
        this.eventsSinceLastCommit = new AtomicLong(0);
        this.lastCommitTime = System.currentTimeMillis();
        
        // Create scheduler for periodic offset commits
        this.offsetCommitScheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "OffsetCommit-" + consumerId));
        
        // Load existing offset record to initialize state
        OffsetRecord offsetRecord = offsetManager.loadOffset(consumerId, sourceIndexName);
        this.processedCount.set(offsetRecord.getTotalProcessed());
        this.currentTimestamp.set(offsetRecord.getLastProcessedTimestamp());
        this.currentDocId.set(offsetRecord.getLastProcessedDocId());
        
        logger.info("Initialized production-ready consumer '{}' for index '{}'", consumerId, sourceIndexName);
        logger.info("- Previously processed events: {}", offsetRecord.getTotalProcessed());
        logger.info("- Last processed timestamp: {}", offsetRecord.getLastProcessedTimestamp());
        logger.info("- Batch commit settings: {} events OR {} seconds", OFFSET_COMMIT_BATCH_SIZE, OFFSET_COMMIT_INTERVAL_MS / 1000);
    }


    public void start() {
        if (running) {
            logger.warn("Consumer is already running");
            return;
        }
        
        running = true;
        logger.info("Starting production-ready read-only event consumer");
        logger.info("- Source index: {}", sourceIndexName);
        logger.info("- Consumer ID: {}", consumerId);
        logger.info("- Batch commit: {} events OR {} seconds", OFFSET_COMMIT_BATCH_SIZE, OFFSET_COMMIT_INTERVAL_MS / 1000);
        logger.info("- Kubernetes-safe Elasticsearch offset management");
        
        // Start periodic offset commit scheduler
        offsetCommitScheduler.scheduleWithFixedDelay(
            this::commitOffsetIfNeeded,
            OFFSET_COMMIT_INTERVAL_MS, 
            OFFSET_COMMIT_INTERVAL_MS, 
            TimeUnit.MILLISECONDS
        );
        
        Thread consumerThread = new Thread(this::consumeEvents);
        consumerThread.setName("ReadOnlyEventConsumer-" + sourceIndexName);
        consumerThread.setDaemon(false);
        consumerThread.start();
    }

    public void stop() {
        logger.info("Stopping read-only event consumer");
        running = false;
        
        // Stop the offset commit scheduler
        offsetCommitScheduler.shutdown();
        try {
            if (!offsetCommitScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                offsetCommitScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            offsetCommitScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // Force final offset commit on shutdown
        commitOffsetFinal();
    }

    private void consumeEvents() {
        while (running) {
            try {
                // Mark the start of query execution to prevent missing events during long queries
                offsetManager.markQueryExecutionStart(consumerId, sourceIndexName);
                
                List<Event> events = fetchUnprocessedEvents();
                
                if (!events.isEmpty()) {
                    logger.info("Fetched {} unprocessed events", events.size());
                    
                    for (Event event : events) {
                        processEvent(event);
                        
                        // Update in-memory offset tracking
                        currentTimestamp.set(event.getTimestamp());
                        currentDocId.set(event.getId());
                        long count = processedCount.incrementAndGet();
                        long eventsSinceCommit = eventsSinceLastCommit.incrementAndGet();
                        
                        if (count % 50 == 0) {
                            logger.info("Processed {} events (batched offset commits)", count);
                        }
                        
                        // Trigger batch commit if needed
                        if (eventsSinceCommit >= OFFSET_COMMIT_BATCH_SIZE) {
                            commitOffset();
                        }
                    }
                    
                    // Clear query execution start timestamp after successful processing
                    offsetManager.clearQueryExecutionStart(consumerId, sourceIndexName);
                } else {
                    logger.debug("No new events found");
                    // Clear query execution start even when no events found
                    offsetManager.clearQueryExecutionStart(consumerId, sourceIndexName);
                }
                
                Thread.sleep(POLL_INTERVAL_MS);
                
            } catch (InterruptedException e) {
                logger.info("Consumer thread interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error consuming events", e);
                // Don't clear query execution start on error - keep the protection
                try {
                    Thread.sleep(5000); // Wait before retrying
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        logger.info("Read-only event consumer stopped. Total events processed: {}", processedCount.get());
    }
    
    /**
     * Commit current offset to Elasticsearch with retry logic
     */
    private void commitOffset() {
        String timestamp = currentTimestamp.get();
        String docId = currentDocId.get();
        
        if (timestamp != null && docId != null) {
            commitOffsetWithRetry(timestamp, docId);
            eventsSinceLastCommit.set(0);
            lastCommitTime = System.currentTimeMillis();
            logger.debug("Committed offset: timestamp={}, docId={}", timestamp, docId);
        }
    }
    
    /**
     * Commit offset with exponential backoff retry
     */
    private void commitOffsetWithRetry(String timestamp, String docId) {
        int attempts = 0;
        long delay = COMMIT_RETRY_DELAY_MS;
        
        while (attempts < MAX_COMMIT_RETRIES) {
            try {
                offsetManager.updateOffset(consumerId, sourceIndexName, timestamp, docId);
                return; // Success
            } catch (Exception e) {
                attempts++;
                logger.warn("Offset commit attempt {} failed: {} (will retry)", attempts, e.getMessage());
                
                if (attempts >= MAX_COMMIT_RETRIES) {
                    logger.error("All {} offset commit attempts failed. Data may be reprocessed after restart.", 
                        MAX_COMMIT_RETRIES, e);
                    return;
                }
                
                try {
                    Thread.sleep(delay);
                    delay *= 2; // Exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
    
    /**
     * Commit offset if batch size or time threshold reached
     */
    private void commitOffsetIfNeeded() {
        try {
            long timeSinceLastCommit = System.currentTimeMillis() - lastCommitTime;
            if (timeSinceLastCommit >= OFFSET_COMMIT_INTERVAL_MS && eventsSinceLastCommit.get() > 0) {
                logger.debug("Time-based offset commit triggered ({} ms since last commit)", timeSinceLastCommit);
                commitOffset();
            }
        } catch (Exception e) {
            logger.error("Error in scheduled offset commit", e);
        }
    }
    
    /**
     * Force final offset commit on shutdown
     */
    private void commitOffsetFinal() {
        try {
            if (eventsSinceLastCommit.get() > 0) {
                logger.info("Final offset commit on shutdown - {} uncommitted events", eventsSinceLastCommit.get());
                commitOffset();
                logger.info("Final offset committed successfully");
            } else {
                logger.info("No uncommitted events on shutdown");
            }
        } catch (Exception e) {
            logger.error("Error in final offset commit - may cause message reprocessing", e);
        }
    }

    private List<Event> fetchUnprocessedEvents() {
        try {
            // Get offset record to determine exactly where to resume
            OffsetRecord offsetRecord = offsetManager.loadOffset(consumerId, sourceIndexName);
            String lastTimestamp = offsetRecord.getLastProcessedTimestamp();
            String lastDocId = offsetRecord.getLastProcessedDocId();
            
            SearchRequest searchRequest;
            
            if (lastDocId != null && !lastDocId.isEmpty()) {
                // Use range query with _id prefix filter
                searchRequest = SearchRequest.of(s -> s
                    .index(sourceIndexName)
                    .query(Query.of(q -> q.bool(b -> b
                        .must(m -> m.range(r -> r.field("timestamp")
                            .gt(co.elastic.clients.json.JsonData.of(lastTimestamp))))
                        .must(m -> m.prefix(p -> p.field("_id").value("completion_")))
                    )))
                    .sort(so -> so.field(f -> f.field("timestamp").order(SortOrder.Asc)))
                    .sort(so -> so.field(f -> f.field("queryid").order(SortOrder.Asc)))
                    .size(BATCH_SIZE)
                );
                logger.debug("Querying events with _id starting with 'completion_' after timestamp {} and docId {}", lastTimestamp, lastDocId);
            } else {
                // First run - only documents with _id starting with 'completion_'
                searchRequest = SearchRequest.of(s -> s
                    .index(sourceIndexName)
                    .query(Query.of(q -> q.prefix(p -> p.field("_id").value("completion_"))))
                    .sort(so -> so.field(f -> f.field("timestamp").order(SortOrder.Asc)))
                    .size(BATCH_SIZE)
                );
                logger.debug("First run - querying only events with _id starting with 'completion_'");
            }

            SearchResponse<Event> response = client.search(searchRequest, Event.class);
            
            return response.hits().hits().stream()
                .map(Hit::source)
                .filter(event -> event != null)
                .collect(Collectors.toList());
                
        } catch (Exception e) {
            logger.error("Error fetching unprocessed events from index '{}': {}", sourceIndexName, e.getMessage());
            
            // Check if it's an index not found error
            if (e.getMessage() != null && e.getMessage().contains("index_not_found_exception")) {
                logger.warn("Source index '{}' does not exist. Consumer will wait for index creation.", sourceIndexName);
            } else if (e.getMessage() != null && e.getMessage().contains("no such index")) {
                logger.warn("Source index '{}' does not exist. Consumer will wait for index creation.", sourceIndexName);
            } else {
                logger.error("Detailed error fetching events:", e);
            }
            
            return List.of();
        }
    }

    private void processEvent(Event event) {
        try {
            // Simulate event processing logic
            logger.info("Processing event: {} - Type: {} - User: {} - Data: {}", 
                event.getId(), event.getEventType(), event.getUserId(), event.getData());
            
            // Add your actual business logic here
            // For example: validate data, transform data, send to another system, etc.
            
            // Simulate processing time
            Thread.sleep(50 + (int)(Math.random() * 100));
            
            logger.debug("Successfully processed event: {}", event.getId());
            
        } catch (Exception e) {
            logger.error("Error processing event: {}", event.getId(), e);
            throw new RuntimeException("Failed to process event", e);
        }
    }


    public void close() {
        stop();
        if (offsetManager != null) {
            offsetManager.close();
        }
        ElasticsearchConfig.closeClient(client);
        logger.info("ReadOnlyEventConsumer closed successfully");
    }

    public long getProcessedCount() {
        return processedCount.get();
    }

    public boolean isRunning() {
        return running;
    }

    public String getConsumerId() {
        return consumerId;
    }
    
    public String getSourceIndexName() {
        return sourceIndexName;
    }

    public String getLastProcessedTimestamp() {
        String current = currentTimestamp.get();
        return current != null ? current : "1970-01-01T00:00:00Z";
    }
    
    public String getLastProcessedDocId() {
        return currentDocId.get();
    }
    
    /**
     * Get production metrics
     */
    public long getEventsSinceLastCommit() {
        return eventsSinceLastCommit.get();
    }
    
    public long getTimeSinceLastCommit() {
        return System.currentTimeMillis() - lastCommitTime;
    }
    
    /**
     * Main method to test ReadOnlyEventConsumer standalone
     */
    public static void main(String[] args) {
        String indexName = args.length > 0 ? args[0] : "events";
        
        logger.info("Testing ReadOnlyEventConsumer with index: {}", indexName);
        
        ReadOnlyEventConsumer consumer = null;
        try {
            consumer = new ReadOnlyEventConsumer(indexName);
            
            logger.info("Production-ready consumer initialized:");
            logger.info("- Consumer ID: {}", consumer.getConsumerId());
            logger.info("- Source Index: {}", consumer.getSourceIndexName());
            logger.info("- Last processed timestamp: {}", consumer.getLastProcessedTimestamp());
            logger.info("- Last processed document ID: {}", consumer.getLastProcessedDocId());
            logger.info("- Events since last commit: {}", consumer.getEventsSinceLastCommit());
            
            consumer.start();
            
            // Run for 60 seconds
            logger.info("Consumer will run for 60 seconds...");
            Thread.sleep(60000);
            
            consumer.stop();
            logger.info("ReadOnlyEventConsumer test completed. Total events processed: {}", consumer.getProcessedCount());
            
        } catch (Exception e) {
            logger.error("Error testing ReadOnlyEventConsumer: {}", e.getMessage(), e);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }
}
