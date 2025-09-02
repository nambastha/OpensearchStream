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
import java.util.stream.Collectors;

/**
 * Read-only consumer that tracks processed events using Elasticsearch-based offset management
 * Kubernetes-safe - no local file dependencies, persists state in Elasticsearch
 * Does not write to source index at all - completely safe for shared environments
 */
public class ReadOnlyEventConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ReadOnlyEventConsumer.class);
    
    private final ElasticsearchClient client;
    private final String sourceIndexName;
    private final ElasticsearchOffsetManager offsetManager;
    private final String consumerId;
    private final AtomicLong processedCount;
    private volatile boolean running;
    
    private static final int BATCH_SIZE = 50;
    private static final long POLL_INTERVAL_MS = 2000;

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
        
        // Load existing offset record to initialize processedCount
        OffsetRecord offsetRecord = offsetManager.loadOffset(consumerId, sourceIndexName);
        this.processedCount.set(offsetRecord.getTotalProcessed());
        logger.info("Initialized consumer '{}' for index '{}' with {} previously processed events", 
            consumerId, sourceIndexName, offsetRecord.getTotalProcessed());
    }


    public void start() {
        if (running) {
            logger.warn("Consumer is already running");
            return;
        }
        
        running = true;
        logger.info("Starting read-only event consumer for source index: {}", sourceIndexName);
        logger.info("Consumer ID: {}", consumerId);
        logger.info("Using Elasticsearch offset management (Kubernetes-safe)");
        
        Thread consumerThread = new Thread(this::consumeEvents);
        consumerThread.setName("ReadOnlyEventConsumer-" + sourceIndexName);
        consumerThread.setDaemon(false);
        consumerThread.start();
    }

    public void stop() {
        logger.info("Stopping read-only event consumer");
        running = false;
    }

    private void consumeEvents() {
        while (running) {
            try {
                List<Event> events = fetchUnprocessedEvents();
                
                if (!events.isEmpty()) {
                    logger.info("Fetched {} unprocessed events", events.size());
                    
                    for (Event event : events) {
                        processEvent(event);
                        
                        long count = processedCount.incrementAndGet();
                        if (count % 50 == 0) {
                            logger.info("Processed {} events", count);
                        }
                        
                        // Update offset in Elasticsearch - this handles deduplication
                        offsetManager.updateOffset(consumerId, sourceIndexName, 
                            event.getTimestamp(), event.getId());
                    }
                } else {
                    logger.debug("No new events found");
                }
                
                Thread.sleep(POLL_INTERVAL_MS);
                
            } catch (InterruptedException e) {
                logger.info("Consumer thread interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error consuming events", e);
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


    private List<Event> fetchUnprocessedEvents() {
        try {
            // Get offset record to determine exactly where to resume
            OffsetRecord offsetRecord = offsetManager.loadOffset(consumerId, sourceIndexName);
            String lastTimestamp = offsetRecord.getLastProcessedTimestamp();
            String lastDocId = offsetRecord.getLastProcessedDocId();
            
            SearchRequest searchRequest;
            
            if (lastDocId != null && !lastDocId.isEmpty()) {
                // We have a last processed document - use compound query to get events after this specific point
                // This prevents duplicates by using both timestamp and document ID for precise positioning
                searchRequest = SearchRequest.of(s -> s
                    .index(sourceIndexName)
                    .query(Query.of(q -> q.bool(b -> b
                        .should(should -> should.range(r -> r.field("timestamp")
                            .gt(co.elastic.clients.json.JsonData.of(lastTimestamp))))
                        .should(should -> should.bool(b2 -> b2
                            .must(must -> must.term(t -> t.field("timestamp").value(lastTimestamp)))
                            .must(must -> must.range(r -> r.field("_id")
                                .gt(co.elastic.clients.json.JsonData.of(lastDocId))))
                        ))
                        .minimumShouldMatch("1")
                    )))
                    .sort(so -> so.field(f -> f.field("timestamp").order(SortOrder.Asc)))
                    .sort(so -> so.field(f -> f.field("_id").order(SortOrder.Asc)))
                    .size(BATCH_SIZE)
                );
                logger.debug("Querying events after timestamp {} and docId {}", lastTimestamp, lastDocId);
            } else {
                // First run or no previous document - start from timestamp
                searchRequest = SearchRequest.of(s -> s
                    .index(sourceIndexName)
                    .query(Query.of(q -> q.range(r -> r.field("timestamp")
                        .gte(co.elastic.clients.json.JsonData.of(lastTimestamp)))))
                    .sort(so -> so.field(f -> f.field("timestamp").order(SortOrder.Asc)))
                    .sort(so -> so.field(f -> f.field("_id").order(SortOrder.Asc)))
                    .size(BATCH_SIZE)
                );
                logger.debug("First run - querying events from timestamp {}", lastTimestamp);
            }

            SearchResponse<Event> response = client.search(searchRequest, Event.class);
            
            return response.hits().hits().stream()
                .map(Hit::source)
                .filter(event -> event != null)
                .collect(Collectors.toList());
                
        } catch (Exception e) {
            logger.error("Error fetching unprocessed events", e);
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
        OffsetRecord offset = offsetManager.loadOffset(consumerId, sourceIndexName);
        return offset.getLastProcessedTimestamp();
    }
    
    public String getLastProcessedDocId() {
        OffsetRecord offset = offsetManager.loadOffset(consumerId, sourceIndexName);
        return offset.getLastProcessedDocId();
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
            
            logger.info("Consumer initialized:");
            logger.info("- Consumer ID: {}", consumer.getConsumerId());
            logger.info("- Source Index: {}", consumer.getSourceIndexName());
            logger.info("- Last processed timestamp: {}", consumer.getLastProcessedTimestamp());
            logger.info("- Last processed document ID: {}", consumer.getLastProcessedDocId());
            
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
