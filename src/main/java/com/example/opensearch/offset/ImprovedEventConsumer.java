package com.example.opensearch.offset;

import com.example.opensearch.config.OpenSearchConfig;
import com.example.opensearch.model.Event;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.UpdateRequest;
import org.opensearch.client.opensearch.core.search.Hit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Improved Event Consumer using OpenSearch sequence numbers for offset management
 * Provides reliable, persistent offset tracking that survives container restarts
 */
public class ImprovedEventConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ImprovedEventConsumer.class);
    
    private final OpenSearchClient client;
    private final String indexName;
    private final String consumerGroupId;
    private final OffsetManager offsetManager;
    private final AtomicLong processedCount;
    private volatile boolean running;
    
    private ConsumerOffset currentOffset;
    
    private static final int BATCH_SIZE = 50;
    private static final long POLL_INTERVAL_MS = 2000;
    private static final int OFFSET_COMMIT_INTERVAL = 10; // Commit every 10 processed events
    
    public ImprovedEventConsumer(String indexName, String consumerGroupId) {
        this.client = OpenSearchConfig.createClient();
        this.indexName = indexName;
        this.consumerGroupId = consumerGroupId;
        this.offsetManager = new OffsetManager(client);
        this.processedCount = new AtomicLong(0);
        this.running = false;
        
        // Load existing offset or create new one
        loadOrCreateOffset();
    }
    
    private void loadOrCreateOffset() {
        Optional<ConsumerOffset> existingOffset = offsetManager.loadOffset(consumerGroupId, indexName);
        
        if (existingOffset.isPresent()) {
            this.currentOffset = existingOffset.get();
            logger.info("Resuming from offset - seq_no: {}, primary_term: {}", 
                currentOffset.getLastSeqNo(), currentOffset.getLastPrimaryTerm());
        } else {
            this.currentOffset = new ConsumerOffset(consumerGroupId, indexName);
            logger.info("Starting fresh consumer for group: {}, index: {}", consumerGroupId, indexName);
        }
    }
    
    public void start() {
        if (running) {
            logger.warn("Consumer is already running");
            return;
        }
        
        running = true;
        logger.info("Starting improved event consumer - Group: {}, Index: {}", consumerGroupId, indexName);
        
        Thread consumerThread = new Thread(this::consumeEvents);
        consumerThread.setName("ImprovedEventConsumer-" + consumerGroupId + "-" + indexName);
        consumerThread.setDaemon(false);
        consumerThread.start();
    }
    
    public void stop() {
        logger.info("Stopping improved event consumer");
        running = false;
    }
    
    private void consumeEvents() {
        int eventsSinceLastCommit = 0;
        
        while (running) {
            try {
                List<EventWithMetadata> events = fetchUnprocessedEvents();
                
                if (!events.isEmpty()) {
                    logger.info("Fetched {} unprocessed events", events.size());
                    
                    for (EventWithMetadata eventWithMeta : events) {
                        processEvent(eventWithMeta.getEvent());
                        markEventAsProcessed(eventWithMeta.getEvent());
                        
                        // Update current offset position
                        currentOffset.updatePosition(eventWithMeta.getSeqNo(), eventWithMeta.getPrimaryTerm());
                        
                        long count = processedCount.incrementAndGet();
                        eventsSinceLastCommit++;
                        
                        if (count % 50 == 0) {
                            logger.info("Processed {} events", count);
                        }
                        
                        // Periodic offset commits
                        if (eventsSinceLastCommit >= OFFSET_COMMIT_INTERVAL) {
                            commitCurrentOffset();
                            eventsSinceLastCommit = 0;
                        }
                    }
                    
                    // Final commit after batch processing
                    if (eventsSinceLastCommit > 0) {
                        commitCurrentOffset();
                        eventsSinceLastCommit = 0;
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
        
        // Final offset commit on shutdown
        commitCurrentOffset();
        logger.info("Improved event consumer stopped. Total events processed: {}", processedCount.get());
    }
    
    private List<EventWithMetadata> fetchUnprocessedEvents() {
        try {
            SearchRequest searchRequest = SearchRequest.of(s -> s
                .index(indexName)
                .query(Query.of(q -> q.bool(b -> b
                    .must(Query.of(mustQ -> mustQ.term(t -> t.field("processed").value(v -> v.booleanValue(false)))))
                    .must(Query.of(mustQ -> mustQ.range(r -> r
                        .field("_seq_no")
                        .gt(org.opensearch.client.json.JsonData.of(currentOffset.getLastSeqNo()))
                    )))
                )))
                .sort(so -> so.field(f -> f.field("_seq_no").order(SortOrder.Asc)))
                .size(BATCH_SIZE)
                .seqNoPrimaryTerm(true) // Include sequence numbers in response
            );
            
            SearchResponse<Event> response = client.search(searchRequest, Event.class);
            
            List<EventWithMetadata> eventsWithMetadata = new ArrayList<>();
            
            for (Hit<Event> hit : response.hits().hits()) {
                if (hit.source() != null && hit.seqNo() != null && hit.primaryTerm() != null) {
                    EventWithMetadata eventWithMeta = new EventWithMetadata(
                        hit.source(),
                        hit.seqNo().longValue(),
                        hit.primaryTerm().longValue(),
                        hit.id()
                    );
                    eventsWithMetadata.add(eventWithMeta);
                }
            }
            
            return eventsWithMetadata;
            
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
    
    private void markEventAsProcessed(Event event) {
        try {
            UpdateRequest<Event, Event> updateRequest = UpdateRequest.of(u -> u
                .index(indexName)
                .id(event.getId())
                .doc(new Event() {{
                    setProcessed(true);
                }})
            );
            
            client.update(updateRequest, Event.class);
            logger.debug("Marked event as processed: {}", event.getId());
            
        } catch (Exception e) {
            logger.error("Error marking event as processed: {}", event.getId(), e);
        }
    }
    
    private void commitCurrentOffset() {
        try {
            boolean success = offsetManager.commitOffset(currentOffset);
            if (success) {
                logger.debug("Committed offset - seq_no: {}, primary_term: {}", 
                    currentOffset.getLastSeqNo(), currentOffset.getLastPrimaryTerm());
            } else {
                logger.warn("Failed to commit offset");
            }
        } catch (Exception e) {
            logger.error("Error committing offset", e);
        }
    }
    
    /**
     * Reset consumer offset to start from beginning
     */
    public void resetOffset() {
        logger.info("Resetting consumer offset for group: {}, index: {}", consumerGroupId, indexName);
        offsetManager.deleteOffset(consumerGroupId, indexName);
        this.currentOffset = new ConsumerOffset(consumerGroupId, indexName);
    }
    
    public void close() {
        stop();
        commitCurrentOffset(); // Final commit
        offsetManager.close();
        OpenSearchConfig.closeClient(client);
    }
    
    // Getters for monitoring
    public long getProcessedCount() {
        return processedCount.get();
    }
    
    public boolean isRunning() {
        return running;
    }
    
    public ConsumerOffset getCurrentOffset() {
        return currentOffset;
    }
    
    public String getConsumerGroupId() {
        return consumerGroupId;
    }
}