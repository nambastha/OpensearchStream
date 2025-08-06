package com.streaming.opensearch.consumer;


import com.streaming.opensearch.config.OpenSearchConfig;
import com.streaming.opensearch.model.Event;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.UpdateRequest;
import org.opensearch.client.opensearch.core.search.Hit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumer that continuously reads events from OpenSearch and processes them without duplicates
 */
public class EventConsumer {
    private static final Logger logger = LoggerFactory.getLogger(EventConsumer.class);

    private final OpenSearchClient client;
    private final String indexName;
    private final Set<String> processedEventIds;
    private final AtomicLong processedCount;
    private volatile boolean running;
    private String lastProcessedTimestamp;

    private static final int BATCH_SIZE = 50;
    private static final long POLL_INTERVAL_MS = 2000;

    public EventConsumer(String indexName) {
        this.client = OpenSearchConfig.createClient();
        this.indexName = indexName;
        this.processedEventIds = new HashSet<>();
        this.processedCount = new AtomicLong(0);
        this.running = false;
        this.lastProcessedTimestamp = "1970-01-01T00:00:00Z"; // Start from epoch
    }

    public void start() {
        if (running) {
            logger.warn("Consumer is already running");
            return;
        }

        running = true;
        logger.info("Starting event consumer for index: {}", indexName);

        Thread consumerThread = new Thread(this::consumeEvents);
        consumerThread.setName("EventConsumer-" + indexName);
        consumerThread.setDaemon(false);
        consumerThread.start();
    }

    public void stop() {
        logger.info("Stopping event consumer");
        running = false;
    }

    private void consumeEvents() {
        while (running) {
            try {
                List<Event> events = fetchUnprocessedEvents();

                if (!events.isEmpty()) {
                    logger.info("Fetched {} unprocessed events", events.size());

                    for (Event event : events) {
                        if (!processedEventIds.contains(event.getId())) {
                            processEvent(event);
                            markEventAsProcessed(event);
                            processedEventIds.add(event.getId());

                            long count = processedCount.incrementAndGet();
                            if (count % 50 == 0) {
                                logger.info("Processed {} events", count);
                            }

                            // Update last processed timestamp
                            lastProcessedTimestamp = event.getTimestamp();
                        } else {
                            logger.debug("Skipping duplicate event: {}", event.getId());
                        }
                    }
                } else {
                    logger.debug("No new events found");
                }

                // Clean up old processed IDs to prevent memory leak
                if (processedEventIds.size() > 10000) {
                    logger.info("Cleaning up processed event IDs cache");
                    processedEventIds.clear();
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

        logger.info("Event consumer stopped. Total events processed: {}", processedCount.get());
    }

    private List<Event> fetchUnprocessedEvents() {
        try {
            // Query for unprocessed events newer than the last processed timestamp
            BoolQuery boolQuery = BoolQuery.of(b -> b
                    .must(Query.of(q -> q.term(t -> t.field("processed").value(v -> v.booleanValue(false)))))
                    .must(Query.of(q -> q.range(r -> r.field("timestamp").gt(org.opensearch.client.json.JsonData.of(lastProcessedTimestamp)))))
            );

            SearchRequest searchRequest = SearchRequest.of(s -> s
                    .index(indexName)
                    .query(Query.of(q -> q.bool(boolQuery)))
                    .sort(so -> so.field(f -> f.field("timestamp").order(SortOrder.Asc)))
                    .size(BATCH_SIZE)
            );

            SearchResponse<Event> response = client.search(searchRequest, Event.class);

            return response.hits().hits().stream()
                    .map(Hit::source)
                    .toList();

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

    public void close() {
        stop();
        OpenSearchConfig.closeClient(client);
    }

    public long getProcessedCount() {
        return processedCount.get();
    }

    public boolean isRunning() {
        return running;
    }

    public int getProcessedEventsCacheSize() {
        return processedEventIds.size();
    }
}
