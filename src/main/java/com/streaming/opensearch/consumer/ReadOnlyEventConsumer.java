package com.streaming.opensearch.consumer;

import com.streaming.opensearch.config.OpenSearchConfig;
import com.streaming.opensearch.model.Event;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Read-only consumer that tracks processed events using local file persistence
 * Does not write to OpenSearch at all - completely safe for shared environments
 */
public class ReadOnlyEventConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ReadOnlyEventConsumer.class);
    
    private final OpenSearchClient client;
    private final String sourceIndexName;
    private final String trackingFilePath;
    private final String checkpointFilePath;
    private final Set<String> processedEventIds;
    private final AtomicLong processedCount;
    private volatile boolean running;
    private String lastProcessedTimestamp;
    
    private static final int BATCH_SIZE = 50;
    private static final long POLL_INTERVAL_MS = 2000;
    private static final int MAX_MEMORY_CACHE = 10000;

    public ReadOnlyEventConsumer(String sourceIndexName) {
        this.client = OpenSearchConfig.createClient();
        this.sourceIndexName = sourceIndexName;
        this.trackingFilePath = "processed_events_" + sourceIndexName + ".txt";
        this.checkpointFilePath = "checkpoint_" + sourceIndexName + ".txt";
        this.processedEventIds = new HashSet<>();
        this.processedCount = new AtomicLong(0);
        this.running = false;
        this.lastProcessedTimestamp = "1970-01-01T00:00:00Z";
        
        // Load previously processed events from file
        loadProcessedEventsFromFile();
        loadCheckpointFromFile();
    }

    private void loadProcessedEventsFromFile() {
        Path filePath = Paths.get(trackingFilePath);
        if (!Files.exists(filePath)) {
            logger.info("No existing tracking file found. Starting fresh.");
            return;
        }
        
        try {
            List<String> lines = Files.readAllLines(filePath);
            processedEventIds.addAll(lines);
            logger.info("Loaded {} processed event IDs from file: {}", 
                processedEventIds.size(), trackingFilePath);
        } catch (IOException e) {
            logger.warn("Could not load processed events from file: {}", e.getMessage());
        }
    }

    private void loadCheckpointFromFile() {
        Path filePath = Paths.get(checkpointFilePath);
        if (!Files.exists(filePath)) {
            logger.info("No checkpoint file found. Starting from epoch.");
            return;
        }
        
        try {
            List<String> lines = Files.readAllLines(filePath);
            if (!lines.isEmpty()) {
                lastProcessedTimestamp = lines.get(0).trim();
                logger.info("Loaded checkpoint timestamp: {}", lastProcessedTimestamp);
            }
        } catch (IOException e) {
            logger.warn("Could not load checkpoint from file: {}", e.getMessage());
        }
    }

    private void saveProcessedEventToFile(String eventId) {
        try {
            Files.write(Paths.get(trackingFilePath), 
                (eventId + System.lineSeparator()).getBytes(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            logger.error("Error saving processed event to file: {}", e.getMessage());
        }
    }

    private void saveCheckpointToFile(String timestamp) {
        try {
            Files.write(Paths.get(checkpointFilePath), 
                timestamp.getBytes(),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            logger.error("Error saving checkpoint to file: {}", e.getMessage());
        }
    }

    public void start() {
        if (running) {
            logger.warn("Consumer is already running");
            return;
        }
        
        running = true;
        logger.info("Starting read-only event consumer for source index: {}", sourceIndexName);
        logger.info("Tracking file: {}", trackingFilePath);
        logger.info("Checkpoint file: {}", checkpointFilePath);
        
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
                        if (!processedEventIds.contains(event.getId())) {
                            processEvent(event);
                            recordEventAsProcessed(event);
                            
                            long count = processedCount.incrementAndGet();
                            if (count % 50 == 0) {
                                logger.info("Processed {} events", count);
                            }
                            
                            // Update checkpoint
                            lastProcessedTimestamp = event.getTimestamp();
                            if (count % 10 == 0) { // Save checkpoint every 10 events
                                saveCheckpointToFile(lastProcessedTimestamp);
                            }
                        } else {
                            logger.debug("Skipping duplicate event: {}", event.getId());
                        }
                    }
                } else {
                    logger.debug("No new events found");
                }
                
                // Manage memory usage
                if (processedEventIds.size() > MAX_MEMORY_CACHE) {
                    logger.info("Memory cache size exceeded. Clearing cache and relying on file persistence.");
                    processedEventIds.clear();
                    // Reload recent events to avoid immediate duplicates
                    loadRecentProcessedEvents();
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
        
        // Save final checkpoint
        saveCheckpointToFile(lastProcessedTimestamp);
        logger.info("Read-only event consumer stopped. Total events processed: {}", processedCount.get());
    }

    private void loadRecentProcessedEvents() {
        try {
            Path filePath = Paths.get(trackingFilePath);
            if (!Files.exists(filePath)) return;
            
            List<String> lines = Files.readAllLines(filePath);
            // Load only the last 5000 processed events to manage memory
            int startIndex = Math.max(0, lines.size() - 5000);
            processedEventIds.addAll(lines.subList(startIndex, lines.size()));
            
            logger.info("Reloaded {} recent processed event IDs", processedEventIds.size());
        } catch (IOException e) {
            logger.warn("Could not reload recent processed events: {}", e.getMessage());
        }
    }

    private List<Event> fetchUnprocessedEvents() {
        try {
            // Query for events newer than the last processed timestamp
            SearchRequest searchRequest = SearchRequest.of(s -> s
                .index(sourceIndexName)
                .query(Query.of(q -> q.range(r -> r.field("timestamp")
                    .gt(org.opensearch.client.json.JsonData.of(lastProcessedTimestamp)))))
                .sort(so -> so.field(f -> f.field("timestamp").order(SortOrder.Asc)))
                .size(BATCH_SIZE)
            );

            SearchResponse<Event> response = client.search(searchRequest, Event.class);
            
            return response.hits().hits().stream()
                .map(Hit::source)
                .filter(event -> event != null && !processedEventIds.contains(event.getId()))
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

    private void recordEventAsProcessed(Event event) {
        processedEventIds.add(event.getId());
        saveProcessedEventToFile(event.getId());
        logger.debug("Recorded event as processed in file: {}", event.getId());
    }

    public void close() {
        stop();
        // Save final checkpoint
        saveCheckpointToFile(lastProcessedTimestamp);
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

    public String getTrackingFilePath() {
        return trackingFilePath;
    }

    public String getCheckpointFilePath() {
        return checkpointFilePath;
    }

    public String getLastProcessedTimestamp() {
        return lastProcessedTimestamp;
    }
}
