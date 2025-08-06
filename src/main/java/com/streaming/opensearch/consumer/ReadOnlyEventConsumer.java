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
    private static final long CLEANUP_INTERVAL_DAYS = 7;
    private static final long CLEANUP_INTERVAL_MS = CLEANUP_INTERVAL_DAYS * 24 * 60 * 60 * 1000;
    private static final int MAX_TRACKING_FILE_LINES = 50000; // Limit tracking file size
    private static final long DISASTER_RECOVERY_HOURS = 24;   // Look back 24 hours on file loss

    public ReadOnlyEventConsumer(String sourceIndexName) {
        this(sourceIndexName, null);
    }
    
    public ReadOnlyEventConsumer(String sourceIndexName, String offsetStoragePath) {
        this.client = OpenSearchConfig.createClient();
        this.sourceIndexName = sourceIndexName;
        
        // Support configurable storage path for Kubernetes PVC mounting
        String basePath = offsetStoragePath != null ? offsetStoragePath : 
            System.getenv().getOrDefault("OFFSET_STORAGE_PATH", ".");
        
        this.trackingFilePath = basePath + "/processed_events_" + sourceIndexName + ".txt";
        this.checkpointFilePath = basePath + "/checkpoint_" + sourceIndexName + ".txt";
        this.processedEventIds = new HashSet<>();
        this.processedCount = new AtomicLong(0);
        this.running = false;
        this.lastProcessedTimestamp = "1970-01-01T00:00:00Z";
        
        // Ensure offset storage directory exists
        try {
            Files.createDirectories(Paths.get(basePath));
        } catch (IOException e) {
            logger.warn("Could not create offset storage directory: {}", e.getMessage());
        }
        
        // Load previously processed events from file
        loadProcessedEventsFromFile();
        loadCheckpointFromFile();
        
        // Check if cleanup is needed
        checkAndCleanupOldFiles();
    }

    /**
     * Load processed events from file with memory optimization
     * Only loads recent events to prevent memory issues with large files
     */
    private void loadProcessedEventsFromFile() {
        Path filePath = Paths.get(trackingFilePath);
        if (!Files.exists(filePath)) {
            logger.info("No existing tracking file found. Starting fresh.");
            return;
        }
        
        try {
            List<String> lines = Files.readAllLines(filePath);
            
            // Memory optimization: Only load recent events if file is large
            if (lines.size() > MAX_TRACKING_FILE_LINES) {
                logger.warn("Tracking file has {} lines, loading only recent {} events for memory efficiency", 
                    lines.size(), MAX_TRACKING_FILE_LINES);
                
                int startIndex = lines.size() - MAX_TRACKING_FILE_LINES;
                processedEventIds.addAll(lines.subList(startIndex, lines.size()));
                
                logger.info("Loaded {} recent processed event IDs from large file: {}", 
                    processedEventIds.size(), trackingFilePath);
            } else {
                processedEventIds.addAll(lines);
                logger.info("Loaded {} processed event IDs from file: {}", 
                    processedEventIds.size(), trackingFilePath);
            }
            
        } catch (IOException e) {
            logger.warn("Could not load processed events from file: {}", e.getMessage());
        }
    }

    /**
     * Load checkpoint from file with disaster recovery fallback
     * If checkpoint is missing, attempts intelligent recovery based on tracking file
     */
    private void loadCheckpointFromFile() {
        Path filePath = Paths.get(checkpointFilePath);
        if (!Files.exists(filePath)) {
            logger.warn("No checkpoint file found. Attempting disaster recovery...");
            attemptDisasterRecovery();
            return;
        }
        
        try {
            List<String> lines = Files.readAllLines(filePath);
            if (!lines.isEmpty()) {
                lastProcessedTimestamp = lines.get(0).trim();
                logger.info("Loaded checkpoint timestamp: {}", lastProcessedTimestamp);
            } else {
                logger.warn("Checkpoint file is empty. Attempting disaster recovery...");
                attemptDisasterRecovery();
            }
        } catch (IOException e) {
            logger.warn("Could not load checkpoint from file: {}. Attempting disaster recovery...", e.getMessage());
            attemptDisasterRecovery();
        }
    }
    
    /**
     * Disaster recovery when checkpoint file is lost
     * Uses intelligent fallback strategies to minimize reprocessing
     */
    private void attemptDisasterRecovery() {
        logger.info("=== DISASTER RECOVERY MODE ===");
        
        // Strategy 1: Check for archived checkpoint files
        String recoveredTimestamp = tryRecoverFromArchives();
        if (recoveredTimestamp != null) {
            lastProcessedTimestamp = recoveredTimestamp;
            logger.info("✅ Recovered checkpoint from archive: {}", lastProcessedTimestamp);
            return;
        }
        
        // Strategy 2: Use recent timestamp (24 hours ago) to minimize reprocessing
        String safeTimestamp = calculateSafeRecoveryTimestamp();
        lastProcessedTimestamp = safeTimestamp;
        logger.warn("⚠️  Using safe recovery timestamp ({}h ago): {}", 
            DISASTER_RECOVERY_HOURS, lastProcessedTimestamp);
        logger.warn("⚠️  This may cause reprocessing of events from the last {} hours", 
            DISASTER_RECOVERY_HOURS);
        
        // Save the recovery timestamp as new checkpoint
        saveCheckpointToFile(lastProcessedTimestamp);
        logger.info("💾 Saved recovery checkpoint for future use");
    }
    
    /**
     * Try to recover checkpoint from archived files
     */
    private String tryRecoverFromArchives() {
        try {
            String baseFileName = checkpointFilePath.replace(".txt", "_archived_");
            Path parentDir = Paths.get(checkpointFilePath).getParent();
            if (parentDir == null) parentDir = Paths.get(".");
            
            // Look for archived checkpoint files
            List<Path> archiveFiles = Files.list(parentDir)
                .filter(path -> path.getFileName().toString().startsWith(
                    Paths.get(baseFileName).getFileName().toString()))
                .sorted((p1, p2) -> {
                    try {
                        return Files.getLastModifiedTime(p2).compareTo(Files.getLastModifiedTime(p1));
                    } catch (IOException e) {
                        return 0;
                    }
                })
                .collect(Collectors.toList());
            
            if (!archiveFiles.isEmpty()) {
                Path latestArchive = archiveFiles.get(0);
                List<String> lines = Files.readAllLines(latestArchive);
                if (!lines.isEmpty()) {
                    logger.info("Found archived checkpoint: {}", latestArchive);
                    return lines.get(0).trim();
                }
            }
            
        } catch (Exception e) {
            logger.debug("Could not recover from archives: {}", e.getMessage());
        }
        
        return null;
    }
    
    /**
     * Calculate a safe recovery timestamp (24 hours ago)
     * This minimizes reprocessing while ensuring no events are missed
     */
    private String calculateSafeRecoveryTimestamp() {
        long recoveryTimeMs = System.currentTimeMillis() - (DISASTER_RECOVERY_HOURS * 60 * 60 * 1000);
        return java.time.Instant.ofEpochMilli(recoveryTimeMs).toString();
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
    
    /**
     * Check if tracking file is older than 7 days and cleanup if needed
     */
    private void checkAndCleanupOldFiles() {
        try {
            Path trackingFile = Paths.get(trackingFilePath);
            if (!Files.exists(trackingFile)) {
                return; // No file to cleanup
            }
            
            long fileAge = System.currentTimeMillis() - Files.getLastModifiedTime(trackingFile).toMillis();
            
            if (fileAge > CLEANUP_INTERVAL_MS) {
                logger.info("Tracking file is {} days old, performing cleanup", fileAge / (24 * 60 * 60 * 1000));
                cleanupTrackingFile();
            } else {
                logger.debug("Tracking file is {} days old, no cleanup needed", fileAge / (24 * 60 * 60 * 1000));
            }
            
        } catch (Exception e) {
            logger.warn("Error checking file age for cleanup: {}", e.getMessage());
        }
    }
    
    /**
     * Cleanup old tracking file by archiving and creating fresh file
     */
    private void cleanupTrackingFile() {
        try {
            Path trackingFile = Paths.get(trackingFilePath);
            
            if (Files.exists(trackingFile)) {
                // Create archive filename with timestamp
                String timestamp = java.time.LocalDateTime.now().format(
                    java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
                String archiveFileName = trackingFilePath.replace(".txt", "_archived_" + timestamp + ".txt");
                
                // Move current file to archive
                Files.move(trackingFile, Paths.get(archiveFileName));
                logger.info("Archived old tracking file to: {}", archiveFileName);
                
                // Clear in-memory cache since we archived the file
                processedEventIds.clear();
                logger.info("Cleared in-memory processed events cache after cleanup");
                
                // Optional: Keep only last 3 archive files to prevent disk space issues
                cleanupOldArchives();
            }
            
        } catch (Exception e) {
            logger.error("Error during tracking file cleanup: {}", e.getMessage());
        }
    }
    
    /**
     * Keep only the 3 most recent archive files
     */
    private void cleanupOldArchives() {
        try {
            String baseFileName = trackingFilePath.replace(".txt", "_archived_");
            Path parentDir = Paths.get(trackingFilePath).getParent();
            if (parentDir == null) parentDir = Paths.get(".");
            
            List<Path> archiveFiles = Files.list(parentDir)
                .filter(path -> path.getFileName().toString().startsWith(
                    Paths.get(baseFileName).getFileName().toString()))
                .sorted((p1, p2) -> {
                    try {
                        return Files.getLastModifiedTime(p2).compareTo(Files.getLastModifiedTime(p1));
                    } catch (IOException e) {
                        return 0;
                    }
                })
                .collect(Collectors.toList());
            
            // Keep only the 3 most recent archives
            for (int i = 3; i < archiveFiles.size(); i++) {
                Files.deleteIfExists(archiveFiles.get(i));
                logger.info("Deleted old archive file: {}", archiveFiles.get(i));
            }
            
        } catch (Exception e) {
            logger.warn("Error cleaning up old archive files: {}", e.getMessage());
        }
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
            logger.info("- Tracking file: {}", consumer.getTrackingFilePath());
            logger.info("- Checkpoint file: {}", consumer.getCheckpointFilePath());
            logger.info("- Last processed timestamp: {}", consumer.getLastProcessedTimestamp());
            logger.info("- Cached processed events: {}", consumer.getProcessedEventsCacheSize());
            
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
