package com.streaming.opensearch.consumer;

import com.streaming.opensearch.config.ElasticsearchConfig;
import com.streaming.opensearch.model.Event;
import com.streaming.opensearch.model.OffsetRecord;
import com.streaming.opensearch.offset.ResilientElasticsearchOffsetManager;
import com.streaming.opensearch.monitoring.ConsumerMetrics;
import com.streaming.opensearch.resilience.RetryPolicy;
import com.streaming.opensearch.resilience.CircuitBreaker;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Enhanced Kafka-like event consumer with resilience, performance optimization, and monitoring
 * Features: retry policies, circuit breaker, bulk operations, comprehensive metrics
 */
public class EnhancedKafkaLikeEventConsumer {
    private static final Logger logger = LoggerFactory.getLogger(EnhancedKafkaLikeEventConsumer.class);
    
    private final ElasticsearchClient client;
    private final String sourceIndexName;
    private final String consumerId;
    private final ResilientElasticsearchOffsetManager offsetManager;
    private final ConsumerMetrics metrics;
    private final RetryPolicy retryPolicy;
    private final CircuitBreaker circuitBreaker;
    private final Set<String> recentlyProcessedIds;
    private final AtomicLong sessionProcessedCount;
    private final ScheduledExecutorService metricsScheduler;
    private volatile boolean running;
    private OffsetRecord currentOffset;
    
    // Configurable parameters
    private static final int BATCH_SIZE = 100;
    private static final long POLL_INTERVAL_MS = 1000;
    private static final int RECENT_CACHE_SIZE = 2000;
    private static final int OFFSET_COMMIT_INTERVAL = 50;
    private static final int SAFETY_WINDOW_SECONDS = 30;
    private static final long METRICS_REPORT_INTERVAL_MS = 30000;
    
    public static class Builder {
        private String sourceIndexName;
        private String consumerId = generateConsumerId();
        private int batchSize = BATCH_SIZE;
        private long pollIntervalMs = POLL_INTERVAL_MS;
        private int offsetCommitInterval = OFFSET_COMMIT_INTERVAL;
        private int safetyWindowSeconds = SAFETY_WINDOW_SECONDS;
        
        public Builder sourceIndex(String sourceIndexName) {
            this.sourceIndexName = sourceIndexName;
            return this;
        }
        
        public Builder consumerId(String consumerId) {
            this.consumerId = consumerId;
            return this;
        }
        
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }
        
        public Builder pollIntervalMs(long pollIntervalMs) {
            this.pollIntervalMs = pollIntervalMs;
            return this;
        }
        
        public Builder offsetCommitInterval(int offsetCommitInterval) {
            this.offsetCommitInterval = offsetCommitInterval;
            return this;
        }
        
        public Builder safetyWindowSeconds(int safetyWindowSeconds) {
            this.safetyWindowSeconds = safetyWindowSeconds;
            return this;
        }
        
        public EnhancedKafkaLikeEventConsumer build() {
            if (sourceIndexName == null) {
                throw new IllegalArgumentException("Source index name is required");
            }
            return new EnhancedKafkaLikeEventConsumer(this);
        }
    }
    
    private EnhancedKafkaLikeEventConsumer(Builder builder) {
        this.sourceIndexName = builder.sourceIndexName;
        this.consumerId = builder.consumerId;
        this.client = ElasticsearchConfig.createClient();
        this.recentlyProcessedIds = new HashSet<>(RECENT_CACHE_SIZE);
        this.sessionProcessedCount = new AtomicLong(0);
        this.running = false;
        
        // Initialize resilient offset manager
        this.offsetManager = ResilientElasticsearchOffsetManager.builder()
            .consumerId(consumerId)
            .sourceIndex(sourceIndexName)
            .maxRetryAttempts(3)
            .retryBaseDelay(Duration.ofMillis(100))
            .retryMaxDelay(Duration.ofSeconds(10))
            .circuitBreakerFailureThreshold(5)
            .circuitBreakerTimeout(Duration.ofMinutes(2))
            .bulkBatchSize(20)
            .bulkFlushIntervalMs(5000)
            .build();
        
        // Get metrics from offset manager
        this.metrics = offsetManager.getMetrics();
        
        // Initialize retry policy for data fetching
        this.retryPolicy = RetryPolicy.builder()
            .maxAttempts(3)
            .baseDelay(Duration.ofMillis(200))
            .maxDelay(Duration.ofSeconds(5))
            .retryOn(this::isRetryableException)
            .build();
        
        // Initialize circuit breaker for data fetching
        this.circuitBreaker = CircuitBreaker.builder()
            .name("DataFetch")
            .failureThreshold(3)
            .timeout(Duration.ofMinutes(1))
            .build();
        
        // Initialize metrics reporting scheduler
        this.metricsScheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "metrics-reporter");
            t.setDaemon(true);
            return t;
        });
        
        logger.info("Enhanced consumer initialized: sourceIndex={}, consumerId={}", 
            sourceIndexName, consumerId);
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public void start() {
        if (running) {
            logger.warn("Consumer {} is already running", consumerId);
            return;
        }
        
        running = true;
        logger.info("Starting enhanced consumer {} for index {}", consumerId, sourceIndexName);
        
        // Load current offset
        currentOffset = offsetManager.loadOffset(consumerId, sourceIndexName);
        logger.info("Loaded offset: {}", currentOffset);
        
        // Start metrics reporting
        startMetricsReporting();
        
        // Start main processing loop
        long lastOffsetCommit = 0;
        
        while (running) {
            try {
                long batchStart = System.currentTimeMillis();
                
                List<Event> events = fetchUnprocessedEventsWithResilience();
                
                if (!events.isEmpty()) {
                    logger.debug("Fetched {} unprocessed events for consumer {}", events.size(), consumerId);
                    
                    int processed = processEventBatch(events);
                    long batchDuration = System.currentTimeMillis() - batchStart;
                    
                    if (processed > 0) {
                        metrics.recordBatchProcessed(processed, batchDuration);
                        long sessionCount = sessionProcessedCount.addAndGet(processed);
                        
                        // Commit offset periodically
                        if (sessionCount - lastOffsetCommit >= OFFSET_COMMIT_INTERVAL) {
                            commitOffset();
                            lastOffsetCommit = sessionCount;
                        }
                        
                        if (sessionCount % 100 == 0) {
                            logger.info("Consumer {} processed {} events this session (total: {})", 
                                consumerId, sessionCount, currentOffset.getTotalProcessed());
                        }
                    }
                } else {
                    logger.trace("Consumer {} found no new events", consumerId);
                }
                
                // Manage memory usage of recent cache
                if (recentlyProcessedIds.size() > RECENT_CACHE_SIZE) {
                    recentlyProcessedIds.clear();
                    logger.debug("Consumer {} cleared recently processed cache", consumerId);
                }
                
                // Sleep before next poll
                Thread.sleep(POLL_INTERVAL_MS);
                
            } catch (InterruptedException e) {
                logger.info("Consumer {} interrupted, stopping", consumerId);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                metrics.incrementErrors();
                logger.error("Error in consumer {} main loop", consumerId, e);
                
                // Exponential backoff on error
                try {
                    Thread.sleep(Math.min(5000, POLL_INTERVAL_MS * 2));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        // Final offset commit and cleanup
        try {
            commitOffset();
            logger.info("Consumer {} stopped. Session events processed: {}, Total: {}", 
                consumerId, sessionProcessedCount.get(), currentOffset.getTotalProcessed());
        } catch (Exception e) {
            logger.error("Error during consumer {} shutdown", consumerId, e);
        }
    }
    
    private List<Event> fetchUnprocessedEventsWithResilience() {
        try {
            return circuitBreaker.execute(() -> {
                try {
                    return retryPolicy.execute(() -> fetchUnprocessedEvents(), "fetchEvents");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (CircuitBreaker.CircuitBreakerOpenException e) {
            logger.warn("Data fetch circuit breaker is open, returning empty list");
            return List.of();
        } catch (Exception e) {
            metrics.incrementErrors();
            logger.error("Failed to fetch events for consumer {}", consumerId, e);
            return List.of();
        }
    }
    
    private List<Event> fetchUnprocessedEvents() {
        try {
            // Use effective start timestamp
            String effectiveStartTimestamp = currentOffset.getLastProcessedTimestamp();
            String safetyTimestamp = subtractSeconds(effectiveStartTimestamp, SAFETY_WINDOW_SECONDS);
            
            logger.trace("Consumer {} querying with timestamp: {}, safety: {}, lastDocId: {}", 
                consumerId, effectiveStartTimestamp, safetyTimestamp, currentOffset.getLastProcessedDocId());
            
            // Build query with safety window and exclude already processed document
            Query.Builder queryBuilder = new Query.Builder();
            
            if (currentOffset.getLastProcessedDocId() != null) {
                // Use composite query: timestamp range + exclude last processed doc
                queryBuilder.bool(b -> b
                    .must(m -> m.range(r -> r
                        .field("timestamp")
                        .gte(co.elastic.clients.json.JsonData.of(safetyTimestamp))
                    ))
                    .mustNot(mn -> mn.term(t -> t
                        .field("_id")
                        .value(currentOffset.getLastProcessedDocId())
                    ))
                );
            } else {
                // Just timestamp range
                queryBuilder.range(r -> r
                    .field("timestamp")
                    .gte(co.elastic.clients.json.JsonData.of(safetyTimestamp))
                );
            }
            
            SearchRequest searchRequest = SearchRequest.of(s -> s
                .index(sourceIndexName)
                .query(queryBuilder.build())
                .sort(sort -> sort.field(f -> f.field("timestamp").order(SortOrder.Asc)))
                .size(BATCH_SIZE)
            );
            
            SearchResponse<Event> response = client.search(searchRequest, Event.class);
            
            return response.hits().hits().stream()
                .map(Hit::source)
                .filter(event -> event != null)
                .collect(Collectors.toList());
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch events", e);
        }
    }
    
    private int processEventBatch(List<Event> events) {
        int processedCount = 0;
        
        for (Event event : events) {
            // Double-check to avoid reprocessing recent events
            if (!recentlyProcessedIds.contains(event.getId())) {
                try {
                    processEvent(event);
                    updateOffsetInMemory(event);
                    recentlyProcessedIds.add(event.getId());
                    processedCount++;
                    metrics.incrementProcessed(1);
                    
                } catch (Exception e) {
                    metrics.incrementErrors();
                    logger.error("Error processing event {} in consumer {}", event.getId(), consumerId, e);
                    // Continue with next event instead of failing entire batch
                }
            } else {
                logger.trace("Consumer {} skipping recently processed event: {}", 
                    consumerId, event.getId());
            }
        }
        
        return processedCount;
    }
    
    private void processEvent(Event event) {
        // Simulate event processing
        logger.debug("Processing event: {} at {}", event.getId(), event.getTimestamp());
        
        // Add your actual event processing logic here
        // This could include:
        // - Data transformation
        // - External API calls
        // - Database updates
        // - Message publishing
        
        // Simulate processing time
        try {
            Thread.sleep(10); // 10ms processing time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    private void updateOffsetInMemory(Event event) {
        currentOffset.updateOffset(event.getTimestamp(), event.getId());
    }
    
    private void commitOffset() {
        try {
            offsetManager.saveOffset(currentOffset);
            logger.debug("Committed offset for consumer {}: {}", consumerId, currentOffset);
        } catch (Exception e) {
            metrics.incrementErrors();
            logger.error("Failed to commit offset for consumer {}", consumerId, e);
        }
    }
    
    private void startMetricsReporting() {
        metricsScheduler.scheduleAtFixedRate(() -> {
            try {
                metrics.printMetricsReport();
                offsetManager.printStatusReport();
            } catch (Exception e) {
                logger.error("Error reporting metrics", e);
            }
        }, METRICS_REPORT_INTERVAL_MS, METRICS_REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
    
    private boolean isRetryableException(Exception e) {
        return e instanceof java.net.ConnectException ||
               e instanceof java.net.SocketTimeoutException ||
               e instanceof java.io.IOException;
    }
    
    private String subtractSeconds(String isoTimestamp, int seconds) {
        try {
            Instant instant = Instant.parse(isoTimestamp);
            return instant.minus(seconds, ChronoUnit.SECONDS).toString();
        } catch (Exception e) {
            logger.warn("Error parsing timestamp {}, using current time", isoTimestamp);
            return Instant.now().minus(seconds, ChronoUnit.SECONDS).toString();
        }
    }
    
    public void stop() {
        running = false;
        logger.info("Stop signal sent to consumer {}", consumerId);
    }
    
    public void close() {
        try {
            stop();
            
            // Shutdown metrics scheduler
            metricsScheduler.shutdown();
            if (!metricsScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                metricsScheduler.shutdownNow();
            }
            
            // Close offset manager
            offsetManager.close();
            
            // Close client
            ElasticsearchConfig.closeClient(client);
            
            logger.info("Enhanced consumer {} closed successfully", consumerId);
        } catch (Exception e) {
            logger.error("Error closing enhanced consumer {}", consumerId, e);
        }
    }
    
    // Getters for monitoring
    public boolean isRunning() { return running; }
    public String getConsumerId() { return consumerId; }
    public String getSourceIndexName() { return sourceIndexName; }
    public long getSessionProcessedCount() { return sessionProcessedCount.get(); }
    public long getTotalProcessedCount() { return currentOffset.getTotalProcessed(); }
    public String getLastProcessedTimestamp() { return currentOffset.getLastProcessedTimestamp(); }
    public String getLastProcessedDocId() { return currentOffset.getLastProcessedDocId(); }
    public ConsumerMetrics getMetrics() { return metrics; }
    public ConsumerMetrics.HealthStatus getHealthStatus() { return metrics.getHealthStatus(); }
    public CircuitBreaker.State getCircuitBreakerState() { return offsetManager.getCircuitBreakerState(); }
    
    private static String generateConsumerId() {
        return "consumer-" + UUID.randomUUID().toString().substring(0, 8);
    }
}