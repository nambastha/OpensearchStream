package com.streaming.opensearch.offset;

import com.streaming.opensearch.config.ElasticsearchConfig;
import com.streaming.opensearch.model.OffsetRecord;
import com.streaming.opensearch.resilience.CircuitBreaker;
import com.streaming.opensearch.resilience.RetryPolicy;
import com.streaming.opensearch.performance.BulkOperationManager;
import com.streaming.opensearch.monitoring.ConsumerMetrics;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Enhanced Elasticsearch offset manager with resilience, performance optimization, and monitoring
 * Features: retry policies, circuit breaker, bulk operations, metrics collection
 */
public class ResilientElasticsearchOffsetManager {
    private static final Logger logger = LoggerFactory.getLogger(ResilientElasticsearchOffsetManager.class);
    
    private final ElasticsearchClient client;
    private final String offsetIndexName;
    private final ReentrantLock offsetLock;
    private final RetryPolicy retryPolicy;
    private final CircuitBreaker circuitBreaker;
    private final BulkOperationManager bulkManager;
    private final ConsumerMetrics metrics;
    private final ScheduledExecutorService scheduler;
    
    // Bulk operation batching
    private final ConcurrentLinkedQueue<PendingOffsetUpdate> pendingUpdates;
    private final int bulkBatchSize;
    private final long bulkFlushIntervalMs;
    
    public static final String DEFAULT_OFFSET_INDEX = "consumer_offsets";
    
    public static class Builder {
        private String offsetIndexName = DEFAULT_OFFSET_INDEX;
        private int maxRetryAttempts = 3;
        private Duration retryBaseDelay = Duration.ofMillis(100);
        private Duration retryMaxDelay = Duration.ofSeconds(30);
        private int circuitBreakerFailureThreshold = 5;
        private Duration circuitBreakerTimeout = Duration.ofMinutes(1);
        private int bulkBatchSize = 50;
        private long bulkFlushIntervalMs = 5000;
        private String consumerId = "default";
        private String sourceIndex = "unknown";
        
        public Builder offsetIndexName(String offsetIndexName) {
            this.offsetIndexName = offsetIndexName;
            return this;
        }
        
        public Builder maxRetryAttempts(int maxRetryAttempts) {
            this.maxRetryAttempts = maxRetryAttempts;
            return this;
        }
        
        public Builder retryBaseDelay(Duration retryBaseDelay) {
            this.retryBaseDelay = retryBaseDelay;
            return this;
        }
        
        public Builder retryMaxDelay(Duration retryMaxDelay) {
            this.retryMaxDelay = retryMaxDelay;
            return this;
        }
        
        public Builder circuitBreakerFailureThreshold(int threshold) {
            this.circuitBreakerFailureThreshold = threshold;
            return this;
        }
        
        public Builder circuitBreakerTimeout(Duration timeout) {
            this.circuitBreakerTimeout = timeout;
            return this;
        }
        
        public Builder bulkBatchSize(int bulkBatchSize) {
            this.bulkBatchSize = bulkBatchSize;
            return this;
        }
        
        public Builder bulkFlushIntervalMs(long bulkFlushIntervalMs) {
            this.bulkFlushIntervalMs = bulkFlushIntervalMs;
            return this;
        }
        
        public Builder consumerId(String consumerId) {
            this.consumerId = consumerId;
            return this;
        }
        
        public Builder sourceIndex(String sourceIndex) {
            this.sourceIndex = sourceIndex;
            return this;
        }
        
        public ResilientElasticsearchOffsetManager build() {
            return new ResilientElasticsearchOffsetManager(this);
        }
    }
    
    private ResilientElasticsearchOffsetManager(Builder builder) {
        this.client = ElasticsearchConfig.createClient();
        this.offsetIndexName = builder.offsetIndexName;
        this.offsetLock = new ReentrantLock();
        this.bulkBatchSize = builder.bulkBatchSize;
        this.bulkFlushIntervalMs = builder.bulkFlushIntervalMs;
        this.pendingUpdates = new ConcurrentLinkedQueue<>();
        
        // Initialize retry policy
        this.retryPolicy = RetryPolicy.builder()
            .maxAttempts(builder.maxRetryAttempts)
            .baseDelay(builder.retryBaseDelay)
            .maxDelay(builder.retryMaxDelay)
            .retryOn(this::isRetryableException)
            .build();
        
        // Initialize circuit breaker
        this.circuitBreaker = CircuitBreaker.builder()
            .name("ElasticsearchOffsetManager")
            .failureThreshold(builder.circuitBreakerFailureThreshold)
            .timeout(builder.circuitBreakerTimeout)
            .build();
        
        // Initialize bulk manager
        this.bulkManager = BulkOperationManager.builder()
            .maxBatchSize(builder.bulkBatchSize)
            .maxFlushIntervalMs(builder.bulkFlushIntervalMs)
            .build(client);
        
        // Initialize metrics
        this.metrics = new ConsumerMetrics(builder.consumerId, builder.sourceIndex);
        
        // Initialize scheduler for bulk operations
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "offset-bulk-scheduler");
            t.setDaemon(true);
            return t;
        });
        
        // Create offset index and start bulk flusher
        createOffsetIndexIfNotExists();
        startBulkFlusher();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    private void createOffsetIndexIfNotExists() {
        try {
            retryPolicy.executeVoid(() -> {
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
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create offset index", e);
                }
            }, "createOffsetIndex");
        } catch (Exception e) {
            logger.error("Failed to create offset index after retries", e);
            throw new RuntimeException("Failed to initialize offset manager", e);
        }
    }
    
    /**
     * Load offset record with retry policy and circuit breaker
     */
    @SuppressWarnings("unchecked")
    public OffsetRecord loadOffset(String consumerId, String indexName) {
        try {
            return (OffsetRecord) circuitBreaker.execute(() -> {
                try {
                    return retryPolicy.execute(() -> {
                        String documentId = generateDocumentId(consumerId, indexName);
                        
                        GetRequest getRequest = GetRequest.of(g -> g
                            .index(offsetIndexName)
                            .id(documentId)
                        );
                        
                        GetResponse<OffsetRecord> response;
                        try {
                            response = client.get(getRequest, OffsetRecord.class);
                        } catch (java.io.IOException e) {
                            throw new RuntimeException("IO error getting offset", e);
                        }
                        
                        if (response.found() && response.source() != null) {
                            OffsetRecord offset = response.source();
                            logger.debug("Loaded offset for consumer {}, index {}", consumerId, indexName);
                            return offset;
                        } else {
                            logger.info("No existing offset found for consumer {}, index {}. Creating new record.", 
                                consumerId, indexName);
                            return new OffsetRecord(consumerId, indexName);
                        }
                    }, "loadOffset");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (CircuitBreaker.CircuitBreakerOpenException e) {
            metrics.incrementCircuitBreakerTrips();
            logger.error("Circuit breaker open, falling back to new offset record");
            return new OffsetRecord(consumerId, indexName);
        } catch (Exception e) {
            metrics.incrementErrors();
            logger.error("Error loading offset for consumer {}, index {}: {}", 
                consumerId, indexName, e.getMessage());
            return new OffsetRecord(consumerId, indexName);
        }
    }
    
    /**
     * Save offset record with retry policy and circuit breaker
     */
    public void saveOffset(OffsetRecord offsetRecord) {
        try {
            circuitBreaker.executeVoid(() -> {
                try {
                    retryPolicy.executeVoid(() -> {
                        String documentId = offsetRecord.generateDocumentId();
                        offsetRecord.setLastUpdated(Instant.now().toString());
                        
                        IndexRequest<OffsetRecord> indexRequest = IndexRequest.of(i -> i
                            .index(offsetIndexName)
                            .id(documentId)
                            .document(offsetRecord)
                        );
                        
                        IndexResponse response;
                        try {
                            response = client.index(indexRequest);
                        } catch (java.io.IOException e) {
                            throw new RuntimeException("IO error indexing offset", e);
                        }
                        
                        if (response.result().jsonValue().equals("created") || 
                            response.result().jsonValue().equals("updated")) {
                            metrics.incrementOffsetCommits();
                            logger.debug("Offset saved successfully for consumer {}, index {}", 
                                offsetRecord.getConsumerId(), offsetRecord.getIndexName());
                        }
                    }, "saveOffset");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (CircuitBreaker.CircuitBreakerOpenException e) {
            metrics.incrementCircuitBreakerTrips();
            logger.error("Circuit breaker open, queueing offset update for later");
            queueOffsetUpdate(offsetRecord);
        } catch (Exception e) {
            metrics.incrementErrors();
            logger.error("Error saving offset for consumer {}, index {}", 
                offsetRecord.getConsumerId(), offsetRecord.getIndexName(), e);
            queueOffsetUpdate(offsetRecord);
        }
    }
    
    /**
     * Bulk save multiple offset records for better performance
     */
    public CompletableFuture<BulkOperationManager.BulkResult> saveBulkOffsets(List<OffsetRecord> offsetRecords) {
        List<BulkOperation> operations = new ArrayList<>();
        
        for (OffsetRecord offset : offsetRecords) {
            offset.setLastUpdated(Instant.now().toString());
            BulkOperation operation = BulkOperation.of(b -> b
                .index(i -> i
                    .index(offsetIndexName)
                    .id(offset.generateDocumentId())
                    .document(offset)
                )
            );
            operations.add(operation);
        }
        
        return bulkManager.executeBulkAsync(operations)
            .thenApply(result -> {
                metrics.incrementOffsetCommits();
                if (result.hasErrors()) {
                    metrics.incrementErrors();
                    logger.warn("Bulk offset save had {} errors out of {} operations", 
                        result.getFailed(), result.getTotal());
                }
                return result;
            });
    }
    
    /**
     * Queue offset update for bulk processing
     */
    private void queueOffsetUpdate(OffsetRecord offsetRecord) {
        pendingUpdates.offer(new PendingOffsetUpdate(offsetRecord, Instant.now()));
        logger.debug("Queued offset update for consumer {}, index {}", 
            offsetRecord.getConsumerId(), offsetRecord.getIndexName());
    }
    
    /**
     * Start bulk flusher that periodically processes queued updates
     */
    private void startBulkFlusher() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                flushPendingUpdates();
            } catch (Exception e) {
                logger.error("Error flushing pending updates", e);
            }
        }, bulkFlushIntervalMs, bulkFlushIntervalMs, TimeUnit.MILLISECONDS);
    }
    
    private void flushPendingUpdates() {
        if (pendingUpdates.isEmpty()) {
            return;
        }
        
        List<OffsetRecord> updates = new ArrayList<>();
        PendingOffsetUpdate update;
        
        while ((update = pendingUpdates.poll()) != null && updates.size() < bulkBatchSize) {
            updates.add(update.offsetRecord);
        }
        
        if (!updates.isEmpty()) {
            logger.info("Flushing {} pending offset updates", updates.size());
            saveBulkOffsets(updates)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        logger.error("Failed to flush pending updates", throwable);
                        // Re-queue failed updates
                        updates.forEach(this::queueOffsetUpdate);
                    } else {
                        logger.debug("Successfully flushed {} offset updates", updates.size());
                    }
                });
        }
    }
    
    private boolean isRetryableException(Exception e) {
        if (e instanceof ElasticsearchException) {
            ElasticsearchException ese = (ElasticsearchException) e;
            // Don't retry on 4xx errors (client errors)
            return ese.status() >= 500;
        }
        // Retry on network/connection issues
        return e instanceof java.net.ConnectException ||
               e instanceof java.net.SocketTimeoutException ||
               e instanceof java.io.IOException;
    }
    
    private String generateDocumentId(String consumerId, String indexName) {
        return consumerId + "_" + indexName;
    }
    
    public ConsumerMetrics getMetrics() {
        return metrics;
    }
    
    public CircuitBreaker.State getCircuitBreakerState() {
        return circuitBreaker.getState();
    }
    
    public void close() {
        try {
            // Flush any remaining updates
            flushPendingUpdates();
            
            // Shutdown scheduler
            scheduler.shutdown();
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            
            // Close client
            ElasticsearchConfig.closeClient(client);
            logger.info("ResilientElasticsearchOffsetManager closed successfully");
        } catch (Exception e) {
            logger.error("Error closing ResilientElasticsearchOffsetManager", e);
        }
    }
    
    /**
     * Print comprehensive status report
     */
    public void printStatusReport() {
        logger.info("=== OFFSET MANAGER STATUS ===");
        logger.info("Circuit Breaker State: {}", circuitBreaker.getState());
        logger.info("Pending Updates: {}", pendingUpdates.size());
        logger.info("Bulk Operations: {}", bulkManager.getTotalOperations());
        logger.info("Bulk Batches: {}", bulkManager.getTotalBatches());
        logger.info("Bulk Errors: {}", bulkManager.getTotalErrors());
        logger.info("Average Batch Size: {:.2f}", bulkManager.getAverageBatchSize());
        
        metrics.printMetricsReport();
        logger.info("=============================");
    }
    
    /**
     * Pending offset update for bulk processing
     */
    private static class PendingOffsetUpdate {
        final OffsetRecord offsetRecord;
        final Instant timestamp;
        
        PendingOffsetUpdate(OffsetRecord offsetRecord, Instant timestamp) {
            this.offsetRecord = offsetRecord;
            this.timestamp = timestamp;
        }
    }
}