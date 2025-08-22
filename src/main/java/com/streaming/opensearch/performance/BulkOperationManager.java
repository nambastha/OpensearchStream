package com.streaming.opensearch.performance;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-performance bulk operations manager for Elasticsearch
 * Provides batching, async operations, and performance monitoring
 */
public class BulkOperationManager {
    private static final Logger logger = LoggerFactory.getLogger(BulkOperationManager.class);
    
    private final ElasticsearchClient client;
    private final int maxBatchSize;
    private final long maxFlushIntervalMs;
    private final Executor executor;
    
    private final AtomicLong totalOperations = new AtomicLong(0);
    private final AtomicLong totalBatches = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    
    public static class Builder {
        private int maxBatchSize = 100;
        private long maxFlushIntervalMs = 5000;
        private int threadPoolSize = 2;
        
        public Builder maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }
        
        public Builder maxFlushIntervalMs(long maxFlushIntervalMs) {
            this.maxFlushIntervalMs = maxFlushIntervalMs;
            return this;
        }
        
        public Builder threadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }
        
        public BulkOperationManager build(ElasticsearchClient client) {
            return new BulkOperationManager(client, maxBatchSize, maxFlushIntervalMs, threadPoolSize);
        }
    }
    
    private BulkOperationManager(ElasticsearchClient client, int maxBatchSize, 
                                long maxFlushIntervalMs, int threadPoolSize) {
        this.client = client;
        this.maxBatchSize = maxBatchSize;
        this.maxFlushIntervalMs = maxFlushIntervalMs;
        this.executor = Executors.newFixedThreadPool(threadPoolSize, r -> {
            Thread t = new Thread(r, "bulk-operation-" + System.currentTimeMillis());
            t.setDaemon(true);
            return t;
        });
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Execute bulk operations synchronously
     */
    public BulkResult executeBulk(List<BulkOperation> operations) {
        if (operations.isEmpty()) {
            return new BulkResult(0, 0, 0);
        }
        
        long startTime = System.currentTimeMillis();
        List<List<BulkOperation>> batches = createBatches(operations);
        
        int totalSuccessful = 0;
        int totalFailed = 0;
        
        for (List<BulkOperation> batch : batches) {
            try {
                BulkRequest bulkRequest = BulkRequest.of(b -> {
                    batch.forEach(b::operations);
                    return b;
                });
                
                BulkResponse response = client.bulk(bulkRequest);
                BulkResult batchResult = processBulkResponse(response);
                
                totalSuccessful += batchResult.getSuccessful();
                totalFailed += batchResult.getFailed();
                
                totalBatches.incrementAndGet();
                
                if (batchResult.getFailed() > 0) {
                    logger.warn("Bulk batch had {} failures out of {} operations", 
                        batchResult.getFailed(), batch.size());
                }
                
            } catch (Exception e) {
                logger.error("Failed to execute bulk batch of {} operations", batch.size(), e);
                totalFailed += batch.size();
                totalErrors.incrementAndGet();
            }
        }
        
        totalOperations.addAndGet(operations.size());
        long duration = System.currentTimeMillis() - startTime;
        
        logger.debug("Bulk operation completed: {} ops in {} ms, {} successful, {} failed", 
            operations.size(), duration, totalSuccessful, totalFailed);
        
        return new BulkResult(totalSuccessful, totalFailed, duration);
    }
    
    /**
     * Execute bulk operations asynchronously
     */
    public CompletableFuture<BulkResult> executeBulkAsync(List<BulkOperation> operations) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return executeBulk(operations);
            } catch (Exception e) {
                logger.error("Async bulk operation failed", e);
                throw new RuntimeException(e);
            }
        }, executor);
    }
    
    private List<List<BulkOperation>> createBatches(List<BulkOperation> operations) {
        List<List<BulkOperation>> batches = new ArrayList<>();
        
        for (int i = 0; i < operations.size(); i += maxBatchSize) {
            int end = Math.min(i + maxBatchSize, operations.size());
            batches.add(operations.subList(i, end));
        }
        
        return batches;
    }
    
    private BulkResult processBulkResponse(BulkResponse response) {
        int successful = 0;
        int failed = 0;
        
        for (BulkResponseItem item : response.items()) {
            if (item.error() != null) {
                failed++;
                logger.warn("Bulk operation failed: {} - {}", 
                    item.error().type(), item.error().reason());
            } else {
                successful++;
            }
        }
        
        return new BulkResult(successful, failed, 0);
    }
    
    public long getTotalOperations() {
        return totalOperations.get();
    }
    
    public long getTotalBatches() {
        return totalBatches.get();
    }
    
    public long getTotalErrors() {
        return totalErrors.get();
    }
    
    public double getAverageBatchSize() {
        long batches = totalBatches.get();
        if (batches == 0) return 0.0;
        return (double) totalOperations.get() / batches;
    }
    
    /**
     * Result of bulk operation
     */
    public static class BulkResult {
        private final int successful;
        private final int failed;
        private final long durationMs;
        
        public BulkResult(int successful, int failed, long durationMs) {
            this.successful = successful;
            this.failed = failed;
            this.durationMs = durationMs;
        }
        
        public int getSuccessful() {
            return successful;
        }
        
        public int getFailed() {
            return failed;
        }
        
        public int getTotal() {
            return successful + failed;
        }
        
        public long getDurationMs() {
            return durationMs;
        }
        
        public boolean hasErrors() {
            return failed > 0;
        }
        
        public double getSuccessRate() {
            int total = getTotal();
            if (total == 0) return 1.0;
            return (double) successful / total;
        }
    }
}