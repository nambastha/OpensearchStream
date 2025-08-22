package com.streaming.opensearch.monitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Consumer metrics collection and reporting
 */
public class ConsumerMetrics {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerMetrics.class);
    
    private final String consumerId;
    private final String sourceIndex;
    
    // Counters
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong totalRetries = new AtomicLong(0);
    private final AtomicLong offsetCommits = new AtomicLong(0);
    private final AtomicLong batchesProcessed = new AtomicLong(0);
    
    // Timing
    private final AtomicReference<Instant> startTime = new AtomicReference<>(Instant.now());
    private final AtomicReference<Instant> lastProcessedTime = new AtomicReference<>();
    private final AtomicReference<Instant> lastOffsetCommitTime = new AtomicReference<>();
    
    // Performance tracking
    private final AtomicLong totalProcessingTimeMs = new AtomicLong(0);
    private final AtomicLong maxProcessingTimeMs = new AtomicLong(0);
    private final AtomicLong totalBatchSize = new AtomicLong(0);
    
    // Circuit breaker metrics
    private final AtomicLong circuitBreakerTrips = new AtomicLong(0);
    
    public ConsumerMetrics(String consumerId, String sourceIndex) {
        this.consumerId = consumerId;
        this.sourceIndex = sourceIndex;
    }
    
    public void incrementProcessed(int count) {
        totalProcessed.addAndGet(count);
        lastProcessedTime.set(Instant.now());
    }
    
    public void incrementErrors() {
        totalErrors.incrementAndGet();
    }
    
    public void incrementRetries() {
        totalRetries.incrementAndGet();
    }
    
    public void incrementOffsetCommits() {
        offsetCommits.incrementAndGet();
        lastOffsetCommitTime.set(Instant.now());
    }
    
    public void recordBatchProcessed(int batchSize, long processingTimeMs) {
        batchesProcessed.incrementAndGet();
        totalBatchSize.addAndGet(batchSize);
        totalProcessingTimeMs.addAndGet(processingTimeMs);
        
        // Update max processing time
        maxProcessingTimeMs.updateAndGet(current -> Math.max(current, processingTimeMs));
    }
    
    public void incrementCircuitBreakerTrips() {
        circuitBreakerTrips.incrementAndGet();
    }
    
    public long getTotalProcessed() {
        return totalProcessed.get();
    }
    
    public long getTotalErrors() {
        return totalErrors.get();
    }
    
    public long getTotalRetries() {
        return totalRetries.get();
    }
    
    public long getOffsetCommits() {
        return offsetCommits.get();
    }
    
    public long getBatchesProcessed() {
        return batchesProcessed.get();
    }
    
    public long getCircuitBreakerTrips() {
        return circuitBreakerTrips.get();
    }
    
    public double getProcessingRate() {
        Duration uptime = getUptime();
        if (uptime.isZero()) return 0.0;
        return (double) totalProcessed.get() / uptime.getSeconds();
    }
    
    public double getErrorRate() {
        long total = totalProcessed.get() + totalErrors.get();
        if (total == 0) return 0.0;
        return (double) totalErrors.get() / total;
    }
    
    public double getAverageBatchSize() {
        long batches = batchesProcessed.get();
        if (batches == 0) return 0.0;
        return (double) totalBatchSize.get() / batches;
    }
    
    public double getAverageProcessingTimeMs() {
        long batches = batchesProcessed.get();
        if (batches == 0) return 0.0;
        return (double) totalProcessingTimeMs.get() / batches;
    }
    
    public long getMaxProcessingTimeMs() {
        return maxProcessingTimeMs.get();
    }
    
    public Duration getUptime() {
        return Duration.between(startTime.get(), Instant.now());
    }
    
    public Duration getTimeSinceLastProcessed() {
        Instant last = lastProcessedTime.get();
        if (last == null) return Duration.ZERO;
        return Duration.between(last, Instant.now());
    }
    
    public Duration getTimeSinceLastCommit() {
        Instant last = lastOffsetCommitTime.get();
        if (last == null) return Duration.ZERO;
        return Duration.between(last, Instant.now());
    }
    
    /**
     * Print comprehensive metrics report
     */
    public void printMetricsReport() {
        logger.info("=== CONSUMER METRICS REPORT ===");
        logger.info("Consumer ID: {}", consumerId);
        logger.info("Source Index: {}", sourceIndex);
        logger.info("Uptime: {} seconds", getUptime().getSeconds());
        logger.info("");
        
        logger.info("--- Processing Stats ---");
        logger.info("Total Processed: {}", getTotalProcessed());
        logger.info("Total Errors: {}", getTotalErrors());
        logger.info("Total Retries: {}", getTotalRetries());
        logger.info("Processing Rate: {:.2f} events/sec", getProcessingRate());
        logger.info("Error Rate: {:.2%}", getErrorRate());
        logger.info("");
        
        logger.info("--- Batch Stats ---");
        logger.info("Batches Processed: {}", getBatchesProcessed());
        logger.info("Average Batch Size: {:.2f}", getAverageBatchSize());
        logger.info("Average Processing Time: {:.2f} ms/batch", getAverageProcessingTimeMs());
        logger.info("Max Processing Time: {} ms/batch", getMaxProcessingTimeMs());
        logger.info("");
        
        logger.info("--- Offset Stats ---");
        logger.info("Offset Commits: {}", getOffsetCommits());
        logger.info("Time Since Last Processed: {} seconds", getTimeSinceLastProcessed().getSeconds());
        logger.info("Time Since Last Commit: {} seconds", getTimeSinceLastCommit().getSeconds());
        logger.info("");
        
        logger.info("--- Resilience Stats ---");
        logger.info("Circuit Breaker Trips: {}", getCircuitBreakerTrips());
        logger.info("=============================");
    }
    
    /**
     * Get health status based on metrics
     */
    public HealthStatus getHealthStatus() {
        Duration timeSinceLastProcessed = getTimeSinceLastProcessed();
        double errorRate = getErrorRate();
        
        if (timeSinceLastProcessed.toMinutes() > 10) {
            return HealthStatus.UNHEALTHY;
        } else if (errorRate > 0.1 || circuitBreakerTrips.get() > 5) {
            return HealthStatus.DEGRADED;
        } else {
            return HealthStatus.HEALTHY;
        }
    }
    
    public enum HealthStatus {
        HEALTHY, DEGRADED, UNHEALTHY
    }
}