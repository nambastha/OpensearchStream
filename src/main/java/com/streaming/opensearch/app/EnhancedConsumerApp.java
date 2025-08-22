package com.streaming.opensearch.app;

import com.streaming.opensearch.consumer.EnhancedKafkaLikeEventConsumer;
import com.streaming.opensearch.monitoring.ConsumerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enhanced production-ready consumer application with comprehensive monitoring, 
 * resilience features, and performance optimizations
 */
public class EnhancedConsumerApp {
    private static final Logger logger = LoggerFactory.getLogger(EnhancedConsumerApp.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            logger.error("Usage: java EnhancedConsumerApp <sourceIndexName> <consumerId>");
            logger.error("Example: java EnhancedConsumerApp events prod-consumer-1");
            System.exit(1);
        }

        String sourceIndexName = args[0];
        String consumerId = args[1];

        logger.info("Starting Enhanced Consumer Application");
        logger.info("Source Index: {}", sourceIndexName);
        logger.info("Consumer ID: {}", consumerId);

        try {
            // Create enhanced consumer with builder pattern
            EnhancedKafkaLikeEventConsumer consumer = EnhancedKafkaLikeEventConsumer.builder()
                .sourceIndex(sourceIndexName)
                .consumerId(consumerId)
                .batchSize(100)
                .pollIntervalMs(1000)
                .offsetCommitInterval(50)
                .safetyWindowSeconds(30)
                .build();

            // Setup shutdown hook for graceful shutdown
            final EnhancedKafkaLikeEventConsumer finalConsumer = consumer;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown signal received, stopping consumer gracefully...");
                finalConsumer.stop();
                
                // Wait a bit for graceful stop
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                
                finalConsumer.close();
                logger.info("Enhanced consumer shutdown completed");
            }));

            // Start consumer in separate thread
            Thread consumerThread = new Thread(() -> {
                try {
                    consumer.start();
                } catch (Exception e) {
                    logger.error("Consumer thread failed", e);
                }
            }, "consumer-main");
            
            consumerThread.start();
            
            logger.info("Enhanced consumer started successfully!");
            logger.info("Features enabled: Retry policies, Circuit breaker, Bulk operations, Comprehensive monitoring");
            logger.info("Press Ctrl+C to stop the consumer gracefully");

            // Status monitoring loop
            long lastStatusTime = 0;
            while (consumer.isRunning()) {
                Thread.sleep(5000);
                
                // Print detailed status every 60 seconds
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastStatusTime > 60000) {
                    printDetailedStatus(consumer);
                    lastStatusTime = currentTime;
                }
                
                // Check health status
                ConsumerMetrics.HealthStatus health = consumer.getHealthStatus();
                if (health == ConsumerMetrics.HealthStatus.UNHEALTHY) {
                    logger.warn("Consumer health is UNHEALTHY - consider intervention");
                } else if (health == ConsumerMetrics.HealthStatus.DEGRADED) {
                    logger.warn("Consumer health is DEGRADED - monitoring closely");
                }
            }
            
            // Wait for consumer thread to finish
            consumerThread.join(10000);

        } catch (Exception e) {
            logger.error("Error running enhanced consumer application", e);
        }
    }

    private static void printDetailedStatus(EnhancedKafkaLikeEventConsumer consumer) {
        logger.info("=== ENHANCED CONSUMER STATUS ===");
        logger.info("Consumer ID: {}", consumer.getConsumerId());
        logger.info("Source Index: {}", consumer.getSourceIndexName());
        logger.info("Running: {}", consumer.isRunning());
        logger.info("Health Status: {}", consumer.getHealthStatus());
        logger.info("Circuit Breaker State: {}", consumer.getCircuitBreakerState());
        logger.info("");
        logger.info("--- Processing Stats ---");
        logger.info("Session Processed: {}", consumer.getSessionProcessedCount());
        logger.info("Total Processed: {}", consumer.getTotalProcessedCount());
        logger.info("Processing Rate: {:.2f} events/sec", consumer.getMetrics().getProcessingRate());
        logger.info("Error Rate: {:.2%}", consumer.getMetrics().getErrorRate());
        logger.info("");
        logger.info("--- Offset Info ---");
        logger.info("Last Timestamp: {}", consumer.getLastProcessedTimestamp());
        logger.info("Last Doc ID: {}", consumer.getLastProcessedDocId());
        logger.info("Time Since Last Processed: {} seconds", 
            consumer.getMetrics().getTimeSinceLastProcessed().getSeconds());
        logger.info("");
        logger.info("--- Performance Stats ---");
        logger.info("Average Batch Size: {:.2f}", consumer.getMetrics().getAverageBatchSize());
        logger.info("Average Processing Time: {:.2f} ms/batch", consumer.getMetrics().getAverageProcessingTimeMs());
        logger.info("Max Processing Time: {} ms/batch", consumer.getMetrics().getMaxProcessingTimeMs());
        logger.info("");
        logger.info("--- Resilience Stats ---");
        logger.info("Total Retries: {}", consumer.getMetrics().getTotalRetries());
        logger.info("Circuit Breaker Trips: {}", consumer.getMetrics().getCircuitBreakerTrips());
        logger.info("Total Errors: {}", consumer.getMetrics().getTotalErrors());
        logger.info("Offset Commits: {}", consumer.getMetrics().getOffsetCommits());
        logger.info("Uptime: {} seconds", consumer.getMetrics().getUptime().getSeconds());
        logger.info("===============================");
    }
}