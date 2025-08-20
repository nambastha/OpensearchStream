package com.streaming.opensearch.app;

import com.streaming.opensearch.consumer.KafkaLikeEventConsumer;
import com.streaming.opensearch.offset.ElasticsearchOffsetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production-ready single consumer application
 * Uses separate Elasticsearch index for offset management
 */
public class SingleConsumerApp {
    private static final Logger logger = LoggerFactory.getLogger(SingleConsumerApp.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            logger.error("Usage: java SingleConsumerApp <sourceIndexName> <consumerId>");
            logger.error("Example: java SingleConsumerApp events prod-consumer-1");
            System.exit(1);
        }

        String sourceIndexName = args[0];
        String consumerId = args[1];

        logger.info("Starting Single Consumer Application");
        logger.info("Source Index: {}", sourceIndexName);
        logger.info("Consumer ID: {}", consumerId);

        KafkaLikeEventConsumer consumer = null;
        ElasticsearchOffsetManager offsetManager = null;

        try {
            // Create offset manager and consumer
            offsetManager = new ElasticsearchOffsetManager();
            consumer = new KafkaLikeEventConsumer(sourceIndexName, consumerId, offsetManager);

            // Setup shutdown hook for graceful shutdown
            final KafkaLikeEventConsumer finalConsumer = consumer;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown signal received, stopping consumer gracefully...");
                finalConsumer.stop();
                finalConsumer.close();
                logger.info("Consumer shutdown completed");
            }));

            // Start consumer
            consumer.start();
            
            logger.info("Consumer started successfully!");
            logger.info("Press Ctrl+C to stop the consumer gracefully");

            // Keep main thread alive
            while (consumer.isRunning()) {
                Thread.sleep(5000);
                
                // Print status every 30 seconds
                if (System.currentTimeMillis() % 30000 < 5000) {
                    printConsumerStatus(consumer);
                }
            }

        } catch (Exception e) {
            logger.error("Error running single consumer application", e);
        } finally {
            // Cleanup
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    private static void printConsumerStatus(KafkaLikeEventConsumer consumer) {
        logger.info("=== CONSUMER STATUS ===");
        logger.info("Consumer ID: {}", consumer.getConsumerId());
        logger.info("Source Index: {}", consumer.getSourceIndexName());
        logger.info("Running: {}", consumer.isRunning());
        logger.info("Session Processed: {}", consumer.getSessionProcessedCount());
        logger.info("Total Processed: {}", consumer.getTotalProcessedCount());
        logger.info("Last Timestamp: {}", consumer.getLastProcessedTimestamp());
        logger.info("Last Doc ID: {}", consumer.getLastProcessedDocId());
        logger.info("=======================");
    }
}