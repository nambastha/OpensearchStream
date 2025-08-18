package com.streaming.opensearch.app;

import com.streaming.opensearch.consumer.KafkaLikeEventConsumer;
import com.streaming.opensearch.offset.ElasticsearchOffsetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * Main application for Kafka-like event consumer with Elasticsearch offset management
 * Demonstrates usage of separate consumer_offsets index for offset tracking
 */
public class KafkaLikeConsumerApp {
    private static final Logger logger = LoggerFactory.getLogger(KafkaLikeConsumerApp.class);

    private static final String DEFAULT_INDEX_NAME = "events";
    private static final String DEFAULT_CONSUMER_ID = "test-consumer";

    public static void main(String[] args) {
        String indexName = args.length > 0 ? args[0] : DEFAULT_INDEX_NAME;
        String consumerId = args.length > 1 ? args[1] : DEFAULT_CONSUMER_ID;

        logger.info("Starting Kafka-like Event Consumer Application");
        logger.info("Source Index: {}", indexName);
        logger.info("Consumer ID: {}", consumerId);

        KafkaLikeEventConsumer consumer = null;
        ElasticsearchOffsetManager offsetManager = null;

        try {
            // Create offset manager and consumer
            offsetManager = new ElasticsearchOffsetManager();
            consumer = new KafkaLikeEventConsumer(indexName, consumerId, offsetManager);

            // Setup shutdown hook
            final KafkaLikeEventConsumer finalConsumer = consumer;
            final ElasticsearchOffsetManager finalOffsetManager = offsetManager;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down consumer application...");
                finalConsumer.close();
                logger.info("Consumer application shutdown complete");
            }));

            // Start consumer
            consumer.start();

            // Start monitoring thread
            startMonitoringThread(consumer, offsetManager);

            // Interactive command loop
            logger.info("Consumer started successfully!");
            printHelp();

            try (Scanner scanner = new Scanner(System.in)) {
                String input;

                while (true) {
                    System.out.print("kafka-consumer> ");
                    input = scanner.nextLine().trim().toLowerCase();

                    if ("q".equals(input) || "quit".equals(input)) {
                        logger.info("Quit command received");
                        break;
                    } else if ("s".equals(input) || "status".equals(input)) {
                        printConsumerStatus(consumer);
                    } else if ("o".equals(input) || "offset".equals(input)) {
                        printOffsetStatus(consumer, offsetManager);
                    } else if ("commit".equals(input)) {
                        consumer.commitOffset();
                        logger.info("Manual offset commit completed");
                    } else if ("reset".equals(input)) {
                        logger.info("Resetting consumer to beginning...");
                        consumer.resetToBeginning();
                        logger.info("Consumer reset completed");
                    } else if (input.startsWith("seek ")) {
                        String timestamp = input.substring(5).trim();
                        logger.info("Seeking to timestamp: {}", timestamp);
                        consumer.seekToTimestamp(timestamp);
                        logger.info("Seek completed");
                    } else if ("stats".equals(input)) {
                        consumer.printConsumerStats();
                        offsetManager.printOffsetStats();
                    } else if ("h".equals(input) || "help".equals(input)) {
                        printHelp();
                    } else if (!input.isEmpty()) {
                        logger.info("Unknown command: '{}'. Type 'h' for help", input);
                    }
                }
            }

        } catch (Exception e) {
            logger.error("Error running Kafka-like consumer application", e);
        } finally {
            // Cleanup
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    private static void startMonitoringThread(KafkaLikeEventConsumer consumer, 
                                            ElasticsearchOffsetManager offsetManager) {
        Thread monitoringThread = new Thread(() -> {
            while (consumer.isRunning()) {
                try {
                    Thread.sleep(60000); // Print status every 60 seconds
                    printConsumerStatus(consumer);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        monitoringThread.setName("ConsumerMonitoringThread");
        monitoringThread.setDaemon(true);
        monitoringThread.start();
    }

    private static void printConsumerStatus(KafkaLikeEventConsumer consumer) {
        logger.info("=== CONSUMER STATUS ===");
        logger.info("Consumer ID: {}", consumer.getConsumerId());
        logger.info("Source Index: {}", consumer.getSourceIndexName());
        logger.info("Running: {}", consumer.isRunning());
        logger.info("Session Processed: {}", consumer.getSessionProcessedCount());
        logger.info("Total Processed: {}", consumer.getTotalProcessedCount());
        logger.info("=======================");
    }

    private static void printOffsetStatus(KafkaLikeEventConsumer consumer, 
                                        ElasticsearchOffsetManager offsetManager) {
        logger.info("=== OFFSET STATUS ===");
        logger.info("Consumer ID: {}", consumer.getConsumerId());
        logger.info("Index: {}", consumer.getSourceIndexName());
        logger.info("Offset Index: {}", offsetManager.getOffsetIndexName());
        logger.info("Last Timestamp: {}", consumer.getLastProcessedTimestamp());
        logger.info("Last Doc ID: {}", consumer.getLastProcessedDocId());
        logger.info("Total Processed: {}", consumer.getTotalProcessedCount());
        logger.info("Last Updated: {}", consumer.getCurrentOffset().getLastUpdated());
        logger.info("=====================");
    }

    private static void printHelp() {
        logger.info("=== KAFKA-LIKE CONSUMER COMMANDS ===");
        logger.info("q, quit        - Quit the consumer");
        logger.info("s, status      - Show consumer status");
        logger.info("o, offset      - Show offset information");
        logger.info("commit         - Manually commit current offset");
        logger.info("reset          - Reset consumer to beginning");
        logger.info("seek <timestamp> - Seek to specific timestamp (ISO format)");
        logger.info("stats          - Show detailed consumer and offset stats");
        logger.info("h, help        - Show this help message");
        logger.info("=====================================");
        logger.info("Example: seek 2024-01-01T00:00:00Z");
    }
}