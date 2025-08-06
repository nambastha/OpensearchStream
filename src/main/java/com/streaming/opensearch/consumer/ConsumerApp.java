package com.streaming.opensearch.consumer;

import com.streaming.opensearch.consumer.EventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * Standalone Consumer Application
 * Continuously reads and processes events from OpenSearch without duplicates
 */
public class ConsumerApp {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerApp.class);

    private static final String DEFAULT_INDEX_NAME = "events";

    public static void main(String[] args) {
        String indexName = args.length > 0 ? args[0] : DEFAULT_INDEX_NAME;

        logger.info("Starting OpenSearch Event Consumer with index: {}", indexName);

        EventConsumer consumer = null;

        try {
            // Create and start consumer
            consumer = new EventConsumer(indexName);
            consumer.start();

            // Setup shutdown hook
            final EventConsumer finalConsumer = consumer;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down consumer...");
                finalConsumer.close();
                logger.info("Consumer shutdown complete");
            }));

            // Start monitoring thread
            startMonitoringThread(consumer);

            // Wait for user input to stop
            logger.info("Consumer started successfully!");
            logger.info("Press 'q' and Enter to quit, 's' and Enter for status, 'h' for help");

            try (Scanner scanner = new Scanner(System.in)) {
                String input;

                while (true) {
                    input = scanner.nextLine().trim().toLowerCase();

                    if ("q".equals(input) || "quit".equals(input)) {
                        logger.info("Quit command received");
                        break;
                    } else if ("s".equals(input) || "status".equals(input)) {
                        printStatus(consumer);
                    } else if ("h".equals(input) || "help".equals(input)) {
                        printHelp();
                    } else {
                        logger.info("Unknown command: {}. Type 'h' for help", input);
                    }
                }
            }

        } catch (Exception e) {
            logger.error("Error running consumer application", e);
        } finally {
            // Cleanup
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    private static void startMonitoringThread(EventConsumer consumer) {
        Thread monitoringThread = new Thread(() -> {
            while (consumer.isRunning()) {
                try {
                    Thread.sleep(30000); // Print status every 30 seconds
                    printStatus(consumer);
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

    private static void printStatus(EventConsumer consumer) {
        logger.info("=== CONSUMER STATUS ===");
        logger.info("Running: {}, Events Processed: {}, Cache Size: {}",
                consumer.isRunning(), consumer.getProcessedCount(), consumer.getProcessedEventsCacheSize());
        logger.info("=======================");
    }

    private static void printHelp() {
        logger.info("=== CONSUMER COMMANDS ===");
        logger.info("q, quit - Quit the consumer");
        logger.info("s, status - Show current status");
        logger.info("h, help - Show this help message");
        logger.info("=========================");
    }
}