package com.streaming.opensearch.app;

import com.streaming.opensearch.consumer.ReadOnlyEventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * Read-Only Consumer Application
 * Uses local file persistence to prevent duplicates without writing to OpenSearch at all
 * Perfect for highly constrained environments where no OpenSearch writes are allowed
 */
public class ReadOnlyConsumerApp {
    private static final Logger logger = LoggerFactory.getLogger(ReadOnlyConsumerApp.class);
    
    private static final String DEFAULT_INDEX_NAME = "events";
    
    public static void main(String[] args) {
        String indexName = args.length > 0 ? args[0] : DEFAULT_INDEX_NAME;
        
        logger.info("Starting Read-Only OpenSearch Event Consumer with source index: {}", indexName);
        
        ReadOnlyEventConsumer consumer = null;
        
        try {
            // Create and start consumer
            consumer = new ReadOnlyEventConsumer(indexName);
            consumer.start();
            
            // Setup shutdown hook
            final ReadOnlyEventConsumer finalConsumer = consumer;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down read-only consumer...");
                finalConsumer.close();
                logger.info("Read-only consumer shutdown complete");
            }));
            
            // Start monitoring thread
            startMonitoringThread(consumer);
            
            // Wait for user input to stop
            logger.info("Read-only consumer started successfully!");
            logger.info("Source Index: {}", indexName);
            logger.info("Tracking File: {}", consumer.getTrackingFilePath());
            logger.info("Checkpoint File: {}", consumer.getCheckpointFilePath());
            logger.info("Last Processed: {}", consumer.getLastProcessedTimestamp());
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
            logger.error("Error running read-only consumer application", e);
        } finally {
            // Cleanup
            if (consumer != null) {
                consumer.close();
            }
        }
    }
    
    private static void startMonitoringThread(ReadOnlyEventConsumer consumer) {
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
        
        monitoringThread.setName("ReadOnlyConsumerMonitoringThread");
        monitoringThread.setDaemon(true);
        monitoringThread.start();
    }
    
    private static void printStatus(ReadOnlyEventConsumer consumer) {
        logger.info("=== READ-ONLY CONSUMER STATUS ===");
        logger.info("Running: {}, Events Processed: {}, Cache Size: {}", 
            consumer.isRunning(), consumer.getProcessedCount(), consumer.getProcessedEventsCacheSize());
        logger.info("Source Index: {}", consumer.getTrackingFilePath().replace("processed_events_", "").replace(".txt", ""));
        logger.info("Tracking File: {}", consumer.getTrackingFilePath());
        logger.info("Checkpoint File: {}", consumer.getCheckpointFilePath());
        logger.info("Last Processed: {}", consumer.getLastProcessedTimestamp());
        logger.info("=================================");
    }
    
    private static void printHelp() {
        logger.info("=== READ-ONLY CONSUMER COMMANDS ===");
        logger.info("q, quit - Quit the consumer");
        logger.info("s, status - Show current status");
        logger.info("h, help - Show this help message");
        logger.info("===================================");
    }
}
