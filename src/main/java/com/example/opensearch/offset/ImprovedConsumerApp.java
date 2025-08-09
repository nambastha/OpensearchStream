package com.example.opensearch.offset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * Improved Consumer Application using persistent offset management
 * Demonstrates sequence number based offset tracking with container restart resilience
 */
public class ImprovedConsumerApp {
    private static final Logger logger = LoggerFactory.getLogger(ImprovedConsumerApp.class);
    
    private static final String DEFAULT_INDEX_NAME = "events";
    private static final String DEFAULT_CONSUMER_GROUP = "event-processor-v1";
    
    public static void main(String[] args) {
        String indexName = args.length > 0 ? args[0] : DEFAULT_INDEX_NAME;
        String consumerGroup = args.length > 1 ? args[1] : DEFAULT_CONSUMER_GROUP;
        
        logger.info("Starting Improved OpenSearch Event Consumer");
        logger.info("Index: {}, Consumer Group: {}", indexName, consumerGroup);
        
        ImprovedEventConsumer consumer = null;
        
        try {
            // Create and start consumer
            consumer = new ImprovedEventConsumer(indexName, consumerGroup);
            consumer.start();
            
            // Setup shutdown hook
            final ImprovedEventConsumer finalConsumer = consumer;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down improved consumer...");
                finalConsumer.close();
                logger.info("Improved consumer shutdown complete");
            }));
            
            // Start monitoring thread
            startMonitoringThread(consumer);
            
            // Wait for user input to stop
            logger.info("Improved consumer started successfully!");
            logger.info("Commands: 'q'=quit, 's'=status, 'r'=reset offset, 'h'=help");
            
            try (Scanner scanner = new Scanner(System.in)) {
                String input;
                
                while (true) {
                    input = scanner.nextLine().trim().toLowerCase();
                    
                    if ("q".equals(input) || "quit".equals(input)) {
                        logger.info("Quit command received");
                        break;
                    } else if ("s".equals(input) || "status".equals(input)) {
                        printStatus(consumer);
                    } else if ("r".equals(input) || "reset".equals(input)) {
                        resetConsumerOffset(consumer);
                    } else if ("h".equals(input) || "help".equals(input)) {
                        printHelp();
                    } else {
                        logger.info("Unknown command: {}. Type 'h' for help", input);
                    }
                }
            }
            
        } catch (Exception e) {
            logger.error("Error running improved consumer application", e);
        } finally {
            // Cleanup
            if (consumer != null) {
                consumer.close();
            }
        }
    }
    
    private static void startMonitoringThread(ImprovedEventConsumer consumer) {
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
        
        monitoringThread.setName("ImprovedConsumerMonitoringThread");
        monitoringThread.setDaemon(true);
        monitoringThread.start();
    }
    
    private static void printStatus(ImprovedEventConsumer consumer) {
        ConsumerOffset offset = consumer.getCurrentOffset();
        
        logger.info("=== IMPROVED CONSUMER STATUS ===");
        logger.info("Running: {}", consumer.isRunning());
        logger.info("Consumer Group: {}", consumer.getConsumerGroupId());
        logger.info("Events Processed: {}", consumer.getProcessedCount());
        logger.info("Current Offset - seq_no: {}, primary_term: {}", 
            offset.getLastSeqNo(), offset.getLastPrimaryTerm());
        logger.info("Last Updated: {}", offset.getLastUpdated());
        logger.info("================================");
    }
    
    private static void resetConsumerOffset(ImprovedEventConsumer consumer) {
        logger.info("Are you sure you want to reset the consumer offset? This will reprocess all events. (y/N)");
        
        try (Scanner scanner = new Scanner(System.in)) {
            String confirmation = scanner.nextLine().trim().toLowerCase();
            
            if ("y".equals(confirmation) || "yes".equals(confirmation)) {
                consumer.resetOffset();
                logger.info("Consumer offset has been reset. Consumer will restart from the beginning.");
            } else {
                logger.info("Offset reset cancelled.");
            }
        }
    }
    
    private static void printHelp() {
        logger.info("=== IMPROVED CONSUMER COMMANDS ===");
        logger.info("q, quit   - Quit the consumer");
        logger.info("s, status - Show current status and offset information");
        logger.info("r, reset  - Reset consumer offset (reprocess all events)");
        logger.info("h, help   - Show this help message");
        logger.info("==================================");
        logger.info("");
        logger.info("Features of Improved Consumer:");
        logger.info("- Persistent offset storage using OpenSearch sequence numbers");
        logger.info("- Survives container restarts without reprocessing");
        logger.info("- Atomic offset commits for consistency");
        logger.info("- Consumer group support for multiple instances");
    }
}