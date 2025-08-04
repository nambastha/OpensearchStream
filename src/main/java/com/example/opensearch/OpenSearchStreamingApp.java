package com.example.opensearch;

import com.example.opensearch.consumer.EventConsumer;
import com.example.opensearch.producer.EventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * Main application class that runs both producer and consumer
 */
public class OpenSearchStreamingApp {
    private static final Logger logger = LoggerFactory.getLogger(OpenSearchStreamingApp.class);
    
    private static final String DEFAULT_INDEX_NAME = "events";
    
    public static void main(String[] args) {
        String indexName = args.length > 0 ? args[0] : DEFAULT_INDEX_NAME;
        
        logger.info("Starting OpenSearch Streaming Application with index: {}", indexName);
        
        EventProducer producer = null;
        EventConsumer consumer = null;
        
        try {
            // Create producer and consumer
            producer = new EventProducer(indexName);
            consumer = new EventConsumer(indexName);
            
            // Start both producer and consumer
            producer.start();
            consumer.start();
            
            // Setup shutdown hook
            final EventProducer finalProducer = producer;
            final EventConsumer finalConsumer = consumer;
            
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down application...");
                finalProducer.close();
                finalConsumer.close();
                logger.info("Application shutdown complete");
            }));
            
            // Start monitoring thread
            startMonitoringThread(producer, consumer);
            
            // Wait for user input to stop
            logger.info("Application started successfully!");
            logger.info("Press 'q' and Enter to quit, 's' and Enter for status");
            
            try (Scanner scanner = new Scanner(System.in)) {
                String input;
                
                while (true) {
                    input = scanner.nextLine().trim().toLowerCase();
                    
                    if ("q".equals(input) || "quit".equals(input)) {
                        logger.info("Quit command received");
                        break;
                    } else if ("s".equals(input) || "status".equals(input)) {
                        printStatus(producer, consumer);
                    } else if ("h".equals(input) || "help".equals(input)) {
                        printHelp();
                    } else {
                        logger.info("Unknown command: {}. Type 'h' for help", input);
                    }
                }
            }
            
        } catch (Exception e) {
            logger.error("Error running application", e);
        } finally {
            // Cleanup
            if (producer != null) {
                producer.close();
            }
            if (consumer != null) {
                consumer.close();
            }
        }
    }
    
    private static void startMonitoringThread(EventProducer producer, EventConsumer consumer) {
        Thread monitoringThread = new Thread(() -> {
            while (producer.isRunning() || consumer.isRunning()) {
                try {
                    Thread.sleep(30000); // Print status every 30 seconds
                    printStatus(producer, consumer);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        
        monitoringThread.setName("MonitoringThread");
        monitoringThread.setDaemon(true);
        monitoringThread.start();
    }
    
    private static void printStatus(EventProducer producer, EventConsumer consumer) {
        logger.info("=== STATUS ===");
        logger.info("Producer - Running: {}, Events Produced: {}", 
            producer.isRunning(), producer.getEventCount());
        logger.info("Consumer - Running: {}, Events Processed: {}, Cache Size: {}", 
            consumer.isRunning(), consumer.getProcessedCount(), consumer.getProcessedEventsCacheSize());
        logger.info("=============");
    }
    
    private static void printHelp() {
        logger.info("=== COMMANDS ===");
        logger.info("q, quit - Quit the application");
        logger.info("s, status - Show current status");
        logger.info("h, help - Show this help message");
        logger.info("================");
    }
}
