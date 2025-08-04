package com.example.opensearch;

import com.example.opensearch.producer.EventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * Standalone Producer Application
 * Continuously generates and indexes events to OpenSearch
 */
public class ProducerApp {
    private static final Logger logger = LoggerFactory.getLogger(ProducerApp.class);
    
    private static final String DEFAULT_INDEX_NAME = "events";
    
    public static void main(String[] args) {
        String indexName = args.length > 0 ? args[0] : DEFAULT_INDEX_NAME;
        
        logger.info("Starting OpenSearch Event Producer with index: {}", indexName);
        
        EventProducer producer = null;
        
        try {
            // Create and start producer
            producer = new EventProducer(indexName);
            producer.start();
            
            // Setup shutdown hook
            final EventProducer finalProducer = producer;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down producer...");
                finalProducer.close();
                logger.info("Producer shutdown complete");
            }));
            
            // Start monitoring thread
            startMonitoringThread(producer);
            
            // Wait for user input to stop
            logger.info("Producer started successfully!");
            logger.info("Press 'q' and Enter to quit, 's' and Enter for status, 'h' for help");
            
            try (Scanner scanner = new Scanner(System.in)) {
                String input;
                
                while (true) {
                    input = scanner.nextLine().trim().toLowerCase();
                    
                    if ("q".equals(input) || "quit".equals(input)) {
                        logger.info("Quit command received");
                        break;
                    } else if ("s".equals(input) || "status".equals(input)) {
                        printStatus(producer);
                    } else if ("h".equals(input) || "help".equals(input)) {
                        printHelp();
                    } else {
                        logger.info("Unknown command: {}. Type 'h' for help", input);
                    }
                }
            }
            
        } catch (Exception e) {
            logger.error("Error running producer application", e);
        } finally {
            // Cleanup
            if (producer != null) {
                producer.close();
            }
        }
    }
    
    private static void startMonitoringThread(EventProducer producer) {
        Thread monitoringThread = new Thread(() -> {
            while (producer.isRunning()) {
                try {
                    Thread.sleep(30000); // Print status every 30 seconds
                    printStatus(producer);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        
        monitoringThread.setName("ProducerMonitoringThread");
        monitoringThread.setDaemon(true);
        monitoringThread.start();
    }
    
    private static void printStatus(EventProducer producer) {
        logger.info("=== PRODUCER STATUS ===");
        logger.info("Running: {}, Events Produced: {}", 
            producer.isRunning(), producer.getEventCount());
        logger.info("=======================");
    }
    
    private static void printHelp() {
        logger.info("=== PRODUCER COMMANDS ===");
        logger.info("q, quit - Quit the producer");
        logger.info("s, status - Show current status");
        logger.info("h, help - Show this help message");
        logger.info("=========================");
    }
}
