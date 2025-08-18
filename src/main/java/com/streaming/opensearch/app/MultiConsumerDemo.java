package com.streaming.opensearch.app;

import com.streaming.opensearch.consumer.KafkaLikeEventConsumer;
import com.streaming.opensearch.offset.ElasticsearchOffsetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Demo application showing multiple consumers with independent offset management
 * Demonstrates Kafka-like consumer group behavior using Elasticsearch
 */
public class MultiConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(MultiConsumerDemo.class);

    private static final String DEFAULT_INDEX_NAME = "events";

    public static void main(String[] args) {
        String indexName = args.length > 0 ? args[0] : DEFAULT_INDEX_NAME;
        int numConsumers = args.length > 1 ? Integer.parseInt(args[1]) : 3;

        logger.info("Starting Multi-Consumer Demo");
        logger.info("Source Index: {}", indexName);
        logger.info("Number of Consumers: {}", numConsumers);

        List<KafkaLikeEventConsumer> consumers = new ArrayList<>();
        ElasticsearchOffsetManager offsetManager = null;

        try {
            // Create shared offset manager
            offsetManager = new ElasticsearchOffsetManager();

            // Create multiple consumers with different IDs
            for (int i = 1; i <= numConsumers; i++) {
                String consumerId = "demo-consumer-" + i;
                KafkaLikeEventConsumer consumer = new KafkaLikeEventConsumer(
                    indexName, consumerId, offsetManager);
                consumers.add(consumer);
                
                logger.info("Created consumer: {}", consumerId);
            }

            // Setup shutdown hook
            final List<KafkaLikeEventConsumer> finalConsumers = consumers;
            final ElasticsearchOffsetManager finalOffsetManager = offsetManager;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down multi-consumer demo...");
                finalConsumers.forEach(KafkaLikeEventConsumer::close);
                logger.info("Multi-consumer demo shutdown complete");
            }));

            // Start all consumers
            logger.info("Starting all consumers...");
            consumers.forEach(KafkaLikeEventConsumer::start);

            // Start monitoring thread
            startMonitoringThread(consumers, offsetManager);

            // Interactive command loop
            logger.info("All consumers started successfully!");
            printHelp();

            try (Scanner scanner = new Scanner(System.in)) {
                String input;

                while (true) {
                    System.out.print("multi-consumer> ");
                    input = scanner.nextLine().trim().toLowerCase();

                    if ("q".equals(input) || "quit".equals(input)) {
                        logger.info("Quit command received");
                        break;
                    } else if ("s".equals(input) || "status".equals(input)) {
                        printAllConsumerStatus(consumers);
                    } else if ("o".equals(input) || "offsets".equals(input)) {
                        printAllOffsetStatus(consumers, offsetManager);
                    } else if ("commit".equals(input)) {
                        consumers.forEach(consumer -> {
                            try {
                                consumer.commitOffset();
                                logger.info("Committed offset for {}", consumer.getConsumerId());
                            } catch (Exception e) {
                                logger.error("Error committing offset for {}", 
                                    consumer.getConsumerId(), e);
                            }
                        });
                    } else if ("reset".equals(input)) {
                        logger.info("Resetting all consumers to beginning...");
                        consumers.forEach(consumer -> {
                            try {
                                consumer.resetToBeginning();
                                logger.info("Reset consumer: {}", consumer.getConsumerId());
                            } catch (Exception e) {
                                logger.error("Error resetting consumer {}", 
                                    consumer.getConsumerId(), e);
                            }
                        });
                    } else if (input.startsWith("seek ")) {
                        String timestamp = input.substring(5).trim();
                        logger.info("Seeking all consumers to timestamp: {}", timestamp);
                        consumers.forEach(consumer -> {
                            try {
                                consumer.seekToTimestamp(timestamp);
                                logger.info("Seeked consumer: {}", consumer.getConsumerId());
                            } catch (Exception e) {
                                logger.error("Error seeking consumer {}", 
                                    consumer.getConsumerId(), e);
                            }
                        });
                    } else if ("stats".equals(input)) {
                        consumers.forEach(KafkaLikeEventConsumer::printConsumerStats);
                        offsetManager.printOffsetStats();
                    } else if (input.startsWith("stop ")) {
                        String consumerNum = input.substring(5).trim();
                        try {
                            int num = Integer.parseInt(consumerNum) - 1;
                            if (num >= 0 && num < consumers.size()) {
                                consumers.get(num).stop();
                                logger.info("Stopped consumer: {}", 
                                    consumers.get(num).getConsumerId());
                            } else {
                                logger.warn("Invalid consumer number: {}", consumerNum);
                            }
                        } catch (NumberFormatException e) {
                            logger.warn("Invalid consumer number format: {}", consumerNum);
                        }
                    } else if (input.startsWith("start ")) {
                        String consumerNum = input.substring(6).trim();
                        try {
                            int num = Integer.parseInt(consumerNum) - 1;
                            if (num >= 0 && num < consumers.size()) {
                                consumers.get(num).start();
                                logger.info("Started consumer: {}", 
                                    consumers.get(num).getConsumerId());
                            } else {
                                logger.warn("Invalid consumer number: {}", consumerNum);
                            }
                        } catch (NumberFormatException e) {
                            logger.warn("Invalid consumer number format: {}", consumerNum);
                        }
                    } else if ("h".equals(input) || "help".equals(input)) {
                        printHelp();
                    } else if (!input.isEmpty()) {
                        logger.info("Unknown command: '{}'. Type 'h' for help", input);
                    }
                }
            }

        } catch (Exception e) {
            logger.error("Error running multi-consumer demo", e);
        } finally {
            // Cleanup
            consumers.forEach(consumer -> {
                try {
                    consumer.close();
                } catch (Exception e) {
                    logger.error("Error closing consumer {}", 
                        consumer.getConsumerId(), e);
                }
            });
        }
    }

    private static void startMonitoringThread(List<KafkaLikeEventConsumer> consumers, 
                                            ElasticsearchOffsetManager offsetManager) {
        Thread monitoringThread = new Thread(() -> {
            while (consumers.stream().anyMatch(KafkaLikeEventConsumer::isRunning)) {
                try {
                    Thread.sleep(120000); // Print status every 2 minutes
                    printAllConsumerStatus(consumers);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        monitoringThread.setName("MultiConsumerMonitoringThread");
        monitoringThread.setDaemon(true);
        monitoringThread.start();
    }

    private static void printAllConsumerStatus(List<KafkaLikeEventConsumer> consumers) {
        logger.info("=== ALL CONSUMERS STATUS ===");
        for (int i = 0; i < consumers.size(); i++) {
            KafkaLikeEventConsumer consumer = consumers.get(i);
            logger.info("Consumer {}: ID={}, Running={}, Session={}, Total={}", 
                i + 1, consumer.getConsumerId(), consumer.isRunning(), 
                consumer.getSessionProcessedCount(), consumer.getTotalProcessedCount());
        }
        logger.info("============================");
    }

    private static void printAllOffsetStatus(List<KafkaLikeEventConsumer> consumers, 
                                           ElasticsearchOffsetManager offsetManager) {
        logger.info("=== ALL OFFSETS STATUS ===");
        logger.info("Offset Index: {}", offsetManager.getOffsetIndexName());
        for (int i = 0; i < consumers.size(); i++) {
            KafkaLikeEventConsumer consumer = consumers.get(i);
            logger.info("Consumer {}: ID={}, LastTimestamp={}, LastDocId={}, Total={}", 
                i + 1, consumer.getConsumerId(), 
                consumer.getLastProcessedTimestamp(), 
                consumer.getLastProcessedDocId(),
                consumer.getTotalProcessedCount());
        }
        logger.info("==========================");
    }

    private static void printHelp() {
        logger.info("=== MULTI-CONSUMER DEMO COMMANDS ===");
        logger.info("q, quit            - Quit all consumers");
        logger.info("s, status          - Show all consumer status");
        logger.info("o, offsets         - Show all offset information");
        logger.info("commit             - Manually commit all offsets");
        logger.info("reset              - Reset all consumers to beginning");
        logger.info("seek <timestamp>   - Seek all consumers to timestamp");
        logger.info("stop <num>         - Stop specific consumer (1-N)");
        logger.info("start <num>        - Start specific consumer (1-N)");
        logger.info("stats              - Show detailed stats for all consumers");
        logger.info("h, help            - Show this help message");
        logger.info("=====================================");
        logger.info("Example: stop 2    (stops consumer 2)");
        logger.info("Example: seek 2024-01-01T00:00:00Z");
    }
}