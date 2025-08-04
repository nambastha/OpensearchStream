package com.streaming.opensearch.producer;

import com.streaming.opensearch.config.OpenSearchConfig;
import com.streaming.opensearch.model.Event;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Producer that continuously generates and sends events to OpenSearch
 */
public class EventProducer {
    private static final Logger logger = LoggerFactory.getLogger(EventProducer.class);
    
    private final OpenSearchClient client;
    private final String indexName;
    private final Random random;
    private final AtomicLong eventCounter;
    private volatile boolean running;
    
    private static final String[] EVENT_TYPES = {
        "user_login", "user_logout", "page_view", "button_click", 
        "purchase", "search", "download", "upload"
    };
    
    private static final String[] USER_IDS = {
        "user_001", "user_002", "user_003", "user_004", "user_005",
        "user_006", "user_007", "user_008", "user_009", "user_010"
    };

    public EventProducer(String indexName) {
        this.client = OpenSearchConfig.createClient();
        this.indexName = indexName;
        this.random = new Random();
        this.eventCounter = new AtomicLong(0);
        this.running = false;
        
        // Create index if it doesn't exist
        createIndexIfNotExists();
    }

    private void createIndexIfNotExists() {
        try {
            ExistsRequest existsRequest = ExistsRequest.of(e -> e.index(indexName));
            boolean exists = client.indices().exists(existsRequest).value();
            
            if (!exists) {
                logger.info("Creating index: {}", indexName);
                CreateIndexRequest createIndexRequest = CreateIndexRequest.of(c -> c
                    .index(indexName)
                    .mappings(m -> m
                        .properties("id", p -> p.keyword(k -> k))
                        .properties("timestamp", p -> p.date(d -> d))
                        .properties("eventType", p -> p.keyword(k -> k))
                        .properties("userId", p -> p.keyword(k -> k))
                        .properties("data", p -> p.text(t -> t))
                        .properties("processed", p -> p.boolean_(b -> b))
                    )
                );
                
                client.indices().create(createIndexRequest);
                logger.info("Index {} created successfully", indexName);
            } else {
                logger.info("Index {} already exists", indexName);
            }
        } catch (Exception e) {
            logger.error("Error creating index: {}", indexName, e);
            throw new RuntimeException("Failed to create index", e);
        }
    }

    public void start() {
        if (running) {
            logger.warn("Producer is already running");
            return;
        }
        
        running = true;
        logger.info("Starting event producer for index: {}", indexName);
        
        Thread producerThread = new Thread(this::produceEvents);
        producerThread.setName("EventProducer-" + indexName);
        producerThread.setDaemon(false);
        producerThread.start();
    }

    public void stop() {
        logger.info("Stopping event producer");
        running = false;
    }

    private void produceEvents() {
        while (running) {
            try {
                Event event = generateRandomEvent();
                indexEvent(event);
                
                long count = eventCounter.incrementAndGet();
                if (count % 100 == 0) {
                    logger.info("Produced {} events", count);
                }
                
                // Sleep for a random interval between 100ms to 2000ms
                Thread.sleep(100 + random.nextInt(1900));
                
            } catch (InterruptedException e) {
                logger.info("Producer thread interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error producing event", e);
                try {
                    Thread.sleep(5000); // Wait before retrying
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        logger.info("Event producer stopped. Total events produced: {}", eventCounter.get());
    }

    private Event generateRandomEvent() {
        String id = UUID.randomUUID().toString();
        String eventType = EVENT_TYPES[random.nextInt(EVENT_TYPES.length)];
        String userId = USER_IDS[random.nextInt(USER_IDS.length)];
        String data = String.format("Sample data for %s event by %s at %s", 
            eventType, userId, Instant.now());
        
        return new Event(id, eventType, userId, data);
    }

    private void indexEvent(Event event) {
        try {
            IndexRequest<Event> indexRequest = IndexRequest.of(i -> i
                .index(indexName)
                .id(event.getId())
                .document(event)
            );
            
            IndexResponse response = client.index(indexRequest);
            
            if (response.result().jsonValue().equals("created") || 
                response.result().jsonValue().equals("updated")) {
                logger.debug("Event indexed successfully: {}", event.getId());
            } else {
                logger.warn("Unexpected response for event {}: {}", event.getId(), response.result());
            }
            
        } catch (OpenSearchException e) {
            logger.error("OpenSearch error indexing event {}: {}", event.getId(), e.getMessage());
        } catch (Exception e) {
            logger.error("Error indexing event {}", event.getId(), e);
        }
    }

    public void close() {
        stop();
        OpenSearchConfig.closeClient(client);
    }

    public long getEventCount() {
        return eventCounter.get();
    }

    public boolean isRunning() {
        return running;
    }
    
    /**
     * Main method to test EventProducer standalone
     */
    public static void main(String[] args) {
        String indexName = args.length > 0 ? args[0] : "test-events";
        
        logger.info("Testing EventProducer with index: {}", indexName);
        
        EventProducer producer = null;
        try {
            producer = new EventProducer(indexName);
            producer.start();
            
            // Run for 30 seconds
            logger.info("Producer will run for 30 seconds...");
            Thread.sleep(30000);
            
            producer.stop();
            logger.info("EventProducer test completed. Total events produced: {}", producer.getEventCount());
            
        } catch (Exception e) {
            logger.error("Error testing EventProducer: {}", e.getMessage(), e);
        } finally {
            if (producer != null) {
                producer.close();
            }
        }
    }
}
