# Kafka-like Offset Management for Elasticsearch

This implementation provides Kafka-like consumer offset management using a separate Elasticsearch index to track consumer progress. 

**Note**: This solution uses pure Elasticsearch (not OpenSearch) and avoids all default Elasticsearch fields, using only the custom structure specified.

## Architecture Overview

### Core Components

1. **OffsetRecord** (`model/OffsetRecord.java`)
   - Data model for offset information
   - Fields: `consumerId`, `indexName`, `lastProcessedTimestamp`, `lastProcessedDocId`, `lastUpdated`, `totalProcessed`

2. **ElasticsearchOffsetManager** (`offset/ElasticsearchOffsetManager.java`)
   - Manages offset storage in separate `consumer_offsets` index
   - Thread-safe operations with ReentrantLock
   - Atomic updates using Elasticsearch script updates

3. **KafkaLikeEventConsumer** (`consumer/KafkaLikeEventConsumer.java`)
   - Enhanced consumer using separate offset management
   - Supports seek operations and offset commits
   - Memory-efficient with recent event caching

4. **Application Classes**
   - `KafkaLikeConsumerApp.java` - Single consumer application
   - `MultiConsumerDemo.java` - Multiple consumers demonstration

## Key Features

### Separate Offset Index
- Uses dedicated `consumer_offsets` index (similar to Kafka's `__consumer_offsets`)
- Document ID format: `{consumerId}_{indexName}`
- Proper index mappings for efficient queries

### Offset Management
```java
// Offset structure stored in Elasticsearch
{
  "consumerId": "consumer-12345",
  "indexName": "events", 
  "lastProcessedTimestamp": "2024-01-15T10:30:00Z",
  "lastProcessedDocId": "doc-98765",
  "lastUpdated": "2024-01-15T10:30:05Z",
  "totalProcessed": 1000
}
```

### Consumer Features
- **Automatic offset commits**: Every 10 events (configurable)
- **Manual offset commits**: `commitOffset()` method
- **Seek operations**: `seekToTimestamp()`, `resetToBeginning()`
- **Memory management**: Recent event cache with size limits
- **Thread safety**: Safe for concurrent operations

### Kafka-like Operations
```java
// Create consumer with specific ID
KafkaLikeEventConsumer consumer = new KafkaLikeEventConsumer("events", "my-consumer");

// Start consuming
consumer.start();

// Seek to specific timestamp (replay)
consumer.seekToTimestamp("2024-01-01T00:00:00Z");

// Manual offset commit
consumer.commitOffset();

// Reset to beginning
consumer.resetToBeginning();

// Get current position
String lastTimestamp = consumer.getLastProcessedTimestamp();
long totalProcessed = consumer.getTotalProcessedCount();
```

## Usage Examples

### Single Consumer
```bash
java -cp target/opensearch-streaming-1.0-SNAPSHOT.jar \
  com.streaming.opensearch.app.KafkaLikeConsumerApp events my-consumer
```

### Multiple Consumers
```bash
java -cp target/opensearch-streaming-1.0-SNAPSHOT.jar \
  com.streaming.opensearch.app.MultiConsumerDemo events 3
```

### Interactive Commands
- `status` - Show consumer status
- `offset` - Show offset information  
- `commit` - Manual offset commit
- `reset` - Reset to beginning
- `seek <timestamp>` - Seek to timestamp
- `stats` - Detailed statistics

## Index Structures

### Source Index (events)
```json
{
  "mappings": {
    "properties": {
      "id": {"type": "text"},
      "timestamp": {"type": "text"},
      "eventType": {"type": "text"},
      "userId": {"type": "text"},
      "data": {"type": "text"}
    }
  }
}
```

### Offset Index (consumer_offsets)
```json
{
  "mappings": {
    "properties": {
      "consumerId": {"type": "text"},
      "indexName": {"type": "text"}, 
      "lastProcessedTimestamp": {"type": "text"},
      "lastProcessedDocId": {"type": "text"},
      "lastUpdated": {"type": "text"},
      "totalProcessed": {"type": "long"}
    }
  }
}
```

**Note**: All fields use `text` type instead of Elasticsearch default types like `keyword` or `date` as per organizational requirements.

## Configuration

### Environment Variables
- `ELASTICSEARCH_HOST` - Elasticsearch host (default: localhost)
- `ELASTICSEARCH_PORT` - Elasticsearch port (default: 9200)
- `ELASTICSEARCH_SCHEME` - Protocol (default: http)
- `ELASTICSEARCH_USERNAME` - Authentication username
- `ELASTICSEARCH_PASSWORD` - Authentication password

### Consumer Configuration
```java
// Batch size for fetching events
private static final int BATCH_SIZE = 50;

// Polling interval between fetches
private static final long POLL_INTERVAL_MS = 2000;

// Recent events cache size
private static final int RECENT_CACHE_SIZE = 1000;

// Offset commit frequency
private static final int OFFSET_COMMIT_INTERVAL = 10;
```

## Advantages Over File-based Approach

1. **Distributed**: Multiple consumers can run independently
2. **Persistent**: Offsets survive consumer restarts
3. **Queryable**: Offset information available via Elasticsearch APIs
4. **Scalable**: Leverages Elasticsearch's distributed architecture
5. **Atomic**: Offset updates are atomic operations
6. **Monitoring**: Built-in monitoring through Elasticsearch

## Error Handling

- **Offset Load Failures**: Creates new offset record
- **Offset Save Failures**: Throws exception to prevent data loss
- **Elasticsearch Unavailable**: Consumer stops with error
- **Duplicate Processing**: Prevented by recent event cache

## Monitoring

### Consumer Metrics
- Session processed count
- Total processed count  
- Last processed timestamp
- Last processed document ID
- Consumer running status

### Offset Index Metrics
- Total offset records
- Index size and health
- Update frequency

## Implementation Notes

### Thread Safety
- Uses ReentrantLock for offset operations
- Atomic updates via Elasticsearch scripted updates
- Safe for concurrent consumer instances

### Memory Management
- Recent event cache with configurable size
- Automatic cache cleanup when limit exceeded
- Minimal memory footprint for long-running consumers

### Performance Optimizations
- Batch event fetching
- Periodic offset commits (not per-event)
- Efficient timestamp-based queries
- Index optimization for offset lookups

## Comparison with Kafka

| Feature | Kafka | This Implementation |
|---------|-------|-------------------|
| Offset Storage | `__consumer_offsets` topic | `consumer_offsets` index |
| Consumer Groups | Native support | Manual consumer ID management |
| Partition Assignment | Automatic | Single consumer per index |
| Offset Commits | Auto/Manual | Auto/Manual |
| Seek Operations | Yes | Yes |
| Exactly-once | Transactions | Idempotent processing |

## Future Enhancements

1. **Consumer Groups**: Automatic partition assignment
2. **Rebalancing**: Dynamic consumer coordination  
3. **Dead Letter Queue**: Failed event handling
4. **Metrics Integration**: Prometheus/Grafana support
5. **Schema Evolution**: Offset record versioning
6. **Compression**: Large offset record optimization

This implementation provides enterprise-grade offset management suitable for production Elasticsearch streaming applications.