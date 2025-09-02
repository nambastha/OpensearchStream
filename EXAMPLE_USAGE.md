# Production Single Consumer Example

## Offset Index Structure

Your `consumer_offsets` index will store documents with this exact structure:

```json
{
  "consumerId": "prod-consumer-1",
  "indexName": "events",
  "lastProcessedTimestamp": "2024-01-15T14:30:25.123Z", 
  "lastProcessedDocId": "event-abc123",
  "lastUpdated": "2024-01-15T14:30:25.125Z",
  "totalProcessed": 1247
}
```

## Example Usage

### 1. Start Single Consumer
```bash
java -cp target/opensearch-streaming-1.0-SNAPSHOT.jar \
  com.streaming.opensearch.app.SingleConsumerApp events prod-consumer-1
```

### 2. What Happens

#### Initial State (First Run)
```json
// Document ID: prod-consumer-1_events
{
  "consumerId": "prod-consumer-1",
  "indexName": "events", 
  "lastProcessedTimestamp": "1970-01-01T00:00:00Z",
  "lastProcessedDocId": null,
  "lastUpdated": "2024-01-15T10:00:00.000Z",
  "totalProcessed": 0
}
```

#### After Processing 5 Events
```json
{
  "consumerId": "prod-consumer-1",
  "indexName": "events",
  "lastProcessedTimestamp": "2024-01-15T10:05:30.456Z",
  "lastProcessedDocId": "event-xyz789", 
  "lastUpdated": "2024-01-15T10:05:30.458Z",
  "totalProcessed": 5
}
```

#### After Restart (Resumes from Last Offset)
Consumer will query events with:
```
timestamp > "2024-01-15T10:05:30.456Z"
```

## Document ID Format

Each consumer creates one document in `consumer_offsets` index:
- **Document ID**: `{consumerId}_{indexName}`
- Example: `prod-consumer-1_events`

## Environment Variables

```bash
export ELASTICSEARCH_HOST=your-es-host
export ELASTICSEARCH_PORT=9200
export ELASTICSEARCH_USERNAME=your-username  
export ELASTICSEARCH_PASSWORD=your-password
```

## Production Example

### Consumer 1 Processing Events
```bash
# Terminal 1
java SingleConsumerApp events prod-consumer-1
```

Offset record:
```json
{
  "consumerId": "prod-consumer-1",
  "indexName": "events",
  "lastProcessedTimestamp": "2024-01-15T14:30:00.000Z",
  "lastProcessedDocId": "event-001",
  "lastUpdated": "2024-01-15T14:30:00.002Z", 
  "totalProcessed": 150
}
```

### Different Consumer, Same Index
```bash  
# Terminal 2 (different consumer ID)
java SingleConsumerApp events prod-consumer-2
```

Separate offset record:
```json
{
  "consumerId": "prod-consumer-2", 
  "indexName": "events",
  "lastProcessedTimestamp": "2024-01-15T14:25:00.000Z",
  "lastProcessedDocId": "event-087",
  "lastUpdated": "2024-01-15T14:25:00.003Z",
  "totalProcessed": 87
}
```

## Key Benefits

1. **Independent Tracking**: Each consumer maintains its own offset
2. **Fault Tolerant**: Survives consumer restarts
3. **Simple**: One consumer per application instance
4. **Production Ready**: No complex multi-consumer coordination
5. **Exact Structure**: Uses your specified JSON format

## Monitoring

Check consumer offset via Elasticsearch:
```bash
GET /consumer_offsets/_doc/prod-consumer-1_events
```

Response:
```json
{
  "_source": {
    "consumerId": "prod-consumer-1",
    "indexName": "events", 
    "lastProcessedTimestamp": "2024-01-15T14:30:25.123Z",
    "lastProcessedDocId": "event-abc123",
    "lastUpdated": "2024-01-15T14:30:25.125Z",
    "totalProcessed": 1247
  }
}
```

This is the **production-ready single consumer approach** you requested!