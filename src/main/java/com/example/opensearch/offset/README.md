# Improved OpenSearch Event Consumer with Persistent Offset Management

This package provides a robust, container-friendly solution for consuming events from OpenSearch with persistent offset management that survives pod restarts.

## Key Features

- **Persistent Offset Storage**: Uses OpenSearch sequence numbers (`_seq_no`, `_primary_term`) for reliable ordering
- **Container Restart Resilience**: Survives pod restarts without reprocessing events
- **No External Dependencies**: Uses OpenSearch itself as offset store (no PVC or external DB required)
- **Consumer Group Support**: Multiple consumer instances with different group IDs
- **Atomic Offset Commits**: Ensures data consistency
- **Memory Efficient**: No in-memory caching of processed event IDs

## How It Works

1. **Offset Index**: Creates a dedicated `consumer-offsets` index to store consumer positions
2. **Sequence Numbers**: Uses OpenSearch's built-in `_seq_no` for strict ordering (works in both OpenSearch and Elasticsearch)
3. **Atomic Operations**: Each offset commit is an atomic operation using document versioning
4. **Resume Logic**: On startup, loads last committed offset and resumes from that position

## Usage

### Basic Usage

```java
// Create consumer with group ID
ImprovedEventConsumer consumer = new ImprovedEventConsumer("events", "my-consumer-group");

// Start consuming
consumer.start();

// Stop and cleanup
consumer.close();
```

### Running the Application

```bash
# Use defaults (index: "events", group: "event-processor-v1")
java -cp target/classes com.example.opensearch.offset.ImprovedConsumerApp

# Specify index and consumer group
java -cp target/classes com.example.opensearch.offset.ImprovedConsumerApp events my-consumer-group
```

### Commands

- `s` - Show status and current offset position  
- `r` - Reset offset (reprocess all events from beginning)
- `q` - Quit consumer
- `h` - Show help

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌──────────────────┐
│   Events Index  │    │ Consumer Offsets│    │ ImprovedConsumer │
│                 │    │     Index       │    │                  │
│ - Event docs    │◄───┤ - Group ID      │◄───┤ - Fetch events   │
│ - _seq_no       │    │ - Last seq_no   │    │ - Process        │
│ - _primary_term │    │ - Primary term  │    │ - Commit offset  │
└─────────────────┘    └─────────────────┘    └──────────────────┘
```

## Benefits Over Original Approach

| Aspect | Original Approach | Improved Approach |
|--------|------------------|-------------------|
| **State Management** | In-memory HashSet | Persistent OpenSearch index |
| **Restart Behavior** | Reprocesses from epoch | Resumes from last offset |
| **Memory Usage** | Grows indefinitely | Constant memory usage |
| **Ordering** | Timestamp-based (unreliable) | Sequence number-based (guaranteed) |
| **Race Conditions** | Possible with concurrent consumers | Atomic offset commits |
| **Container Support** | Poor (loses state) | Excellent (persistent state) |

## Container Deployment

Perfect for Kubernetes deployments where:
- Pods can be restarted/rescheduled
- No PVC (Persistent Volume Claims) available
- No external database dependencies allowed

The consumer will automatically resume from the last committed offset after any restart.

## Consumer Groups

Multiple consumer instances can run with different group IDs to process the same events independently:

```java
// Consumer group 1 - for analytics
ImprovedEventConsumer analytics = new ImprovedEventConsumer("events", "analytics-processor");

// Consumer group 2 - for notifications  
ImprovedEventConsumer notifications = new ImprovedEventConsumer("events", "notification-processor");
```

Each group maintains its own offset position.