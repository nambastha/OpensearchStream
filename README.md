# OpenSearch Streaming Application

A Java application that demonstrates continuous data streaming with OpenSearch, featuring a producer that generates events and a read-only consumer that processes them without duplicates using file-based persistence.

## Features

- **Event Producer**: Continuously generates and indexes events to OpenSearch
- **Read-Only Consumer**: Processes events without writing to OpenSearch (safe for shared environments)
- **File-Based Duplicate Prevention**: Uses local files to track processed events
- **Configurable**: Supports environment variables for OpenSearch connection
- **Monitoring**: Built-in status monitoring and logging
- **Graceful Shutdown**: Proper cleanup on application termination
- **Multi-Team Safe**: Never modifies source data or creates new indices

## Prerequisites

- Java 11 or higher
- Maven 3.6+
- OpenSearch cluster (running in Kubernetes pod)

## Configuration

The application can be configured using environment variables:

```bash
export OPENSEARCH_HOST=your-opensearch-host
export OPENSEARCH_PORT=9200
export OPENSEARCH_SCHEME=http
export OPENSEARCH_USERNAME=your-username  # Optional
export OPENSEARCH_PASSWORD=your-password  # Optional
```

For Kubernetes deployment, you can port-forward to access OpenSearch:
```bash
kubectl port-forward service/opensearch-service 9200:9200
```

## Building and Running

1. **Build the project:**
   ```bash
   mvn clean compile
   ```

2. **Run applications:**

   **Producer only:**
   ```bash
   mvn exec:java -Dexec.mainClass="com.streaming.opensearch.app.ProducerApp"
   ```

   **Read-Only Consumer (recommended for shared environments):**
   ```bash
   mvn exec:java
   # or explicitly
   mvn exec:java -Dexec.mainClass="com.streaming.opensearch.app.ReadOnlyConsumerApp"
   ```

   **With custom index name:**
   ```bash
   mvn exec:java -Dexec.mainClass="com.streaming.opensearch.app.ProducerApp" -Dexec.args="my-custom-index"
   mvn exec:java -Dexec.mainClass="com.streaming.opensearch.app.ReadOnlyConsumerApp" -Dexec.args="my-custom-index"
   ```

3. **Package as JAR:**
   ```bash
   mvn clean package
   # Run producer only
   java -cp target/opensearch-streaming-1.0-SNAPSHOT.jar com.streaming.opensearch.app.ProducerApp
   # Run read-only consumer only
   java -cp target/opensearch-streaming-1.0-SNAPSHOT.jar com.streaming.opensearch.app.ReadOnlyConsumerApp
   ```

## Usage

Once running:

**Producer will:**
1. Create an OpenSearch index (default: "events") if it doesn't exist
2. Continuously generate and index random events
3. Display periodic status updates

**Read-Only Consumer will:**
1. Read events from the existing OpenSearch index
2. Process events without modifying the source data
3. Track processed events using local files (processed_events_<index>.txt)
4. Maintain checkpoint for recovery (checkpoint_<index>.txt)
5. Display periodic status updates

### Interactive Commands

While running, you can use these commands:
- `s` or `status` - Show current status
- `h` or `help` - Show help message
- `q` or `quit` - Quit the application

## Architecture

### Event Model
Events contain:
- `id`: Unique identifier
- `timestamp`: Event creation time
- `eventType`: Type of event (login, purchase, etc.)
- `userId`: User identifier
- `data`: Event payload
- `processed`: Processing status flag

### Producer
- Generates random events with various types
- Indexes events to OpenSearch with unique IDs
- Configurable generation rate (100ms - 2s intervals)
- Handles connection failures with retry logic

### Consumer
- Polls for unprocessed events
- Maintains in-memory cache of processed event IDs
- Updates events as processed in OpenSearch
- Prevents duplicate processing
- Automatic cache cleanup to prevent memory leaks

## Monitoring

The application provides:
- Console logging with configurable levels
- File logging with rotation
- Periodic status reports
- Event counters and metrics

## Kubernetes Integration

To connect to OpenSearch running in Kubernetes:

1. **Port forwarding:**
   ```bash
   kubectl port-forward pod/opensearch-pod-name 9200:9200
   ```

2. **Service access:**
   ```bash
   kubectl port-forward service/opensearch-service 9200:9200
   ```

3. **Set environment variables:**
   ```bash
   export OPENSEARCH_HOST=localhost
   export OPENSEARCH_PORT=9200
   ```

## Troubleshooting

### Connection Issues
- Verify OpenSearch is accessible
- Check port forwarding if using Kubernetes
- Verify credentials if authentication is enabled

### Performance Tuning
- Adjust `BATCH_SIZE` in EventConsumer for throughput
- Modify `POLL_INTERVAL_MS` for polling frequency
- Configure JVM heap size for large datasets

### Memory Management
- Consumer automatically cleans processed ID cache
- Monitor heap usage for long-running instances
- Adjust cache size limits if needed

## Dependencies

- OpenSearch Java Client 2.11.1
- Jackson for JSON processing
- SLF4J + Logback for logging
- Apache HTTP Client for connectivity
