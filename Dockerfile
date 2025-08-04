FROM openjdk:17-jre-slim

# Set working directory
WORKDIR /app

# Copy the JAR file
COPY target/opensearch-streaming-1.0-SNAPSHOT.jar app.jar

# Create offset storage directory
RUN mkdir -p /app/offsets

# Set environment variables
ENV JAVA_OPTS="-Xmx512m -Xms256m"
ENV OFFSET_STORAGE_PATH="/app/offsets"

# Expose any ports if needed (for health checks, metrics, etc.)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# Run the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
