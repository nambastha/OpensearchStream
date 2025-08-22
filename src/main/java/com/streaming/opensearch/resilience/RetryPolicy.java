package com.streaming.opensearch.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Retry policy with exponential backoff and jitter for resilient operations
 */
public class RetryPolicy {
    private static final Logger logger = LoggerFactory.getLogger(RetryPolicy.class);
    
    private final int maxAttempts;
    private final Duration baseDelay;
    private final Duration maxDelay;
    private final double jitterFactor;
    private final Predicate<Exception> retryPredicate;
    
    public static class Builder {
        private int maxAttempts = 3;
        private Duration baseDelay = Duration.ofMillis(100);
        private Duration maxDelay = Duration.ofSeconds(30);
        private double jitterFactor = 0.1;
        private Predicate<Exception> retryPredicate = (e) -> true;
        
        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }
        
        public Builder baseDelay(Duration baseDelay) {
            this.baseDelay = baseDelay;
            return this;
        }
        
        public Builder maxDelay(Duration maxDelay) {
            this.maxDelay = maxDelay;
            return this;
        }
        
        public Builder jitterFactor(double jitterFactor) {
            this.jitterFactor = jitterFactor;
            return this;
        }
        
        public Builder retryOn(Predicate<Exception> predicate) {
            this.retryPredicate = predicate;
            return this;
        }
        
        public RetryPolicy build() {
            return new RetryPolicy(maxAttempts, baseDelay, maxDelay, jitterFactor, retryPredicate);
        }
    }
    
    private RetryPolicy(int maxAttempts, Duration baseDelay, Duration maxDelay, 
                       double jitterFactor, Predicate<Exception> retryPredicate) {
        this.maxAttempts = maxAttempts;
        this.baseDelay = baseDelay;
        this.maxDelay = maxDelay;
        this.jitterFactor = jitterFactor;
        this.retryPredicate = retryPredicate;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Execute operation with retry policy
     */
    public <T> T execute(Supplier<T> operation, String operationName) throws Exception {
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return operation.get();
            } catch (Exception e) {
                lastException = e;
                
                if (attempt == maxAttempts || !retryPredicate.test(e)) {
                    logger.error("Operation '{}' failed after {} attempts", operationName, attempt, e);
                    throw e;
                }
                
                Duration delay = calculateDelay(attempt);
                logger.warn("Operation '{}' failed on attempt {}/{}, retrying in {}ms: {}", 
                    operationName, attempt, maxAttempts, delay.toMillis(), e.getMessage());
                
                try {
                    Thread.sleep(delay.toMillis());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Retry interrupted", ie);
                }
            }
        }
        
        throw lastException != null ? lastException : new RuntimeException("Operation failed after " + maxAttempts + " attempts");
    }
    
    /**
     * Execute void operation with retry policy
     */
    public void executeVoid(Runnable operation, String operationName) throws Exception {
        execute(() -> {
            operation.run();
            return null;
        }, operationName);
    }
    
    private Duration calculateDelay(int attempt) {
        // Exponential backoff: baseDelay * 2^(attempt-1)
        long delayMs = baseDelay.toMillis() * (1L << (attempt - 1));
        delayMs = Math.min(delayMs, maxDelay.toMillis());
        
        // Add jitter to prevent thundering herd
        if (jitterFactor > 0) {
            double jitter = ThreadLocalRandom.current().nextDouble(-jitterFactor, jitterFactor);
            delayMs = (long) (delayMs * (1 + jitter));
        }
        
        return Duration.ofMillis(Math.max(delayMs, 0));
    }
}