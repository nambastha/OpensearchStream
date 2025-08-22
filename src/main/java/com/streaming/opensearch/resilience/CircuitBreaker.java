package com.streaming.opensearch.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Circuit breaker pattern implementation for resilient operations
 * States: CLOSED -> OPEN -> HALF_OPEN -> CLOSED
 */
public class CircuitBreaker {
    private static final Logger logger = LoggerFactory.getLogger(CircuitBreaker.class);
    
    public enum State {
        CLOSED,    // Normal operation
        OPEN,      // Circuit is open, calls fail fast
        HALF_OPEN  // Testing if service is back up
    }
    
    private final String name;
    private final int failureThreshold;
    private final Duration timeout;
    private final int halfOpenMaxCalls;
    
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger halfOpenCalls = new AtomicInteger(0);
    private final AtomicLong lastFailureTime = new AtomicLong(0);
    
    public static class Builder {
        private String name = "CircuitBreaker";
        private int failureThreshold = 5;
        private Duration timeout = Duration.ofMinutes(1);
        private int halfOpenMaxCalls = 3;
        
        public Builder name(String name) {
            this.name = name;
            return this;
        }
        
        public Builder failureThreshold(int threshold) {
            this.failureThreshold = threshold;
            return this;
        }
        
        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }
        
        public Builder halfOpenMaxCalls(int maxCalls) {
            this.halfOpenMaxCalls = maxCalls;
            return this;
        }
        
        public CircuitBreaker build() {
            return new CircuitBreaker(name, failureThreshold, timeout, halfOpenMaxCalls);
        }
    }
    
    private CircuitBreaker(String name, int failureThreshold, Duration timeout, int halfOpenMaxCalls) {
        this.name = name;
        this.failureThreshold = failureThreshold;
        this.timeout = timeout;
        this.halfOpenMaxCalls = halfOpenMaxCalls;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Execute operation through circuit breaker
     */
    public <T> T execute(Supplier<T> operation) throws Exception {
        State currentState = state.get();
        
        switch (currentState) {
            case OPEN:
                if (shouldAttemptReset()) {
                    return executeInHalfOpenState(operation);
                } else {
                    throw new CircuitBreakerOpenException("Circuit breaker '" + name + "' is OPEN");
                }
                
            case HALF_OPEN:
                return executeInHalfOpenState(operation);
                
            case CLOSED:
            default:
                return executeInClosedState(operation);
        }
    }
    
    /**
     * Execute void operation through circuit breaker
     */
    public void executeVoid(Runnable operation) throws Exception {
        execute(() -> {
            operation.run();
            return null;
        });
    }
    
    private <T> T executeInClosedState(Supplier<T> operation) throws Exception {
        try {
            T result = operation.get();
            onSuccess();
            return result;
        } catch (Exception e) {
            onFailure();
            throw e;
        }
    }
    
    private <T> T executeInHalfOpenState(Supplier<T> operation) throws Exception {
        if (halfOpenCalls.incrementAndGet() > halfOpenMaxCalls) {
            throw new CircuitBreakerOpenException("Circuit breaker '" + name + "' max half-open calls exceeded");
        }
        
        try {
            T result = operation.get();
            onHalfOpenSuccess();
            return result;
        } catch (Exception e) {
            onHalfOpenFailure();
            throw e;
        }
    }
    
    private void onSuccess() {
        failureCount.set(0);
        successCount.incrementAndGet();
    }
    
    private void onFailure() {
        int failures = failureCount.incrementAndGet();
        lastFailureTime.set(System.currentTimeMillis());
        
        if (failures >= failureThreshold) {
            state.set(State.OPEN);
            logger.warn("Circuit breaker '{}' opened after {} failures", name, failures);
        }
    }
    
    private void onHalfOpenSuccess() {
        state.set(State.CLOSED);
        failureCount.set(0);
        halfOpenCalls.set(0);
        logger.info("Circuit breaker '{}' closed after successful test", name);
    }
    
    private void onHalfOpenFailure() {
        state.set(State.OPEN);
        lastFailureTime.set(System.currentTimeMillis());
        halfOpenCalls.set(0);
        logger.warn("Circuit breaker '{}' remained open after failed test", name);
    }
    
    private boolean shouldAttemptReset() {
        long timeSinceLastFailure = System.currentTimeMillis() - lastFailureTime.get();
        if (timeSinceLastFailure >= timeout.toMillis()) {
            if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                halfOpenCalls.set(0);
                logger.info("Circuit breaker '{}' entering half-open state", name);
                return true;
            }
        }
        return state.get() == State.HALF_OPEN;
    }
    
    public State getState() {
        return state.get();
    }
    
    public int getFailureCount() {
        return failureCount.get();
    }
    
    public int getSuccessCount() {
        return successCount.get();
    }
    
    public String getName() {
        return name;
    }
    
    /**
     * Exception thrown when circuit breaker is open
     */
    public static class CircuitBreakerOpenException extends RuntimeException {
        public CircuitBreakerOpenException(String message) {
            super(message);
        }
    }
}