package com.sephora.services.ismsearchpoc.ipbatch.concurrency;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Abstraction for controlling concurrent execution.
 * Different implementations can use different mechanisms:
 * - Simple Spring ThreadPoolTaskExecutor
 * - Hybrid (Spring + Semaphore)
 * - Pure Semaphore
 */
public interface ConcurrencyController {
    
    /**
     * Submit a work item for execution
     * @return Future representing the submitted work
     */
    CompletableFuture<Void> submitWorkItem(Runnable workItem);
    
    /**
     * Submit a processing task within a work item
     * @return Future representing the submitted task
     */
    CompletableFuture<Void> submitProcessingTask(Runnable task);
    
    /**
     * Adjust concurrency levels
     * @param workItemConcurrency - desired concurrent work items
     * @param processingConcurrency - desired concurrent processing tasks
     */
    void adjustConcurrency(int workItemConcurrency, int processingConcurrency);
    
    /**
     * Get current concurrency settings
     */
    ConcurrencySettings getCurrentSettings();
    
    /**
     * Graceful shutdown
     */
    void shutdown();
    
    /**
     * Await termination
     */
    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;
}
