package com.sephora.services.ismsearchpoc.ipbatch.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Simple implementation using Spring's ThreadPoolTaskExecutor
 * Lets Spring handle all concurrency control
 */
public class SpringThreadPoolConcurrencyController implements ConcurrencyController {
    
    private static final Logger log = LoggerFactory.getLogger(SpringThreadPoolConcurrencyController.class);
    
    private final ThreadPoolTaskExecutor workItemExecutor;
    private final ThreadPoolTaskExecutor processingExecutor;
    
    public SpringThreadPoolConcurrencyController(int initialWorkItemConcurrency,
                                                 int initialProcessingConcurrency,
                                                 int maxWorkItemConcurrency,
                                                 int maxProcessingConcurrency) {
        
        // Work item executor
        this.workItemExecutor = new ThreadPoolTaskExecutor();
        this.workItemExecutor.setCorePoolSize(initialWorkItemConcurrency);
        this.workItemExecutor.setMaxPoolSize(maxWorkItemConcurrency);
        this.workItemExecutor.setQueueCapacity(500);
        this.workItemExecutor.setThreadNamePrefix("work-item-");
        this.workItemExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        this.workItemExecutor.initialize();
        
        // Processing executor
        this.processingExecutor = new ThreadPoolTaskExecutor();
        this.processingExecutor.setCorePoolSize(initialProcessingConcurrency);
        this.processingExecutor.setMaxPoolSize(maxProcessingConcurrency);
        this.processingExecutor.setQueueCapacity(1000);
        this.processingExecutor.setThreadNamePrefix("processing-");
        this.processingExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        this.processingExecutor.initialize();
        
        log.info("SpringThreadPoolConcurrencyController initialized: workItem={}, processing={}",
            initialWorkItemConcurrency, initialProcessingConcurrency);
    }
    
    @Override
    public CompletableFuture<Void> submitWorkItem(Runnable workItem) {
        return CompletableFuture.runAsync(workItem, workItemExecutor);
    }
    
    @Override
    public CompletableFuture<Void> submitProcessingTask(Runnable task) {
        return CompletableFuture.runAsync(task, processingExecutor);
    }
    
    @Override
    public void adjustConcurrency(int workItemConcurrency, int processingConcurrency) {
        log.info("Adjusting concurrency: workItem {} -> {}, processing {} -> {}",
            workItemExecutor.getCorePoolSize(), workItemConcurrency,
            processingExecutor.getCorePoolSize(), processingConcurrency);
        
        // Spring will gradually adjust pool sizes
        workItemExecutor.setCorePoolSize(workItemConcurrency);
        processingExecutor.setCorePoolSize(processingConcurrency);
        
        // Note: Adjustment is NOT instant - Spring manages thread lifecycle
    }
    
    @Override
    public ConcurrencySettings getCurrentSettings() {
        return ConcurrencySettings.builder()
            .workItemConcurrency(workItemExecutor.getCorePoolSize())
            .processingConcurrency(processingExecutor.getCorePoolSize())
            .implementationType("Spring ThreadPool")
            .build();
    }
    
    @Override
    public void shutdown() {
        workItemExecutor.shutdown();
        processingExecutor.shutdown();
    }
    
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        
        if (!workItemExecutor.getThreadPoolExecutor().awaitTermination(timeout, unit)) {
            return false;
        }
        
        long remaining = deadlineNanos - System.nanoTime();
        return processingExecutor.getThreadPoolExecutor().awaitTermination(
            remaining, TimeUnit.NANOSECONDS);
    }
}
