package com.sephora.services.ismsearchpoc.ipbatch.engine;

import com.sephora.services.ismsearchpoc.ipbatch.concurrency.ConcurrencyController;
import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionContext;
import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionResult;
import com.sephora.services.ismsearchpoc.ipbatch.model.ProcessingResult;
import com.sephora.services.ismsearchpoc.ipbatch.worker.BatchProcessor;
import com.sephora.services.ismsearchpoc.ipbatch.worker.ProgressTracker;
import com.sephora.services.ismsearchpoc.ipbatch.worker.WorkItemFetcher;
import com.sephora.services.ismsearchpoc.ipbatch.worker.WorkItemReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Generic parallel batch execution engine
 * Knows nothing about ISM, files, or specific domains
 * Completely reusable for any batch processing task
 * 
 * Generic parameters:
 * T - Work item type (e.g., String for file names)
 * R - Record type (e.g., SkulocRecord)
 * V - Processing result type (e.g., Void for inserts)
 */
public class ParallelBatchEngine<T, R, V> {
    
    private static final Logger log = LoggerFactory.getLogger(ParallelBatchEngine.class);
    
    private final ConcurrencyController concurrencyController;
    private final WorkItemFetcher<T> workItemFetcher;
    private final WorkItemReader<T, R> workItemReader;
    private final BatchProcessor<R, V> batchProcessor;
    private final ProgressTracker<T, V> progressTracker;
    private final int batchSize;
    
    private final AtomicInteger workItemsProcessed = new AtomicInteger(0);
    private final AtomicInteger totalRecordsProcessed = new AtomicInteger(0);
    private final AtomicInteger totalErrors = new AtomicInteger(0);
    private final AtomicReference<String> abortReason = new AtomicReference<>(null);
    
    private volatile Instant startTime;
    private volatile int totalWorkItems;
    
    public ParallelBatchEngine(ConcurrencyController concurrencyController,
                               WorkItemFetcher<T> workItemFetcher,
                               WorkItemReader<T, R> workItemReader,
                               BatchProcessor<R, V> batchProcessor,
                               ProgressTracker<T, V> progressTracker,
                               int batchSize) {
        this.concurrencyController = concurrencyController;
        this.workItemFetcher = workItemFetcher;
        this.workItemReader = workItemReader;
        this.batchProcessor = batchProcessor;
        this.progressTracker = progressTracker;
        this.batchSize = batchSize;
    }
    
    /**
     * Execute the entire batch process
     */
    public ExecutionResult execute(ExecutionContext context) {
        startTime = Instant.now();
        log.info("Starting batch execution");
        
        try {
            // Fetch all work items
            List<T> workItems = workItemFetcher.fetchWorkItems(context);
            totalWorkItems = workItems.size();
            log.info("Fetched {} work items", totalWorkItems);

            progressTracker.onStart(totalWorkItems);

            if (workItems.isEmpty()) {
                return buildResult(true, Instant.now());
            }
            
            // Process work items in parallel
            List<CompletableFuture<Void>> futures = workItems.stream()
                .map(workItem -> processWorkItemAsync(workItem, context))
                .collect(Collectors.toList());
            
            // Wait for all to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            
            // Check if aborted
            String abort = abortReason.get();
            boolean success = abort == null;
            
            return buildResult(success, Instant.now());
            
        } catch (Exception e) {
            log.error("Fatal error during execution", e);
            return buildResult(false, Instant.now(), "Fatal error: " + e.getMessage());
        } finally {
            concurrencyController.shutdown();
        }
    }
    
    /**
     * Process a single work item asynchronously
     */
    private CompletableFuture<Void> processWorkItemAsync(T workItem, ExecutionContext context) {
        return concurrencyController.submitWorkItem(() -> {
            // Check if we should abort before starting
            if (shouldAbort()) {
                log.info("Aborting - skipping work item: {}", workItem);
                return;
            }
            
            try {
                progressTracker.onWorkItemStart(workItem);
                
                // Read records from work item
                List<R> records = workItemReader.readWorkItem(workItem, context);
                log.debug("Work item {} has {} records", workItem, records.size());
                
                // Process records in batches
                List<ProcessingResult<V>> allResults = new ArrayList<>();
                for (int i = 0; i < records.size(); i += batchSize) {
                    // Check abort before each batch
                    if (shouldAbort()) {
                        log.info("Aborting during work item: {}", workItem);
                        break;
                    }
                    
                    int endIndex = Math.min(i + batchSize, records.size());
                    List<R> batch = records.subList(i, endIndex);
                    
                    // Process batch (potentially in parallel)
                    List<ProcessingResult<V>> batchResults = processBatchAsync(batch, context);
                    allResults.addAll(batchResults);
                }
                
                // Track results
                int errorCount = (int) allResults.stream().filter(r -> !r.isSuccess()).count();
                totalRecordsProcessed.addAndGet(allResults.size());
                totalErrors.addAndGet(errorCount);
                
                progressTracker.onWorkItemComplete(workItem, allResults.size(), allResults);
                
                int processed = workItemsProcessed.incrementAndGet();
                if (processed % 10 == 0) {
                    progressTracker.reportProgress(processed, totalWorkItems);
                }
                
            } catch (Exception e) {
                log.error("Error processing work item: {}", workItem, e);
                progressTracker.onWorkItemFailure(workItem, e);
                totalErrors.incrementAndGet();
            }
        });
    }
    
    /**
     * Process a batch of records, potentially in parallel
     */
    private List<ProcessingResult<V>> processBatchAsync(List<R> batch, ExecutionContext context) throws Exception {
        // For now, process the batch as a unit
        // Could be enhanced to process individual records in parallel
        return batchProcessor.processBatch(batch, context);
    }
    
    /**
     * Check if execution should abort
     * Hook for runtime manager to signal abort
     */
    public boolean shouldAbort() {
        return abortReason.get() != null;
    }
    
    /**
     * Signal that execution should abort
     */
    public void abort(String reason) {
        abortReason.set(reason);
        log.warn("Abort signaled: {}", reason);
    }
    
    /**
     * Adjust concurrency levels dynamically
     */
    public void adjustConcurrency(int workItemConcurrency, int processingConcurrency) {
        concurrencyController.adjustConcurrency(workItemConcurrency, processingConcurrency);
    }
    
    /**
     * Get current metrics
     */
    public EngineMetrics getMetrics() {
        return new EngineMetrics(
            workItemsProcessed.get(),
            totalWorkItems,
            totalRecordsProcessed.get(),
            totalErrors.get(),
            startTime
        );
    }
    
    private ExecutionResult buildResult(boolean success, Instant endTime) {
        return buildResult(success, endTime, abortReason.get());
    }

    private ExecutionResult buildResult(boolean success, Instant endTime, String abort) {
        return ExecutionResult.builder()
                .success(success)
                .abortReason(abort)
                .workItemsProcessed(workItemsProcessed.get())
                .totalWorkItems(totalWorkItems)
                .recordsProcessed(totalRecordsProcessed.get())
                .totalErrors(totalErrors.get())
                .startTime(startTime)
                .endTime(endTime)
                .build();
    }
    
    /**
     * Simple metrics snapshot
     */
    public static class EngineMetrics {
        private final int workItemsProcessed;
        private final int totalWorkItems;
        private final int recordsProcessed;
        private final int totalErrors;
        private final Instant startTime;
        
        public EngineMetrics(int workItemsProcessed, int totalWorkItems,
                           int recordsProcessed, int totalErrors, Instant startTime) {
            this.workItemsProcessed = workItemsProcessed;
            this.totalWorkItems = totalWorkItems;
            this.recordsProcessed = recordsProcessed;
            this.totalErrors = totalErrors;
            this.startTime = startTime;
        }
        
        public int getWorkItemsProcessed() { return workItemsProcessed; }
        public int getTotalWorkItems() { return totalWorkItems; }
        public int getRecordsProcessed() { return recordsProcessed; }
        public int getTotalErrors() { return totalErrors; }
        public Instant getStartTime() { return startTime; }
    }
}
