package com.sephora.services.ismsearchpoc.framework.worker;

import com.sephora.services.ismsearchpoc.framework.model.ProcessingResult;
import java.util.List;

/**
 * Tracks progress and metrics during execution
 * Generic over work item type T and result type V
 */
public interface ProgressTracker<T, V> {
    
    /**
     * Called when a work item starts processing
     * @param workItem The work item being processed
     */
    void onWorkItemStart(T workItem);
    
    /**
     * Called when a work item completes (success or failure)
     * @param workItem The work item that completed
     * @param recordCount Number of records processed
     * @param results Processing results for the records
     */
    void onWorkItemComplete(T workItem, int recordCount, List<ProcessingResult<V>> results);
    
    /**
     * Called when a work item fails
     * @param workItem The work item that failed
     * @param error The error that occurred
     */
    void onWorkItemFailure(T workItem, Throwable error);
    
    /**
     * Called periodically to report progress
     * @param processed Number of work items processed so far
     * @param total Total number of work items
     */
    void reportProgress(int processed, int total);
}
