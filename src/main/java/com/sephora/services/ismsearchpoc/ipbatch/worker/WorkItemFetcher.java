package com.sephora.services.ismsearchpoc.ipbatch.worker;

import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionContext;
import java.util.List;

/**
 * Fetches work items to be processed
 * Generic over work item type T
 */
public interface WorkItemFetcher<T> {
    
    /**
     * Fetch all work items for processing
     * @param context Execution context with configuration
     * @return List of work items (e.g., file names, API endpoints, queue messages)
     * @throws Exception if fetching fails
     */
    List<T> fetchWorkItems(ExecutionContext context) throws Exception;
}
