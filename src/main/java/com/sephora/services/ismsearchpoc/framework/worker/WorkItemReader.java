package com.sephora.services.ismsearchpoc.framework.worker;

import com.sephora.services.ismsearchpoc.framework.model.ExecutionContext;
import java.util.List;

/**
 * Reads records from a work item
 * Generic over work item type T and record type R
 */
public interface WorkItemReader<T, R> {
    
    /**
     * Read all records from a work item
     * @param workItem The work item to read (e.g., file name)
     * @param context Execution context
     * @return List of records extracted from the work item
     * @throws Exception if reading fails
     */
    List<R> readWorkItem(T workItem, ExecutionContext context) throws Exception;
}
