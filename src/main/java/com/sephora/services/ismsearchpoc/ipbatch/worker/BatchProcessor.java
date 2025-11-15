package com.sephora.services.ismsearchpoc.ipbatch.worker;

import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionContext;
import com.sephora.services.ismsearchpoc.ipbatch.model.ProcessingResult;
import java.util.List;

/**
 * Processes batches of records
 * Generic over record type R and result type V
 */
public interface BatchProcessor<R, V> {
    
    /**
     * Process a batch of records
     * @param records The batch of records to process
     * @param context Execution context
     * @return List of processing results, one per record
     * @throws Exception if batch processing fails critically
     */
    List<ProcessingResult<V>> processBatch(List<R> records, ExecutionContext context) throws Exception;
}
