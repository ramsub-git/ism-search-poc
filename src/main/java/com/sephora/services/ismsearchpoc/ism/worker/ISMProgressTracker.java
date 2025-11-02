package com.sephora.services.ismsearchpoc.ism.worker;

import com.sephora.services.ismsearchpoc.framework.model.ProcessingResult;
import com.sephora.services.ismsearchpoc.framework.worker.ProgressTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * ISM-specific progress tracker
 * Logs progress and can integrate with metrics/monitoring systems
 */
@Component
public class ISMProgressTracker implements ProgressTracker<String, Void> {
    
    private static final Logger log = LoggerFactory.getLogger(ISMProgressTracker.class);
    
    // TODO: Inject ISMMetricsCollector when ready
    // private final ISMMetricsCollector metricsCollector;
    
    @Override
    public void onWorkItemStart(String workItem) {
        log.info("Starting processing of file: {}", workItem);
    }
    
    @Override
    public void onWorkItemComplete(String workItem, int recordCount, List<ProcessingResult<Void>> results) {
        long successCount = results.stream().filter(ProcessingResult::isSuccess).count();
        long failureCount = results.stream().filter(r -> !r.isSuccess()).count();
        
        log.info("Completed file: {} - {} records ({} success, {} failed)", 
            workItem, recordCount, successCount, failureCount);
        
        // TODO: Record metrics
        // metricsCollector.recordFileSuccess(recordCount);
    }
    
    @Override
    public void onWorkItemFailure(String workItem, Throwable error) {
        log.error("Failed to process file: {}", workItem, error);
        
        // TODO: Record metrics
        // metricsCollector.recordFileFailure();
    }
    
    @Override
    public void reportProgress(int processed, int total) {
        double percentComplete = (double) processed / total * 100;
        log.info("Progress: {}/{} files ({:.1f}%)", processed, total, percentComplete);
    }
}
