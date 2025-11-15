package com.sephora.services.ismsearchpoc.ism.service;

import com.sephora.services.ismsearchpoc.ipbatch.engine.ParallelBatchEngine;
import com.sephora.services.ismsearchpoc.ipbatch.metrics.ISMMetricsCollector;
import com.sephora.services.ismsearchpoc.ipbatch.metrics.MetricsSnapshot;
import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionContext;
import com.sephora.services.ismsearchpoc.ipbatch.model.ExecutionResult;
import com.sephora.services.ismsearchpoc.ipbatch.runtime.ISMRuntimeManager;
import com.sephora.services.ismsearchpoc.ism.model.SkulocRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Orchestrates the ISM skuloc data load
 * Coordinates engine execution with runtime management
 */
@Service
public class ISMLoadOrchestrator {
    
    private static final Logger log = LoggerFactory.getLogger(ISMLoadOrchestrator.class);
    
    private final ParallelBatchEngine<String, SkulocRecord, Void> engine;
    private final ISMRuntimeManager runtimeManager;
    private final ISMMetricsCollector metricsCollector;
    private final ScheduledExecutorService scheduler;
    
    public ISMLoadOrchestrator(
            ParallelBatchEngine<String, SkulocRecord, Void> engine,
            ISMRuntimeManager runtimeManager,
            ISMMetricsCollector metricsCollector) {
        
        this.engine = engine;
        this.runtimeManager = runtimeManager;
        this.metricsCollector = metricsCollector;
        this.scheduler = Executors.newScheduledThreadPool(1);
    }
    
    /**
     * Execute the ISM data load
     * 
     * @param blobFolderPath Azure blob folder path containing CSV files
     * @param filePattern Optional file pattern filter (e.g., "Inv_.*\\.csv")
     * @return Load result with metrics
     */
    public LoadResult executeLoad(String blobFolderPath, String filePattern) {
        log.info("=== Starting ISM Data Load ===");
        log.info("Blob folder: {}", blobFolderPath);
        log.info("File pattern: {}", filePattern != null ? filePattern : "none");
        
        // Setup execution context
        ExecutionContext context = new ExecutionContext();
        context.setAttribute("folderPath", blobFolderPath);
        if (filePattern != null) {
            context.setAttribute("filePattern", filePattern);
        }
        
        // Start periodic goal evaluation (every 5 minutes)
        ScheduledFuture<?> evaluationTask = scheduler.scheduleAtFixedRate(
            () -> {
                try {
                    MetricsSnapshot metrics = metricsCollector.snapshot();
                    runtimeManager.evaluateAndAdjust(metrics);
                } catch (Exception e) {
                    log.error("Error during goal evaluation", e);
                }
            },
            1,  // Initial delay
            5,  // Period
            TimeUnit.MINUTES
        );
        
        try {
            // Execute the load
            log.info("Starting parallel batch engine...");
            ExecutionResult result = engine.execute(context);
            
            // Final metrics snapshot
            MetricsSnapshot finalMetrics = metricsCollector.snapshot();
            
            // Build result
            LoadResult loadResult = LoadResult.builder()
                .success(result.isSuccess())
                .abortReason(result.getAbortReason())
                .filesProcessed(result.getWorkItemsProcessed())
                .totalFiles(result.getTotalWorkItems())
                .recordsProcessed(finalMetrics.getRecordsProcessed())
                .totalErrors(finalMetrics.getTotalErrors())
                .duration(result.getDuration())
                .build();
            
            // Log summary
            if (result.isSuccess()) {
                log.info("=== ISM Data Load COMPLETED SUCCESSFULLY ===");
            } else {
                log.error("=== ISM Data Load FAILED ===");
                log.error("Abort reason: {}", result.getAbortReason());
            }
            
            log.info("Files processed: {}/{}", loadResult.getFilesProcessed(), loadResult.getTotalFiles());
            log.info("Records processed: {}", loadResult.getRecordsProcessed());
            log.info("Total errors: {}", loadResult.getTotalErrors());
            log.info("Duration: {}", loadResult.getDuration());
            
            return loadResult;
            
        } catch (Exception e) {
            log.error("Unexpected error during ISM data load", e);
            return LoadResult.builder()
                .success(false)
                .abortReason("Unexpected error: " + e.getMessage())
                .build();
                
        } finally {
            // Stop evaluation task
            evaluationTask.cancel(false);
        }
    }
    
    /**
     * Execute load with default file pattern
     */
    public LoadResult executeLoad(String blobFolderPath) {
        return executeLoad(blobFolderPath, null);
    }
    
    /**
     * Result of a data load execution
     */
    public static class LoadResult {
        private final boolean success;
        private final String abortReason;
        private final int filesProcessed;
        private final int totalFiles;
        private final long recordsProcessed;
        private final int totalErrors;
        private final Duration duration;
        
        private LoadResult(Builder builder) {
            this.success = builder.success;
            this.abortReason = builder.abortReason;
            this.filesProcessed = builder.filesProcessed;
            this.totalFiles = builder.totalFiles;
            this.recordsProcessed = builder.recordsProcessed;
            this.totalErrors = builder.totalErrors;
            this.duration = builder.duration;
        }
        
        public boolean isSuccess() { return success; }
        public String getAbortReason() { return abortReason; }
        public int getFilesProcessed() { return filesProcessed; }
        public int getTotalFiles() { return totalFiles; }
        public long getRecordsProcessed() { return recordsProcessed; }
        public int getTotalErrors() { return totalErrors; }
        public Duration getDuration() { return duration; }
        
        public static Builder builder() {
            return new Builder();
        }
        
        public static class Builder {
            private boolean success;
            private String abortReason;
            private int filesProcessed;
            private int totalFiles;
            private long recordsProcessed;
            private int totalErrors;
            private Duration duration;
            
            public Builder success(boolean success) {
                this.success = success;
                return this;
            }
            
            public Builder abortReason(String abortReason) {
                this.abortReason = abortReason;
                return this;
            }
            
            public Builder filesProcessed(int filesProcessed) {
                this.filesProcessed = filesProcessed;
                return this;
            }
            
            public Builder totalFiles(int totalFiles) {
                this.totalFiles = totalFiles;
                return this;
            }
            
            public Builder recordsProcessed(long recordsProcessed) {
                this.recordsProcessed = recordsProcessed;
                return this;
            }
            
            public Builder totalErrors(int totalErrors) {
                this.totalErrors = totalErrors;
                return this;
            }
            
            public Builder duration(Duration duration) {
                this.duration = duration;
                return this;
            }
            
            public LoadResult build() {
                return new LoadResult(this);
            }
        }
    }
}
