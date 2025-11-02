package com.sephora.services.ismsearchpoc.ism.controller;

import com.sephora.services.ismsearchpoc.ism.service.ISMLoadOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST controller for triggering ISM data loads
 */
@RestController
@RequestMapping("/api/ism/load")
public class ISMLoadController {
    
    private static final Logger log = LoggerFactory.getLogger(ISMLoadController.class);
    
    private final ISMLoadOrchestrator orchestrator;
    
    public ISMLoadController(ISMLoadOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }
    
    /**
     * Trigger ISM skuloc data load
     * 
     * POST /api/ism/load
     * {
     *   "folderPath": "/data/ISM/conversion",
     *   "filePattern": "Inv_.*\\.csv"  // optional
     * }
     */
    @PostMapping
    public ResponseEntity<LoadResponse> triggerLoad(@RequestBody LoadRequest request) {
        log.info("Received load request: folderPath={}, filePattern={}", 
            request.getFolderPath(), request.getFilePattern());
        
        // Validate request
        if (request.getFolderPath() == null || request.getFolderPath().trim().isEmpty()) {
            return ResponseEntity.badRequest()
                .body(LoadResponse.error("folderPath is required"));
        }
        
        try {
            // Execute load
            ISMLoadOrchestrator.LoadResult result = orchestrator.executeLoad(
                request.getFolderPath(),
                request.getFilePattern()
            );
            
            // Build response
            LoadResponse response = LoadResponse.builder()
                .success(result.isSuccess())
                .message(result.isSuccess() ? "Load completed successfully" : "Load failed")
                .abortReason(result.getAbortReason())
                .filesProcessed(result.getFilesProcessed())
                .totalFiles(result.getTotalFiles())
                .recordsProcessed(result.getRecordsProcessed())
                .totalErrors(result.getTotalErrors())
                .durationSeconds(result.getDuration() != null ? result.getDuration().getSeconds() : 0)
                .build();
            
            if (result.isSuccess()) {
                return ResponseEntity.ok(response);
            } else {
                return ResponseEntity.status(500).body(response);
            }
            
        } catch (Exception e) {
            log.error("Error processing load request", e);
            return ResponseEntity.status(500)
                .body(LoadResponse.error("Unexpected error: " + e.getMessage()));
        }
    }
    
    /**
     * Get load status (placeholder for future enhancement)
     */
    @GetMapping("/status")
    public ResponseEntity<String> getLoadStatus() {
        // TODO: Implement status tracking
        return ResponseEntity.ok("Not implemented yet");
    }
    
    // ============================================
    // Request/Response DTOs
    // ============================================
    
    public static class LoadRequest {
        private String folderPath;
        private String filePattern;
        
        public String getFolderPath() { return folderPath; }
        public void setFolderPath(String folderPath) { this.folderPath = folderPath; }
        
        public String getFilePattern() { return filePattern; }
        public void setFilePattern(String filePattern) { this.filePattern = filePattern; }
    }
    
    public static class LoadResponse {
        private boolean success;
        private String message;
        private String abortReason;
        private int filesProcessed;
        private int totalFiles;
        private long recordsProcessed;
        private int totalErrors;
        private long durationSeconds;
        
        private LoadResponse(Builder builder) {
            this.success = builder.success;
            this.message = builder.message;
            this.abortReason = builder.abortReason;
            this.filesProcessed = builder.filesProcessed;
            this.totalFiles = builder.totalFiles;
            this.recordsProcessed = builder.recordsProcessed;
            this.totalErrors = builder.totalErrors;
            this.durationSeconds = builder.durationSeconds;
        }
        
        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
        public String getAbortReason() { return abortReason; }
        public int getFilesProcessed() { return filesProcessed; }
        public int getTotalFiles() { return totalFiles; }
        public long getRecordsProcessed() { return recordsProcessed; }
        public int getTotalErrors() { return totalErrors; }
        public long getDurationSeconds() { return durationSeconds; }
        
        public static Builder builder() {
            return new Builder();
        }
        
        public static LoadResponse error(String message) {
            return builder()
                .success(false)
                .message(message)
                .build();
        }
        
        public static class Builder {
            private boolean success;
            private String message;
            private String abortReason;
            private int filesProcessed;
            private int totalFiles;
            private long recordsProcessed;
            private int totalErrors;
            private long durationSeconds;
            
            public Builder success(boolean success) {
                this.success = success;
                return this;
            }
            
            public Builder message(String message) {
                this.message = message;
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
            
            public Builder durationSeconds(long durationSeconds) {
                this.durationSeconds = durationSeconds;
                return this;
            }
            
            public LoadResponse build() {
                return new LoadResponse(this);
            }
        }
    }
}
