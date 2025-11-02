package com.sephora.services.ismsearchpoc.framework.metrics;

import java.time.Instant;
import java.util.Set;

/**
 * Immutable snapshot of system metrics at a point in time
 */
public class MetricsSnapshot {
    
    private final Instant timestamp;
    
    // Progress metrics
    private final int filesProcessed;
    private final int totalFiles;
    private final long recordsProcessed;
    
    // Performance metrics
    private final double filesPerMinute;
    private final double recordsPerSecond;
    
    // Resource metrics
    private final int activeDbConnections;
    private final double heapUtilization;
    
    // Error metrics
    private final int totalErrors;
    private final int failedFiles;
    private final Set<String> criticalErrorTypes;
    
    private MetricsSnapshot(Builder builder) {
        this.timestamp = builder.timestamp;
        this.filesProcessed = builder.filesProcessed;
        this.totalFiles = builder.totalFiles;
        this.recordsProcessed = builder.recordsProcessed;
        this.filesPerMinute = builder.filesPerMinute;
        this.recordsPerSecond = builder.recordsPerSecond;
        this.activeDbConnections = builder.activeDbConnections;
        this.heapUtilization = builder.heapUtilization;
        this.totalErrors = builder.totalErrors;
        this.failedFiles = builder.failedFiles;
        this.criticalErrorTypes = builder.criticalErrorTypes;
    }
    
    public Instant getTimestamp() {
        return timestamp;
    }
    
    public int getFilesProcessed() {
        return filesProcessed;
    }
    
    public int getTotalFiles() {
        return totalFiles;
    }
    
    public long getRecordsProcessed() {
        return recordsProcessed;
    }
    
    public double getFilesPerMinute() {
        return filesPerMinute;
    }
    
    public double getRecordsPerSecond() {
        return recordsPerSecond;
    }
    
    public int getActiveDbConnections() {
        return activeDbConnections;
    }
    
    public double getHeapUtilization() {
        return heapUtilization;
    }
    
    public int getTotalErrors() {
        return totalErrors;
    }
    
    public int getFailedFiles() {
        return failedFiles;
    }
    
    public double getPercentComplete() {
        return totalFiles > 0 ? (double) filesProcessed / totalFiles * 100 : 0;
    }
    
    public boolean hasCriticalError(Set<String> errorTypes) {
        return criticalErrorTypes.stream().anyMatch(errorTypes::contains);
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private Instant timestamp = Instant.now();
        private int filesProcessed;
        private int totalFiles;
        private long recordsProcessed;
        private double filesPerMinute;
        private double recordsPerSecond;
        private int activeDbConnections;
        private double heapUtilization;
        private int totalErrors;
        private int failedFiles;
        private Set<String> criticalErrorTypes = Set.of();
        
        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
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
        
        public Builder filesPerMinute(double filesPerMinute) {
            this.filesPerMinute = filesPerMinute;
            return this;
        }
        
        public Builder recordsPerSecond(double recordsPerSecond) {
            this.recordsPerSecond = recordsPerSecond;
            return this;
        }
        
        public Builder activeDbConnections(int activeDbConnections) {
            this.activeDbConnections = activeDbConnections;
            return this;
        }
        
        public Builder heapUtilization(double heapUtilization) {
            this.heapUtilization = heapUtilization;
            return this;
        }
        
        public Builder totalErrors(int totalErrors) {
            this.totalErrors = totalErrors;
            return this;
        }
        
        public Builder failedFiles(int failedFiles) {
            this.failedFiles = failedFiles;
            return this;
        }
        
        public Builder criticalErrorTypes(Set<String> criticalErrorTypes) {
            this.criticalErrorTypes = criticalErrorTypes;
            return this;
        }
        
        public MetricsSnapshot build() {
            return new MetricsSnapshot(this);
        }
    }
}
