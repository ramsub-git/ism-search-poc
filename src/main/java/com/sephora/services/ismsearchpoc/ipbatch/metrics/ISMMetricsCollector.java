package com.sephora.services.ismsearchpoc.ipbatch.metrics;

import com.sephora.services.ismsearchpoc.ipbatch.engine.ParallelBatchEngine;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collects metrics from various sources and creates snapshots
 * Leverages existing infrastructure: Micrometer, HikariCP, JVM MBeans
 */
public class ISMMetricsCollector {
    
    private static final Logger log = LoggerFactory.getLogger(ISMMetricsCollector.class);
    
    private final MeterRegistry meterRegistry;
    private final HikariDataSource dataSource;
    private final ParallelBatchEngine<?, ?, ?> engine;
    private final MemoryMXBean memoryBean;
    
    // Track critical errors
    private final Set<String> criticalErrorTypes = ConcurrentHashMap.newKeySet();
    
    // Track start time for rate calculations
    private final Instant startTime;
    private Instant lastSnapshotTime;
    private int lastFilesProcessed = 0;
    private long lastRecordsProcessed = 0;
    
    public ISMMetricsCollector(MeterRegistry meterRegistry,
                              HikariDataSource dataSource,
                              ParallelBatchEngine<?, ?, ?> engine) {
        this.meterRegistry = meterRegistry;
        this.dataSource = dataSource;
        this.engine = engine;
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.startTime = Instant.now();
        this.lastSnapshotTime = startTime;
    }
    
    /**
     * Create an immutable snapshot of current metrics
     */
    public MetricsSnapshot snapshot() {
        Instant now = Instant.now();
        
        // Get engine metrics
        ParallelBatchEngine.EngineMetrics engineMetrics = engine.getMetrics();
        
        // Calculate rates
        Duration timeSinceLastSnapshot = Duration.between(lastSnapshotTime, now);
        double minutesSinceLastSnapshot = timeSinceLastSnapshot.toMillis() / 60000.0;
        
        int filesDelta = engineMetrics.getWorkItemsProcessed() - lastFilesProcessed;
        long recordsDelta = engineMetrics.getRecordsProcessed() - lastRecordsProcessed;
        
        double filesPerMinute = minutesSinceLastSnapshot > 0 ? 
            filesDelta / minutesSinceLastSnapshot : 0;
        double recordsPerSecond = timeSinceLastSnapshot.getSeconds() > 0 ?
            (double) recordsDelta / timeSinceLastSnapshot.getSeconds() : 0;
        
        // Get DB connection metrics from HikariCP
        int activeDbConnections = dataSource.getHikariPoolMXBean().getActiveConnections();
        
        // Get heap utilization from JVM
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        double heapUtilization = (double) heapUsage.getUsed() / heapUsage.getMax();
        
        // Get error metrics from Micrometer
        int totalErrors = engineMetrics.getTotalErrors();
        int failedFiles = getFailedFilesCount();
        
        // Update tracking variables
        lastSnapshotTime = now;
        lastFilesProcessed = engineMetrics.getWorkItemsProcessed();
        lastRecordsProcessed = engineMetrics.getRecordsProcessed();
        
        // Build snapshot
        MetricsSnapshot snapshot = MetricsSnapshot.builder()
            .timestamp(now)
            .filesProcessed(engineMetrics.getWorkItemsProcessed())
            .totalFiles(engineMetrics.getTotalWorkItems())
            .recordsProcessed(engineMetrics.getRecordsProcessed())
            .filesPerMinute(filesPerMinute)
            .recordsPerSecond(recordsPerSecond)
            .activeDbConnections(activeDbConnections)
            .heapUtilization(heapUtilization)
            .totalErrors(totalErrors)
            .failedFiles(failedFiles)
            .criticalErrorTypes(Set.copyOf(criticalErrorTypes))
            .build();
        
        log.debug("Metrics snapshot: files={}/{}, records={}, rate={} files/min, errors={}",
            snapshot.getFilesProcessed(), snapshot.getTotalFiles(),
            snapshot.getRecordsProcessed(), 
            String.format("%.2f", snapshot.getFilesPerMinute()),
            snapshot.getTotalErrors());
        
        return snapshot;
    }
    
    /**
     * Record a critical error
     */
    public void recordCriticalError(String errorType) {
        criticalErrorTypes.add(errorType);
        log.error("Critical error recorded: {}", errorType);
    }
    
    /**
     * Get failed files count from Micrometer metrics
     */
    private int getFailedFilesCount() {
        // Try to get from Micrometer counter
        try {
            return (int) meterRegistry.counter("ism.files.failed").count();
        } catch (Exception e) {
            log.debug("Could not get failed files count from metrics", e);
            return 0;
        }
    }
    
    /**
     * Get current heap usage percentage
     */
    public double getCurrentHeapUtilization() {
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        return (double) heapUsage.getUsed() / heapUsage.getMax();
    }
    
    /**
     * Get current active DB connections
     */
    public int getCurrentActiveConnections() {
        return dataSource.getHikariPoolMXBean().getActiveConnections();
    }
}
